export SchedulerHaltedException
export sch_handle, halt!, exec!, get_dag_ids, add_thunk!

"A handle to the scheduler, used by dynamic thunks."
struct SchedulerHandle
    thunk_id::ThunkID
    out_chan::RemoteChannel
    inp_chan::RemoteChannel
end

"Gets the scheduler handle for the currently-executing thunk."
sch_handle() = Dagger.get_tls().sch_handle::SchedulerHandle

"Thrown when the scheduler halts before finishing processing the DAG."
struct SchedulerHaltedException <: Exception end

"Thrown when a dynamic thunk encounters an exception in Dagger's utilities."
struct DynamicThunkException <: Exception
    reason::String
end

struct RescheduleSignal end

function safepoint(state)
    if state.halt.set
        # Force dynamic thunks and listeners to terminate
        for (inp_chan,out_chan) in values(state.worker_chans)
            # Closing these channels will fail if the worker died, which we
            # allow.
            try
                close(inp_chan)
                close(out_chan)
            catch ex
                if !(ex isa ProcessExitedException)
                    rethrow()
                end
            end
        end

        # Throw out of scheduler
        throw(SchedulerHaltedException())
    end
end

"Processes dynamic messages from worker-executing thunks."
function dynamic_listener!(ctx, state, wid)
    task = current_task() # The scheduler's main task
    inp_chan, out_chan = state.worker_chans[wid]
    listener_task = Threads.@spawn begin
        while !state.halt.set
            tid, f, data = try
                take!(inp_chan)
            catch err
                if !(unwrap_nested_exception(err) isa Union{SchedulerHaltedException,
                                                            ProcessExitedException,
                                                            InvalidStateException})
                    iob = IOContext(IOBuffer(), :color=>true)
                    println(iob, "Error in receiving dynamic request:")
                    Base.showerror(iob, err)
                    Base.show_backtrace(iob, catch_backtrace())
                    println(iob)
                    seek(iob.io, 0)
                    write(stderr, iob)
                end
                break
            end
            res = try
                (false, lock(state.lock) do
                    Base.invokelatest(f, ctx, state, task, tid, data)
                end)
            catch err
                (true, RemoteException(CapturedException(err,catch_backtrace())))
            end
            try
                put!(out_chan, res)
            catch err
                if !(unwrap_nested_exception(err) isa Union{SchedulerHaltedException,
                                                            ProcessExitedException,
                                                            InvalidStateException})
                    iob = IOContext(IOBuffer(), :color=>true)
                    println(iob, "Error in sending dynamic result from $f:")
                    Base.showerror(iob, err)
                    Base.show_backtrace(iob, catch_backtrace())
                    println(iob)
                    seek(iob.io, 0)
                    write(stderr, iob)
                    println(stderr, res)
                end
            end
        end
        return
    end
    errormonitor_tracked("dynamic_listener! $wid", listener_task)
    errormonitor_tracked("dynamic_listener! (halt) $wid", Threads.@spawn begin
        wait(state.halt)
        # Stop the listener by closing its channels, which unblocks its
        # `take!(inp_chan)` and lets it exit cleanly (the listener tolerates
        # `InvalidStateException`/`SchedulerHaltedException` from a closed
        # channel). We intentionally avoid `Base.throwto` here: throwing into a
        # task that is merely runnable (rather than blocked) forces an
        # out-of-band context switch that corrupts Julia's task run-queue,
        # leading to a later fatal "attempt to switch to exited task" crash
        # during scheduler teardown. Closing the channels is idempotent and
        # safe even if `safepoint` already closed them.
        for chan in (inp_chan, out_chan)
            try
                close(chan)
            catch err
                unwrap_nested_exception(err) isa Union{InvalidStateException,
                                                        ProcessExitedException} ||
                    rethrow()
            end
        end
        return
    end)
end

## Worker-side methods for dynamic communication

const DYNAMIC_EXEC_LOCK = Threads.ReentrantLock()

"Executes an arbitrary function within the scheduler, returning the result."
function exec!(f, h::SchedulerHandle, args...)
    failed, res = lock(DYNAMIC_EXEC_LOCK) do
        put!(h.out_chan, (h.thunk_id.id, f, args))
        take!(h.inp_chan)
    end
    failed && throw(res)
    res
end

"Commands the scheduler to halt execution immediately."
halt!(h::SchedulerHandle) = exec!(_halt, h, nothing)
function _halt(ctx, state, task, tid, _)
    notify(state.halt)
    put!(state.chan, TaskResult(1, OSProc(), 0, SchedulerHaltedException(), nothing))
    Base.throwto(task, SchedulerHaltedException())
end

"Waits on a thunk to complete, and fetches its result."
function Base.fetch(h::SchedulerHandle, id::ThunkID)
    future = ThunkFuture()
    exec!(_register_future!, h, future, id, true)
    fetch(future; proc=task_processor())
end
"""
Waits on a thunk to complete, and fetches its result. If `check` is set to
`true` (the default), then a domination check will occur to ensure that the
future isn't being registered on a thunk dominated by the calling thunk.
"""
register_future!(h::SchedulerHandle, id::ThunkID, future::ThunkFuture, check::Bool=true) =
        exec!(_register_future!, h, future, id, check)
function _register_future!(ctx, state, task, tid, (future, id, check)::Tuple{ThunkFuture,ThunkID,Bool})
    tid != id.id || throw(DynamicThunkException("Cannot fetch own result"))
    GC.@preserve id begin
        function dominates(target, t)
            t == target && return true
            seen = Set{Thunk}()
            to_visit = Thunk[t]
            while !isempty(to_visit)
                t = pop!(to_visit)
                if t == target
                    return true
                end
                for (_, input) in t.inputs
                    # N.B. Skips expired tasks
                    input = Dagger.unwrap_weak(input)
                    istask(input) || continue
                    input in seen && continue
                    push!(seen, input)
                    push!(to_visit, input)
                end
            end
            return false
        end
        thunk = lock(state.thunk_dict) do d; unwrap_weak_checked(d[id.id]); end
        if check
            ownthunk = lock(state.thunk_dict) do d; unwrap_weak_checked(d[tid]); end
            if dominates(ownthunk, thunk)
                throw(DynamicThunkException("Cannot fetch result of dominated thunk"))
            end
        end
        # Fast path: thunk already has a result; fulfill immediately.
        # Slow path: register in the Treiber futures list. If the list was
        # already sealed (thunk finished between the has_result check and the
        # push), futures_push! returns false and we fulfill directly, so the future
        # is always fulfilled exactly once.
        if has_result(state, thunk) || !futures_push!(thunk, future)
            put!(future, load_result(state, thunk); error=(@atomic thunk.errored))
        end
    end
    return
end

# TODO: Optimize wait() to not serialize a Chunk
"Waits on a thunk to complete."
function Base.wait(h::SchedulerHandle, id::ThunkID; future=ThunkFuture())
    register_future!(h, id, future)
    wait(future)
end

"Returns all Thunks IDs as a Dict, mapping a Thunk to its downstream dependents."
get_dag_ids(h::SchedulerHandle) =
    exec!(_get_dag_ids, h, nothing)::Dict{ThunkID,Set{ThunkID}}
function _get_dag_ids(ctx, state, task, tid, _)
    deps = Dict{ThunkID,Set{ThunkID}}()
    # Snapshot the thunk_dict to avoid holding its lock across the entire loop.
    thunk_dict_snapshot = lock(state.thunk_dict) do d; collect(d); end
    # Initialize empty sets for all known thunks.
    for (id, thunk) in thunk_dict_snapshot
        thunk = unwrap_weak_checked(thunk)
        deps[ThunkID(id, nothing)] = Set{ThunkID}()
    end
    # Reconstruct the downstream map from syncdeps of non-finished thunks.
    # A finished thunk's dependents list has already been sealed and drained, so
    # we can no longer read it from `dependents_head`. Instead we derive the
    # "who is still waiting on me" relationship from the inverse direction:
    # for every thunk T that is not yet finished, each of its syncdep upstreams
    # is an upstream whose result T is still consuming (or will consume).
    # We use `options.syncdeps` (not `inputs`) because `collect_task_inputs!`
    # replaces Thunk-typed values in `inputs` with their Chunk results before
    # firing the task, making `inputs` unreliable as a dependency source.
    for (id, thunk) in thunk_dict_snapshot
        thunk = unwrap_weak_checked(thunk)
        (@atomic thunk.finished) && continue
        thunk.options === nothing && continue
        thunk.options.syncdeps === nothing && continue
        thunk_id = ThunkID(id, nothing)
        for input in Dagger.syncdeps_iterator(thunk)
            input_id = ThunkID(input.id, nothing)
            haskey(deps, input_id) || continue
            push!(deps[input_id], thunk_id)
        end
    end
    deps
end

"Adds a new Thunk to the DAG."
function add_thunk!(f, h::SchedulerHandle, args...; future=nothing, ref=nothing, options...)
    if ref !== nothing
        @warn "`ref` is no longer supported in `add_thunk!`" maxlog=1
    end
    return exec!(_add_thunk!, h, f, args, options, future)
end
function _add_thunk!(ctx, state, task, tid, (f, args, options, future))
    if future === nothing
        future = ThunkFuture()
    end
    _options = Dagger.Options(;options...)
    fargs = Dagger.Argument[]
    push!(fargs, Dagger.Argument(Dagger.ArgPosition(true, 0, :NULL), f))
    pos_idx = 1
    for (pos, arg) in args
        if pos === nothing
            push!(fargs, Dagger.Argument(pos_idx, arg))
            pos_idx += 1
        else
            push!(fargs, Dagger.Argument(pos, arg))
        end
    end
    payload = Dagger.PayloadOne(UInt(0), future, fargs, _options, true)
    return Dagger.eager_submit_internal!(ctx, state, task, tid, payload)
end
