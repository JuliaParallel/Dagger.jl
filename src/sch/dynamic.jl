export SchedulerHaltedException
export sch_handle, halt!, exec!, get_dag_ids, add_thunk!

"Identifies a thunk by its ID, and preserves the thunk in the scheduler."
struct ThunkID
    id::Int
    ref::Union{DRef,Nothing}
end
ThunkID(id::Int) = ThunkID(id, nothing)
Dagger.istask(::ThunkID) = true

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
    errormonitor_tracked("dynamic_listener! (halt+throw) $wid", Threads.@spawn begin
        wait(state.halt)
        # TODO: Not sure why we need the Threads.@spawn here, but otherwise we
        # don't stop all the listener tasks
        Threads.@spawn begin
            Base.throwto(listener_task, SchedulerHaltedException())
            return
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
        thunk = unwrap_weak_checked(state.thunk_dict[id.id])
        if check
            ownthunk = unwrap_weak_checked(state.thunk_dict[tid])
            if dominates(ownthunk, thunk)
                throw(DynamicThunkException("Cannot fetch result of dominated thunk"))
            end
        end
        # TODO: Assert that future will be fulfilled
        if has_result(state, thunk)
            put!(future, load_result(state, thunk); error=state.errored[thunk])
        else
            futures = get!(()->ThunkFuture[], state.futures, thunk)
            push!(futures, future)
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
    for (id,thunk) in state.thunk_dict
        thunk = unwrap_weak_checked(thunk)
        # TODO: Get at `thunk_ref` for `thunk_id.ref`
        thunk_id = ThunkID(id, nothing)
        if haskey(state.waiting_data, thunk)
            deps[thunk_id] = Set(map(t->ThunkID(t.id, nothing), collect(state.waiting_data[thunk])))
        else
            deps[thunk_id] = Set{ThunkID}()
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
