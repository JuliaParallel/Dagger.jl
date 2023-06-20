export SchedulerHaltedException
export sch_handle, halt!, exec!, get_dag_ids, add_thunk!

"Identifies a thunk by its ID, and preserves the thunk in the scheduler."
struct ThunkID
    id::Int
    ref::Union{DRef,Nothing}
end
ThunkID(id::Int) = ThunkID(id, nothing)

"A handle to the scheduler, used by dynamic thunks."
struct SchedulerHandle
    thunk_id::ThunkID
    out_chan::RemoteChannel
    inp_chan::RemoteChannel
end

"Gets the scheduler handle for the currently-executing thunk."
sch_handle() = task_local_storage(:_dagger_sch_handle)::SchedulerHandle

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
            close(inp_chan)
            close(out_chan)
        end
        # Throw out of scheduler
        throw(SchedulerHaltedException())
    end
end

"Processes dynamic messages from worker-executing thunks."
function dynamic_listener!(ctx, state, wid)
    task = current_task() # The scheduler's main task
    inp_chan, out_chan = state.worker_chans[wid]
    listener_task = @async begin
        while isopen(inp_chan) && !state.halt.set
            tid, f, data = try
                take!(inp_chan)
            catch err
                if !(unwrap_nested_exception(err) isa Union{SchedulerHaltedException,
                                                            ProcessExitedException,
                                                            InvalidStateException})
                    iob = IOContext(IOBuffer(), :color=>true)
                    println(iob, "Error in sending dynamic request:")
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
    end
    @async begin
        wait(state.halt)
        # TODO: Not sure why we need the @async here, but otherwise we
        # don't stop all the listener tasks
        @async Base.throwto(listener_task, SchedulerHaltedException())
    end
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
    put!(state.chan, (1, nothing, SchedulerHaltedException(), nothing))
    Base.throwto(task, SchedulerHaltedException())
end

"Waits on a thunk to complete, and fetches its result."
function Base.fetch(h::SchedulerHandle, id::ThunkID)
    future = ThunkFuture(Future(1))
    exec!(_register_future!, h, future, id, true)
    fetch(future; proc=thunk_processor())
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
        if haskey(state.cache, thunk)
            put!(future, state.cache[thunk]; error=state.errored[thunk])
        else
            futures = get!(()->ThunkFuture[], state.futures, thunk)
            push!(futures, future)
        end
    end
    return
end

# TODO: Optimize wait() to not serialize a Chunk
"Waits on a thunk to complete."
function Base.wait(h::SchedulerHandle, id::ThunkID; future=ThunkFuture(1))
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
add_thunk!(f, h::SchedulerHandle, args...; future=nothing, ref=nothing, options...) =
    exec!(_add_thunk!, h, f, args, options, future, ref)
function _add_thunk!(ctx, state, task, tid, (f, args, options, future, ref))
    timespan_start(ctx, :add_thunk, tid, 0)
    _args = map(args) do pos_arg
        if pos_arg[2] isa ThunkID
            return pos_arg[1] => state.thunk_dict[pos_arg[2].id]
        else
            return pos_arg[1] => pos_arg[2]
        end
    end
    GC.@preserve _args begin
        thunk = Thunk(f, _args...; options...)
        # Create a `DRef` to `thunk` so that the caller can preserve it
        thunk_ref = poolset(thunk; size=64, device=MemPool.CPURAMDevice())
        thunk_id = ThunkID(thunk.id, thunk_ref)
        state.thunk_dict[thunk.id] = WeakThunk(thunk)
        reschedule_syncdeps!(state, thunk)
        @dagdebug thunk :submit "Added to scheduler"
        if future !== nothing
            # Ensure we attach a future before the thunk is scheduled
            _register_future!(ctx, state, task, tid, (future, thunk_id, false))
            @dagdebug thunk :submit "Registered future"
        end
        if ref !== nothing
            # Preserve the `EagerThunkFinalizer` through `thunk`
            thunk.eager_ref = ref
        end
        state.valid[thunk] = nothing
        put!(state.chan, RescheduleSignal())
        timespan_finish(ctx, :add_thunk, tid, 0)
        return thunk_id
    end
end
