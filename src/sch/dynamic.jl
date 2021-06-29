export SchedulerHaltedException
export sch_handle, halt!, exec!, get_dag_ids, add_thunk!

"Identifies a thunk by its ID."
struct ThunkID
    id::Int
end

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
function dynamic_listener!(ctx, state)
    task = current_task() # The scheduler's main task
    listener_tasks = Task[]
    for tid in keys(state.worker_chans)
        inp_chan, out_chan = state.worker_chans[tid]
        push!(listener_tasks, @async begin
            while isopen(inp_chan) && !state.halt.set
                tid, f, data = try
                    take!(inp_chan)
                catch err
                    if !(unwrap_nested_exception(err) isa Union{SchedulerHaltedException,
                                                                ProcessExitedException,
                                                                InvalidStateException})
                        @error exception=(err,catch_backtrace())
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
                        @error exception=(err,catch_backtrace())
                    end
                end
            end
        end)
    end
    @async begin
        wait(state.halt)
        for ltask in listener_tasks
            # TODO: Not sure why we need the @async here, but otherwise we
            # don't stop all the listener tasks
            @async Base.throwto(ltask, SchedulerHaltedException())
        end
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
    exec!(_register_future!, h, future, id.id)
    fetch(future; proc=thunk_processor())
end
"Waits on a thunk to complete, and fetches its result."
function register_future!(h::SchedulerHandle, id::ThunkID, future::ThunkFuture)
    exec!(_register_future!, h, future, id.id)
end
function _register_future!(ctx, state, task, tid, (future, id))
    tid != id || throw(DynamicThunkException("Cannot fetch own result"))
    thunk = state.thunk_dict[id]
    ownthunk = state.thunk_dict[tid]
    dominates(target, t) = (t == target) || any(_t->dominates(target, _t), filter(istask, unwrap_weak.(t.inputs)))
    !dominates(ownthunk, thunk) || throw(DynamicThunkException("Cannot fetch result of dominated thunk"))
    # TODO: Assert that future will be fulfilled
    if haskey(state.cache, thunk)
        put!(future, state.cache[thunk]; error=state.errored[thunk])
    else
        futures = get!(()->ThunkFuture[], state.futures, thunk)
        push!(futures, future)
    end
    nothing
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
        if haskey(state.waiting_data, thunk)
            deps[ThunkID(id)] = Set(map(t->ThunkID(t.id), collect(state.waiting_data[thunk])))
        else
            deps[ThunkID(id)] = Set{ThunkID}()
        end
    end
    deps
end

"Adds a new Thunk to the DAG."
add_thunk!(f, h::SchedulerHandle, args...; future=nothing, eager=false, kwargs...) =
    ThunkID(exec!(_add_thunk!, h, f, args, kwargs, future, eager))
function _add_thunk!(ctx, state, task, tid, (f, args, kwargs, future, eager))
    _args = map(arg->arg isa ThunkID ? Dagger.WeakThunk(state.thunk_dict[arg.id]) : arg, args)
    thunk = Thunk(f, _args...; kwargs...)
    state.thunk_dict[thunk.id] = thunk
    @assert reschedule_inputs!(state, thunk)
    if future !== nothing
        # Ensure we attach a future before the thunk is scheduled
        _register_future!(ctx, state, task, tid, (future, thunk.id))
    end
    put!(state.chan, RescheduleSignal())
    if eager
        EAGER_THUNK_MAP[thunk.id] = thunk
    end
    return thunk.id::Int
end
