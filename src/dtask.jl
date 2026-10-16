export DTask

# N.B. Backed by `MemPool.DFuture` rather than `Distributed.Future`, when available.
# The latter is unsafe under concurrent multithreaded access within a single process (every
# `put!`/`fetch`/`wait` mutates process-global Distributed tables and `fetch`
# auto-deletes the backing ref, which races and can block a waiter forever).
# `DFuture` signals readiness via a thread-safe `Base.Event` on the owner and
# manages lifetime via MemPool's `DRef` refcounting. See `MemPool/src/devent.jl`.

const DFuture = @static if isdefined(MemPool, :DFuture)
    MemPool.DFuture
else
    Distributed.Future
end

"A future holding the result of a `Thunk`."
struct ThunkFuture
    future::DFuture
end
ThunkFuture(x::Integer) = ThunkFuture(DFuture(x))
ThunkFuture() = ThunkFuture(DFuture())
Base.isready(t::ThunkFuture) = isready(t.future)
Base.wait(t::ThunkFuture) = Dagger.Sch.thunk_yield() do
    wait(t.future)
    return
end
# N.B. `proc` defaults to `nothing` and is only materialized as `OSProc()` when a
# `move` is actually performed: constructing an `OSProc` takes a lock and toggles
# finalizers (see `OSProc(pid)`), which is pure overhead for the common
# `move_value=false` fetches. `unwrap` is accepted (callers pass it) but unused.
# N.B. `uniform` is accepted (callers pass it) but unused; its default must not
# be `uniform_execution()`, which costs a task-local lookup + dynamic dispatch
# per fetch.
function Base.fetch(t::ThunkFuture; proc=nothing, raw=false, move_value=!raw, unwrap=!raw, uniform=false)
    # N.B. `thunk_yield(f)` is exactly `f()` outside of a Dagger task, so skip it
    # (and the closure it would heap-allocate) when not running in one.
    error, value = if Dagger.in_task()
        Dagger.Sch.thunk_yield() do
            fetch(t.future)
        end::Tuple{Bool,Any}
    else
        fetch(t.future)::Tuple{Bool,Any}
    end
    if error
        throw(value)
    end
    if !move_value
        return value
    else
        return move(@something(proc, OSProc()), value)
    end
end
Base.put!(t::ThunkFuture, x; error=false) = put!(t.future, (error, x))

"""
    DTaskMetadata

Represents some useful metadata pertaining to a `DTask`:
- `return_type::Type` - The inferred return type of the task
"""
mutable struct DTaskMetadata
    return_type::Type
end

"""
    DTask

Returned from `Dagger.@spawn`/`Dagger.spawn` calls. Represents a task that is
in the scheduler, potentially ready to execute, executing, or finished
executing. May be `fetch`'d or `wait`'d on at any time. See `Dagger.@spawn` for
more details.
"""
mutable struct DTask
    uid::UInt
    future::ThunkFuture
    metadata::DTaskMetadata
    # Result placeholder of a task this process never built a `Thunk` for (see
    # `elide_task!`); `nothing` for every ordinary task.
    stub::Any
    # Rank/worker known to own this task's result at spawn time, `-1` when unknown
    owner::Int
    thunk_ref::DRef

    DTask(uid, future, metadata) = new(uid, future, metadata, nothing, -1)
end

const EagerThunk = DTask

Base.isready(t::DTask) = isready(t.future)
Base.istaskdone(t::DTask) = isready(t.future)
Base.istaskstarted(t::DTask) = t.stub !== nothing || isdefined(t, :thunk_ref)
function Base.wait(t::DTask)
    if !istaskstarted(t)
        throw(ConcurrencyViolationError("Cannot `wait` on an unlaunched `DTask`"))
    end
    wait(t.future)
    return
end
function Base.fetch(t::DTask; raw=false, move_value=!raw, unwrap=!raw, uniform=false)
    if !istaskstarted(t)
        throw(ConcurrencyViolationError("Cannot `fetch` an unlaunched `DTask`"))
    end
    if t.owner >= 0
        # The owner learns of a failure from its own task, every other process from
        # a message, and a fetch must do the same on all of them: see
        # `fetch_failed_uniform!`
        value = try
            fetch(t.future; move_value=false, unwrap=false, uniform)
        catch
            fetch_failed_uniform!(current_acceleration())
            rethrow()
        end
        return move_value ? move(OSProc(), value) : value
    end
    return fetch(t.future; move_value, unwrap, uniform)
end
function waitany(tasks::Vector{DTask})
    if isempty(tasks)
        return
    end
    cond = Threads.Condition()
    done = Ref(false)
    for task in tasks
        Sch.errormonitor_tracked("waitany listener", Threads.@spawn begin
            wait(task)
            @lock cond begin
                done[] = true
                notify(cond)
            end
        end)
    end
    @lock cond begin
        while !done[]
            wait(cond)
        end
    end
    return
end
function waitall(tasks::Vector{DTask})
    if isempty(tasks)
        return
    end
    @sync for task in tasks
        Threads.@spawn begin
            wait(task)
            @lock cond notify(cond)
        end
    end
    return
end
function Base.show(io::IO, t::DTask)
    status = if istaskstarted(t)
        isready(t) ? "finished" : "running"
    else
        "not launched"
    end
    print(io, "DTask ($status)")
end
istask(t::DTask) = true
function Base.convert(::Type{ThunkSyncdep}, task::Dagger.DTask)
    return ThunkSyncdep(ThunkID(task.uid, isdefined(task, :thunk_ref) ? task.thunk_ref : nothing))
end
ThunkSyncdep(task::DTask) = convert(ThunkSyncdep, task)

function eager_next_id()
    if myid() == 1
        return UInt64(next_id())
    else
        return remotecall_fetch(eager_next_id, 1)::UInt64
    end
end
