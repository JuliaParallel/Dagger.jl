export DTask

"""
    LocalFuture

A fast, shared-memory alternative to Distributed's Future.
"""
mutable struct LocalFuture
    const ready::Base.Event
    errored::Bool
    value::Union{Some{Any}, Nothing}

    LocalFuture() = new(Base.Event(), false, nothing)
end
Base.isready(f::LocalFuture) = f.ready.set # FIXME: Use isready(f.ready)
function Base.wait(f::LocalFuture)
    wait(f.ready)
    return
end

"A future holding the result of a `Thunk`."
mutable struct ThunkFuture
    const from::Int
    local_future::Union{LocalFuture, Nothing}
    remote_future::Union{Future, Nothing}
end
function ThunkFuture(from::Int=myid())
    if from == myid()
        return ThunkFuture(from, LocalFuture(), nothing)
    else
        return ThunkFuture(from, nothing, Future())
    end
end
function Base.isready(t::ThunkFuture)
    if t.local_future !== nothing
        return isready(t.local_future::LocalFuture)
    else
        return isready(t.remote_future::Future)::Bool
    end
end
Base.wait(t::ThunkFuture) = Dagger.Sch.thunk_yield() do
    if t.from == myid()
        wait(t.local_future)
    else
        wait(t.remote_future)
    end
    return
end
function Base.fetch(t::ThunkFuture; proc=OSProc(), raw=false)
    if t.from == myid()
        if !isready(t.local_future)
            Dagger.Sch.thunk_yield() do
                wait(t.local_future)
            end
        end
        value = something(t.local_future.value)
        error = t.local_future.errored
    else
        error, value = Dagger.Sch.thunk_yield() do
            fetch(t.remote_future)
        end
    end
    if error
        throw(value)
    end
    if raw
        return value
    else
        return move(proc, value)
    end
end
function Base.put!(t::ThunkFuture, x; error=false)
    if isready(t)
        throw(ConcurrencyViolationError("ThunkFuture can't be set twice"))
    end

    # Notify either or both futures
    if t.local_future !== nothing
        t.local_future.value = Some{Any}(x)
        t.local_future.errored = error
        notify(t.local_future.ready)
    end
    if t.remote_future !== nothing
        put!(t.future, (error, x))
    end

    return x
end
function Serialization.serialize(io::AbstractSerializer, t::ThunkFuture)
    if t.remote_future === nothing
        # Add a Future
        t.remote_future = Future()
    end

    # Serialize normally
    return invoke(serialize, Tuple{typeof(io), Any}, io, t)
end
function Serialization.deserialize(io::AbstractSerializer, ::Type{ThunkFuture})
    # Deserialize normally
    t = invoke(deserialize, Tuple{typeof(io), Any}, io, ThunkFuture)

    if t.local_future !== nothing
        # Remove the (now useless) LocalFuture
        t.local_future = nothing
    end

    return t
end

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
    const uid::UInt
    future::ThunkFuture
    const metadata::DTaskMetadata
    thunk_ref::DRef

    DTask(uid, future, metadata) = new(uid, future, metadata)
end

const EagerThunk = DTask

Base.isready(t::DTask) = isready(t.future)::Bool
Base.istaskdone(t::DTask) = isready(t.future)
Base.istaskstarted(t::DTask) = isdefined(t, :thunk_ref)
function Base.wait(t::DTask)
    if !istaskstarted(t)
        throw(ConcurrencyViolationError("Cannot `wait` on an unlaunched `DTask`"))
    end
    if !isready(t)
        wait(t.future)
    end
    return
end
function Base.fetch(t::DTask; raw=false)
    if !istaskstarted(t)
        throw(ConcurrencyViolationError("Cannot `fetch` an unlaunched `DTask`"))
    end
    return fetch(t.future; raw)
end
function waitany(tasks::Vector{DTask})
    if isempty(tasks)
        return
    end
    cond = Threads.Condition()
    for task in tasks
        Sch.errormonitor_tracked("waitany listener", Threads.@spawn begin
            wait(task)
            @lock cond notify(cond)
        end)
    end
    @lock cond wait(cond)
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

const EAGER_ID_COUNTER = Threads.Atomic{UInt64}(1)
function eager_next_id()
    if myid() == 1
        Threads.atomic_add!(EAGER_ID_COUNTER, one(UInt64))
    else
        remotecall_fetch(eager_next_id, 1)
    end
end
