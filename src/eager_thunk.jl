"A future holding the result of a `Thunk`."
struct ThunkFuture
    future::Future
end
ThunkFuture(x::Integer) = ThunkFuture(Future(x))
ThunkFuture() = ThunkFuture(Future())
Base.isready(t::ThunkFuture) = isready(t.future)
Base.wait(t::ThunkFuture) = Dagger.Sch.thunk_yield() do
    wait(t.future)
end
function Base.fetch(t::ThunkFuture; proc=OSProc(), raw=false)
    error, value = Dagger.Sch.thunk_yield() do
        fetch(t.future)
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
Base.put!(t::ThunkFuture, x; error=false) = put!(t.future, (error, x))

"""
    Options(::NamedTuple)
    Options(; kwargs...)

Options for thunks and the scheduler. See [Task Spawning](@ref) for more
information.
"""
struct Options
    options::NamedTuple
end
Options(;options...) = Options((;options...))
Options(options...) = Options((;options...))

"""
    EagerThunkMetadata

Represents some useful metadata pertaining to an `EagerThunk`:
- `return_type::Type` - The inferred return type of the task
"""
mutable struct EagerThunkMetadata
    return_type::Type
end

"""
    EagerThunk

Returned from `spawn`/`@spawn` calls. Represents a task that is in the
scheduler, potentially ready to execute, executing, or finished executing. May
be `fetch`'d or `wait`'d on at any time.
"""
mutable struct EagerThunk
    uid::UInt
    future::ThunkFuture
    metadata::EagerThunkMetadata
    finalizer_ref::DRef
    thunk_ref::DRef
    EagerThunk(uid, future, metadata, finalizer_ref) =
        new(uid, future, metadata, finalizer_ref)
end

Base.isready(t::EagerThunk) = isready(t.future)
function Base.wait(t::EagerThunk)
    if !isdefined(t, :thunk_ref)
        throw(ConcurrencyViolationError("Cannot `wait` on an unlaunched `EagerThunk`"))
    end
    wait(t.future)
end
function Base.fetch(t::EagerThunk; raw=false)
    if !isdefined(t, :thunk_ref)
        throw(ConcurrencyViolationError("Cannot `fetch` an unlaunched `EagerThunk`"))
    end
    return fetch(t.future; raw)
end
function Base.show(io::IO, t::EagerThunk)
    status = if isdefined(t, :thunk_ref)
        isready(t) ? "finished" : "running"
    else
        "not launched"
    end
    print(io, "EagerThunk ($status)")
end
istask(t::EagerThunk) = true

"When finalized, cleans-up the associated `EagerThunk`."
mutable struct EagerThunkFinalizer
    uid::UInt
    function EagerThunkFinalizer(uid)
        x = new(uid)
        finalizer(Sch.eager_cleanup, x)
        x
    end
end

const EAGER_ID_COUNTER = Threads.Atomic{UInt64}(1)
function eager_next_id()
    if myid() == 1
        Threads.atomic_add!(EAGER_ID_COUNTER, one(UInt64))
    else
        remotecall_fetch(eager_next_id, 1)
    end
end
