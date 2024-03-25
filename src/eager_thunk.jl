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
    EagerThunkMetadata

Represents some useful metadata pertaining to an `EagerThunk`:
- `return_type::Type` - The inferred return type of the task
- `scope::AbstractScope` - The scope for the task
"""
mutable struct EagerThunkMetadata
    return_type::Type
    scope::AbstractScope
end

"""
    EagerThunk

Returned from `spawn`/`@spawn` calls. Represents a task that is in the
scheduler, potentially ready to execute, executing, or finished executing. May
be `fetch`'d or `wait`'d on at any time.
"""
mutable struct EagerThunk
    id::ThunkID
    future::ThunkFuture
    metadata::EagerThunkMetadata
    finalizer_ref::DRef
    thunk_ref::DRef
    EagerThunk(id, future, metadata, finalizer_ref) =
        new(id, future, metadata, finalizer_ref)
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
    print(io, "EagerThunk[$(t.id)] ($status)")
end
istask(t::EagerThunk) = true

"When finalized, cleans-up the associated `EagerThunk`."
mutable struct EagerThunkFinalizer
    id::ThunkID
    function EagerThunkFinalizer(id::ThunkID)
        x = new(id)
        finalizer(Sch.eager_cleanup, x)
        return x
    end
end
