"A future holding the result of a `Thunk`."
struct ThunkFuture
    future::Future
end
ThunkFuture(x::Integer) = ThunkFuture(Future(x))
ThunkFuture() = ThunkFuture(Future())
Base.isready(t::ThunkFuture) = isready(t.future)
Base.wait(t::ThunkFuture) = Dagger.Sch.thunk_yield() do
    wait(t.future)
    return
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
    DTask

Returned from `Dagger.@spawn`/`Dagger.spawn` calls. Represents a task that is
in the scheduler, potentially ready to execute, executing, or finished
executing. May be `fetch`'d or `wait`'d on at any time. See `Dagger.@spawn` for
more details.
"""
mutable struct DTask
    uid::UInt
    future::ThunkFuture
    finalizer_ref::DRef
    thunk_ref::DRef
    DTask(uid, future, finalizer_ref) = new(uid, future, finalizer_ref)
end

const EagerThunk = DTask

Base.isready(t::DTask) = isready(t.future)
Base.istaskdone(t::DTask) = isready(t.future)
Base.istaskstarted(t::DTask) = isdefined(t, :thunk_ref)
function Base.wait(t::DTask)
    if !istaskstarted(t)
        throw(ConcurrencyViolationError("Cannot `wait` on an unlaunched `DTask`"))
    end
    wait(t.future)
    return
end
function Base.fetch(t::DTask; raw=false)
    if !istaskstarted(t)
        throw(ConcurrencyViolationError("Cannot `fetch` an unlaunched `DTask`"))
    end
    return fetch(t.future; raw)
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

"When finalized, cleans-up the associated `DTask`."
mutable struct DTaskFinalizer
    uid::UInt
    function DTaskFinalizer(uid)
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
