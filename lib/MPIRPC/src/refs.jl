"""
    MPIFuture

Future-like handle returned by `remotecall` and (internally) used by
`remotecall_fetch` / `remotecall_wait`. Mirrors `Distributed.Future`:

* `where` — MPI rank running the call (the eventual source of the reply).
* `rrid::MPIRRID` — id allocated by the *caller* (`whence == my rank`).
  This same id is echoed back in `MsgHeader.response_oid` of the
  corresponding `ResultMsg`.
* `v::Atomic{Union{Some{Any}, Nothing}}` — set once when the reply arrives.

A `MPIFuture` is registered in the backend's waiter table from creation
until the value is delivered or it is finalized (whichever comes first).
"""
mutable struct MPIFuture
    backend::Any
    where::Int
    rrid::MPIRRID
    @atomic v::Union{Some{Any}, Nothing}
    cond::Threads.Condition

    function MPIFuture(backend, where::Integer, rrid::MPIRRID)
        return new(backend, Int(where), rrid, nothing, Threads.Condition())
    end
end

Base.show(io::IO, f::MPIFuture) =
    print(io, "MPIFuture(where=", f.where, ", rrid=", f.rrid, ", ready=", isready(f), ")")

"""
    isready(f::MPIFuture) -> Bool

True if the reply has been delivered (success or remote exception) and a
subsequent `fetch(f)` will not block.
"""
Base.isready(f::MPIFuture) = (@atomic :acquire f.v) !== nothing

"""
    deliver!(f::MPIFuture, value)

Internal: store `value` in `f` and wake any waiters. Idempotent — repeated
deliveries (which can occur if a stale duplicate reply ever lands) are
ignored after the first.
"""
function deliver!(f::MPIFuture, value)
    @lock f.cond begin
        prev = (@atomic :acquire f.v)
        if prev === nothing
            @atomic :release f.v = Some{Any}(value)
            notify(f.cond, all=true)
        end
    end
    return nothing
end
