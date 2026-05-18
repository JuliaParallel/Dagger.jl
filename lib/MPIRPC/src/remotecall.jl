"""
    remotecall(f, dest_rank, args...; kwargs...) -> MPIFuture

Send a call to `dest_rank` and return a [`MPIFuture`](@ref) that completes
when the reply arrives. Mirrors `Distributed.remotecall`.

The reply carries either the function's return value or a
`MPIRemoteException`; both are delivered into the future, and
[`fetch`](@ref) rethrows the exception case while returning the value
otherwise.
"""
function remotecall(f, dest_rank::Integer, args...; kwargs...)
    backend = current_mpi_rpc_backend()
    return _remotecall_internal(backend, Val(:call), f, Int(dest_rank), args, kwargs)
end

"""
    remotecall_fetch(f, dest_rank, args...; kwargs...) -> result

Send a call, wait for the reply, and return the result. Throws
`MPIRemoteException` if the remote call raised. Mirrors
`Distributed.remotecall_fetch`. Uses [`current_mpi_rpc_backend`](@ref).
"""
function remotecall_fetch(f, dest_rank::Integer, args...; kwargs...)
    return remotecall_fetch(f, current_mpi_rpc_backend(), dest_rank, args...; kwargs...)
end

"""
    remotecall_fetch(f, backend::AbstractMPIRPCBackend, dest_rank, args...; kwargs...) -> result

Send a call, wait for the reply, and return the result. Throws
`MPIRemoteException` if the remote call raised. Mirrors
`Distributed.remotecall_fetch` with an explicit backend (e.g. for Dagger).
"""
function remotecall_fetch(f, backend::AbstractMPIRPCBackend, dest_rank::Integer, args...; kwargs...)
    fut = _remotecall_internal(backend, Val(:call_fetch), f, Int(dest_rank), args, kwargs)
    return fetch(fut)
end

"""
    remotecall_wait(f, dest_rank, args...; kwargs...) -> MPIFuture

Send a call, wait for the server to acknowledge completion, and return the
future (containing `:OK` or an `MPIRemoteException`). Use this when you want
back-pressure on remote completion without paying for marshaling the result.
"""
function remotecall_wait(f, dest_rank::Integer, args...; kwargs...)
    backend = current_mpi_rpc_backend()
    fut = _remotecall_wait_internal(backend, f, Int(dest_rank), args, kwargs)
    wait(fut)
    val = something((@atomic :acquire fut.v))
    val isa MPIRemoteException && throw(val)
    return fut
end

"""
    remote_do(f, dest_rank, args...; kwargs...) -> nothing

Fire-and-forget: send a call to `dest_rank` and return immediately. The
remote function's return value (and any exception) is **not** observed by
the caller. Mirrors `Distributed.remote_do`.
"""
function remote_do(f, dest_rank::Integer, args...; kwargs...)
    backend = current_mpi_rpc_backend()
    _remote_do_internal(backend, f, Int(dest_rank), args, kwargs)
    return nothing
end

"""
    bcast_remotecall(backend::AbstractMPIRPCBackend, f, args...; kwargs...) -> Vector{MPIFuture}

Issue one [`remotecall`](@ref)-style `CallMsg{:call}` to **every other rank**
in `backend`'s communicator: all ranks in `0:backend.size-1` **except**
`backend.rank`, in increasing order. Returns a vector of length
`max(0, backend.size - 1)` (empty when `backend.size == 1`).

`out[i]` is the future for the `i`-th destination in that filtered list (not
indexed by global rank). Progress and `fetch`/`wait` semantics match
[`remotecall`](@ref).

For [`NonUniformMPIRPCBackend`](@ref), destinations that are not listeners
raise from [`_validate_dest!`](@ref); use the explicit-ranks form with only
listeners if needed.
"""
function bcast_remotecall(backend::AbstractMPIRPCBackend, f, args...; kwargs...)
    dests = Int[d for d in 0:(backend.size - 1) if d != backend.rank]
    return _bcast_remotecall_destinations!(backend, f, dests, Tuple(args), kwargs)
end

"""
    bcast_remotecall(backend::AbstractMPIRPCBackend, f, ranks::AbstractVector{<:Integer}, args...; kwargs...) -> Vector{MPIFuture}
    bcast_remotecall(f, ranks::AbstractVector{<:Integer}, args...; kwargs...) -> Vector{MPIFuture}

Issue [`remotecall`](@ref) to each rank in `ranks` **in list order** (no
automatic skip of `backend.rank`; filter `ranks` yourself if you want to omit
the caller). The `i`-th future corresponds to `ranks[i]`.

The no-`backend` form uses [`current_mpi_rpc_backend`](@ref).
"""
function bcast_remotecall(backend::AbstractMPIRPCBackend, f,
                          ranks::AbstractVector{<:Integer}, args...; kwargs...)
    dests = Int[Int(r) for r in ranks]
    return _bcast_remotecall_destinations!(backend, f, dests, Tuple(args), kwargs)
end

function bcast_remotecall(f, ranks::AbstractVector{<:Integer}, args...; kwargs...)
    return bcast_remotecall(current_mpi_rpc_backend(), f, ranks, args...; kwargs...)
end

function _bcast_remotecall_destinations!(backend::AbstractMPIRPCBackend, f,
                                        dests::Vector{Int}, args::Tuple, kwargs)
    n = length(dests)
    out = Vector{MPIFuture}(undef, n)
    for i in 1:n
        out[i] = _remotecall_internal(backend, Val(:call), f, dests[i], args, kwargs)
    end
    return out
end

# ---------------------------------------------------------------------------

function _validate_dest!(backend::AbstractMPIRPCBackend, dest::Int)
    backend.initialized || throw(ArgumentError(
        "MPIRPC backend not initialized; call `select_mpi_rpc_backend!` first"))
    if dest < 0 || dest >= backend.size
        throw(ArgumentError("dest rank $dest outside [0, $(backend.size))"))
    end
    if backend isa NonUniformMPIRPCBackend && !(dest in backend.listener_ranks)
        throw(ArgumentError(
            "dest rank $dest is not a listener; only listener ranks can service RPC " *
            "in a NonUniformMPIRPCBackend (listeners=$(sort!(collect(backend.listener_ranks))))"))
    end
    return nothing
end

function _remotecall_internal(backend::AbstractMPIRPCBackend, ::Val{Mode}, f,
                              dest::Int, args::Tuple, kwargs) where {Mode}
    _validate_dest!(backend, dest)
    rrid = next_rrid(backend)
    fut = MPIFuture(backend, dest, rrid)
    register_waiter!(backend, fut)
    header = MsgHeader(NULL_RRID, rrid)
    msg = CallMsg{Mode}(f, args, _kwargs_to_pairs(kwargs))
    buf = encode_frame(header, msg)
    _post_isend!(backend, dest, backend.request_tag, buf)
    return fut
end

function _remotecall_wait_internal(backend::AbstractMPIRPCBackend, f,
                                   dest::Int, args::Tuple, kwargs)
    _validate_dest!(backend, dest)
    rrid = next_rrid(backend)
    fut = MPIFuture(backend, dest, rrid)
    register_waiter!(backend, fut)
    header = MsgHeader(NULL_RRID, rrid)
    msg = CallWaitMsg(f, args, _kwargs_to_pairs(kwargs))
    buf = encode_frame(header, msg)
    _post_isend!(backend, dest, backend.request_tag, buf)
    return fut
end

function _remote_do_internal(backend::AbstractMPIRPCBackend, f,
                             dest::Int, args::Tuple, kwargs)
    _validate_dest!(backend, dest)
    header = MsgHeader(NULL_RRID, NULL_RRID)
    msg = RemoteDoMsg(f, args, _kwargs_to_pairs(kwargs))
    buf = encode_frame(header, msg)
    _post_isend!(backend, dest, backend.request_tag, buf)
    return nothing
end

# Materialize kwargs as a `Vector{Pair{Symbol,Any}}` so the on-wire form is
# stable regardless of whether the caller passed `; kwargs...`, a `NamedTuple`,
# or a `Base.Pairs`. This avoids serializing the iterator object itself, which
# can drag in unexpected closures.
function _kwargs_to_pairs(kwargs)
    out = Vector{Pair{Symbol, Any}}()
    for (k, v) in pairs(kwargs)
        push!(out, k => v)
    end
    return out
end

# ---------------------------------------------------------------------------
# wait / fetch — two strategies depending on whether the backend has a
# progress daemon driving the wire on this rank.

"""
    wait(f::MPIFuture) -> f

Block until the reply for `f` has been delivered. Two implementations,
selected at call time by inspecting the future's backend:

* **`backend.daemon == true`** — park on `f.cond` (a `Threads.Condition`).
  The waiter consumes zero CPU until [`deliver!`](@ref) (called by the
  daemon's `_dispatch_reply!`) holds the same lock, sets the value, and
  `notify`s. This is the same shape as `Distributed`'s `take!(fut.v)` on
  a `Channel`, with one important difference covered below.

* **`backend.daemon == false`** — fall back to the v1 spin: call
  `rpc_progress!` and `yield` in a loop. We *cannot* park here because
  the backend has no other progress driver: the reply that would
  eventually call `deliver!` only arrives when *some task* drains the
  wire, and `wait` itself is the only candidate. Parking would deadlock.

The `backend.daemon` check is read off `f.backend`, not the
*currently installed* backend, so a future created on a daemon-backed
backend is still cheap to wait on even from a task that has scoped a
non-daemon backend via [`with_mpi_rpc_backend`](@ref).
"""
function Base.wait(f::MPIFuture)
    isready(f) && return f
    backend = f.backend::AbstractMPIRPCBackend
    if backend.daemon
        # Cond-park path. The standard CV idiom: hold the lock, recheck
        # the predicate, `wait` if false, recheck on wake. `deliver!`
        # holds the same lock when it sets `f.v` and `notify`s, so the
        # check / park / wake transitions are atomic with delivery.
        @lock f.cond begin
            while !isready(f)
                wait(f.cond)
            end
        end
    else
        # No daemon ⇒ this task is the progress driver. A bare
        # `wait(f.cond)` here would never wake because nobody is
        # receiving the reply that triggers the notify. Yield-rate
        # spin is the only correct choice; it costs CPU but it
        # actually makes forward progress on the wire.
        while !isready(f)
            rpc_progress!(backend)
            isready(f) && break
            yield()
        end
    end
    return f
end

"""
    fetch(f::MPIFuture) -> value

Block until the reply has arrived, then return the value. If the remote
function threw, that exception is rethrown locally as a `MPIRemoteException`.
Park / spin behavior follows [`wait`](@ref) — see its docstring.
"""
function Base.fetch(f::MPIFuture)
    wait(f)
    val = something((@atomic :acquire f.v))
    val isa MPIRemoteException && throw(val)
    return val
end

# ---------------------------------------------------------------------------

"""
    @with_progress expr

Convenience macro: evaluate `expr` in a scope where the current MPIRPC
backend's progress engine is driven by `wait`/`fetch`. This is a no-op
today (since `wait`/`fetch` already pump progress) and is provided as a
forward-compatible attachment point for future work that might run user
code in a context without an automatic progress drain.
"""
macro with_progress(expr)
    return esc(expr)
end
