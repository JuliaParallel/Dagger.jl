"""
    NonUniformMPIRPCBackend(comm; listener_ranks, ...)

Heterogeneous backend with explicit roles. Ranks in `listener_ranks` service
inbound requests via [`rpc_progress!`](@ref); all other ranks are clients
that only initiate calls and only drain replies addressed to them.

# Roles

* **Listener**: must call `rpc_progress!` regularly (e.g. from a dedicated
  service loop or interleaved with computation). Listeners may also issue
  RPC themselves, in which case they additionally drain replies via the
  same entrypoint.
* **Client-only**: never services requests from peers. Calling `remotecall*`
  with a non-listener `dest_rank` raises `ArgumentError`.

# Communicators

If `dup_comm` is `true` (default), the backend duplicates `comm` with
`MPI.Comm_dup`. This isolates RPC traffic from collectives the user runs on
the original communicator, so for instance an `MPI.Barrier` on
`MPI.COMM_WORLD` cannot be matched against — or block — pending RPC sends.

# Constructor

    NonUniformMPIRPCBackend(comm = MPI.COMM_WORLD;
                            listener_ranks::AbstractVector{<:Integer},
                            request_tag = $(REQUEST_TAG_DEFAULT),
                            reply_tag   = $(REPLY_TAG_DEFAULT),
                            dup_comm    = true,
                            daemon      = false)

If `daemon` is `true`, [`select_mpi_rpc_backend!`](@ref) spawns a yield-only
progress loop on every rank (listener or client) so listener-side request
draining and client-side reply draining both happen automatically without
the user calling `rpc_progress!`. See `UniformMPIRPCBackend`'s docstring
for the threading and CPU caveats — the daemon never sleeps; it polls
continuously and uses ≈ 100% of one OS thread.
"""
mutable struct NonUniformMPIRPCBackend <: AbstractMPIRPCBackend
    base_comm::MPI.Comm
    comm::MPI.Comm
    request_tag::Int32
    reply_tag::Int32
    dup_comm::Bool
    rank::Int
    size::Int

    listener_ranks::Set{Int}
    is_listener::Bool

    rrid_counter::Threads.Atomic{UInt64}
    waiters::Dict{MPIRRID, MPIFuture}
    waiters_lock::ReentrantLock

    pending_sends::Vector{Tuple{MPI.Request, Vector{UInt8}}}

    mpi_lock::ReentrantLock

    running::Threads.Atomic{Bool}
    initialized::Bool

    daemon::Bool
    daemon_task::Union{Task, Nothing}

    function NonUniformMPIRPCBackend(base_comm::MPI.Comm = MPI.COMM_WORLD;
                                     listener_ranks::AbstractVector{<:Integer},
                                     request_tag::Integer = REQUEST_TAG_DEFAULT,
                                     reply_tag::Integer   = REPLY_TAG_DEFAULT,
                                     dup_comm::Bool = true,
                                     daemon::Bool = false)
        request_tag == reply_tag && throw(ArgumentError("request_tag and reply_tag must differ"))
        isempty(listener_ranks) && throw(ArgumentError("listener_ranks must be non-empty"))
        ls = Set{Int}(Int(r) for r in listener_ranks)
        return new(base_comm, base_comm, Int32(request_tag), Int32(reply_tag),
                   dup_comm, -1, -1,
                   ls, false,
                   Threads.Atomic{UInt64}(1),
                   Dict{MPIRRID, MPIFuture}(),
                   ReentrantLock(),
                   Tuple{MPI.Request, Vector{UInt8}}[],
                   ReentrantLock(),
                   Threads.Atomic{Bool}(true),
                   false,
                   daemon,
                   nothing)
    end
end

function initialize_mpi_rpc!(b::NonUniformMPIRPCBackend)
    b.initialized && return nothing
    if !MPI.Initialized()
        MPI.Init(; threadlevel=:multiple)
    end
    b.comm = b.dup_comm ? MPI.Comm_dup(b.base_comm) : b.base_comm
    b.rank = MPI.Comm_rank(b.comm)
    b.size = MPI.Comm_size(b.comm)
    for r in b.listener_ranks
        if r < 0 || r >= b.size
            throw(ArgumentError("listener rank $r outside [0, $(b.size))"))
        end
    end
    b.is_listener = b.rank in b.listener_ranks
    b.initialized = true
    return nothing
end

"""
    is_listener(backend) -> Bool

True if the *current* rank services inbound RPC requests on this backend.
"""
is_listener(b::NonUniformMPIRPCBackend) = b.is_listener
is_listener(::UniformMPIRPCBackend) = true

"""
    listener_ranks(backend) -> Vector{Int}
"""
listener_ranks(b::NonUniformMPIRPCBackend) = sort!(collect(b.listener_ranks))
listener_ranks(b::UniformMPIRPCBackend) = collect(0:(b.size-1))

next_rrid(b::NonUniformMPIRPCBackend) =
    MPIRRID(Int32(b.rank), Threads.atomic_add!(b.rrid_counter, UInt64(1)))

function register_waiter!(b::NonUniformMPIRPCBackend, fut::MPIFuture)
    @lock b.waiters_lock begin
        b.waiters[fut.rrid] = fut
    end
    return fut
end

function take_waiter!(b::NonUniformMPIRPCBackend, rrid::MPIRRID)
    @lock b.waiters_lock begin
        fut = get(b.waiters, rrid, nothing)
        fut === nothing && return nothing
        delete!(b.waiters, rrid)
        return fut
    end
end

function _post_isend!(b::NonUniformMPIRPCBackend, dest::Integer, tag::Integer,
                      buf::Vector{UInt8})
    @lock b.mpi_lock begin
        req = MPI.Isend(buf, b.comm; dest=Int(dest), tag=Int(tag))
        push!(b.pending_sends, (req, buf))
    end
    return nothing
end

function _reap_pending_sends!(b::NonUniformMPIRPCBackend)
    @lock b.mpi_lock begin
        i = 1
        while i <= length(b.pending_sends)
            req, _ = b.pending_sends[i]
            if MPI.Test(req)
                deleteat!(b.pending_sends, i)
            else
                i += 1
            end
        end
    end
    return nothing
end

# See `uniform.jl::_try_recv_one!` for the rationale; the body is identical
# because the receive path differs from the request/reply dispatch only at
# the `rpc_progress!` level (listener-vs-not gating), not here.
function _try_recv_one!(b::NonUniformMPIRPCBackend, tag::Integer)
    local req::MPI.Request
    local buf::Vector{UInt8}
    local src::Int
    @lock b.mpi_lock begin
        got, m, status = MPI.Improbe(MPI.ANY_SOURCE, Int(tag), b.comm, MPI.Status)
        got || return nothing
        src = Int(status.MPI_SOURCE)
        count = MPI.Get_count(status, UInt8)
        buf = Vector{UInt8}(undef, count)
        req = MPI.Imrecv!(buf, m)
    end
    while true
        done = @lock b.mpi_lock MPI.Test(req)
        done && return (src, buf)
        yield()
    end
end

function rpc_progress!(b::NonUniformMPIRPCBackend)
    return _rpc_progress_impl!(b)
end

function _rpc_progress_impl!(b::NonUniformMPIRPCBackend)
    _reap_pending_sends!(b)

    if b.is_listener
        while true
            r = _try_recv_one!(b, b.request_tag)
            r === nothing && break
            src, buf = r
            header, msg, body_err = decode_frame(buf)
            if body_err === nothing && msg isa RPCProgressHaltMsg
                return false
            end
            # Spawn handler so the listener loop never holds any lock
            # during user code. See uniform.jl for the rationale.
            errormonitor(Threads.@spawn _run_handler_task(b, src, header, msg, body_err))
        end
    end

    while true
        r = _try_recv_one!(b, b.reply_tag)
        r === nothing && break
        _, buf = r
        _dispatch_reply!(b, buf)
    end

    return true
end

function _execute_request!(b::NonUniformMPIRPCBackend, src::Int, header::MsgHeader, msg::AbstractMsg)
    if msg isa CallMsg{:call} || msg isa CallMsg{:call_fetch}
        v = run_work_thunk(() -> invokelatest(msg.f, msg.args...; pairs(msg.kwargs)...), b.rank)
        if !is_null(header.notify_oid)
            _send_reply!(b, header.notify_oid, v)
        end
    elseif msg isa CallWaitMsg
        v = run_work_thunk(() -> invokelatest(msg.f, msg.args...; pairs(msg.kwargs)...), b.rank)
        if !is_null(header.notify_oid)
            ack = v isa MPIRemoteException ? v : :OK
            _send_reply!(b, header.notify_oid, ack)
        end
    elseif msg isa RemoteDoMsg
        run_work_thunk(() -> invokelatest(msg.f, msg.args...; pairs(msg.kwargs)...), b.rank;
                       print_error=true)
    else
        throw(ProtocolError("unhandled request message $(typeof(msg)) on rank $(b.rank))"))
    end
    return nothing
end

function _dispatch_reply!(b::NonUniformMPIRPCBackend, buf::Vector{UInt8})
    header, msg, body_err = decode_frame(buf)
    if body_err !== nothing
        _deliver_reply_deserialize_failure!(b, header.response_oid, b.rank, body_err)
        return
    end
    msg isa ResultMsg || throw(ProtocolError("expected ResultMsg on reply tag, got $(typeof(msg))"))
    fut = take_waiter!(b, header.response_oid)
    if fut === nothing
        @warn "MPIRPC: ResultMsg for unknown waiter (response_oid=$(header.response_oid)) on rank $(b.rank); dropping"
        return
    end
    deliver!(fut, msg.value)
    return nothing
end

function _send_reply!(b::NonUniformMPIRPCBackend, response_oid::MPIRRID, value)
    header = MsgHeader(response_oid, NULL_RRID)
    body = ResultMsg(value)
    buf = encode_frame(header, body)
    _post_isend!(b, response_oid.whence, b.reply_tag, buf)
    return nothing
end
