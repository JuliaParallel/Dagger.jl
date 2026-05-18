using MPI

const REQUEST_TAG_DEFAULT = Int32(0xC0DE)
const REPLY_TAG_DEFAULT   = Int32(0xC0DF)

"""
    UniformMPIRPCBackend(comm; ...)

SPMD backend: every rank both issues and services RPC. Every rank must
call [`rpc_progress!`](@ref) regularly (e.g. once per main-loop iteration);
clients block in `fetch` / `wait` by looping `rpc_progress!` themselves so
no rank ever needs a dedicated background task to receive.

# Wire layout

* Two disjoint MPI tags carry traffic, regardless of how many calls are in
  flight: `request_tag` for `CallMsg` / `CallWaitMsg` / `RemoteDoMsg`, and
  `reply_tag` for `ResultMsg`. **Tags do not encode call identity.**
* Each call carries a unique `MPIRRID` in `MsgHeader.notify_oid`. The server
  echoes it back as `MsgHeader.response_oid` on the reply, and the client
  routes the reply to the matching `MPIFuture` through a waiter table keyed
  by `MPIRRID`.

This layout is deliberately ABBA-safe: two ranks calling each other
simultaneously cannot collide on `(peer, tag)` because requests and replies
travel on different tags, and concurrent calls to the same peer cannot be
confused because correlation lives in the header, not the tag. See
`docs/ARCHITECTURE.md` for the full deadlock argument.

# Constructor

    UniformMPIRPCBackend(comm = MPI.COMM_WORLD;
                        request_tag = $(REQUEST_TAG_DEFAULT),
                        reply_tag   = $(REPLY_TAG_DEFAULT),
                        dup_comm    = true,
                        daemon      = false)

If `dup_comm` is `true` (default), the backend duplicates `comm` with
`MPI.Comm_dup` so RPC traffic cannot interleave with collectives the user
runs on the original communicator. Pass `dup_comm = false` if MPI is not
yet initialized at construction time and you intend to call
[`select_mpi_rpc_backend!`](@ref) (which initializes MPI if necessary).

If `daemon` is `true`, [`select_mpi_rpc_backend!`](@ref) spawns a
`Threads.@spawn`-backed task that runs [`rpc_progress!`](@ref) in a tight
yield-only loop until [`shutdown!`](@ref) is called. This removes the
"every rank must call `rpc_progress!`" requirement: as long as Julia has
≥ 2 threads, the daemon makes forward progress on inbound requests
without any user pumping. Under `julia -t 1` the daemon shares a thread
with the main task and only runs at yield points, so the requirement is
effectively unchanged. The daemon does **not** sleep — it polls
continuously — so expect ≈ 100% utilization of one OS thread. If that is
unacceptable for your workload, leave `daemon = false` and drive
progress manually via `rpc_progress!`, `serve_listener`, `wait`/`fetch`,
or `rpc_barrier`.
"""
mutable struct UniformMPIRPCBackend <: AbstractMPIRPCBackend
    base_comm::MPI.Comm
    comm::MPI.Comm
    request_tag::Int32
    reply_tag::Int32
    dup_comm::Bool
    rank::Int
    size::Int

    rrid_counter::Threads.Atomic{UInt64}
    waiters::Dict{MPIRRID, MPIFuture}
    waiters_lock::ReentrantLock

    pending_sends::Vector{Tuple{MPI.Request, Vector{UInt8}}}

    mpi_lock::ReentrantLock

    running::Threads.Atomic{Bool}
    initialized::Bool

    daemon::Bool
    daemon_task::Union{Task, Nothing}

    function UniformMPIRPCBackend(base_comm::MPI.Comm = MPI.COMM_WORLD;
                                  request_tag::Integer = REQUEST_TAG_DEFAULT,
                                  reply_tag::Integer   = REPLY_TAG_DEFAULT,
                                  dup_comm::Bool = true,
                                  daemon::Bool = false)
        request_tag == reply_tag && throw(ArgumentError("request_tag and reply_tag must differ"))
        return new(base_comm, base_comm, Int32(request_tag), Int32(reply_tag),
                   dup_comm, -1, -1,
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

function initialize_mpi_rpc!(b::UniformMPIRPCBackend)
    b.initialized && return nothing
    if !MPI.Initialized()
        MPI.Init(; threadlevel=:multiple)
    end
    b.comm = b.dup_comm ? MPI.Comm_dup(b.base_comm) : b.base_comm
    b.rank = MPI.Comm_rank(b.comm)
    b.size = MPI.Comm_size(b.comm)
    b.initialized = true
    return nothing
end

"""
    next_rrid(backend) -> MPIRRID

Allocate a new id local to this rank. The caller's rank is encoded in the
`whence` field, so the same `MPIRRID` is meaningful across the whole
communicator (only the originating rank ever looks it up in the waiter
table).
"""
next_rrid(b::UniformMPIRPCBackend) =
    MPIRRID(Int32(b.rank), Threads.atomic_add!(b.rrid_counter, UInt64(1)))

function register_waiter!(b::UniformMPIRPCBackend, fut::MPIFuture)
    @lock b.waiters_lock begin
        b.waiters[fut.rrid] = fut
    end
    return fut
end

function take_waiter!(b::UniformMPIRPCBackend, rrid::MPIRRID)
    @lock b.waiters_lock begin
        fut = get(b.waiters, rrid, nothing)
        fut === nothing && return nothing
        delete!(b.waiters, rrid)
        return fut
    end
end

# ---------------------------------------------------------------------------
# Send path

function _post_isend!(b::UniformMPIRPCBackend, dest::Integer, tag::Integer,
                      buf::Vector{UInt8})
    @lock b.mpi_lock begin
        req = MPI.Isend(buf, b.comm; dest=Int(dest), tag=Int(tag))
        push!(b.pending_sends, (req, buf))
    end
    return nothing
end

function _reap_pending_sends!(b::UniformMPIRPCBackend)
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

# ---------------------------------------------------------------------------
# Receive / dispatch path

"""
    _try_recv_one!(backend, tag) -> (src, buf) | nothing

Try to receive one message that's already in MPI's matching engine on
`tag`. Two phases:

1. **Match-and-post under `mpi_lock`.** `MPI.Improbe` is the only way to
   atomically peek at a matched message and remove it from the queue;
   the buffer must be allocated and `MPI.Imrecv!` posted while still
   holding the message handle. Both steps live in a single critical
   section because they are semantically one operation.

2. **Wait outside `mpi_lock`, with yield.** `MPI.Imrecv!` returns a
   non-blocking request; we then loop on `MPI.Test` with `yield()` in
   between, **releasing `mpi_lock` between polls**. This is what makes
   the daemon thread non-blocking even on a slow rendezvous-protocol
   transfer: while the receive is in flight, other tasks (handlers
   doing their own `Isend`s, user code on this rank) can acquire
   `mpi_lock` and progress their own MPI calls.

Eager-protocol messages (the typical case for RPC payloads up to
MPI's eager threshold, often 64 KiB) almost always complete `Test`
on the first poll, so the only added cost vs. blocking `Mrecv!` is
one extra Test call and one lock acquire/release pair — single-digit
microseconds. The win is for large payloads on rendezvous protocol,
where the previous `Mrecv!` would have held `mpi_lock` for the entire
network round-trip.
"""
function _try_recv_one!(b::UniformMPIRPCBackend, tag::Integer)
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

"""
    rpc_progress!([backend])

Drive one non-blocking pass of the MPIRPC engine: reap completed sends,
service every inbound request currently matched on this rank, and deliver
every inbound reply currently matched on this rank.

For [`UniformMPIRPCBackend`](@ref), **every** rank must call this regularly
(typically wrapped in [`@with_progress`](@ref) inside `wait` / `fetch`).
For [`NonUniformMPIRPCBackend`](@ref), only listener ranks need to call
the request-draining half; clients still need to call it to drain replies,
Returns `true` if the pass completed normally, or `false` if a
[`RPCProgressHaltMsg`](@ref) was consumed on the request tag (no further
requests are drained in that pass); see [`rpc_progress_halt!`](@ref).
"""
function rpc_progress!(b::UniformMPIRPCBackend)
    return _rpc_progress_impl!(b)
end

function _rpc_progress_impl!(b::UniformMPIRPCBackend)
    _reap_pending_sends!(b)

    while true
        r = _try_recv_one!(b, b.request_tag)
        r === nothing && break
        src, buf = r
        header, msg, body_err = decode_frame(buf)
        @debug "Received request" header msg body_err
        if body_err === nothing && msg isa RPCProgressHaltMsg
            return false
        end
        # Run the handler on a freshly spawned task so user code never
        # executes while the progress pump holds any state. Without this,
        # a handler that itself blocks on `Threads.@spawn`-then-`fetch`
        # would deadlock: the spawned task would need another thread to
        # drive RPC progress, but every thread that called `rpc_progress!`
        # would be waiting on the handler that the previous progress pass
        # had begun running synchronously. See `docs/ARCHITECTURE.md` §6.
        errormonitor(Threads.@spawn _run_handler_task(b, src, header, msg, body_err))
    end

    while true
        r = _try_recv_one!(b, b.reply_tag)
        r === nothing && break
        @debug "Received reply" r
        _, buf = r
        # Reply dispatch only touches MPIRPC's own state (waiter table and
        # future condition); it cannot block on user code, so we keep it
        # inline.
        _dispatch_reply!(b, buf)
    end

    return true
end

function _execute_request!(b::UniformMPIRPCBackend, src::Int, header::MsgHeader, msg::AbstractMsg)
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

function _dispatch_reply!(b::UniformMPIRPCBackend, buf::Vector{UInt8})
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

function _send_reply!(b::UniformMPIRPCBackend, response_oid::MPIRRID, value)
    header = MsgHeader(response_oid, NULL_RRID)
    body = ResultMsg(value)
    buf = encode_frame(header, body)
    _post_isend!(b, response_oid.whence, b.reply_tag, buf)
    return nothing
end
