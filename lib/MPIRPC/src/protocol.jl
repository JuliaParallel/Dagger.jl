using Serialization

"""
    MPIRRID(whence::Int32, id::UInt64)

Routing identifier for an in-flight remote call. Mirrors `Distributed.RRID`,
with `whence` storing the *MPI rank* (within the backend's communicator) that
allocated the id rather than the Distributed worker pid.

`whence == -1` and `id == 0` is reserved as the null id (`NULL_RRID`),
analogous to Distributed's `RRID(0, 0)`.
"""
struct MPIRRID
    whence::Int32
    id::UInt64
end

const NULL_RRID = MPIRRID(Int32(-1), UInt64(0))

is_null(r::MPIRRID) = r.whence == NULL_RRID.whence && r.id == NULL_RRID.id

Base.hash(r::MPIRRID, h::UInt) = hash(r.whence, hash(r.id, hash(MPIRRID, h)))
Base.:(==)(a::MPIRRID, b::MPIRRID) = a.whence == b.whence && a.id == b.id

"""
    MsgHeader(response_oid::MPIRRID, notify_oid::MPIRRID)

Two-OID header preceding every MPIRPC body, mirroring `Distributed.MsgHeader`.

* `response_oid` identifies a `MPIFuture` on the receiver of a `ResultMsg`
  (i.e. on the *client* of the original call) so the value can be delivered
  to the right waiter.
* `notify_oid` is the OID the *server* must echo back as `response_oid` when
  it produces a `ResultMsg`. It is set on outgoing `CallMsg` / `CallWaitMsg`
  by the client and is null on `RemoteDoMsg` (fire-and-forget).
"""
struct MsgHeader
    response_oid::MPIRRID
    notify_oid::MPIRRID
end
MsgHeader() = MsgHeader(NULL_RRID, NULL_RRID)
MsgHeader(response_oid::MPIRRID) = MsgHeader(response_oid, NULL_RRID)

abstract type AbstractMsg end

"""
    CallMsg{Mode}(f, args::Tuple, kwargs)

`Mode` is `:call` for `remotecall` (client expects a future) or `:call_fetch`
for `remotecall_fetch` (client expects the value to be returned in a
`ResultMsg`). MPIRPC v1 actually treats both modes identically on the wire:
the server always replies with a `ResultMsg` carrying the value or a
`MPIRemoteException`. `Mode` is preserved so the server can mirror Distributed's
behavior of distinguishing `:call` from `:call_fetch` in error reporting later.
"""
struct CallMsg{Mode} <: AbstractMsg
    f::Any
    args::Tuple
    kwargs::Any
end

"""
    CallWaitMsg(f, args, kwargs)

`remotecall_wait`: server runs the call and replies with `:OK` (or an
exception) so the client can confirm completion without paying for marshaling
the result.
"""
struct CallWaitMsg <: AbstractMsg
    f::Any
    args::Tuple
    kwargs::Any
end

"""
    RemoteDoMsg(f, args, kwargs)

Fire-and-forget: server runs the call and discards the result. No reply is
sent. Errors are printed to stderr on the server rank (like
`Distributed.remote_do`); nothing is delivered to the client.
"""
struct RemoteDoMsg <: AbstractMsg
    f::Any
    args::Tuple
    kwargs::Any
end

"""
    ResultMsg(value)

Carries either a successful return value or an `MPIRemoteException`. The
client routes it to a waiter via `MsgHeader.response_oid`.
"""
struct ResultMsg <: AbstractMsg
    value::Any
end

"""
    RPCProgressHaltMsg

Control message on the request tag: tells [`rpc_progress!`](@ref) to stop
draining further inbound requests in the **current** progress pass (returns
`false`). Framed like other MPIRPC bodies; see [`rpc_progress_halt!`](@ref).
"""
struct RPCProgressHaltMsg <: AbstractMsg end

"""
    MSG_BOUNDARY

Ten-byte sentinel appended after every serialized frame. MPI is
message-oriented so we do not need it for stream resynchronization, but we
keep it as a fail-fast protocol-version / corruption check, matching
Distributed's convention.
"""
const MSG_BOUNDARY = UInt8[0x4d, 0x50, 0x49, 0x52, 0x50, 0x43, 0x46, 0x52, 0x4d, 0x31]

"""
    encode_frame(header, msg) -> Vector{UInt8}

Serialize `header` then `msg` through a single `Serializer` over a fresh
`IOBuffer`, append `MSG_BOUNDARY`, and return the byte vector ready for
`MPI.Isend`. A fresh `Serializer` per message bounds the serializer's
back-reference table to one frame, so cross-frame state cannot leak.
"""
function encode_frame(header::MsgHeader, msg::AbstractMsg)
    io = IOBuffer()
    s = Serializer(io)
    serialize(s, header)
    serialize(s, msg)
    write(io, MSG_BOUNDARY)
    return take!(io)
end

"""
    decode_frame(buf) -> (header, msg, body_error)

Decode a frame previously produced by `encode_frame`. The header is decoded
first so that, on body failures, the caller can still reply to the right
waiter (mirrors Distributed's two-stage parse in `process_messages.jl`).

`body_error === nothing` on success; otherwise `msg === nothing` and
`body_error` is the captured exception. The `MSG_BOUNDARY` is verified on
success and a `ProtocolError` is raised if it is missing or wrong, which
forces a fail-fast rather than silently delivering garbage.

`Base.invokelatest` wraps the body deserialization so that types defined
after world-age advances are reachable, matching Distributed's
`invokelatest(deserialize_msg, ...)` call site.
"""
function decode_frame(buf::Vector{UInt8})
    io = IOBuffer(buf)
    s = Serializer(io)
    header = deserialize(s)::MsgHeader
    msg = nothing
    body_error = nothing
    try
        msg = invokelatest(deserialize, s)::AbstractMsg
        verify_boundary!(io)
    catch e
        body_error = e
    end
    return header, msg, body_error
end

struct ProtocolError <: Exception
    msg::String
end
Base.showerror(io::IO, e::ProtocolError) = print(io, "MPIRPC.ProtocolError: ", e.msg)

function verify_boundary!(io::IOBuffer)
    n = length(MSG_BOUNDARY)
    bytes_left = io.size - io.ptr + 1
    if bytes_left < n
        throw(ProtocolError("frame ended before MSG_BOUNDARY (have $(bytes_left) of $(n) bytes)"))
    end
    tail = read(io, n)
    if tail != MSG_BOUNDARY
        throw(ProtocolError("MSG_BOUNDARY mismatch (got $(tail))"))
    end
    return nothing
end
