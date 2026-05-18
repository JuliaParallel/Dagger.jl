"""
Shared request/reply dispatch helpers for concrete backends. See
`uniform.jl` / `nonuniform.jl` for `take_waiter!` / `_send_reply!` methods.
"""

function _reply_exception!(b::AbstractMPIRPCBackend, notify_oid::MPIRRID, rank::Int, ex)
    ce = ex isa CapturedException ? ex : CapturedException(ex, catch_backtrace())
    _send_reply!(b, notify_oid, MPIRemoteException(rank, ce))
    return nothing
end

"""
Deliver a reply-frame body deserialization failure into the client's
`MPIFuture` when `response_oid` is known. Returns `true` if a waiter
was found and delivered.
"""
function _deliver_reply_deserialize_failure!(b::AbstractMPIRPCBackend,
                                             response_oid::MPIRRID, rank::Int, body_err)
    is_null(response_oid) && return false
    fut = take_waiter!(b, response_oid)
    if fut === nothing
        @warn "MPIRPC: reply for unknown waiter (response_oid=$(response_oid)) on rank $(rank); dropping"
        return false
    end
    deliver!(fut, MPIRemoteException(rank, CapturedException(body_err, catch_backtrace())))
    return true
end

function _notify_request_decode_failure!(b::AbstractMPIRPCBackend, header::MsgHeader,
                                         rank::Int, body_err)
    if !is_null(header.notify_oid)
        _reply_exception!(b, header.notify_oid, rank, body_err)
    else
        showerror(stderr, CapturedException(body_err, catch_backtrace()))
    end
    return nothing
end
