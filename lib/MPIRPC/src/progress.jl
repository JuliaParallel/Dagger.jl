"""
    rpc_progress!() = rpc_progress!(current_mpi_rpc_backend())

Drive one non-blocking progress pass on the currently installed backend.
See the per-backend method docstrings for who must call this.
"""
rpc_progress!() = rpc_progress!(current_mpi_rpc_backend())

"""
    _run_handler_task(backend, src, header, msg, body_err)

Internal entry point for the spawned task that runs an inbound request.
Two important effects:

* The current backend is re-installed as task-local so the user closure's
  calls to `current_mpi_rpc_backend()` (e.g. nested `remotecall_fetch`)
  see the same backend the request arrived on, even when the parent task
  was inside a `with_mpi_rpc_backend` scope that does not propagate to
  spawned children.
* Request frames are decoded in the parent [`rpc_progress!`](@ref) pass;
  this task receives the triple from [`decode_frame`](@ref). Body
  deserialization failures are turned into `MPIRemoteException` replies when
  `header.notify_oid` is set, or printed to stderr when not (e.g.
  fire-and-forget). User handler failures are already wrapped by
  [`run_work_thunk`](@ref); unexpected exceptions after decode (e.g. from
  `_send_reply!`) trigger a best-effort `MPIRemoteException` reply when
  possible, then are rethrown so the task's `errormonitor` surfaces them.
"""
function _run_handler_task(b::AbstractMPIRPCBackend, src::Int,
                           header::MsgHeader, msg, body_err)
    return with_mpi_rpc_backend(b) do
        if body_err !== nothing
            _notify_request_decode_failure!(b, header, b.rank, body_err)
            return nothing
        end
        if msg isa RPCProgressHaltMsg
            return nothing
        end
        try
            _execute_request!(b, src, header, msg::AbstractMsg)
        catch e
            if !is_null(header.notify_oid)
                _reply_exception!(b, header.notify_oid, b.rank, e)
            end
            rethrow()
        end
        return nothing
    end
end

"""
    _daemon_loop(backend)

Background progress driver, spawned by [`select_mpi_rpc_backend!`](@ref)
when the backend was constructed with `daemon = true`. The loop runs on
its own task (typically on its own OS thread under `julia -t >= 2`) and
calls [`rpc_progress!`](@ref) in a tight, **yield-only** loop until
`backend.running[]` flips to `false` (which happens inside
[`shutdown!`](@ref)).

There is intentionally no `sleep` form: a sleeping daemon would delay
inbound requests by up to `poll_interval` seconds, which is the worst
kind of latency-vs-cpu trade-off for low-rate workloads. If you need
that trade-off, leave `daemon = false` and pump progress on your own
schedule.

A `try / catch` wraps the body so an unexpected MPI or framing error
does not silently kill the daemon and leave the rank unresponsive.
Errors are logged via `@error` and then rethrown so `shutdown!`'s `wait`
on the daemon task surfaces the failure to the caller.
"""
function _daemon_loop(backend::AbstractMPIRPCBackend)
    try
        while backend.running[]
            rpc_progress!(backend)
            yield()
        end
        rpc_progress!(backend)  # final drain after shutdown! flipped the flag
    catch e
        @error "MPIRPC: daemon loop crashed; this rank will stop making progress" exception = (e, catch_backtrace())
        rethrow()
    end
    return nothing
end

"""
    serve_listener(backend = current_mpi_rpc_backend(); poll_interval=0.0)

Blocking helper for non-uniform listeners: loop calling
[`rpc_progress!`](@ref) until [`shutdown!`](@ref) flips
`backend.running[]` to `false`. `poll_interval` is the time, in seconds,
to `sleep` between progress passes (`0` yields without sleeping).

This is a convenience for examples and tests; production code is free to
interleave `rpc_progress!` with its own work loop instead.
"""
function serve_listener(backend::AbstractMPIRPCBackend = current_mpi_rpc_backend();
                        poll_interval::Real = 0.0)
    while backend.running[]
        rpc_progress!(backend)
        if poll_interval > 0
            sleep(poll_interval)
        else
            yield()
        end
    end
    rpc_progress!(backend)  # final drain so in-flight messages are reaped
    return nothing
end

"""
    rpc_barrier([backend])

Phase boundary for MPIRPC traffic: a non-blocking `MPI.Ibarrier` whose
completion is awaited *while every rank pumps* [`rpc_progress!`](@ref).
Use this between phases of an SPMD program, or before exiting an RPC
session, to drain in-flight work from peers.

Why a plain `MPI.Barrier` is not enough: a rank `R` whose own
`remotecall_fetch` has just completed may still hold *unprocessed
requests* sent by other ranks (e.g. nested calls those ranks issued from
inside a handler running on `R`). If `R` enters `MPI.Barrier` it stops
pumping RPC progress, and the peers waiting on `R`'s replies hang.
`rpc_barrier` solves this by pumping progress until **all** ranks have
arrived, then doing a final drain pass.

Calling pattern is identical to `MPI.Barrier`: every rank in the backend's
communicator must call it.
"""
function rpc_barrier(backend::AbstractMPIRPCBackend = current_mpi_rpc_backend())
    req = @lock backend.mpi_lock MPI.Ibarrier(backend.comm)
    while true
        rpc_progress!(backend)
        done = @lock backend.mpi_lock MPI.Test(req)
        done && break
        yield()
    end
    rpc_progress!(backend)  # one last drain so any reply Isend posted under
                            # the barrier is reaped on this rank
    return nothing
end

"""
    rpc_progress_halt!(backend, dest_rank::Int) -> nothing

Enqueue a framed [`RPCProgressHaltMsg`](@ref) to `dest_rank` on the backend's
`request_tag` via the same `_post_isend!` / pending-send path as ordinary RPC.

The **destination** rank must call [`rpc_progress!`](@ref) (or run its daemon)
to match and decode the message. When consumed, that `rpc_progress!` pass
returns `false` and does **not** spawn a handler task for further matched
requests in that pass—useful to break out of a progress loop without executing
user RPC bodies for additional queued messages.

The halt message carries an empty [`MsgHeader`](@ref); it is not a call/reply
pair and does not touch waiter tables.
"""
function rpc_progress_halt!(backend::AbstractMPIRPCBackend, dest_rank::Int)
    buf = encode_frame(MsgHeader(), RPCProgressHaltMsg())
    _post_isend!(backend, dest_rank, backend.request_tag, buf)
    return nothing
end