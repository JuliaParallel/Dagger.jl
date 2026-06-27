"""
    AbstractMPIRPCBackend

Marker supertype for an installed MPIRPC backend. Concrete backends
(`UniformMPIRPCBackend`, `NonUniformMPIRPCBackend`) hold the communicator,
tag layout, OID counter, waiter table, and MPI lock that drive the wire
protocol; the public RPC surface (`remotecall`, `remotecall_fetch`,
`rpc_progress!`, ...) dispatches dynamically on this type so call sites
never branch on mode.

The selection model mirrors Dagger's acceleration: see
[`Dagger.jl/src/acceleration.jl`](../../Dagger.jl/src/acceleration.jl)
(`accelerate!`, `current_acceleration`, `_with_default_acceleration`).
[`select_mpi_rpc_backend!`](@ref) installs a backend once for the whole
process; [`with_mpi_rpc_backend`](@ref) layers a task-local override for
scoped tests.
"""
abstract type AbstractMPIRPCBackend end

const _BACKEND_KEY = :mpi_rpc_backend

# Process-global default backend, written once at startup by
# `select_mpi_rpc_backend!`. Reads are unsynchronized; the assumption is the
# same as Distributed's `init_parallel` / Dagger's `accelerate!`: install
# happens before any RPC traffic and there is at most one installed backend
# per process for the duration of its life.
const _GLOBAL_BACKEND = Ref{Union{AbstractMPIRPCBackend, Nothing}}(nothing)

"""
    initialize_mpi_rpc!(backend::AbstractMPIRPCBackend)

Per-backend hook (analog of Dagger's `initialize_acceleration!`). Concrete
backends override this to do MPI initialization, communicator splits, tag
range allocation, etc. The default is a no-op.
"""
initialize_mpi_rpc!(::AbstractMPIRPCBackend) = nothing

"""
    select_mpi_rpc_backend!(backend::AbstractMPIRPCBackend) -> backend

Install `backend` as the current backend for the whole process and run
[`initialize_mpi_rpc!`](@ref) once. After this call, every public RPC
function (`remotecall`, `remotecall_fetch`, `rpc_progress!`, ...) — from
*any* task — dispatches through this backend, unless overridden inside
[`with_mpi_rpc_backend`](@ref).

Re-entering with a different backend is **not supported** in v1: calling
this twice in the same process replaces the slot but the on-the-wire state
(communicators, in-flight `Isend` buffers, OID counters) of the first
backend is not reset. To switch modes mid-job, call [`shutdown!`](@ref) on
the existing backend, finalize MPI if appropriate, and start a fresh process.
"""
function select_mpi_rpc_backend!(backend::AbstractMPIRPCBackend)
    initialize_mpi_rpc!(backend)
    _GLOBAL_BACKEND[] = backend
    # Spawn the optional yield-only progress daemon. We do this *after*
    # initialize_mpi_rpc! so the daemon's first `rpc_progress!` finds a
    # fully constructed communicator, and *after* the global slot has been
    # written so a re-entrant `current_mpi_rpc_backend()` from inside the
    # daemon resolves to the right object.
    #
    # Threadpool placement: prefer `:interactive` so a CPU-bound user
    # computation on the `:default` pool cannot starve the wire pump.
    # The interactive pool exists exactly for latency-sensitive tasks
    # that must keep getting CPU regardless of default-pool load. Fall
    # back to `:default` only if the user did not configure any
    # interactive threads (`julia -t N,0` or implicit zero); in that
    # case we emit a one-time info message so the operator knows why
    # tail latency might rise under heavy compute.
    if backend.daemon && backend.daemon_task === nothing
        if Threads.nthreads(:interactive) > 0
            backend.daemon_task = errormonitor(Threads.@spawn :interactive _daemon_loop(backend))
        else
            @info """MPIRPC: no `:interactive` threads configured; the daemon will share \
                     the `:default` pool with user computation. CPU-bound user code on the \
                     default pool can starve the wire pump. Start Julia with `-t N,M` (M >= 1) \
                     to give the daemon a dedicated interactive thread."""
            backend.daemon_task = errormonitor(Threads.@spawn _daemon_loop(backend))
        end
    end
    return backend
end

"""
    current_mpi_rpc_backend() -> AbstractMPIRPCBackend

Return the backend installed for the current task. Prefers a task-local
override (set by [`with_mpi_rpc_backend`](@ref)); otherwise falls back to
the process-global slot installed by [`select_mpi_rpc_backend!`](@ref).
Throws `ArgumentError` if no backend has been installed yet.
"""
function current_mpi_rpc_backend()
    local_backend = task_local_storage(_BACKEND_KEY, nothing)
    if local_backend !== nothing
        return local_backend::AbstractMPIRPCBackend
    end
    g = _GLOBAL_BACKEND[]
    g === nothing && throw(ArgumentError(
        "no MPIRPC backend installed; call `select_mpi_rpc_backend!` first"))
    return g::AbstractMPIRPCBackend
end

"""
    with_mpi_rpc_backend(f, backend) -> f()

Scoped variant: install `backend` for the duration of `f()` (in this task
only) and restore the previous task-local override on exit. The analog of
Dagger's `_with_default_acceleration`. This does **not** call
`initialize_mpi_rpc!`; pass an already-initialized backend, or call
`select_mpi_rpc_backend!` once at startup and use this helper only to layer
scoped overrides during tests.
"""
function with_mpi_rpc_backend(f, backend::AbstractMPIRPCBackend)
    prev = task_local_storage(_BACKEND_KEY, nothing)
    task_local_storage(_BACKEND_KEY, backend)
    try
        return f()
    finally
        if prev === nothing
            delete!(task_local_storage(), _BACKEND_KEY)
        else
            task_local_storage(_BACKEND_KEY, prev)
        end
    end
end

"""
    shutdown!([backend])

Stop the listener loop on this rank by clearing the backend's `running`
flag. Outstanding `Isend` requests are reaped on a best-effort basis.
This is a *local* operation; in non-uniform mode the application should
broadcast a stop signal across listener ranks before calling this on each.

If a progress daemon was spawned by [`select_mpi_rpc_backend!`](@ref)
(`daemon = true` on the backend), this function also `wait`s on the
daemon task so the caller can rely on no further `rpc_progress!`
running once `shutdown!` returns. The wait is bounded only by the
daemon's own loop body; because the loop is yield-only and checks the
flag every iteration, it terminates promptly.
"""
function shutdown!(backend::AbstractMPIRPCBackend = current_mpi_rpc_backend())
    backend.running[] = false
    t = backend.daemon_task
    if t !== nothing
        wait(t)
        backend.daemon_task = nothing
    end
    return nothing
end
