# Non-uniform driver: half the ranks are dedicated listeners ("workers"),
# the rest are clients that submit work and collect results.
#
# Run with:
#   mpiexec -n 4 julia --project=. examples/nonuniform_driver.jl

using MPI
using MPIRPC

MPI.Init(; threadlevel=:multiple)

const WORLD_RANK = MPI.Comm_rank(MPI.COMM_WORLD)
const NPROC      = MPI.Comm_size(MPI.COMM_WORLD)

NPROC >= 2 || error("non-uniform example needs at least 2 ranks")

# Half the ranks are listeners. Adjust to your topology.
const HALF           = max(1, NPROC ÷ 2)
const LISTENER_RANKS = collect(0:(HALF - 1))

backend = MPIRPC.select_mpi_rpc_backend!(
    NonUniformMPIRPCBackend(MPI.COMM_WORLD; listener_ranks = LISTENER_RANKS))

if MPIRPC.is_listener(backend)
    println("[rank $WORLD_RANK] LISTENER; serving until shutdown")
    flush(stdout)

    # Listener loop: pump progress, exit when a client tells us to stop.
    # `rpc_progress!` itself does not block; this loop is the listener's
    # main service loop. In production code, interleave `rpc_progress!`
    # with whatever else the listener does.
    while backend.running[]
        MPIRPC.rpc_progress!(backend)
        yield()
    end
    println("[rank $WORLD_RANK] LISTENER exit")
else
    println("[rank $WORLD_RANK] CLIENT; submitting calls to $LISTENER_RANKS")
    flush(stdout)

    # Submit calls to every listener.
    K = 8
    futs = MPIFuture[]
    for ℓ in LISTENER_RANKS, k in 1:K
        push!(futs, MPIRPC.remotecall(+, ℓ, WORLD_RANK * 100, k))
    end
    for f in futs
        v = fetch(f)
        println("[rank $WORLD_RANK] got $v from listener (expected WORLD_RANK*100 + k)")
    end

    # Tell every listener to wind down. `remote_do` is fire-and-forget; the
    # listeners flip their own `running` flag and exit their service loop.
    for ℓ in LISTENER_RANKS
        MPIRPC.remote_do(MPIRPC.shutdown!, ℓ)
    end
end

# Both roles must reach the same barrier. `rpc_barrier` is preferred over
# `MPI.Barrier` so any in-flight RPC drains before the program exits.
MPIRPC.rpc_barrier()
MPIRPC.shutdown!()
MPI.Finalize()
