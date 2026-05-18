# Minimal uniform / SPMD driver for MPIRPC.
#
# Run with:
#   mpiexec -n 4 julia --project=. examples/uniform_driver.jl
#
# Every rank issues calls and services calls; progress is pumped implicitly
# by `wait`/`fetch` and explicitly via `rpc_barrier` between phases.

using MPI
using MPIRPC

MPI.Init(; threadlevel=:multiple)
backend = MPIRPC.select_mpi_rpc_backend!(UniformMPIRPCBackend(MPI.COMM_WORLD))
const RANK  = MPI.Comm_rank(backend.comm)
const NPROC = MPI.Comm_size(backend.comm)

println("[rank $RANK] up; world size = $NPROC")

# Phase 1: every rank asks its right-hand neighbor what its rank is.
peer = mod(RANK + 1, NPROC)
neighbor_rank = MPIRPC.remotecall_fetch(peer) do
    return MPI.Comm_rank(MPI.COMM_WORLD)
end
println("[rank $RANK] right neighbor reports rank=$neighbor_rank (expected $peer)")

# Phase boundary: pump progress until every rank arrives. Without this, a
# rank that finishes early could exit while a peer's request to it is still
# unprocessed in its inbox.
MPIRPC.rpc_barrier()

# Phase 2: scatter "compute" work all-to-all.
peers = [r for r in 0:(NPROC-1) if r != RANK]
futs  = [MPIRPC.remotecall(*, p, RANK + 1, p + 1) for p in peers]
for (p, f) in zip(peers, futs)
    @assert fetch(f) == (RANK + 1) * (p + 1)
end
println("[rank $RANK] all-to-all phase 2 done")

MPIRPC.rpc_barrier()
MPIRPC.shutdown!()
MPI.Finalize()
