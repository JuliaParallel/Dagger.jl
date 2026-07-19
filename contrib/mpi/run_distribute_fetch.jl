#!/usr/bin/env julia
# Create a matrix with a fixed reproducible pattern, distribute it with an
# MPI procgrid, then on each rank fetch and println the chunk(s) it owns.
# Usage (from repo root, use full path to Dagger.jl):
#   mpiexec -n 4 julia --project=/path/to/Dagger.jl benchmarks/run_distribute_fetch.jl

using MPI
using Dagger

if !isdefined(Dagger, :accelerate!)
    error("Dagger.accelerate! not found. Run with the local Dagger project: julia --project=/path/to/Dagger.jl ...")
end
Dagger.accelerate!(:mpi)

const comm = MPI.COMM_WORLD
const rank = MPI.Comm_rank(comm)
const nranks = MPI.Comm_size(comm)

# Fixed reproducible pattern: 6×6 matrix, M[i,j] = 10*i + j (same on all ranks)
const N = 6
const BLOCK = 2
A = [10 * i + j for i in 1:N, j in 1:N]

# Procgrid: use Dagger's compatible processors so the procgrid passes validation
availprocs = collect(Dagger.compatible_processors())
nblocks = (cld(N, BLOCK), cld(N, BLOCK))
procgrid = reshape(
    [availprocs[mod(i - 1, length(availprocs)) + 1] for i in 1:prod(nblocks)],
    nblocks,
)

# Distribute so chunk (i,j) is computed on procgrid[i,j]
D = distribute(A, Blocks(BLOCK, BLOCK), procgrid)
D_fetched = fetch(D)

# On each rank: fetch and print only the chunk(s) this rank owns
for (idx, ch) in enumerate(D_fetched.chunks)
    if ch isa Dagger.Chunk && ch.handle isa Dagger.MPIRef && ch.handle.rank == rank
        data = fetch(ch)
        println("rank $rank chunk $idx: ", data)
    end
end
