#!/usr/bin/env julia
# 10k×10k QR + matmul benchmark; block size scales with number of ranks.
# Usage: mpiexec -n 100 julia --project=/path/to/Dagger.jl benchmarks/bench_100rank_qr_matmul.jl
# Or: bash benchmarks/run_100rank_qr_matmul.sh .

using MPI
using Dagger
using LinearAlgebra

Dagger.accelerate!(:mpi)

const N = 10_000
const comm = MPI.COMM_WORLD
const rank = MPI.Comm_rank(comm)
const nranks = MPI.Comm_size(comm)
# Block size proportional to ranks: ~nranks blocks in 2D => side blocks ≈ √nranks
const BLOCK = max(1, ceil(Int, N / ceil(Int, sqrt(nranks))))

if rank == 0
    println("Benchmark: ", nranks, " ranks, N=", N, ", block size ", BLOCK, "×", BLOCK, " (QR + matmul)")
end

# Allocate and fill 10k×10k matrix in 1k×1k blocks
A = rand(Blocks(BLOCK, BLOCK), Float64, N, N)
MPI.Barrier(comm)

# QR factorization (computing Q runs the full factorization)
t_qr = @elapsed begin
    qr!(A)
end
MPI.Barrier(comm)

if rank == 0
    println("QR time: ", round(t_qr; digits=4), " s")
end

# Matrix multiply A * A
t_matmul = @elapsed begin
    C = A * A
end
MPI.Barrier(comm)

if rank == 0
    println("Matmul time: ", round(t_matmul; digits=4), " s")
end

