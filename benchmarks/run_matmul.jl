#!/usr/bin/env julia
# N×N matmul benchmark (Float32); block size scales with number of ranks.
# Usage (use the full path to Dagger.jl, not "..."):
#   mpiexec -n 10 julia --project=/home/felipetome/dagger-dev/mpi/Dagger.jl benchmarks/run_matmul.jl
# Set CHECK_CORRECTNESS=true to collect and compare against GPU baseline:
#   CHECK_CORRECTNESS=true mpiexec -n 10 julia --project=/home/felipetome/dagger-dev/mpi/Dagger.jl benchmarks/run_matmul.jl

using MPI
using Dagger
using LinearAlgebra

if !isdefined(Dagger, :accelerate!)
    error("Dagger.accelerate! not found. Run with the local Dagger project: julia --project=/path/to/Dagger.jl ...")
end
Dagger.accelerate!(:mpi)

const N = 2_000
const comm = MPI.COMM_WORLD
const rank = MPI.Comm_rank(comm)
const nranks = MPI.Comm_size(comm)
# Block size proportional to ranks: ~nranks blocks in 2D => side blocks ≈ √nranks
const BLOCK = max(1, ceil(Int, N / ceil(Int, sqrt(nranks))))

const CHECK_CORRECTNESS = parse(Bool, get(ENV, "CHECK_CORRECTNESS", "false"))

if rank == 0
    println("Benchmark: ", nranks, " ranks, N=", N, ", block size ", BLOCK, "×", BLOCK, " (matmul)")
end

# Allocate and fill matrices in blocks (Float32)
A = rand(Blocks(BLOCK, BLOCK), Float32, N, N)
B = rand(Blocks(BLOCK, BLOCK), Float32, N, N)

# Matrix multiply C = A * B
t_matmul = @elapsed begin
    C = A * B
end

if rank == 0
    println("Matmul time: ", round(t_matmul; digits=4), " s")
end

# Optional: collect via datadeps (root=0). All ranks participate in the datadeps region.
if CHECK_CORRECTNESS
    t_collect = @elapsed begin
        A_full = Dagger.collect_datadeps(A; root=0)
        B_full = Dagger.collect_datadeps(B; root=0)
        C_dagger = Dagger.collect_datadeps(C; root=0)
    end
    if rank == 0
        println("Collecting result and computing baseline for correctness check (GPU)...")
        using CUDA
        CUDA.functional() || error("CUDA not functional; cannot compute GPU baseline. Check CUDA driver and device.")
        t_upload = @elapsed begin
            A_g = CUDA.cu(A_full)
            B_g = CUDA.cu(B_full)
        end
        println("Collect + upload time: ", round(t_collect + t_upload; digits=4), " s")

        t_baseline = @elapsed begin
            C_ref_g = A_g * B_g
        end
        println("Baseline (GPU/CUDA) time: ", round(t_baseline; digits=4), " s")

        # Require all elements within 100× machine epsilon relative error (componentwise)
        C_dagger_cpu = C_dagger
        C_ref_cpu = Array(C_ref_g)
        eps_f = eps(Float32)
        rtol = 50.0f0 * eps_f
        diff = C_dagger_cpu .- C_ref_cpu
        # rel_ij = |diff|/|C_ref|, denominator at least eps to avoid div by zero
        denom = max.(abs.(C_ref_cpu), eps_f)
        rel_err = abs.(diff) ./ denom
        max_rel_err = Float32(maximum(rel_err))
        ok = max_rel_err <= rtol
        if ok
            println("Correctness: OK (max rel_err = ", max_rel_err, " <= 100×eps = ", rtol, ")")
        else
            println("Correctness: FAIL (max rel_err = ", max_rel_err, " > 100×eps = ", rtol, ")")
        end

        # Per-block: which blocks have any element with rel_err > 100×eps
        n_bi = ceil(Int, N / BLOCK)
        n_bj = ceil(Int, N / BLOCK)
        bad_blocks = Tuple{Int,Int,Float32}[]
        for bi in 1:n_bi, bj in 1:n_bj
            ri = (bi - 1) * BLOCK + 1 : min(bi * BLOCK, N)
            rj = (bj - 1) * BLOCK + 1 : min(bj * BLOCK, N)
            block_rel = Float32(maximum(@view(rel_err[ri, rj])))
            if block_rel > rtol
                push!(bad_blocks, (bi, bj, block_rel))
            end
        end
        if isempty(bad_blocks)
            println("Per-block: all ", n_bi * n_bj, " blocks within 100×eps rel_err.")
        else
            println("Per-block: ", length(bad_blocks), " block(s) exceed 100×eps rel_err (block size ", BLOCK, "×", BLOCK, "):")
            sort!(bad_blocks; by = x -> -x[3])
            for (bi, bj, block_rel) in bad_blocks
                println("  block [", bi, ",", bj, "] rows ", (bi - 1) * BLOCK + 1, ":", min(bi * BLOCK, N),
                        ", cols ", (bj - 1) * BLOCK + 1, ":", min(bj * BLOCK, N), "  max rel_err = ", block_rel)
            end
        end
    end
end
