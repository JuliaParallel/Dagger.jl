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

const N = 10_000
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
            C_dagger_g = CUDA.cu(C_dagger)
        end
        println("Collect + upload time: ", round(t_collect + t_upload; digits=4), " s")

        t_baseline = @elapsed begin
            C_ref_g = A_g * B_g
        end
        println("Baseline (GPU/CUDA) time: ", round(t_baseline; digits=4), " s")

        rtol = 1f-5
        atol = 1f-6
        err = norm(C_dagger_g - C_ref_g)
        ref_norm = norm(C_ref_g)
        rel_err = ref_norm > 0 ? err / ref_norm : err
        ok = err <= atol + rtol * ref_norm
        if ok
            println("Correctness: OK (rel_err = ", Float32(rel_err), ", abs_err = ", Float32(err), ")")
        else
            println("Correctness: FAIL (rel_err = ", Float32(rel_err), ", abs_err = ", Float32(err), ", rtol=$rtol, atol=$atol)")
        end

        # Per-block analysis: which sections exceed tolerance (same block size as Dagger layout)
        C_dagger_cpu = Array(C_dagger_g)
        C_ref_cpu = Array(C_ref_g)
        n_bi = ceil(Int, N / BLOCK)
        n_bj = ceil(Int, N / BLOCK)
        bad_blocks = Tuple{Int,Int,Float32,Float32}[]
        for bi in 1:n_bi, bj in 1:n_bj
            ri = (bi - 1) * BLOCK + 1 : min(bi * BLOCK, N)
            rj = (bj - 1) * BLOCK + 1 : min(bj * BLOCK, N)
            diff_block = @view(C_dagger_cpu[ri, rj]) .- @view(C_ref_cpu[ri, rj])
            ref_block = @view(C_ref_cpu[ri, rj])
            block_err = norm(diff_block)
            block_ref = norm(ref_block)
            block_rel = block_ref > 0 ? block_err / block_ref : block_err
            if block_err > atol + rtol * block_ref
                push!(bad_blocks, (bi, bj, Float32(block_rel), Float32(block_err)))
            end
        end
        if isempty(bad_blocks)
            println("Per-block: all ", n_bi * n_bj, " blocks within tolerance.")
        else
            println("Per-block: ", length(bad_blocks), " block(s) exceed tolerance (block size ", BLOCK, "×", BLOCK, "):")
            sort!(bad_blocks; by = x -> -x[3])
            for (bi, bj, brel, babs) in bad_blocks
                println("  block [", bi, ",", bj, "] rows ", (bi - 1) * BLOCK + 1, ":", min(bi * BLOCK, N),
                        ", cols ", (bj - 1) * BLOCK + 1, ":", min(bj * BLOCK, N), "  rel_err = ", brel, "  abs_err = ", babs)
            end
        end
    end
end
