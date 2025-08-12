#!/usr/bin/env julia

# MPI Dagger.jl Cholesky Benchmark - FIXED VERSION
# Run with: mpirun -np 4 julia --project=. benchmarks/mpi_dagger_bench.jl

using Distributed
using Dagger
using MPI
using LinearAlgebra
using Random
using Statistics
using Printf
using Dates
using SparseArrays
import TaskLocalValues: TaskLocalValue
import Dagger: Acceleration, Chunk, Processor, Sch, AbstractScope, TaintScope, MemorySpace, @dagdebug
import MemPool: DRef
import MemPool

# ================================================================================
# MPI INITIALIZATION
# ================================================================================

# Initialize MPI properly BEFORE including mpi.jl
if !MPI.Initialized()
    MPI.Init(; threadlevel=:multiple)
end

comm = MPI.COMM_WORLD
rank = MPI.Comm_rank(comm)
comm_size = MPI.Comm_size(comm)

# ================================================================================
# TASK LOCAL VALUES FOR MPI
# ================================================================================

# Note: MPI_TID and MPI_UID are defined in Dagger's datadeps.jl as ScopedValue

# ================================================================================
# INCLUDE MPI.JL FROM DAGGER SOURCE
# ================================================================================

# Include the MPI module from Dagger source
mpi_path = joinpath(@__DIR__, "..", "src", "mpi.jl")
if !isfile(mpi_path)
    if rank == 0
        error("Cannot find mpi.jl at $mpi_path. Make sure you're running from Dagger.jl/benchmarks/")
    end
    MPI.Finalize()
    exit(1)
end

# Load the MPI module
include(mpi_path)

# ================================================================================
# INITIALIZE DAGGER WITH MPI ACCELERATION
# ================================================================================

# Create and initialize MPI acceleration PROPERLY
accel = Dagger.MPIAcceleration(comm)
Dagger.initialize_acceleration!(accel)

# ================================================================================
# CONFIGURATION CONSTANTS
# ================================================================================

const DATA_TYPES = [Float32, Float64]
const MATRIX_SIZES = [256, 512, 1024]  # Reduced for testing
const NUM_TRIALS = 2
const NUM_WARMUP = 1

# ================================================================================
# UTILITY FUNCTIONS
# ================================================================================

# Generate positive definite matrix
function generate_posdef_matrix(T::Type, n::Int)
    try
        Random.seed!(42)  # Same seed for all ranks for consistency
        A = randn(T, n, n)
        A = A * A'  # Make symmetric positive semi-definite
        A[diagind(A)] .+= T(n)  # Make positive definite
        return A
    catch e
        error("Failed to generate positive definite matrix: $e")
    end
end

# ================================================================================
# SIMPLE MPI CHOLESKY (WITHOUT DAGGER)
# ================================================================================

function simple_mpi_cholesky!(A::Matrix{T}) where T
    n = size(A, 1)
    
    # Simple column-cyclic distribution
    for j = 1:n
        # Owner of column j
        owner = (j - 1) % comm_size
        
        if owner == rank
            # Update and factorize column j
            if j > 1
                # Update column j based on previous columns
                for k = 1:(j-1)
                    A[j:n, j] -= A[j:n, k] * A[j, k]
                end
            end
            
            # Compute L[j,j] and scale column
            A[j, j] = sqrt(A[j, j])
            if j < n
                A[(j+1):n, j] /= A[j, j]
            end
        end
        
        # Broadcast column j from owner to all ranks
        col_data = owner == rank ? A[j:n, j] : zeros(T, n - j + 1)
        MPI.Bcast!(col_data, owner, comm)
        
        if owner != rank
            A[j:n, j] = col_data
        end
        
        MPI.Barrier(comm)
    end
    
    # Make lower triangular
    for i = 1:n, j = (i+1):n
        A[i, j] = zero(T)
    end
    
    return A
end

# ================================================================================
# DAGGER DISTRIBUTED CHOLESKY
# ================================================================================

function dagger_distributed_cholesky(A::Matrix{T}) where T
    n = size(A, 1)
    
    # Create a simple block distribution
    nblocks = min(comm_size, 4)  # Limit number of blocks
    block_size = cld(n, nblocks)
    
    # Create Dagger context with MPI processors
    ctx = Dagger.Context()
    for i in 0:(comm_size-1)
        proc = Dagger.MPIOSProc(comm, i)
        push!(ctx.procs, proc)
    end
    
    # Set up proper task IDs for MPI operations
    task_id = Dagger.eager_next_id()
    Dagger.with(Dagger.MPI_TID => task_id) do
    
    # Create distributed array using Dagger's Blocks
    dist = Dagger.Blocks(block_size, block_size)
    
    # Distribute the matrix (simplified version)
    # Each rank gets assigned chunks in round-robin fashion
    chunks = Matrix{Any}(undef, nblocks, nblocks)
    
    chunk_id = 0
    for bi = 1:nblocks, bj = 1:nblocks
        row_range = ((bi-1)*block_size + 1):min(bi*block_size, n)
        col_range = ((bj-1)*block_size + 1):min(bj*block_size, n)
        
        # Assign chunk to a rank
        assigned_rank = chunk_id % comm_size
        chunk_id += 1
        
        if assigned_rank == rank
            # This rank owns this chunk
            block_data = A[row_range, col_range]
            proc = Dagger.MPIOSProc(comm, rank)
            space = Dagger.MPIMemorySpace(Dagger.CPURAMMemorySpace(myid()), comm, rank)
            chunks[bi, bj] = Dagger.tochunk(block_data, proc, space)
        else
            # Create reference to remote chunk
            proc = Dagger.MPIOSProc(comm, assigned_rank)
            space = Dagger.MPIMemorySpace(Dagger.CPURAMMemorySpace(myid()), comm, assigned_rank)
            # Create empty chunk reference
            chunks[bi, bj] = Dagger.tochunk(nothing, proc, space; type=Matrix{T})
        end
    end
    
    MPI.Barrier(comm)
    
    # Perform Cholesky on the distributed array
    # For now, just collect and compute on rank 0
    if rank == 0
        # Collect all chunks
        A_collected = zeros(T, n, n)
        chunk_id = 0
        for bi = 1:nblocks, bj = 1:nblocks
            row_range = ((bi-1)*block_size + 1):min(bi*block_size, n)
            col_range = ((bj-1)*block_size + 1):min(bj*block_size, n)
            
            assigned_rank = chunk_id % comm_size
            chunk_id += 1
            
            if assigned_rank == rank
                # Local chunk
                A_collected[row_range, col_range] = fetch(chunks[bi, bj])
            else
                # Remote chunk - receive via MPI
                tag = Dagger.to_tag(hash(bi, hash(bj, hash(T))))
                chunk_data = Dagger.recv_yield(comm, assigned_rank, tag)
                A_collected[row_range, col_range] = chunk_data
            end
        end
        
        # Compute Cholesky
        result = cholesky(A_collected)
        return result
    else
        # Send chunks to rank 0
        chunk_id = 0
        for bi = 1:nblocks, bj = 1:nblocks
            assigned_rank = chunk_id % comm_size
            chunk_id += 1
            
            if assigned_rank == rank
                row_range = ((bi-1)*block_size + 1):min(bi*block_size, n)
                col_range = ((bj-1)*block_size + 1):min(bj*block_size, n)
                
                tag = Dagger.to_tag(hash(bi, hash(bj, hash(T))))
                Dagger.send_yield(A[row_range, col_range], comm, 0, tag)
            end
        end
        
        return nothing
    end
    end  # Close the with block
end

# ================================================================================
# BENCHMARK FUNCTIONS
# ================================================================================

function benchmark_simple_mpi(A::Matrix{T}) where T
    # Warmup
    for _ in 1:NUM_WARMUP
        A_copy = copy(A)
        simple_mpi_cholesky!(A_copy)
    end
    
    # Timing runs
    times = Float64[]
    for _ in 1:NUM_TRIALS
        A_copy = copy(A)
        t = @elapsed simple_mpi_cholesky!(A_copy)
        push!(times, t)
    end
    
    return mean(times), std(times)
end

function benchmark_dagger(A::Matrix{T}) where T
    # Warmup
    for _ in 1:NUM_WARMUP
        dagger_distributed_cholesky(copy(A))
    end
    
    # Timing runs
    times = Float64[]
    for _ in 1:NUM_TRIALS
        t = @elapsed dagger_distributed_cholesky(copy(A))
        push!(times, t)
    end
    
    return mean(times), std(times)
end

# ================================================================================
# MAIN BENCHMARK FUNCTION
# ================================================================================

function main()
    all_results = []
    
    # Ensure proper cleanup on exit
    atexit() do
        if MPI.Initialized() && !MPI.Finalized()
            MPI.Finalize()
        end
    end
    
    if rank == 0
        println("=" ^ 80)
        println("MPI Dagger.jl Cholesky Benchmark - FIXED VERSION")
        println("=" ^ 80)
        println("Timestamp: $(Dates.now())")
        println("MPI Ranks: $(comm_size)")
        println("Julia Version: $(VERSION)")
        println("Matrix Sizes: $(MATRIX_SIZES)")
        println("Data Types: $(DATA_TYPES)")
        println("-" ^ 80)
    end
    
    for T in DATA_TYPES
        if rank == 0
            println("\n📐 Testing data type: $T")
            println("-" ^ 40)
        end
        
        for N in MATRIX_SIZES
            # Check memory
            try
                mem_required_gb = (N * N * sizeof(T) * 4) / (1024^3)
                total_mem_gb = Sys.total_memory() / (1024^3)
                
                if mem_required_gb > total_mem_gb * 0.5
                    if rank == 0
                        println("  ⏭️  Skipping N=$N (needs $(round(mem_required_gb, digits=1)) GB)")
                    end
                    continue
                end
            catch e
                if rank == 0
                    println("  ⚠️  Memory check failed, proceeding anyway: $e")
                end
            end
            
            if rank == 0
                print("  Size $(N)×$(N): ")
                flush(stdout)
            end
            
            try
                # Generate test matrix (same on all ranks)
                A = generate_posdef_matrix(T, N)
                
                # Benchmark standard sequential (rank 0 only)
                std_time = 0.0
                if rank == 0
                    std_time = @elapsed cholesky(copy(A))
                end
                std_time = MPI.bcast(std_time, 0, comm)
                
                # Benchmark simple MPI version
                mpi_mean, mpi_std = benchmark_simple_mpi(A)
                
                # Benchmark Dagger version
                dagger_mean = 0.0
                dagger_std = 0.0
                try
                    dagger_mean, dagger_std = benchmark_dagger(A)
                catch e
                    if rank == 0
                        println("\n      Dagger failed: $(sprint(showerror, e))")
                    end
                    dagger_mean = NaN
                    dagger_std = NaN
                end
                
                # Calculate metrics
                gflops_std = (N^3 / 3) / (std_time * 1e9)
                gflops_mpi = (N^3 / 3) / (mpi_mean * 1e9)
                gflops_dagger = isnan(dagger_mean) ? NaN : (N^3 / 3) / (dagger_mean * 1e9)
                speedup_mpi = std_time / mpi_mean
                speedup_dagger = isnan(dagger_mean) ? NaN : std_time / dagger_mean
                
                # Store results
                result = (
                    rank = rank,
                    dtype = T,
                    size = N,
                    std_time = std_time,
                    mpi_time = mpi_mean,
                    mpi_std = mpi_std,
                    dagger_time = dagger_mean,
                    dagger_std = dagger_std,
                    gflops_std = gflops_std,
                    gflops_mpi = gflops_mpi,
                    gflops_dagger = gflops_dagger,
                    speedup_mpi = speedup_mpi,
                    speedup_dagger = speedup_dagger
                )
                push!(all_results, result)
                
                if rank == 0
                    println("✓")
                    @printf("      Standard: %.4fs (%.2f GFLOPS)\n", std_time, gflops_std)
                    @printf("      MPI:      %.4fs (%.2f GFLOPS, %.2fx speedup)\n", 
                            mpi_mean, gflops_mpi, speedup_mpi)
                    if !isnan(dagger_mean)
                        @printf("      Dagger:   %.4fs (%.2f GFLOPS, %.2fx speedup)\n",
                                dagger_mean, gflops_dagger, speedup_dagger)
                    else
                        println("      Dagger:   Failed")
                    end
                end
                
            catch e
                if rank == 0
                    println("✗ ERROR: $(sprint(showerror, e))")
                end
            end
            
            MPI.Barrier(comm)  # Synchronize between tests
        end
    end
    
    # Display summary on rank 0
    if rank == 0 && !isempty(all_results)
        println("\n" * "=" ^ 80)
        println("SUMMARY RESULTS")
        println("=" ^ 80)
        
        println("\n┌────────┬──────────┬────────────┬────────────┬──────────┬──────────┐")
        println("│  Type  │   Size   │  MPI (s)   │ Dagger (s) │ MPI-GF   │ DAG-GF   │")
        println("├────────┼──────────┼────────────┼────────────┼──────────┼──────────┤")
        
        for r in all_results
            @printf("│ %-6s │  %6d  │   %.4f   │",
                    r.dtype == Float32 ? "F32" : "F64",
                    r.size,
                    r.mpi_time)
            
            if !isnan(r.dagger_time)
                @printf("   %.4f   │  %6.2f  │  %6.2f  │\n",
                        r.dagger_time,
                        r.gflops_mpi,
                        r.gflops_dagger)
            else
                @printf("     N/A    │  %6.2f  │    N/A   │\n",
                        r.gflops_mpi)
            end
        end
        println("└────────┴──────────┴────────────┴────────────┴──────────┴──────────┘")
        
        # Performance summary
        println("\n📊 Performance Summary:")
        valid_results = filter(r -> !isnan(r.speedup_mpi), all_results)
        if !isempty(valid_results)
            best_mpi = argmax(r -> r.gflops_mpi, valid_results)
            println("  • Best MPI: $(best_mpi.dtype) @ $(best_mpi.size)×$(best_mpi.size) → $(round(best_mpi.gflops_mpi, digits=2)) GFLOPS")
            
            # Average efficiency
            avg_eff_mpi = mean(r -> r.speedup_mpi / comm_size * 100, valid_results)
            println("  • Average MPI Parallel Efficiency: $(round(avg_eff_mpi, digits=1))%")
        end
        
        println("\n📝 System Information:")
        try
            println("  • Hostname: $(gethostname())")
        catch e
            println("  • Hostname: Unknown (error: $e)")
        end
        println("  • CPU Threads: $(Threads.nthreads())")
        try
            println("  • Total Memory: $(round(Sys.total_memory() / 2^30, digits=2)) GB")
        catch e
            println("  • Total Memory: Unknown (error: $e)")
        end
    end
    
    return all_results
end

# ================================================================================
# EXECUTION
# ================================================================================

# Run benchmark
results = try
    main()
catch e
    if rank == 0
        println("\n❌ Fatal error: $e")
        println(sprint(showerror, e))
        for (exc, bt) in Base.catch_stack()
            showerror(stdout, exc, bt)
            println()
        end
    end
    []
finally
    try
        MPI.Barrier(comm)
    catch e
        if rank == 0
            println("Warning: MPI.Barrier failed during cleanup: $e")
        end
    end
end

if rank == 0
    println("\n✅ MPI Benchmark completed!")
    println("=" ^ 80)
end

# Finalize MPI
try
    if MPI.Initialized() && !MPI.Finalized()
        MPI.Finalize()
    end
catch e
    if rank == 0
        println("Warning: MPI.Finalize failed: $e")
    end
end