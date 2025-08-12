using Dagger, MPI, LinearAlgebra
using CSV, DataFrames

Dagger.accelerate!(:mpi)
comm = MPI.COMM_WORLD
rank = MPI.Comm_rank(comm)
sz = MPI.Comm_size(comm)

mpidagger_all_results = []

# Define constants
# You need to define the MPI workers before running the benchmark
# Example: mpirun -n 4 julia --project benchmarks/DaggerMPI_Strong_scale.jl
datatype = [Float32, Float64]
datasize = [128]
blocksize = 4

for T in datatype
    println("  Testing data type: $T")
    
    for N in datasize
        A = rand(T, N, N)
        A = A * A' 
        A[diagind(A)] .+= size(A, 1)
        B = copy(A)
        @assert ishermitian(B)
        DA = distribute(A, Blocks(blocksize,blocksize))
        DB = distribute(B, Blocks(blocksize,blocksize))
      
        
        LinearAlgebra._chol!(DA, UpperTriangular)
        elapsed_time = @elapsed chol_DB = LinearAlgebra._chol!(DB, UpperTriangular)
        

        
        # Verify results
        #@show chol_DB

        #@assert chol_DA isa Cholesky
        #@assert chol_DB isa UpperTriangular
        #@assert chol_A.L ≈ chol_DA.L
        #@assert chol_A.U ≈ chol_DA.U
        #@assert UpperTriangular(collect(DB)) ≈ UpperTriangular(collect(chol_DB))
    
        # Store results
        result = (
            procs = sz,
            dtype = T,
            size = N,
            blocksize = "$(blocksize) x $(blocksize)",
            time = elapsed_time,
            gflops = (N^3 / 3) / (elapsed_time * 1e9)
        )
        push!(mpidagger_all_results, result)   
        
    end
    println()
end

# Write results to CSV
if !isempty(mpidagger_all_results)
    df = DataFrame(mpidagger_all_results)
    CSV.write("benchmarks/results/DaggerMPI_Weak_scale_results.csv", df)
    println("Results written to benchmarks/results/DaggerMPI_Weak_scale_results.csv")
end

# Summary statistics
for result in mpidagger_all_results
    println(result.procs, " ", result.dtype, " ", result.size, " ", result.blocksize, " ", result.time, " ", result.gflops)
end
println("\nAll Cholesky tests completed!")

