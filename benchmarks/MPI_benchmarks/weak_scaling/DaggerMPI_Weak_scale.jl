using Dagger, MPI, LinearAlgebra
using CSV, DataFrames, Logging
disable_logging(LogLevel(2999))

a = Dagger.accelerate!(:mpi)
comm = a.comm
rank = MPI.Comm_rank(comm)
sz = MPI.Comm_size(comm)

mpidagger_all_results = []

# Define constants
# You need to define the MPI workers before running the benchmark
# Example: mpirun -n 4 julia --project benchmarks/DaggerMPI_Weak_scale.jl
datatype = [Float32, Float64]
datasize = 256 * floor(Int, sqrt(sz))

for T in datatype
    A = rand(T, datasize, datasize)
    A = A * A' 
    A[diagind(A)] .+= size(A, 1)
    B = copy(A)
    @assert ishermitian(B)
    DA = distribute(A, Blocks(256,256))
    DB = distribute(B, Blocks(256,256))

    try
        LinearAlgebra._chol!(DA, UpperTriangular)
    catch e
        if rank == 0 && T == Float64
            Base.showerror(stderr, e, stacktrace(catch_backtrace()))
        end
    end
    elapsed_time = @elapsed chol_DB = LinearAlgebra._chol!(DB, UpperTriangular)
    
    # Store results
    result = (
        procs = sz,
        dtype = T,
        size = datasize,
        time = elapsed_time,
        gflops = (datasize^3 / 3) / (elapsed_time * 1e9)
    )
    push!(mpidagger_all_results, result)   
    

end

if rank == 0
    #= Write results to CSV
    mkpath("benchmarks/results")
    if !isempty(mpidagger_all_results)
        df = DataFrame(mpidagger_all_results)
        CSV.write("benchmarks/results/DaggerMPI_Weak_scale_results.csv", df)
        
    end
    # Summary statistics
    =#
    for result in mpidagger_all_results
        println(result.procs, ",", result.dtype, ",", result.size, ",", result.time, ",", result.gflops)
    end
    #println("\nAll Cholesky tests completed!")
end

