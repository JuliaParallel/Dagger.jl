using Distributed
using Dates

#=
println("Standalone Dagger.jl Cholesky Test with Multiple Configurations")
base_system_info = Dict(
    "julia_version" => string(VERSION),
    "num_threads" => Threads.nthreads(),
    "hostname" => gethostname(),
    "cpu_info" => Sys.cpu_info()[1].model,
    "total_memory" => Sys.total_memory(),
    "timestamp" => Dates.now()
)
println("Base System Info:")
for (key, value) in base_system_info
    println("  $key: $value")
end
=#
all_results = []

#Define constants
addprocs(3)
datasize = [8, 16]
blocksize = 4
@everywhere using Dagger, LinearAlgebra, Random, Test, Logging
@everywhere disable_logging(LogLevel(2999))

#println("\nActive workers: $(nworkers()) (Total processes: $(nprocs()))")

for T in (Float32, Float64)
    #println("  Testing data type: $T")
    
    for N in datasize
        try
            A = rand(T, N, N)
            A = A * A' 
            A[diagind(A)] .+= size(A, 1)
            B = copy(A)
            @assert ishermitian(A)
            DA = distribute(A, Blocks(blocksize, blocksize))
            DB = distribute(B, Blocks(blocksize,blocksize))
            
            LinearAlgebra._chol!(DA, UpperTriangular)
            
            elapsed_time = @elapsed LinearAlgebra._chol!(DB, UpperTriangular)

            # Store results
            result = (
                procs = nprocs(),
                dtype = T,
                size = N,
                blocksize = blocksize,
                time = elapsed_time,
                gflops = 2 * N^3 / elapsed_time * 1e-9
            )
            push!(all_results, result)
            
            
        catch e
            #println("ERROR: $e")
        end
    end
    #println()
end

#= Clean up workers at the end
if nworkers() > 0
    rmprocs(workers())
end
=#
# Summary statistics
for result in all_results
    println(result.procs, ",", result.dtype, ",", result.size, ",", result.blocksize, ",", result.time, ",", result.gflops)
end
#println("\nAll Cholesky tests completed!")

