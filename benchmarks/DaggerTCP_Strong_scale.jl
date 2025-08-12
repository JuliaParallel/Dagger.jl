using Distributed
using Dates

using Logging
disable_logging(LogLevel(2999))

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

# Define constants
DaggerTCP_results = []
number_of_processes = [16, 32, 64]
data_size = [8192]
blocksize = 64

addprocs(1)
for target_procs in number_of_processes
    println("TESTING WITH $target_procs PROCESSES")
    # Add only missing workers
    needed_workers = target_procs - 1
    current_workers = nworkers()
    if current_workers < needed_workers
        addprocs(needed_workers - current_workers)
    end
    @everywhere using Dagger, LinearAlgebra, Random, Test
    
    println()
    println("Active workers: $(nworkers()) (Total processes: $(nprocs()))")
    
    for T in (Float32, Float64)
        
        for N in data_size
            println("  Testing data type: $T, size: $N")
            
            try
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
                #@show chol_DA isa Cholesky
                #@show chol_A.L ≈ chol_DA.L
                #@show chol_A.U ≈ chol_DA.U
                #@show UpperTriangular(collect(DA)) ≈ UpperTriangular(collect(A))
                
                # Store results
                result = (
                    procs = nprocs(),
                    dtype = T,
                    size = N,
                    blocksize = "$(blocksize) x $(blocksize)",
                    time = elapsed_time,
                    gflops = (N^3 / 3) / (elapsed_time * 1e9)
                )
                push!(DaggerTCP_results, result)
                
                
            catch e
                println("ERROR: $e")
            end
        end
        println()
    end
    println()
end
# Clean up workers at the end
if nworkers() > 0
    rmprocs(workers())
end
# Summary statistics
for result in DaggerTCP_results
    println(result.procs, " ", result.dtype, " ", result.size, " ", result.blocksize, " ", result.time)
end
println("\nAll Cholesky tests completed!")