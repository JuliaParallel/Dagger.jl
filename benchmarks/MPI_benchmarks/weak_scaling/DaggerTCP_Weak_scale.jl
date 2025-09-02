using Distributed
using Dates

all_results = []

#Define constants
addprocs(1)
number_of_processes = [1, 4, 9, 16, 25, 36, 49, 64, 81]
for target_workers in number_of_processes
    current_workers = nworkers()
    if current_workers < target_workers
        addprocs(target_workers - current_workers)
    elseif current_workers > target_workers
        rmprocs(workers()[1:(current_workers - target_workers)])
    end
    #println("\n nprocs: $(nprocs()), nworkers: $(nworkers()) ")
    @everywhere using Dagger, LinearAlgebra, Random, Test, Logging
    @everywhere disable_logging(LogLevel(2999))

    #Define constants
    datatype = [Float32, Float64]
    datasize = 256 * floor(Int, sqrt(nworkers()))
    #blocksize = 4
    
    for T in datatype
        #println("  Testing data type: $T")
        
        #blocksize = div(datasize, 4)
        A = rand(T, datasize, datasize)
        A = A * A' 
        A[diagind(A)] .+= size(A, 1)
        B = copy(A)
        @assert ishermitian(B)
        DA = distribute(A, Blocks(256, 256))
        DB = distribute(B, Blocks(256,256))
        
        
        LinearAlgebra._chol!(DA, UpperTriangular)
        elapsed_time = @elapsed chol_DB = LinearAlgebra._chol!(DB, UpperTriangular)
        
        # Store results
        result = (
            procs = nworkers(),
            dtype = T,
            size = datasize,
            time = elapsed_time,
            gflops = (datasize^3 / 3) / (elapsed_time * 1e9)
        )
        push!(all_results, result)
                
    end
end

# Summary statistics
for result in all_results
    println(result.procs, ",", result.dtype, ",", result.size, ",", result.time, ",", result.gflops)
end
#println("\nAll Cholesky tests completed!")

