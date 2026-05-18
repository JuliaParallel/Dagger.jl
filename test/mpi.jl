using Dagger, MPI, LinearAlgebra

Dagger.accelerate!(:mpi)
Dagger.check_uniformity!(true)
comm = MPI.COMM_WORLD
rank = MPI.Comm_rank(comm)
sz = MPI.Comm_size(comm)

mpidagger_all_results = []

# Define constants
# You need to define the MPI workers before running the benchmark
# Example: mpirun -n 4 julia --project benchmarks/DaggerMPI_Weak_scale.jl
datatype = [Float32, Float64]
datasize = 40
try
    for T in datatype
        A = rand(T, datasize, datasize)
        A = A * A'
        A[diagind(A)] .+= size(A, 1)
        B = copy(A)
        @assert ishermitian(B)
        DA = zeros(Blocks(20,20), T, datasize, datasize)
        for chunk in DA.chunks
            Dagger.check_uniform(fetch(chunk; move_value=false, unwrap=false).space)
        end
        copyto!(DA, A)
        DB = zeros(Blocks(20,20), T, datasize, datasize)
        for chunk in DB.chunks
            Dagger.check_uniform(fetch(chunk; move_value=false, unwrap=false).space)
        end
        copyto!(DB, B)
        LinearAlgebra._chol!(DA, UpperTriangular)
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
catch
    if rank == 0
        Core.print("Rank 0:\n")
        rethrow()
    elseif rank == 1
        Core.print("Rank 1:\n")
        sleep(1)
        rethrow()
    end
finally
    MPI.Barrier(comm)
end
if rank == 0
    #= Write results to CSV
    mkpath("benchmarks/results")
    if !isempty(mpidagger_all_results)
        df = DataFrame(mpidagger_all_results)
        CSV.write("benchmarks/results/DaggerMPI_Weak_scale_results.csv", df)

    end
    =#
    # Summary statistics
    for result in mpidagger_all_results
        println(result.procs, ",", result.dtype, ",", result.size, ",", result.time, ",", result.gflops)
    end
    #println("\nAll Cholesky tests completed!")
end
