using Dagger, LinearAlgebra, Test, CUDA
using Dagger: Blocks
using NVTX


NVTX.@range "profile_zone" begin
    # Skip entire suite if no CUDA device is available
    if !CUDA.has_cuda()
        @warn "No CUDA device found, skipping test suite"
        exit(0)
    end

    gpu_scope1 = Dagger.scope(cuda_gpu=1)
    #gpu_scope2 = Dagger.scope(cuda_gpu=2)
    #gpu_scope_multi = Dagger.scope(cuda_gpus=:)

    function matrix()
        tsize = 8192
        blsize = tsize ÷ 4
        for i in 1:5
            Dagger.with_options(; scope=gpu_scope1) do 
                A = rand(Blocks(blsize, blsize), tsize, tsize)
                B = rand(Blocks(blsize, blsize), tsize, tsize)
                collect(A)
                collect(B)
                result = collect(A * B)  # force materialization
                @assert collect(A) * collect(B) ≈ result
                nothing
            end
            GC.gc(true)
            CUDA.reclaim()
        end
        println("matrix mult pass")
    end
    
    function irregular_matrix()
        for i in 1:5
            println(i)
            tsize = 1440
            Dagger.with_options(; scope=gpu_scope1) do 
                A = rand(Blocks(160, 9), tsize, tsize)
                B = rand(Blocks(160, 9), tsize, tsize)
                collect(A)
                collect(B)
                result = collect(A * B)  # force materialization
                @assert collect(A) * collect(B) ≈ result
                nothing
            end
            GC.gc(true)
            CUDA.reclaim()
        end
        println("irregular matrix mult pass")
    end
    #matrix()

    #irregular_matrix()

    CUDA.allowscalar(true) #
    GC.gc(true); CUDA.reclaim()

    Dagger.with_options(; scope= gpu_scope1) do
        println("RTX 5060TI + RTX3060")
        #println("\n-----------------------cholesky-----------------------\n")
        #include("array/linalg/cholesky.jl")
        GC.gc(true); CUDA.reclaim()
        #println("\n-----------------------lu-----------------------\n")
        #include("array/linalg/lu.jl")
        GC.gc(true); CUDA.reclaim()
        println("\n-----------------------matmul-----------------------\n")
        include("array/linalg/matmul.jl")
        GC.gc(true); CUDA.reclaim()
    end


end