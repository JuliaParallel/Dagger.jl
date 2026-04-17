using Dagger
using CUDA

function validate()
    if !CUDA.has_cuda()
        @warn "No CUDA device found, skipping test suite"
        exit(0)
    end
    gpu_scope = Dagger.scope(cuda_gpu=1)

    Dagger.with_options(; scope=gpu_scope) do 
        A = rand(Blocks(32, 32), 512, 512)
        B = rand(Blocks(32, 32), 512, 512)
        result = collect(A * B)  # force materialization
        nothing
    end
    GC.gc(true)
    CUDA.reclaim()
end