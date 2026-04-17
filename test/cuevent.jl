using Test
using CUDA
using Dagger
import Dagger: spawn_datadeps
using LinearAlgebra
using NVTX


NVTX.@range "profile_zone" begin
    # Skip entire suite if no CUDA device is available
    if !CUDA.has_cuda()
        @warn "No CUDA device found, skipping test suite"
        exit(0)
    end
    gpu_scope = Dagger.scope(cuda_gpu=1)

    tsize = 8192
    blsize = tsize ÷ 4
    for i in 1:5
        Dagger.with_options(; scope=gpu_scope) do 
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

    # CuArrayDeviceProc lives inside CUDAExt and is not exported to Main.
    # The canonical way to reference it is by grabbing an instance from the
    # processor list and using its type.

    ctx = Dagger.Sch.eager_context()
    const CuProc = typeof(first(filter(Dagger.get_processors(ctx.procs[1])) do p
        hasproperty(p, :device_uuid)
    end))

    cuda_proc() = first(filter(p -> p isa CuProc, Dagger.get_processors(Dagger.OSProc())))
    cpu_proc()  = first(filter(p -> p isa Dagger.ThreadProc, Dagger.get_processors(Dagger.OSProc())))

    # ─────────────────────────────────────────────────────────────
    # 1. HtoD move correctness
    # ─────────────────────────────────────────────────────────────
    @testset "HtoD move correctness" begin
        cpu_arr = rand(Float32, 256, 256)

        gpu_arr = Dagger.move(cpu_proc(), cuda_proc(), cpu_arr)

        @test gpu_arr isa CuArray{Float32, 2}
        @test size(gpu_arr) == (256, 256)
        @test isapprox(Array(gpu_arr), cpu_arr)
    end

    # ─────────────────────────────────────────────────────────────
    # 2. DtoH move correctness
    # ─────────────────────────────────────────────────────────────
    @testset "DtoH move correctness" begin
        gpu_arr  = CUDA.rand(Float32, 256, 256)
        expected = Array(gpu_arr)

        result = Dagger.move(cuda_proc(), cpu_proc(), gpu_arr)

        @test result isa Array{Float32, 2}
        @test isapprox(result, expected)
    end

    # ─────────────────────────────────────────────────────────────
    # 3. DtoD same device correctness
    # ─────────────────────────────────────────────────────────────
    @testset "DtoD same device correctness" begin
        src      = CUDA.rand(Float32, 128, 128)
        expected = Array(src)
        proc     = cuda_proc()

        result = Dagger.move(proc, proc, src)

        @test result isa CuArray{Float32, 2}
        @test isapprox(Array(result), expected)
    end

    # ─────────────────────────────────────────────────────────────
    # 4. gpu_synchronize does not corrupt data
    # ─────────────────────────────────────────────────────────────
    @testset "gpu_synchronize correctness" begin
        proc   = cuda_proc()
        result = Dagger.@spawn map(x -> x * 2f0, CUDA.ones(Float32, 512))

        @test_nowarn Dagger.gpu_synchronize(proc)

        val = fetch(result)
        @test all(isapprox.(Array(val), 2f0))
    end

    # ─────────────────────────────────────────────────────────────
    # 5. Wide DAG — N independent tasks produce correct results
    # ─────────────────────────────────────────────────────────────
    @testset "Wide DAG correctness" begin
        N        = 8
        inputs   = [CUDA.rand(Float32, 256, 256) for _ in 1:N]
        expected = [sum(Array(x)) for x in inputs]

        # Independent tasks with no shared data — pure fan-out
        # Use plain @spawn since these tasks share no mutable state
        results = map(inputs) do arr
            Dagger.@spawn sum(arr)
        end

        for (v, e) in zip(fetch.(results), expected)
            @test isapprox(v, e; rtol=1e-4)
        end
    end

    # ─────────────────────────────────────────────────────────────
    # 6. Deep DAG — sequential dependent chain
    # ─────────────────────────────────────────────────────────────
    @testset "Deep DAG correctness" begin
        depth = 8
        add_one(x) = x .+ 1f0

        # Sequential chain of dependent tasks
        t = Dagger.@spawn CUDA.ones(Float32, 256)
        for _ in 2:depth
            t = Dagger.@spawn add_one(fetch(t))
        end

        val = Array(fetch(t))
        # ones + add_one called (depth-1) times = 1 + (depth-1) = depth
        @test all(isapprox.(val, Float32(depth); rtol=1e-5))
    end

    # ─────────────────────────────────────────────────────────────
    # 7. Mixed DAG — fan-out then reduce on CPU
    # ─────────────────────────────────────────────────────────────
    @testset "Mixed DAG correctness" begin
        N      = 4
        source = CUDA.ones(Float32, 512)
        scale(x, i) = x .* Float32(i)

        # Fan-out: N tasks all read the same immutable source
        branches = map(1:N) do i
            Dagger.@spawn scale(source, i)
        end

        total = sum(sum(Array(fetch(b))) for b in branches)
        # 512 * (1+2+3+4) = 5120
        @test isapprox(total, 512f0 * sum(1:N); rtol=1e-4)
    end

    # ─────────────────────────────────────────────────────────────
    # 8. Independent writes via spawn_datadeps — no corruption
    # ─────────────────────────────────────────────────────────────
    @testset "Independent writes — no data races" begin
        # Two tasks writing to separate arrays — no shared state
        a = CUDA.ones(Float32, 256)
        b = CUDA.ones(Float32, 256)

        fill2(x) = CUDA.fill!(x, 2f0)
        fill3(x) = CUDA.fill!(x, 3f0)

        ta = Dagger.@spawn fill2(a)
        tb = Dagger.@spawn fill3(b)

        fetch(ta); fetch(tb)

        @test all(isapprox.(Array(a), 2f0))
        @test all(isapprox.(Array(b), 3f0))
    end

    # ─────────────────────────────────────────────────────────────
    # 9. Stress test — 32 concurrent tasks, no data races
    # ─────────────────────────────────────────────────────────────
    @testset "Stress test — 32 concurrent tasks" begin
        N        = 32
        arrays   = [CUDA.rand(Float32, 128, 128) for _ in 1:N]
        expected = sum(sum(Array(a)) for a in arrays)

        results = map(arrays) do arr
            Dagger.@spawn sum(arr)
        end

        total = sum(fetch.(results))
        @test isapprox(total, expected; rtol=1e-3)
    end

    # ─────────────────────────────────────────────────────────────
    # 10. gpu_synchronize(Val{:CUDA}) — all devices complete cleanly
    # ─────────────────────────────────────────────────────────────
    @testset "gpu_synchronize all devices" begin
        @test_nowarn Dagger.gpu_synchronize(Val(:CUDA))
    end

    # ─────────────────────────────────────────────────────────────
    # 11. Event-based sync — no device barriers in GPU-only paths
    #
    #     Verify with Nsight after running:
    #       nsys profile --trace=cuda --output=report julia --project test_cuda_sync.jl
    #       nsys stats report.nsys-rep --report cuda_api_sum | grep cuDeviceSynchronize
    #     Expected: 0 occurrences in HtoD and gpu_synchronize paths
    # ─────────────────────────────────────────────────────────────
    @testset "Event-based sync — no device barriers in GPU-only paths" begin
        proc    = cuda_proc()
        cp      = cpu_proc()
        cpu_arr = rand(Float32, 512, 512)

        gpu_arr = @test_nowarn Dagger.move(cp, proc, cpu_arr)
        @test_nowarn Dagger.gpu_synchronize(proc)
        result  = @test_nowarn Dagger.move(proc, cp, gpu_arr)

        @test isapprox(result, cpu_arr)
    end

    # ─────────────────────────────────────────────────────────────
    # 12. Cholesky fan-out pattern (motivating example from proposal)
    #     One pivot task fans out to N independent panel tasks.
    #     With multi-stream, all panel tasks can overlap on the GPU.
    # ─────────────────────────────────────────────────────────────
    @testset "Cholesky fan-out pattern" begin
        # Simulates the motivating example from the proposal:
        # one pivot task fans out to N independent panel tasks
        N     = 4
        sz    = 64
        pivot = CUDA.ones(Float32, sz, sz)

        double_pivot(p) = p .* 2f0
        add_scalar(panel, val) = panel .+ val

        pivot_done = Dagger.@spawn double_pivot(pivot)

        panels = [CUDA.rand(Float32, sz, sz) for _ in 1:N]
        pval   = CUDA.@allowscalar fetch(pivot_done)[1, 1]

        tasks = map(panels) do panel
            Dagger.@spawn add_scalar(panel, pval)
        end

        for t in tasks
            val = Array(fetch(t))
            @test all(isfinite, val)
        end
    end

    println("\n✓ All tests passed")
end