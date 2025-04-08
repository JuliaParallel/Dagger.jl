using Revise
using Dagger
using FFTW
using AbstractFFTs
using LinearAlgebra
using Profile
using Plots, DataFrames
using GraphViz

function demo()
    Dagger.MemPool.MEM_RESERVED[] = 0

    N = 3000

    # Benchmark
    println("Benchmarking FFTW")
    input_fftw = rand(ComplexF64, N, N)
    input_dagger = copy(input_fftw)
    input_dagger_inner = copy(input_fftw)
    FFTW.set_num_threads(Threads.nthreads())
    @assert FFTW.get_num_threads() == Threads.nthreads()
    @time fft!(input_fftw)

    ENV["JULIA_DEBUG"] = ""
    push!(Dagger.DAGDEBUG_CATEGORIES, :spawn_datadeps)
    FFTW.set_num_threads(1)

    println("Benchmarking Dagger")
    GC.enable(false)
    @time Dagger.fft!(input_dagger)
    GC.enable(true)
    valid = input_fftw ≈ input_dagger

    println("Benchmarking Dagger (inner)")
    A, B = Dagger._fft_prealloc(Dagger.Pencil(), input_dagger_inner)
    copyto!(A, input_dagger_inner)
    wait.(A.chunks)
    wait.(B.chunks)
    GC.enable(false)
    @time Dagger._fft!(Dagger.Pencil(), A, B; dims=(1, 2))
    GC.enable(true)

    #= Profiling
    println("Profiling")
    input_dagger_profiling = rand(ComplexF64, N, N)
    GC.enable(false)
    @profview @time Dagger.fft!(input_dagger_profiling)
    GC.enable(true)
    =#

    # Profiling (inner)
    println("Profiling (inner)")
    input_dagger_profiling_inner = rand(ComplexF64, N, N)
    A, B = Dagger._fft_prealloc(Dagger.Pencil(), input_dagger_profiling_inner)
    copyto!(A, input_dagger_profiling_inner)
    wait.(A.chunks)
    wait.(B.chunks)
    GC.enable(false)
    @profview @time Dagger._fft!(Dagger.Pencil(), A, B; dims=(1, 2))
    GC.enable(true)

    # Plotting
    println("Plotting (inner)")
    input_dagger_plotting = rand(ComplexF64, N, N)
    A, B = Dagger._fft_prealloc(Dagger.Pencil(), input_dagger_plotting)
    copyto!(A, input_dagger_plotting)
    wait.(A.chunks)
    wait.(B.chunks)
    Dagger.enable_logging!(; metrics=false, all_task_deps=true)
    GC.enable(false)
    @time Dagger._fft!(Dagger.Pencil(), A, B; dims=(1, 2))
    GC.enable(true)
    logs = Dagger.fetch_logs!()
    Dagger.disable_logging!()
    display(Dagger.render_logs(logs, :plots_gantt; target=:execution))

    return valid
end

demo()