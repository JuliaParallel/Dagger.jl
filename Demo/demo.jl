using Revise
using Dagger
using FFTW
using AbstractFFTs
using LinearAlgebra
using Profile
using Plots, DataFrames
using GraphViz

function enable_dagdebug()
    ENV["JULIA_DEBUG"] = "Dagger"
    empty!(Dagger.DAGDEBUG_CATEGORIES)
    push!(Dagger.DAGDEBUG_CATEGORIES, :spawn_datadeps)
end
function disable_dagdebug()
    ENV["JULIA_DEBUG"] = ""
end
function demo()
    Dagger.MemPool.MEM_RESERVED[] = 0

    N = 3000
    valid = true

    # Benchmark
    println("Benchmarking FFTW")
    input_fftw = rand(ComplexF64, N, N)
    input_dagger = copy(input_fftw)
    input_dagger_inner = copy(input_fftw)
    FFTW.set_num_threads(Threads.nthreads())
    @assert FFTW.get_num_threads() == Threads.nthreads()
    @time fft!(input_fftw)
    @assert input_fftw ≈ fft(input_dagger)

    FFTW.set_num_threads(1)

    #=
    println("Benchmarking Dagger")
    GC.enable(false)
    @time Dagger.fft!(input_dagger)
    GC.enable(true)
    valid = input_fftw ≈ input_dagger
    @show norm(input_fftw - input_dagger)
    =#

    #==#
    println("Benchmarking Dagger (inner)")
    input_dagger_inner_copy = copy(input_dagger_inner)
    A, B = Dagger._fft_prealloc(Dagger.Pencil(), input_dagger_inner)
    wait.(A.chunks)
    wait.(B.chunks)
    GC.enable(false)
    copyto!(A, input_dagger_inner)
    @time Dagger._fft!(Dagger.Pencil(), A, B; dims=(1, 2))
    copyto!(input_dagger_inner, B)
    GC.enable(true)
    valid = input_fftw ≈ input_dagger_inner
    #==#

    #=
    println("Profiling")
    input_dagger_profiling = rand(ComplexF64, N, N)
    GC.enable(false)
    Profile.@profile @time Dagger.fft!(input_dagger_profiling)
    GC.enable(true)
    VSCodeServer.view_profile()
    =#

    #==#
    println("Profiling (inner)")
    input_dagger_profiling_inner = rand(ComplexF64, N, N)
    A, B = Dagger._fft_prealloc(Dagger.Pencil(), input_dagger_profiling_inner)
    wait.(A.chunks)
    wait.(B.chunks)
    GC.enable(false)
    copyto!(A, input_dagger_profiling_inner)
    Profile.@profile @time Dagger._fft!(Dagger.Pencil(), A, B; dims=(1, 2))
    copyto!(input_dagger_profiling_inner, B)
    GC.enable(true)
    VSCodeServer.view_profile()
    #==#

    #=
    println("Plotting")
    input_dagger_plotting = rand(ComplexF64, N, N)
    Dagger.enable_logging!(; metrics=false, all_task_deps=true)
    GC.enable(false)
    @time Dagger.fft!(input_dagger_plotting)
    GC.enable(true)
    logs = Dagger.fetch_logs!()
    Dagger.disable_logging!()
    display(Dagger.render_logs(logs, :plots_gantt; target=:execution, color_init_hash=UInt(1)))
    =#

    #==#
    println("Plotting (inner)")
    input_dagger_plotting_inner = rand(ComplexF64, N, N)
    A, B = Dagger._fft_prealloc(Dagger.Pencil(), input_dagger_plotting_inner)
    copyto!(A, input_dagger_plotting_inner)
    wait.(A.chunks)
    wait.(B.chunks)
    Dagger.enable_logging!(; metrics=false, all_task_deps=true)
    GC.enable(false)
    @time Dagger._fft!(Dagger.Pencil(), A, B; dims=(1, 2))
    GC.enable(true)
    logs = Dagger.fetch_logs!()
    Dagger.disable_logging!()
    display(Dagger.render_logs(logs, :plots_gantt; target=:execution, color_init_hash=UInt(1)))
    display(Dagger.render_logs(logs, :plots_gantt; target=:scheduler, color_init_hash=UInt(1)))
    #==#

    return valid
end

demo()