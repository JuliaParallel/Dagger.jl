using Revise
using Dagger
using FFTW
using AbstractFFTs
using LinearAlgebra
using Profile
#using Plots, DataFrames
#using GraphViz

function demo()
    Dagger.MemPool.MEM_RESERVED[] = 0
    N = 50
    A = rand(ComplexF64, N, N)
    B = copy(A)
    @time fft!(A)
    ENV["JULIA_DEBUG"] = ""
    push!(Dagger.DAGDEBUG_CATEGORIES, :spawn_datadeps)
    GC.enable(false)
    @time Dagger.fft!(B)
    GC.enable(true)
    C = copy(B)
    Profile.clear()
    Profile.@profile Dagger.fft!(C)
    return A ≈ B
end

demo()