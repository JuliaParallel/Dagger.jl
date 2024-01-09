using Revise
using BenchmarkTools
using Dagger

empty!(Dagger.DAGDEBUG_CATEGORIES); push!(Dagger.DAGDEBUG_CATEGORIES, :spawn_datadeps)
using LinearAlgebra; BLAS.set_num_threads(1)
#=
addprocs(1)

@everywhere begin
    try using CUDA
    catch end

    using Distributed, Dagger, DaggerGPU
    using KernelAbstractions
end
=#

include("QR.jl")