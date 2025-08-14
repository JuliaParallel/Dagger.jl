begin
using Revise
using Dagger
using LinearAlgebra

using Profile
include("filter-traces.jl")
end

function demo(pivot=RowMaximum())
    fetch(Dagger.@spawn 1+1)

    N = 2000
    nt = Threads.nthreads()
    #bs = cld(N, np)
    bs = div(N, 4)
    println("OpenBLAS Initialization:")
    GC.enable(false)
    A = @time rand(N, N)
    GC.enable(true)
    println("Dagger Initialization:")
    GC.enable(false)
    @time begin
        DA = DArray(A, Blocks(bs, bs))
        wait.(DA.chunks)
    end
    GC.enable(true)

    println("OpenBLAS:")
    BLAS.set_num_threads(nt)
    lu_A = @time lu(A, pivot; check=false)
    println("Dagger:")
    BLAS.set_num_threads(1)
    GC.enable(false)
    lu_DA = @time lu(DA, pivot; check=false)
    GC.enable(true)

    Profile.@profile 1+1
    Profile.clear()
    println("Dagger (profiler):")
    GC.enable(false)
    Profile.@profile @time lu(DA, pivot; check=false)
    GC.enable(true)

    @show norm(lu_A.U - UpperTriangular(collect(lu_DA.factors)))

    return
end

demo();

begin
    samples, lidata = Profile.retrieve()
    validate_and_filter_traces!(samples, lidata)
end