# Owner-computes layer over MPI: Cholesky, LU, GEMM, a hopping chain and a failing kernel, each compared with the sequential result (rank 0 gathers).
include(joinpath(@__DIR__, "..", "owner_computes_defs.jl"))
@testset "Owner-computes layer over MPI ($(MPI.Comm_size(comm)) ranks)" begin
    owner_computes_checks(MPI.Comm_rank(comm), MPI.Comm_size(comm))
end
