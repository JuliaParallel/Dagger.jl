using Test
using MPIRPC

@testset "MPIRPC" begin
    include("protocol_tests.jl")
    include("mpi_tests.jl")
end
