# Incremental / one-shot sparse assembly from COO.
#
# Shared body lives in `test/array/sparse_defs.jl` so MPI and GPU entry points
# run the same cases. Run with:
#
#     julia test/runtests.jl --test array/linalg/assembly

include(joinpath(@__DIR__, "..", "sparse_defs.jl"))

@testset "Sparse assembly" begin
    @testset "T=$T" for T in (Float64, Float32)
        test_sparse_assembly(; T)
        test_sparse_collect(; T)
    end
end
