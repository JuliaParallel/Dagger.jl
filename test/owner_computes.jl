include("owner_computes_defs.jl")
@testset "Owner-computes layer (one process)" begin
    owner_computes_checks(0, 1)
end
