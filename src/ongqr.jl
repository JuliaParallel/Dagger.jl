using Dagger, LinearAlgebra

include("Tile.jl")
include("LAPACK/core_geqrt.jl")
include("LAPACK/core_tpqrt.jl")
include("LAPACK/core_tpmqrt.jl")
include("LAPACK/core_ormqr.jl")

"""

ungqr - Parallel construction of Q using tile V (application to identity)
     Generates an M-by-N matrix Q with orthonormal columns, which
     is defined as the first N columns of a product of the elementary reflectors
     returned by geqrf.
     
"""

function ongqr!(::Type{T},tilematA::TileMat, tileworkspace::TileWorkSpace, tilematQ::TileMat, traversal::Symbol) where {T<: Number}

    mt = tilematA.mt
    nt = tilematA.nt
    ib = tileworkspace.ib

    Dagger.spawn_datadeps() do
        for k in min(tilematA.mt, tilematA.nt):-1:1
            for m in tilematQ.mt:-1:k+1
                for n in range(k, tilematQ.nt)
                    Dagger.@spawn core_tpmqrt!(T, 'L', 'N', 0,  In(tilematA.mat[m, k]), 
                    In(tileworkspace.mat[m, k]), InOut(tilematQ.mat[k, n]), InOut(tilematQ.mat[m, n]))
                end
            end
            for n in range(k, tilematQ.nt)
                Dagger.@spawn core_ormqr!(T, 'L', 'N', In(tilematA.mat[k, k]), 
                In(tileworkspace.mat[k, k]), InOut(tilematQ.mat[k, n]))
            end

        end
    end
    
end