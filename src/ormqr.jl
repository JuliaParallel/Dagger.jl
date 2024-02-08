using Dagger, LinearAlgebra

include("Tile.jl")
include("LAPACK/core_geqrt.jl")
include("LAPACK/core_tpqrt.jl")
include("LAPACK/core_tpmqrt.jl")
include("LAPACK/core_ormqr.jl")

#=

  dormqr - Overwrites the general complex M-by-N matrix C with

                  SIDE = 'L'     SIDE = 'R'
  TRANS = 'N':      Q * C          C * Q
  TRANS = 'C':      Q**T * C       C * Q**T

  where Q is a complex unitary matrix defined as the product of k
  elementary reflectors

        Q = H(1) H(2) . . . H(k)

  as returned by geqrf. Q is of order M if SIDE = 'L'
  and of order N if SIDE = 'R'.
     
=#

function ormqr!(::Type{T}, side::Char, trans::Char, tilematA::TileMat, tileworkspace::TileWorkSpace, tilematC::TileMat, traversal::Symbol) where {T<: Number}

    mt = tilematA.mt
    nt = tilematA.nt
    ib = tileworkspace.ib

    if (side == 'L')
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