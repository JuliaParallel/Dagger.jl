using Dagger, LinearAlgebra

include("Tile.jl")
include("LAPACK/core_geqrt.jl")
include("LAPACK/core_tpqrt.jl")
include("LAPACK/core_tpmqrt.jl")
include("LAPACK/core_ormqr.jl")
include("ongqr.jl")

function geqrf!(::Type{T}, tilemat::TileMat, tileworkspace::TileWorkSpace, traversal::Symbol) where {T<: Number}

    mt = tilemat.mt
    nt = tilemat.nt
    
    Dagger.spawn_datadeps() do
        for k in range(1, min(mt, nt))
            Dagger.@spawn core_geqrt!(T, InOut(tilemat.mat[k, k]), Out(tileworkspace.mat[k, k]))
        for n in range(k+1, nt)
           Dagger.@spawn core_ormqr!(T, 'L', 'T', In(tilemat.mat[k, k]), In(tileworkspace.mat[k, k]), InOut(tilemat.mat[k, n]))
        end
        
        for m in range(k+1, mt)
            Dagger.@spawn core_tpqrt!(T, 0, InOut(tilemat.mat[k, k]), InOut(tilemat.mat[m, k]), Out(tileworkspace.mat[m, k]))
            
            for n in range(k+1, nt)
                Dagger.@spawn core_tpmqrt!(T, 'L', 'T', 0,  In(tilemat.mat[m, k]), In(tileworkspace.mat[m, k]), InOut(tilemat.mat[k, n]), InOut(tilemat.mat[m, n]))
            end
        end

    end
end
end

function main_geqrf(::Type{T}, m, n, nb, check::Int64; traversal::Symbol=:inorder) where {T<: Number}
	

    tileA = TileMat(m, n, nb, nb)
    generate_tiles!(T, tileA)

    tileworkspace = TileWorkSpace(m, n, 1, nb)
    init_workspace!(T, tileworkspace)
    
    if check == 1
        denseA = zeros(Float64, m, m)
        tile2lap!(tileA, denseA)
        #display(denseA)
    end
    
    BLAS.set_num_threads(1)
    println("Dagger:")
    @time geqrf!(T, tileA, tileworkspace, traversal)


    if check == 1
        Q = Matrix(1.0I, m, min(m,n))
        tileQ = TileMat(m, min(m,n), nb, nb)
        lap2tile!(tileQ, Q)
        ongqr!(T, tileA, tileworkspace, tileQ, traversal) 
        tile2lap!(tileQ, Q)

        factA = zeros(Float64, m, n)
        tile2lap!(tileA, factA)
        R = triu(factA)

        check=norm(denseA-Q*R)
        println(check)

        BLAS.set_num_threads(4)#Sys.CPU_THREADS)
        println("BLAS:")
        @time LAPACK.geqrf!(denseA)
        # for checking the accuracy we need to do to compute A - Q * R Later
    end
   
end
