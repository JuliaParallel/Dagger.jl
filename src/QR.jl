using Dagger, LinearAlgebra

include("Tile.jl")
include("LAPACK/geqrt.jl")
include("LAPACK/tpqrt.jl")
include("LAPACK/tpmqrt.jl")
include("LAPACK/ormqr.jl")


function geqrf!(::Type{T}, tilemat::TileMat, tileworkspace::TileWorkSpace, traversal::Symbol) where {T<: Number}

    mt = tilemat.mt
    nt = tilemat.nt
    
    Dagger.spawn_datadeps() do
        for k in range(1, min(mt, nt))
            Dagger.@spawn geqrt!(T, InOut(tilemat.mat[k, k]), Out(tileworkspace.mat[k, k]))
        for n in range(k+1, nt)
           Dagger.@spawn ormqr!(T, 'L', 'T', In(tilemat.mat[k, k]), In(tileworkspace.mat[k, k]), InOut(tilemat.mat[k, n]))
        end
        
        for m in range(k+1, mt)
            Dagger.@spawn tpqrt!(T, 0, InOut(tilemat.mat[k, k]), InOut(tilemat.mat[m, k]), Out(tileworkspace.mat[m, k]))
            
            for n in range(k+1, nt)
                Dagger.@spawn tpmqrt!(T, 'L', 'T', 0,  In(tilemat.mat[m, k]), In(tileworkspace.mat[m, k]), InOut(tilemat.mat[k, n]), InOut(tilemat.mat[m, n]))
            end
        end

    end
end
end

function main_geqrf(::Type{T}, m, nb, check::Int64; traversal::Symbol=:inorder) where {T<: Number}
	

    tileA = TileMat(m, m, nb, nb)
    generate_tiles!(T, tileA)

    tileworkspace = TileWorkSpace(m, m, 1, nb)
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
        factA = zeros(Float64, m, m)
        tile2lap!(tileA, factA)
        display(factA)
        BLAS.set_num_threads(10)#Sys.CPU_THREADS)
        println("BLAS:")
        #@time LAPACK.geqrt!(denseA, tau)
        @time LAPACK.geqrf!(denseA)
        display(denseA)
        # for checking the accuracy we need to do to compute A - Q * R Later
    end
   
end
