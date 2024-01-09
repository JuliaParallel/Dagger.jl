using Dagger, LinearAlgebra

include("Tile.jl")
include("LAPACK/geqrt.jl")

function geqrf!(::Type{T}, tilemat::TileMat, tileworkspace::TileWorkSpace, traversal::Symbol) where {T<: Number}

    mt = tilemat.mt
    nt = tilemat.nt


    # I need to provide Julia implmenation of tsqrt, and tsmqr
    # tsqrt is used to compute QR for  rectangular matrix 
    # tsmqr is used to  applies the reflectors to two tiles

    _, tileworkspace.mat[1,1] = geqrt!(tilemat.mat[1,1], tileworkspace.mat[1,1])
    
end

function main_geqrf(::Type{T}, m, nb, ib, check::Int64; traversal::Symbol=:inorder) where {T<: Number}
    tileA = TileMat(m, m, nb, nb)
    generate_tiles!(T, tileA)
    tileworkspace = TileWorkSpace(m, m, ib, nb)
    init_workspace!(T, tileworkspace)
    #diagposv!(Float64, tileA, m)
    
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
        rows = tileworkspace.m
        cols = tileworkspace.n
        tau = zeros(Float64, rows, cols)
        tile2lap!(tileA, factA)
        tile2lap!(tileworkspace, tau)
        #display(factA)
    end

    if check == 1
        BLAS.set_num_threads(3)#Sys.CPU_THREADS)
        println("BLAS:")
        @time LAPACK.geqrf!(denseA)
        BLAS.set_num_threads(1)
        Rnorm = norm(denseA - factA, 2)
        println(Rnorm)
        tilecheck(tileA, denseA)
    end
    
    #gflops =  (((1.0 / 3.0) * m * m * m) / ((duration) * 1.0e+9));
    #println("tile_potrf_cpu_threads!, ", m, ", ", nb, ", ", duration, ", ", gflops)
   
end
