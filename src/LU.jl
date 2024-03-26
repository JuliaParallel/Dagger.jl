using Dagger, LinearAlgebra

include("Tile.jl")

function getrf_nopiv!(::Type{T}, tilemat::TileMat, traversal::Symbol) where {T<: Number}
    #TODO check how to obtain info
    mt = tilemat.mt
    nt = tilemat.nt

    if (tilemat.m != tilemat.n)
        throw(ArgumentError("Number of rows not equal number of columns, matrix must be square"))
    end


    zone = 1.0
    mzone = -1.0
    
    Dagger.spawn_datadeps(;static=true,traversal) do 

        for k in range(1, min(tilemat.mt, tilemat.nt)) #only for upper
            Dagger.@spawn LAPACK.getrf!(InOut(tilemat.mat[k, k]))
            for m in range(k+1, mt)
                Dagger.@spawn BLAS.trsm!('R', 'U', 'N', 'N', zone, In(tilemat.mat[k, k]), InOut(tilemat.mat[m, k]))
            end
            for n in range(k+1, nt)
                Dagger.@spawn BLAS.trsm!('L', 'L', 'N', 'U', zone, In(tilemat.mat[k, k]), InOut(tilemat.mat[k, n]))
                for m in range(k+1, mt)
                    Dagger.@spawn BLAS.gemm!('N', 'N', mzone, In(tilemat.mat[m, k]), In(tilemat.mat[k, n]), zone, InOut(tilemat.mat[m, n]))
                end
            end
        end
    end
end

function main_getrf(::Type{T}, m, nb, check::Int64; traversal::Symbol=:inorder) where {T<: Number}
    tileA = TileMat(m, m, nb, nb)
    generate_tiles!(T, tileA)
    diagposv!(Float64, tileA, m)
    
    if check == 1
        denseA = zeros(Float64, m, m)
        tile2lap!(tileA, denseA)
        #display(denseA)
    end
    
    BLAS.set_num_threads(1)
    println("Dagger:")
    @time getrf_nopiv!(T, tileA, traversal)

    if check == 1
        factA = zeros(Float64, m, m)
        tile2lap!(tileA, factA)
        #display(factA)
    end
    info = 0
    if check == 1
        BLAS.set_num_threads(3)#Sys.CPU_THREADS)
        println("BLAS:")
        @time LAPACK.getrf!(denseA)
        BLAS.set_num_threads(1)
        Rnorm = norm(denseA - factA, 2)
        println(Rnorm)
        tilecheck(tileA, denseA)
    end
    
    #gflops =  (((1.0 / 3.0) * m * m * m) / ((duration) * 1.0e+9));
    #println("tile_potrf_cpu_threads!, ", m, ", ", nb, ", ", duration, ", ", gflops)
   
end
