using Dagger, LinearAlgebra

include("Tile.jl")

function potrf!(::Type{T}, uplo::Char, tilemat::TileMat, traversal::Symbol) where {T<: Number}
    #TODO check how to obtain info
    mt = tilemat.mt
    nt = tilemat.nt

    if (tilemat.m != tilemat.n)
        throw(ArgumentError("Number of rows not equal number of columns, matrix must be square"))
    end

    if (uplo !='L' &&  (uplo !='U'))
        throw(ArgumentError("illegal value of uplo"))
    end

    zone = 1.0
    mzone = -1.0

    # FIXME: @sync implementation not prepared for unlaunched tasks
    #=
        Lower Cholesky
    =#    
    if (uplo=='L')
        Dagger.spawn_datadeps(;static=true,traversal) do
            for k in range(1, mt) #only for upper
                Dagger.@spawn LAPACK.potrf!(uplo, InOut(tilemat.mat[k, k]))
                for m in range(k+1, mt)
                    Dagger.@spawn BLAS.trsm!('R', uplo, 'T', 'N', zone, In(tilemat.mat[k, k]), InOut(tilemat.mat[m, k]))
                end
                for n in range(k+1, nt)
                    Dagger.@spawn BLAS.syrk!(uplo, 'N', mzone, In(tilemat.mat[n, k]), zone, InOut(tilemat.mat[n, n]))
                    for m in range(n+1, mt)
                        Dagger.@spawn BLAS.gemm!('N', 'T', mzone, In(tilemat.mat[m, k]), In(tilemat.mat[n, k]), zone, InOut(tilemat.mat[m, n]))
                    end
                end
            end
        end # Dagger.spawn_datadeps
    else 
    #=
        Upper Cholesky
    =# 
        Dagger.spawn_datadeps(;static=true,traversal) do
            for k in range(1, mt) #only for upper
                Dagger.@spawn LAPACK.potrf!(uplo, InOut(tilemat.mat[k, k]))
                for n in range(k+1, nt)
                    Dagger.@spawn BLAS.trsm!('L', uplo, 'T', 'N', zone, In(tilemat.mat[k, k]), InOut(tilemat.mat[k, n]))
                end
                for m in range(k+1, mt)
                    Dagger.@spawn BLAS.syrk!(uplo, 'T', mzone, In(tilemat.mat[k, m]), zone, InOut(tilemat.mat[m, m]))
                    for n in range(m+1, nt)
                        Dagger.@spawn BLAS.gemm!('T', 'N', mzone, In(tilemat.mat[k, m]), In(tilemat.mat[k, n]), zone, InOut(tilemat.mat[m, n]))
                    end
                end
            end
        end # Dagger.spawn_datadeps

    
    end
    
end

function tile_potrf_view!(::Type{T}, uplo::Char, mat::Matrix{Float64}, nb::Int64, traversal::Symbol) where {T<: Number}
    #TODO check how to obtain info
    m, n= size(mat)
    mt = Int64(m/nb)
    nt = Int64(n/nb)
    zone = 1.0
    mzone = -1.0
    Dagger.spawn_datadeps(;static=false,traversal) do 
        for k in 1:nb:m #only for upper
        #tile = 
        #view(mat, k:(k+nb-1), k:(k+nb-1))
        #display(tile)
        #tile = Base.unsafe_convert(Ptr{Float64}, view(mat, 1:3, 1:3))
        #println("(k,k) ", k, " ", k, " ", (k+nb-1))
            #tileA= view(mat, k:(k+nb-1), k:(k+nb-1))
            Dagger.@spawn LAPACK.potrf!(uplo, InOut(view(mat, k:(k+nb-1), k:(k+nb-1))))

         for m in k+nb:nb:m
            #println("(m,k) ", m, " ", k)
                #tileA = view(mat, k:(k+nb)-1, k:(k+nb)-1)
                #tileB = view(mat, m:(m+nb)-1, k:(k+nb)-1)
                Dagger.@spawn BLAS.trsm!('R', 'L', 'T', 'N', zone, In(view(mat, k:(k+nb)-1, k:(k+nb)-1)), 
                InOut(view(mat, m:(m+nb)-1, k:(k+nb)-1)))
            #display(view(mat, m:(m+nb)-1, k:(k+nb)-1))
            end 
        
            for n in k+nb:nb:m 
                #tileA = view(mat, n:(n+nb)-1, k:(k+nb)-1)
                #tileB = view(mat, n:(n+nb)-1, n:(n+nb)-1)
                Dagger.@spawn BLAS.syrk!(uplo, 'N', mzone, In(view(mat, n:(n+nb)-1, k:(k+nb)-1)), zone, 
                InOut(view(mat, n:(n+nb)-1, n:(n+nb)-1)))
                for m in  n+nb:nb:m
                    #tileA = view(mat, m:(m+nb)-1, k:(k+nb)-1)
                    #tileB = view(mat, n:(n+nb)-1, k:(k+nb)-1)
                    #tileC = view(mat, m:(m+nb)-1, n:(n+nb)-1)
                    Dagger.@spawn BLAS.gemm!('N', 'T', mzone, In(view(mat, m:(m+nb)-1, k:(k+nb)-1)), 
                        In(view(mat, n:(n+nb)-1, k:(k+nb)-1)), zone, InOut(view(mat, m:(m+nb)-1, n:(n+nb)-1)))
                end
            end
        end
    end
end

function main_potrf(::Type{T}, m, nb, uplo::Char, check::Int64; traversal::Symbol=:inorder) where {T<: Number}
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
    @time potrf!(T, uplo, tileA, traversal)
    #@time tile_potrf_view!(T, uplo, denseA, nb, traversal)

    if check == 1
        factA =  zeros(Float64, m, m)
        tile2lap!(tileA, factA)

        BLAS.set_num_threads(3)#Sys.CPU_THREADS)
        println("BLAS:")
        @time LAPACK.potrf!(uplo, denseA)
        BLAS.set_num_threads(1)
        Rnorm = norm(denseA - factA, 2)
        println(Rnorm)
        #tilecheck(tileA, denseA)
    end
    
    #gflops =  (((1.0 / 3.0) * m * m * m) / ((duration) * 1.0e+9));
    #println("tile_potrf_cpu_threads!, ", m, ", ", nb, ", ", duration, ", ", gflops)
   
end
