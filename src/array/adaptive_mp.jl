function tile_precision(uplo, global_norm, scalar_factore, tolerance, A)
    tile_sqr = 0.0

    if uplo == 'G'
        tile_sqr = mapreduce(LinearAlgebra.norm_sqr, +, A)
    elseif  uplo == 'L'
        tile_sqr= mapreduce(LinearAlgebra.norm_sqr, +, LowerTriangular(A))
    elseif  uplo == 'U'
        tile_sqr= mapreduce(LinearAlgebra.norm_sqr, +, UpperTriangular(A))
    end
    tile_norm = sqrt(tile_sqr)
    
    cal = tile_norm * scalar_factore / global_norm 
    decision_hp = tile_norm * scalar_factore / global_norm  < tolerance / eps(Float16); 
    decision_sp = tile_norm * scalar_factore / global_norm < tolerance / eps(Float32); 
    decision_fp8 = tile_norm * scalar_factore / global_norm  < tolerance / 0.0625; 

    if decision_fp8
        return "FP8"
    elseif decision_hp
        return "FP16"
    elseif decision_sp
        return "FP32"
    else
        return "FP64"
    end
end

function adaptive_mp!(A::UpperTriangular{T,<:DArray{T,2}}, MP::UpperTriangular{String,<:DArray{String,2}}, tolerance::Float64) where T

    Ac = parent(A).chunks
    MPc= parent(MP).chunks
    mt, nt = size(Ac)

    global_norm = LinearAlgebra.norm2(A)

    for m in range(1, mt)
        for n in range(m, nt)
            if m==n
                MP[m, n] = Dagger.@spawn tile_precision('U', global_norm, max(mt, nt), tolerance, Ac[m, n])
            else
                MP[m, n] = Dagger.@spawn tile_precision('G', global_norm, max(mt, nt), tolerance, Ac[m, n])
            end
    
        end
    end
    return UpperTriangular(MP)
end

function adaptive_mp!(A::LowerTriangular{T,<:DArray{T,2}}, MP::LowerTriangular{String,<:DArray{String,2}}, tolerance::Float64) where T

    Ac = parent(A).chunks
    MPc= parent(MP).chunks
    mt, nt = size(Ac)

    global_norm = LinearAlgebra.norm2(A)

    for m in range(1, mt)
        for n in range(1, m)
            if m==n
                MP[m, n] = Dagger.@spawn tile_precision('L', global_norm, max(mt, nt), tolerance, Ac[m, n])
            else
                MP[m, n] = Dagger.@spawn tile_precision('G', global_norm, max(mt, nt), tolerance, Ac[m, n])
            end
    
        end
    end
    return LowerTriangular(MP)
end


function adaptive_mp!(A::DArray{T,2}, MP::DArray{String,2}, tolerance::Float64) where T

    Ac = parent(A).chunks
    MPc= parent(MP).chunks
    mt, nt = size(Ac)

    global_norm = LinearAlgebra.norm2(A)

    for m in range(1, mt)
        for n in range(1, nt)
            MP[m, n] = Dagger.@spawn tile_precision('G', global_norm, max(mt, nt), tolerance, Ac[m, n])
        end
    end

    return MP
end

