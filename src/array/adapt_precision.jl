"""
    tile_precision(uplo, global_norm, scalar_factor, tolerance, A)
    
it receives tile and it compute required precision per tile

### Input
- `A` -- tile of size m x n 
- `global_norm` -- global norm of the whole matrix
- `scalar_factor` -- scale tile by this value which is the number of tiles
- `tolerance` -- user defined tolerance as required aby the application

### Output
The required precision of the tile

"""
function tile_precision(A, global_norm, scalar_factor, tolerance)

    tile_sqr = mapreduce(LinearAlgebra.norm_sqr, +, A)

    tile_norm = sqrt(tile_sqr)

    cal = tile_norm * scalar_factor / global_norm
    decision_hp = tile_norm * scalar_factor / global_norm < tolerance / eps(Float16)
    decision_sp = tile_norm * scalar_factor / global_norm < tolerance / eps(Float32)

    #We are planning in near future to support fp8  E4M3 and E5M2 
    #decision_fp8 = tile_norm * scalar_factor / global_norm < tolerance / 0.0625
    #if decision_fp8
    #    return Float8
    if decision_hp
        return Float16
    elseif decision_sp
        return Float32
    else
        return Float64
    end
end

"""
    function adapt_precision( A::UpperTriangular{T,<:DArray{T,2}}, 
        MP::UpperTriangular{String,<:DArray{String,2}}, tolerance::Float64) where {T}
    
it iterates over all tiles and calculates the required precision per tile based on formulation from Nicholas J. Higham

### Input
- `A` -- Dagger UpperTriangular array of tiles with real values
- `MP` -- Dagger UpperTriangular array to associate precision with each tile 
- `tolerance` -- User defined tolerance as required aby the application

### Output
The Dagger array shows the required precision of each tile

"""

function adapt_precision(A::UpperTriangular{T,<:DArray{T,2}}, tolerance::Float64) where {T}

    Ac = parent(A).chunks
    mt, nt = size(Ac)

    global_norm = LinearAlgebra.norm2(A)

    MP = fill(T, mt, nt)
    DMP = view(MP, Blocks(1, 1))
    MPc = parent(DMP).chunks

    for n in range(1, nt)
        for m in range(1, n)
            if m == n
                MPc[m, n] = Dagger.@spawn tile_precision(
                    UpperTriangular(Ac[m, n]),
                    global_norm,
                    max(mt, nt),
                    tolerance)
            else
                MPc[m, n] = Dagger.@spawn tile_precision(
                    Ac[m, n],
                    global_norm,
                    max(mt, nt),
                    tolerance)
            end

        end
    end

    return UpperTriangular(collect(DMP))
end

"""
    adapt_precision( A::LowerTriangular{T,<:DArray{T,2}}, 
        MP::LowerTriangular{String,<:DArray{String,2}}, tolerance::Float64) where {T}
    
it iterates over all tiles and calculates the required precision per tile based on formulation from Nicholas J. Higham

### Input
- `A` -- Dagger LowerTriangular array of tiles with real values
- `MP` -- Dagger LowerTriangular array to associate precision with each tile 
- `tolerance` -- User defined tolerance as required aby the application

### Output
The Dagger array shows the required precision of each tile

"""

function adapt_precision(A::LowerTriangular{T,<:DArray{T,2}}, tolerance::T) where {T}

    Ac = parent(A).chunks
    mt, nt = size(Ac)

    global_norm = LinearAlgebra.norm2(A)

    MP = fill(T, mt, nt)
    DMP = view(MP, Blocks(1, 1))
    MPc = parent(DMP).chunks


    for m in range(1, mt)
        for n in range(1, m)
            if m == n
                MPc[m, n] = Dagger.@spawn tile_precision(
                    LowerTriangular(Ac[m, n]),
                    global_norm,
                    max(mt, nt),
                    tolerance)
            else
                MPc[m, n] = Dagger.@spawn tile_precision(
                    Ac[m, n],
                    global_norm,
                    max(mt, nt),
                    tolerance)
            end

        end
    end

    return LowerTriangular(collect(DMP))
end

"""
    adapt_precision(A::DArray{T,2}, MP::DArray{String,2}, tolerance::T) where {T}
    
it iterates over all tiles and calculates the required precision per tile based on formulation from Nicholas J. Higham

### Input
- `A` -- Dagger array of tiles with real values
- `MP` -- Dagger array to associate precision with each tile 
- `tolerance` -- User defined tolerance as required aby the application

### Output
The Dagger array shows the required precision of each tile

"""

function adapt_precision(A::DArray{T,2}, tolerance::T) where {T}

    Ac = parent(A).chunks
    mt, nt = size(Ac)

    global_norm = LinearAlgebra.norm2(A)

    MP = fill(T, mt, nt)
    DMP = view(MP, Blocks(1, 1))
    MPc = DMP.chunks


    for m in range(1, mt)
        for n in range(1, nt)
            if m!=n
                MPc[m, n] =
                    Dagger.@spawn tile_precision(
                        Ac[m, n],
                        global_norm, 
                        max(mt, nt), 
                        tolerance)
            end
        end
    end

    return collect(DMP)
end


function tile_precision_and_convert(A, MP, global_norm, scalar_factor, tolerance)

    tile_sqr = mapreduce(LinearAlgebra.norm_sqr, +, A)

    tile_norm = sqrt(tile_sqr)

    cal = tile_norm * scalar_factor / global_norm
    decision_hp = tile_norm * scalar_factor / global_norm < tolerance / eps(Float16)
    decision_sp = tile_norm * scalar_factor / global_norm < tolerance / eps(Float32)

    #We are planning in near future to support fp8  E4M3 and E5M2 
    #decision_fp8 = tile_norm * scalar_factor / global_norm < tolerance / 0.0625
    #if decision_fp8
    #    return Float8
    if decision_hp
        return Float16
    elseif decision_sp
        return Float32
    else
        return Float64
    end
end


function adapt_precision_and_convert(A::DArray{T,2}, tolerance::T) where {T}

    Ac = parent(A).chunks
    mt, nt = size(Ac)

    global_norm = LinearAlgebra.norm2(A)

    MP = fill(T, mt, nt)
    DMP = view(MP, Blocks(1, 1))
    MPc = DMP.chunks


    for m in range(1, mt)
        for n in range(1, nt)
                Dagger.@spawn tile_precision(
                    InOut(Ac[m, n]),
                    Out(MPc[m, n]),
                    global_norm, 
                    max(mt, nt), 
                    tolerance)
        end
    end

    return collect(DMP)
end
