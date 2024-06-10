export LowerTriangular, UpperTriangular, tril, triu, tril!, triu!

function _GenericTriangular(f::Union{Function, UndefInitializer}, p::Blocks{2}, eltype::Type, compar::Function, wrap, dims)
    @assert dims[1] == dims[2] "matrix is not square: dimensions are $dims (try using trapezoidal)"
    d = ArrayDomain(map(x->1:x, dims))
    dc = partition(p, d)
    if f isa UndefInitializer
        f = (eltype, x...) -> Array{eltype}(undef, x...)
    end
    diag = [(i,i) for i in 1:min(size(d)...)]
    thunks = []
    alloc(sz) = f(eltype, sz)
    transform = (wrap == LowerTrapezoidal) ? tril! : triu!
    compar = (wrap == LowerTrapezoidal) ? (>) : (<)
    for c in dc
        sz = size(c)
        if any(x -> x[1] in c.indexes[1] && x[2] in c.indexes[2], diag)
            push!(thunks, _DiagBuild(c.indexes, alloc(sz), diag, transform))
        else
            if compar(first(c.indexes[1]), first(c.indexes[2]))
                push!(thunks, Dagger.@spawn alloc(sz))
            else
                push!(thunks, Dagger.@spawn Matrix{Nothing}(nothing, sz))
            end
        end
    end
    thunks = reshape(thunks, size(dc))
    return wrap(Dagger.DArray(eltype, d, dc, thunks, p))
end

LinearAlgebra.LowerTriangular(f::Union{Function, UndefInitializer}, p::Blocks{2}, dims::Integer...) = _GenericTriangular(f, p, eltype, LowerTriangular, dims)
LinearAlgebra.LowerTriangular(f::Union{Function, UndefInitializer}, p::Blocks{2}, dims::Tuple) = _GenericTriangular(f, p, eltype, LowerTriangular, dims)
LinearAlgebra.LowerTriangular(f::Union{Function, UndefInitializer}, p::Blocks{2}, eltype::Type, dims::Integer...) = _GenericTriangular(f, p, eltype, LowerTriangular, dims)
LinearAlgebra.LowerTriangular(f::Union{Function, UndefInitializer}, p::Blocks{2}, eltype::Type, dims::Tuple) = _GenericTriangular(f, p, eltype, LowerTriangular, dims)

LinearAlgebra.UpperTriangular(f::Union{Function, UndefInitializer}, p::Blocks{2}, dims::Integer...) = _GenericTriangular(f, p, eltype, UpperTriangular, dims)
LinearAlgebra.UpperTriangular(f::Union{Function, UndefInitializer}, p::Blocks{2}, dims::Tuple) = _GenericTriangular(f, p, eltype, UpperTriangular, dims)
LinearAlgebra.UpperTriangular(f::Union{Function, UndefInitializer}, p::Blocks{2}, eltype::Type, dims::Integer...) = _GenericTriangular(f, p, eltype, UpperTriangular, dims)
LinearAlgebra.UpperTriangular(f::Union{Function, UndefInitializer}, p::Blocks{2}, eltype::Type, dims::Tuple) = _GenericTriangular(f, p, eltype, UpperTriangular, dims)

function _GenericTri!(A::Dagger.DArray{T, 2}, wrap) where {T}
    LinearAlgebra.checksquare(A)
    d = A.domain
    dc = A.subdomains
    Ac = A.chunks
    diag = [(i,i) for i in 1:min(size(d)...)]
    compar = (wrap == tril!) ? (>) : (<)
    for ind in CartesianIndices(dc)
        sz = size(dc[ind])
        if any(x -> x[1] in dc[ind].indexes[1] && x[2] in dc[ind].indexes[2], diag)
            Ac[ind] = _DiagBuild(dc[ind].indexes, fetch(Ac[ind]), diag, wrap)
        else
            if compar(first(dc[ind].indexes[2]), first(dc[ind].indexes[1]))
                Ac[ind] = Dagger.@spawn Matrix{Nothing}(nothing, sz)
            end
        end
    end
    return A
end

function LinearAlgebra.triu!(A::Dagger.DArray{T,2}) where {T}
    if size(A, 1) != size(A, 2)
        trau!(A)
    else    
        _GenericTri!(A, triu!)
    end
end
LinearAlgebra.triu(A::Dagger.DArray{T,2}) where {T} = triu!(copy(A))

function LinearAlgebra.tril!(A::Dagger.DArray{T,2}) where {T}
    if size(A, 1) != size(A, 2)
        tral!(A)
    else
        _GenericTri!(A, tril!)
    end
end
LinearAlgebra.tril(A::Dagger.DArray{T,2}) where {T} = tril!(copy(A))

#TODO: matmul




