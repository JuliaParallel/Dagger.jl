export LowerTrapezoidal, UnitLowerTrapezoidal, UpperTrapezoidal, UnitUpperTrapezoidal, trau!, trau, tral!, tral
import LinearAlgebra: triu!, tril!, triu, tril
abstract type AbstractTrapezoidal{T} <: AbstractMatrix{T} end

# First loop through all methods that don't need special care for upper/lower and unit diagonal
for t in (:LowerTrapezoidal, :UnitLowerTrapezoidal, :UpperTrapezoidal, :UnitUpperTrapezoidal)
    @eval begin
        struct $t{T,S<:AbstractMatrix{T}} <: AbstractTrapezoidal{T}
            data::S

            function $t{T,S}(data) where {T,S<:AbstractMatrix{T}}
                Base.require_one_based_indexing(data)
                new{T,S}(data)
            end
        end
        $t(A::$t) = A
        $t{T}(A::$t{T}) where {T} = A
        $t(A::AbstractMatrix) = $t{eltype(A), typeof(A)}(A)
        $t{T}(A::AbstractMatrix) where {T} = $t(convert(AbstractMatrix{T}, A))
        $t{T}(A::$t) where {T} = $t(convert(AbstractMatrix{T}, A.data))

        AbstractMatrix{T}(A::$t) where {T} = $t{T}(A)
        AbstractMatrix{T}(A::$t{T}) where {T} = copy(A)

        Base.size(A::$t) = size(A.data)
        Base.axes(A::$t) = axes(A.data)

        Base.similar(A::$t, ::Type{T}) where {T} = $t(similar(parent(A), T))
        Base.similar(A::$t, ::Type{T}, dims::Dims{N}) where {T,N} = similar(parent(A), T, dims)
        Base.parent(A::$t) = A.data

        Base.copy(A::$t) = $t(copy(A.data))

        Base.real(A::$t{<:Real}) = A
        Base.real(A::$t{<:Complex}) = (B = real(A.data); $t(B))
    end
end

Base.getindex(A::UnitLowerTrapezoidal{T}, i::Integer, j::Integer) where {T} =
    i > j ? ifelse(A.data[i,j] == nothing, zero(eltype(A.data)), A.data[i,j]) : ifelse(i == j, oneunit(T), zero(T))
Base.getindex(A::LowerTrapezoidal, i::Integer, j::Integer) =
i >= j ? ifelse(A.data[i,j] == nothing, zero(eltype(A.data)), A.data[i,j]) : zero(eltype(A.data))
Base.getindex(A::UnitUpperTrapezoidal{T}, i::Integer, j::Integer) where {T} =
    i < j ? ifelse(A.data[i,j] == nothing, zero(eltype(A.data)), A.data[i,j]) : ifelse(i == j, oneunit(T), zero(T))
Base.getindex(A::UpperTrapezoidal, i::Integer, j::Integer) =
i <= j ?  ifelse(A.data[i,j] == nothing, zero(eltype(A.data)), A.data[i,j]) : zero(eltype(A.data))

function _DiagBuild(blockdom::Tuple, alloc::AbstractMatrix{T}, diag::Vector{Tuple{Int,Int}}, transform::Function) where {T}
    diagind = findfirst(x-> x[1] in blockdom[1] && x[2] in blockdom[2], diag)
    blockind = (diag[diagind][1] - first(blockdom[1])  + 1, diag[diagind][2] - first(blockdom[2]) + 1) 
    return Dagger.@spawn transform(alloc, blockind[2] - blockind[1])
end

function _GenericTrapezoidal(f::Union{Function, UndefInitializer}, p::Blocks{2}, eltype::Type, wrap, k::Integer, dims)
    d = ArrayDomain(map(x->1:x, dims))
    dc = partition(p, d)
    if f isa UndefInitializer
        f = (eltype, x...) -> Array{eltype}(undef, x...)
    end
    m, n = dims
    if k < 0
        diag = [(i-k, i) for i in 1:min(m, n)]
    else
        diag = [(i, i+k) for i in 1:min(m, n)]
    end
    thunks = []
    alloc(sz) = f(eltype, sz)
    transform = (wrap == LowerTrapezoidal) ? tril! : triu!
    compar = (wrap == LowerTrapezoidal) ? (>) : (<)
    for c in dc
        sz = size(c)
        if any(x -> x[1] in c.indexes[1] && x[2] in c.indexes[2], diag)
            push!(thunks, _DiagBuild(c.indexes, alloc(sz), diag, transform))
        else
            mt, nt = k<0 ? (first(c.indexes[1]), first(c.indexes[2])-k) : (first(c.indexes[1])+k, first(c.indexes[2]))
            if compar(mt, nt)
                push!(thunks, Dagger.@spawn alloc(sz))
            else
                push!(thunks, Dagger.@spawn Matrix{Nothing}(nothing, sz))
            end
        end
    end
    thunks = reshape(thunks, size(dc))
    return wrap(Dagger.DArray(eltype, d, dc, thunks, p))
end

LowerTrapezoidal(f::Union{Function, UndefInitializer}, p::Blocks{2}, dims::Integer...) = _GenericTrapezoidal(f, p, eltype, LowerTrapezoidal, 0, dims)
LowerTrapezoidal(f::Union{Function, UndefInitializer}, p::Blocks{2}, dims::Tuple) = _GenericTrapezoidal(f, p, eltype, LowerTrapezoidal, 0, dims)
LowerTrapezoidal(f::Union{Function, UndefInitializer}, p::Blocks{2}, eltype::Type, dims::Integer...) = _GenericTrapezoidal(f, p, eltype, LowerTrapezoidal, 0, dims)
LowerTrapezoidal(f::Union{Function, UndefInitializer}, p::Blocks{2}, eltype::Type, dims::Tuple) = _GenericTrapezoidal(f, p, eltype, LowerTrapezoidal, 0, dims)
LowerTrapezoidal(f::Union{Function, UndefInitializer}, p::Blocks{2}, k::Integer, dims::Integer...) = _GenericTrapezoidal(f, p, eltype, LowerTrapezoidal, k, dims)
LowerTrapezoidal(f::Union{Function, UndefInitializer}, p::Blocks{2}, k::Integer, dims::Tuple) = _GenericTrapezoidal(f, p, eltype, LowerTrapezoidal, k, dims)
LowerTrapezoidal(f::Union{Function, UndefInitializer}, p::Blocks{2}, eltype::Type, k::Integer, dims::Integer...) = _GenericTrapezoidal(f, p, eltype, LowerTrapezoidal, k, dims)
LowerTrapezoidal(f::Union{Function, UndefInitializer}, p::Blocks{2}, eltype::Type, k::Integer, dims::Tuple) = _GenericTrapezoidal(f, p, eltype, LowerTrapezoidal, k, dims)

UpperTrapezoidal(f::Union{Function, UndefInitializer}, p::Blocks{2}, dims::Integer...) = _GenericTrapezoidal(f, p, eltype, UpperTrapezoidal, 0, dims)
UpperTrapezoidal(f::Union{Function, UndefInitializer}, p::Blocks{2}, dims::Tuple) = _GenericTrapezoidal(f, p, eltype, UpperTrapezoidal, 0, dims)
UpperTrapezoidal(f::Union{Function, UndefInitializer}, p::Blocks{2}, eltype::Type, dims::Integer...) = _GenericTrapezoidal(f, p, eltype, UpperTrapezoidal, 0, dims)
UpperTrapezoidal(f::Union{Function, UndefInitializer}, p::Blocks{2}, eltype::Type, dims::Tuple) = _GenericTrapezoidal(f, p, eltype, UpperTrapezoidal, 0, dims)
UpperTrapezoidal(f::Union{Function, UndefInitializer}, p::Blocks{2}, k::Integer, dims::Integer...) = _GenericTrapezoidal(f, p, eltype, UpperTrapezoidal, k, dims)
UpperTrapezoidal(f::Union{Function, UndefInitializer}, p::Blocks{2}, k::Integer, dims::Tuple) = _GenericTrapezoidal(f, p, eltype, UpperTrapezoidal, k, dims)
UpperTrapezoidal(f::Union{Function, UndefInitializer}, p::Blocks{2}, eltype::Type, k::Integer, dims::Integer...) = _GenericTrapezoidal(f, p, eltype, UpperTrapezoidal, k, dims)
UpperTrapezoidal(f::Union{Function, UndefInitializer}, p::Blocks{2}, eltype::Type, k::Integer, dims::Tuple) = _GenericTrapezoidal(f, p, eltype, UpperTrapezoidal, k, dims)

function _GenericTra!(A::Dagger.DArray{T, 2}, wrap::Function, k::Integer) where {T}
    d = A.domain
    dc = A.subdomains
    Ac = A.chunks
    m, n = size(A)
    if k < 0
        diag = [(i-k, i) for i in 1:min(m, n)]
    else
        diag = [(i, i+k) for i in 1:min(m, n)]
    end
    compar = (wrap == tril!) ? (≤) : (≥)
    for ind in CartesianIndices(dc)
        sz = size(dc[ind])
        if any(x -> x[1] in dc[ind].indexes[1] && x[2] in dc[ind].indexes[2], diag)
            Ac[ind] = _DiagBuild(dc[ind].indexes, fetch(Ac[ind]), diag, wrap)
        else
            mt, nt = k<0 ? (first(dc[ind].indexes[1]), first(dc[ind].indexes[2])-k) : (first(dc[ind].indexes[1])+k, first(dc[ind].indexes[2]))
            if compar(mt, nt)
                Ac[ind] = Dagger.@spawn Matrix{Nothing}(nothing, sz)
            end
        end
    end
    return A
end



trau!(A::Dagger.DArray{T,2}, k::Integer) where {T} = _GenericTra!(A, triu!, k)
trau!(A::Dagger.DArray{T,2}) where {T} = _GenericTra!(A, triu!, 0)
trau(A::Dagger.DArray{T,2}) where {T} = trau!(copy(A))
trau(A::Dagger.DArray{T,2}, k::Integer) where {T} = trau!(copy(A), k)

tral!(A::Dagger.DArray{T,2}, k::Integer) where {T} = _GenericTra!(A, tril!, k)
tral!(A::Dagger.DArray{T,2}) where {T} = _GenericTra!(A, tril!, 0)
tral(A::Dagger.DArray{T,2}) where {T} = tral!(copy(A))
tral(A::Dagger.DArray{T,2}, k::Integer) where {T} = tral!(copy(A), k)

#TODO: map, reduce, sum, mean, prod, reducedim, collect, distribute
