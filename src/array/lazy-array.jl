import Base: ==
using Compat

@compat abstract type LazyArray{T, N} <: AbstractArray{T, N} end
@compat Base.IndexStyle(::Type{<:LazyArray}) = IndexCartesian()

function save(p::LazyArray, name::AbstractString)
    Save(p, name)
end

@compat function Base.show(io::IO, ::MIME"text/plain", x::LazyArray)
    write(io, string(typeof(x)))
    write(io, string(size(x)))
end

function Base.show(io::IO, x::LazyArray)
    m = MIME"text/plain"()
    @compat show(io, m, x)
end

immutable DArray{T,N} <: LazyArray{T, N}
    result::AbstractChunk
end

function DArray(x::AbstractChunk)
    persist!(x)
    nd = ndims(domain(x))
    DArray{_eltype(chunktype(x)), nd}(x)
end

_eltype(x) = eltype(x)
_eltype(x::Type{Any}) = Any

size(x::DArray) = size(domain(x.result))

compute(ctx, x::DArray) = x
gather(ctx, x::DArray) = gather(x.result)
stage(ctx, c::DArray) = c.result
compute(ctx, x::LazyArray) =
    DArray(compute(ctx, cached_stage(ctx, x)))

function (==)(x::LazyArray, y::LazyArray)
    x === y
end

function Base.hash(x::LazyArray, i::UInt64)
    7*object_id(x)-2
end

function Base.isequal(x::LazyArray, y::LazyArray)
    x === y
end
