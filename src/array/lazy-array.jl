import Base: ==
using Compat

@compat abstract type LazyArray{T, N} <: AbstractArray{T, N} end
Base.linearindexing(x::LazyArray) = Base.LinearSlow()

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

immutable ComputedArray{T,N} <: LazyArray{T, N}
    result::AbstractChunk
end

function ComputedArray(x::AbstractChunk)
    persist!(x)
    nd = ndims(domain(x))
    ComputedArray{_eltype(chunktype(x)), nd}(x)
end

_eltype(x) = eltype(x)
_eltype(x::Type{Any}) = Any

size(x::ComputedArray) = size(domain(x.result))

compute(ctx, x::ComputedArray) = x
gather(ctx, x::ComputedArray) = gather(x.result)
stage(ctx, c::ComputedArray) = c.result
compute(ctx, x::LazyArray) =
    ComputedArray(compute(ctx, cached_stage(ctx, x)))

function (==)(x::LazyArray, y::LazyArray)
    x === y
end

function Base.hash(x::LazyArray, i::UInt64)
    7*object_id(x)-2
end

function Base.isequal(x::LazyArray, y::LazyArray)
    x === y
end
