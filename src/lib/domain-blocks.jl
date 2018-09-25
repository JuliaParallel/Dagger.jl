import Base: ndims, size, getindex

struct DomainBlocks{N} <: AbstractArray{ArrayDomain{N}, N}
    start::NTuple{N, Int}
    cumlength::Tuple
end
Base.@deprecate_binding BlockedDomains DomainBlocks

ndims(x::DomainBlocks{N}) where {N} = N
size(x::DomainBlocks) = map(length, x.cumlength)
function _getindex(x::DomainBlocks{N}, idx::Tuple) where N
    starts = map((vec, i) -> i == 0 ? 0 : getindex(vec,i), x.cumlength, map(x->x-1, idx))
    ends = map(getindex, x.cumlength, idx)
    ArrayDomain(map(UnitRange, map(+, starts, x.start), map((x,y)->x+y-1, ends, x.start)))
end

function getindex(x::DomainBlocks{N}, idx::Int) where N
    if N == 1
        _getindex(x, (idx,))
    else
        _getindex(x, ind2sub(x, idx))
    end
end

getindex(x::DomainBlocks, idx::Int...) = _getindex(x,idx)

Base.IndexStyle(::Type{<:DomainBlocks}) = IndexCartesian()

function transpose(x::DomainBlocks{2})
    DomainBlocks(reverse(x.start), reverse(x.cumlength))
end
function transpose(x::DomainBlocks{1})
    DomainBlocks((1, x.start[1]), ([1], x.cumlength[1]))
end

function Base.adjoint(x::DomainBlocks{2})
    DomainBlocks(reverse(x.start), reverse(x.cumlength))
end
function Base.adjoint(x::DomainBlocks{1})
    DomainBlocks((1, x.start[1]), ([1], x.cumlength[1]))
end

function (*)(x::DomainBlocks{2}, y::DomainBlocks{2})
    if x.cumlength[2] != y.cumlength[1]
        throw(DimensionMismatch("Block distributions being multiplied are not compatible"))
    end
    DomainBlocks((x.start[1],y.start[2]), (x.cumlength[1], y.cumlength[2]))
end

function (*)(x::DomainBlocks{2}, y::DomainBlocks{1})
    if x.cumlength[2] != y.cumlength[1]
        throw(DimensionMismatch("Block distributions being multiplied are not compatible"))
    end
    DomainBlocks((x.start[1],), (x.cumlength[1],))
end

merge_cumsums(x,y) = vcat(x, y .+ x[end])

function Base.cat(x::DomainBlocks, y::DomainBlocks; dims::Int)
    N = max(ndims(x), ndims(y))
    get_i(x,y, i) = length(x) <= i ? x[i] : length(y) <= i ? y[i] : Int[]
    for i=1:N
        i == dims && continue
        if get_i(x,y,i) != get_i(y,x,i)
            throw(DimensionMismatch("Blocked domains being concatenated have different distributions along dimension $i"))
        end
    end
    output = Any[x.cumlength...]
    output[dims] = merge_cumsums(x.cumlength[dims], y.cumlength[dims])
    DomainBlocks(x.start, (output...,))
end

Base.hcat(xs::DomainBlocks...) = cat(xs..., dims=2)
Base.vcat(xs::DomainBlocks...) = cat(xs..., dims=1)

function reduce(xs::DomainBlocks; dims)
    if dims isa Int
        DomainBlocks(xs.start,
                     setindex(xs.cumlength, dims, [1]))
    else
        reduce((a,d)->reduce(a,dims=d), dims, init=xs)
    end
end

cumulative_domains(x::DomainBlocks) = x
