import Base: ndims, size, getindex, reducedim

immutable BlockedDomains{N} <: AbstractArray{DenseDomain{N}, N}
    start::NTuple{N, Int}
    cumlength::NTuple{N, AbstractArray{Int}}
end

ndims{N}(x::BlockedDomains{N}) = N
size(x::BlockedDomains) = map(length, x.cumlength)
function _getindex{N}(x::BlockedDomains{N}, idx::Tuple)
    starts = map((vec, i) -> i == 0 ? 0 : getindex(vec,i), x.cumlength, map(x->x-1, idx))
    ends = map(getindex, x.cumlength, idx)
    DenseDomain(map(UnitRange, map(+, starts, x.start), map((x,y)->x+y-1, ends, x.start)))
end

function getindex{N}(x::BlockedDomains{N}, idx::Int)
    if N == 1
        _getindex(x, (idx,))
    else
        _getindex(x, ind2sub(x, idx))
    end
end

getindex(x::BlockedDomains, idx::Int...) = _getindex(x,idx)

Base.linearindexing(x::BlockedDomains) = Base.LinearSlow()

function Base.ctranspose(x::BlockedDomains{2})
    BlockedDomains(reverse(x.start), reverse(x.cumlength))
end
function Base.ctranspose(x::BlockedDomains{1})
    BlockedDomains((1, x.start[1]), ([1], x.cumlength[1]))
end

function (*)(x::BlockedDomains{2}, y::BlockedDomains{2})
    if x.cumlength[2] != y.cumlength[1]
        throw(DimensionMismatch("Block distributions being multiplied are not compatible"))
    end
    BlockedDomains((x.start[1],y.start[2]), (x.cumlength[1], y.cumlength[2]))
end

function (*)(x::BlockedDomains{2}, y::BlockedDomains{1})
    if x.cumlength[2] != y.cumlength[1]
        throw(DimensionMismatch("Block distributions being multiplied are not compatible"))
    end
    BlockedDomains((x.start[1],), (x.cumlength[1],))
end

merge_cumsums(x,y) = vcat(x, y+x[end])

function Base.cat(idx::Int, x::BlockedDomains, y::BlockedDomains)
    N = max(ndims(x), ndims(y))
    get_i(x,y, i) = length(x) <= i ? x[i] : length(y) <= i ? y[i] : Int[]
    for i=1:N
        i == idx && continue
        if get_i(x,y,i) != get_i(y,x,i)
            throw(DimensionMismatch("Blocked domains being concatenated have different distributions along dimension $i"))
        end
    end
    output = Any[x.cumlength...]
    output[idx] = merge_cumsums(x.cumlength[idx], y.cumlength[idx])
    BlockedDomains(x.start, (output...))
end

Base.hcat(xs::BlockedDomains...) = cat(2, xs...)
Base.vcat(xs::BlockedDomains...) = cat(1, xs...)

function reducedim(xs::BlockedDomains, dim::Int)
    BlockedDomains(xs.start,
        setindex(xs.cumlength,dim, [1]))
end
function reducedim(dom::BlockedDomains, dim::Tuple)
    reduce(reducedim, dom, dim)
end

cumulative_domains(x::BlockedDomains) = x
