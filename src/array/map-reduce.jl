#### Map

struct Map{T,N} <: ArrayOp{T,N}
    f::Function
    inputs::Tuple
end
size(x::Map) = size(x.inputs[1])

Map(f, inputs::Tuple) = Map{Any, ndims(inputs[1])}(f, inputs)

function stage(ctx::Context, node::Map)
    inputs = Any[stage(ctx, n) for n in node.inputs]
    primary = inputs[1] # all others will align to this
    domains = domainchunks(primary)
    thunks = similar(domains, Any)
    partitioning = primary.partitioning
    concat = primary.concat
    f = node.f
    for i=eachindex(domains)
        inps = map(x->chunks(x)[i], inputs)
        thunks[i] = Dagger.@spawn map(f, inps...)
    end
    RT = Base.promote_op(node.f, map(eltype, node.inputs)...)
    return DArray(RT, domain(primary), domainchunks(primary), thunks, partitioning, concat)
end

Base.map(f, x::DArray, xs::DArray...) = _to_darray(Map(f, (x, xs...)))

#### MapReduce

struct InitialValue end

struct MapReduce{T,N} <: ArrayOp{T,N}
    f::Function
    op_inner::Union{Function,Nothing}
    op_outer::Function
    input::DArray
    dims::Union{Tuple,Nothing}
    init
end
function MapReduce(f, op_inner, op_outer, input::DArray{T,N}, dims, init) where {T,N}
    T_new = Base._return_type(op_outer, Tuple{T, T})
    if T_new === Union{}
        T_new = Any
    end
    _dims = dims === nothing ? ntuple(identity, N) : Tuple(dims)
    N_new = N - length(_dims)
    return MapReduce{T_new,N_new}(f, op_inner, op_outer, input, dims, init)
end

function stage(ctx::Context, r::MapReduce{T,N}) where {T,N}
    inp = stage(ctx, r.input)

    # Reduce partitions individually
    dims = r.dims === nothing ? Colon() : r.dims
    reduced_parts = map(chunks(inp)) do part
        if r.op_inner !== nothing
            if r.init isa InitialValue
                Dagger.@spawn r.op_inner(r.f, part; dims)
            else
                Dagger.@spawn r.op_inner(r.f, part; dims, init=r.init)
            end
        else
            if r.init isa InitialValue
                Dagger.@spawn mapreduce(r.f, r.op_outer, part; dims=dims)
            else
                Dagger.@spawn mapreduce(r.f, r.op_outer, part; dims=dims, init=r.init)
            end
        end
    end

    # Tree-reduce intermediate reductions
    dims_materialized = dims === Colon() ? ntuple(identity, ndims(inp)) : dims
    treered_f(op, x, y) = op.(x, y)
    thunks = treereducedim(reduced_parts, dims_materialized) do x, y
        Dagger.@spawn treered_f(r.op_outer, x, y)
    end

    c = domainchunks(inp)
    colons = Any[Colon() for x in size(c)]
    nd = ndims(domain(inp))
    colons[[Iterators.filter(d->d<=nd, dims_materialized)...]] .= 1
    dmn = c[colons...]
    d = reduce(domain(inp); dims=dims_materialized)
    ds = reduce(domainchunks(inp); dims=dims_materialized)
    return DArray(T, d, ds, thunks, inp.partitioning, inp.concat)
end

# Async reduction
_mapreduce_maybesync(f, op_inner, op_outer, x, dims::Int, init) =
    _to_darray(MapReduce(f, op_inner, op_outer, x, (dims,), init))
_mapreduce_maybesync(f, op_inner, op_outer, x, dims::Tuple, init) =
    _to_darray(MapReduce(f, op_inner, op_outer, x, dims, init))
# Sync reduction
_mapreduce_maybesync(f, op_inner, op_outer, x, ::Colon, init) =
    _mapreduce_maybesync(f, op_inner, op_outer, x, nothing, init)
function _mapreduce_maybesync(f, op_inner, op_outer, x::DArray{T,N}, dims::Nothing, init) where {T,N}
    Dx = _to_darray(MapReduce(f, op_inner, op_outer, x, dims, init))
    return collect(Dx)
end

function Base.size(r::MapReduce)
    sz = size(r.input)
    ntuple(length(sz)) do i
        if i in r.dims
            1
        else
            sz[i]
        end
    end
end

function Base.reduce(dom::ArrayDomain; dims)
    if dims isa Int
        ArrayDomain(setindex(indexes(dom), dims, 1:1))
    else
        reduce((a,d)->reduce(a,dims=d), dims, init=dom)
    end
end

#### High-Level MapReduce

import Statistics: mean, var, std
import OnlineStats

Base.mapreduce(f::Function, op::Function, x::DArray; dims=nothing, init=InitialValue()) =
    _mapreduce_maybesync(f, nothing, op, x, dims, init)

Base.sum(x::DArray; dims=nothing, init=InitialValue()) =
    sum(identity, x; dims, init)
Base.sum(f::Function, x::DArray; dims=nothing, init=InitialValue()) =
    _mapreduce_maybesync(f, sum, Base.add_sum, x, dims, init)

Base.prod(x::DArray; dims=nothing, init=InitialValue()) =
    prod(identity, x; dims, init)
Base.prod(f::Function, x::DArray; dims=nothing, init=InitialValue()) =
    _mapreduce_maybesync(f, prod, Base.mul_prod, x, dims, init)

Base.extrema(x::DArray; dims=nothing, init=InitialValue()) =
    extrema(identity, x; dims, init)
function Base.extrema(f::Function, x::DArray; dims=nothing, init=InitialValue())
    if length(x) == 0
        if init == InitialValue()
            # Throws an appropriate error
            Base.reduce_empty(extrema, eltype(x))
        else
            return init
        end
    end
    result = _mapreduce_maybesync(f, _extrema_inner, _extrema_outer, x, dims, init)
    return map(x->(x.min, x.max), result)
end
# We need a custom type because the `Tuple` return type of `extrema` should
# really act as a `Number`
struct Extrema{T} <: Number
    min::T
    max::T
end
Extrema(minmax::NTuple{2,T}) where T = Extrema{T}(minmax[1], minmax[2])
function _extrema_inner(f, X; dims, init)
    result = extrema(f, X; dims, init)
    if dims === Colon()
        return Extrema(result)
    else
        return map(Extrema, result)
    end
end
_extrema_outer(x::Extrema, y::Extrema) =
    Extrema(min(x.min, y.min), max(x.max, y.max))

function _onlinestats_mapreduce(f, x::DArray{T}, stat; dims=nothing) where T
    _f(x) = fit!(stat(T), f(x))
    init = stat(T)
    return _onlinestats_finish(mapreduce(_f, merge, x; dims, init), dims)
end
_onlinestats_finish(result, ::Union{Nothing,Colon}) = OnlineStats.value(result)
_onlinestats_finish(result, ::Union{Int,Dims}) = map(OnlineStats.value, result)

mean(x::DArray; dims=nothing) = mean(identity, x; dims)
mean(f::Function, x::DArray; dims=nothing) =
    _onlinestats_mapreduce(f, x, OnlineStats.Mean; dims)

var(x::DArray; dims=nothing) = var(identity, x; dims)
var(f::Function, x::DArray; dims=nothing) =
    _onlinestats_mapreduce(f, x, OnlineStats.Variance; dims)

std(x::DArray; dims=nothing) = std(identity, x; dims)
std(f::Function, x::DArray; dims=nothing) =
    map(sqrt, var(f, x; dims))
