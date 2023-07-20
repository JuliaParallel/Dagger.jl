import Base: reduce, map, mapreduce

#### Map
struct Map{T,N} <: ArrayOp{T,N}
    f::Function
    inputs::Tuple
end
size(x::Map) = size(x.inputs[1])

Map(f, inputs::Tuple) = Map{Any, ndims(inputs[1])}(f, inputs)

function stage(ctx::Context, node::Map)
    inputs = Any[cached_stage(ctx, n) for n in node.inputs]
    primary = inputs[1] # all others will align to this guy
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

map(f, x::ArrayOp, xs::ArrayOp...) = _to_darray(Map(f, (x, xs...)))

#### Reduce

import Base.reduce
import Statistics: sum, prod, mean

struct ReduceBlock <: Computation
    op::Function
    op_master::Function
    input::ArrayOp
    get_result::Bool
end

function stage(ctx::Context, r::ReduceBlock)
    inp = stage(ctx, r.input)
    reduced_parts = map(x -> (Dagger.@spawn get_result=r.get_result r.op(x)), chunks(inp))
    r_op_master(args...,) = r.op_master(args)
    Dagger.@spawn meta=true r_op_master(reduced_parts...)
end

reduceblock_async(f, x::ArrayOp; get_result=true) = ReduceBlock(f, f, x, get_result)
reduceblock_async(f, g::Function, x::ArrayOp; get_result=true) = ReduceBlock(f, g, x, get_result)

reduceblock(f, x::ArrayOp) = fetch(reduceblock_async(f, x))
reduceblock(f, g::Function, x::ArrayOp) = fetch(reduceblock_async(f, g, x))

reduce_async(f::Function, x::ArrayOp) = reduceblock_async(xs->reduce(f,xs), xs->reduce(f,xs), x)

_reduce_maybeblock(f::Function, _::Function, x, dims::Nothing) = reduceblock(f, f, x)
_reduce_maybeblock(_::Function, f::Function, x, dims::Int) = reduce(f, x; dims)

sum(x::ArrayOp; dims::Union{Int,Nothing} = nothing) =
    _reduce_maybeblock(sum, Base.add_sum, x, dims)
sum(f::Function, x::ArrayOp) = reduceblock(a->sum(f, a), sum, x)

prod(x::ArrayOp; dims::Union{Int,Nothing} = nothing) =
    _reduce_maybeblock(prod, Base.mul_prod, x, dims)
prod(f::Function, x::ArrayOp) = reduceblock(a->prod(f, a), prod, x)

mean(x::ArrayOp; dims::Union{Int,Nothing} = nothing) =
    _reduce_maybeblock(mean, mean, x, dims)
mean(f::Function, x::ArrayOp) = reduceblock(a->mean(f, a), mean, x)

mapreduce(f::Function, g::Function, x::ArrayOp) = reduce(g, map(f, x))

function mapreducebykey_seq(f, op, itr, dict=Dict())
    for x in itr
        y = f(x)
        if haskey(dict, y[1])
            dict[y[1]] = op(dict[y[1]], y[2])
        else
            dict[y[1]] = y[2]
        end
    end
    dict
end

function merge_reducebykey(op)
    xs -> reduce((d,x) -> reducebykey_seq(op, x, d), Dict(), xs)
end
reducebykey_seq(op, itr,dict=Dict()) = mapreducebykey_seq(Base.IdFun(), op, itr, dict)
reducebykey(op, input) = reduceblock(itr->reducebykey_seq(op, itr), merge_reducebykey(op), input)


struct Reducedim{T,N} <: ArrayOp{T,N}
    op::Function
    input::ArrayOp
    dims::Tuple
end

function Base.size(r::Reducedim)
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

function Reducedim(op, input, dims)
    T = eltype(input)
    Reducedim{T,ndims(input)}(op, input, dims)
end

function Base.reduce(f::Function, x::ArrayOp; dims = nothing)
    if dims === nothing
        return fetch(reduce_async(f,x))
    elseif dims isa Int
        dims = (dims,)
    end
    return _to_darray(Reducedim(f, x, dims::Tuple))
end

function stage(ctx::Context, r::Reducedim)
    inp = cached_stage(ctx, r.input)
    thunks = let op = r.op, dims=r.dims
        # do reducedim on each block
        tmp = map(p->Dagger.spawn(b->reduce(op,b,dims=dims), p), chunks(inp))
        # combine the results in tree fashion
        treereducedim(tmp, r.dims) do x,y
            Dagger.@spawn op(x,y)
        end
    end
    c = domainchunks(inp)
    colons = Any[Colon() for x in size(c)]
    nd = ndims(domain(inp))
    colons[[Iterators.filter(d->d<=nd, r.dims)...]] .= 1
    dmn = c[colons...]
    d = reduce(domain(inp), dims=r.dims)
    ds = reduce(domainchunks(inp), dims=r.dims)
    return DArray(eltype(inp), d, ds, thunks, inp.partitioning, inp.concat)
end
