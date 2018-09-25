import Base: reduce, map, mapreduce

export reducebykey, reduce_async

#### Map
struct Map{T,N} <: ArrayOp{T,N}
    f::Function
    inputs::Tuple
end
size(x::Map) = size(x.inputs[1])

Map(f, inputs::Tuple) = Map{Any, ndims(inputs[1])}(f, inputs)

function stage(ctx, node::Map)
    inputs = Any[cached_stage(ctx, n) for n in node.inputs]
    primary = inputs[1] # all others will align to this guy
    domains = domainchunks(primary)
    thunks = similar(domains, Any)
    f = node.f
    for i=eachindex(domains)
        inps = map(x->chunks(x)[i], inputs)
        thunks[i] = Thunk((args...) -> map(f, args...), inps...)
    end
    DArray(Any, domain(primary), domainchunks(primary), thunks)
end

map(f, x::ArrayOp, xs::ArrayOp...) = Map(f, (x, xs...))

#### Reduce

import Base.reduce
import Statistics: sum, prod, mean

struct ReduceBlock <: Computation
    op::Function
    op_master::Function
    input::ArrayOp
    get_result::Bool
end

function stage(ctx, r::ReduceBlock)
    inp = stage(ctx, r.input)
    reduced_parts = map(x -> Thunk(r.op, x; get_result=r.get_result), chunks(inp))
    Thunk((xs...) -> r.op_master(xs), reduced_parts...; meta=true)
end

reduceblock_async(f, x::ArrayOp; get_result=true) = ReduceBlock(f, f, x, get_result)
reduceblock_async(f, g::Function, x::ArrayOp; get_result=true) = ReduceBlock(f, g, x, get_result)

reduceblock(f, x::ArrayOp) = compute(reduceblock_async(f, x))
reduceblock(f, g::Function, x::ArrayOp) =
    compute(reduceblock_async(f, g, x))

reduce_async(f, x::ArrayOp) = reduceblock_async(xs->reduce(f,xs), xs->reduce(f,xs), x)

sum(x::ArrayOp; dims::Union{Int,Nothing} = nothing) = _sum(x, dims)
_sum(x, dims::Nothing) = reduceblock(sum, sum, x)
_sum(x, dims::Int) = reduce(+, x; dims=dims)
sum(f::Function, x::ArrayOp) = reduceblock(a->sum(f, a), sum, x)
prod(x::ArrayOp) = reduceblock(prod, x)
prod(f::Function, x::ArrayOp) = reduceblock(a->prod(f, a), prod, x)

mean(x::ArrayOp) = reduceblock(mean, mean, x)

mapreduce(f::Function, g::Function, x::ArrayOp) = reduce(g, map(f, x))

function mapreducebykey_seq(f, op,  itr, dict=Dict())
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

function reduce(dom::ArrayDomain; dims)
    if dims isa Int
        ArrayDomain(setindex(indexes(dom), dims, 1:1))
    else
        reduce((a,d)->reduce(a,dims=d), dims, init=dom)
    end
end

function size(x::Reducedim)
    reduce(ArrayDomain(map(x->1:x, size(x.input))), dims=x.dims)
end

function Reducedim(op, input, dims)
    T = eltype(input)
    Reducedim{T,ndims(input)}(op, input, dims)
end

function reduce(f, x::ArrayOp; dims = nothing)
    if dims === nothing
        return compute(reduce_async(f,x))
    elseif dims isa Int
        dims = (dims,)
    end
    Reducedim(f, x, dims::Tuple)
end

function stage(ctx, r::Reducedim)
    inp = cached_stage(ctx, r.input)
    thunks = let op = r.op, dims=r.dims
        # do reducedim on each block
        tmp = map(p->Thunk(b->reduce(op,b,dims=dims), p), chunks(inp))
        # combine the results in tree fashion
        treereducedim(tmp, r.dims) do x,y
            Thunk(op, x,y)
        end
    end
    c = domainchunks(inp)
    colons = Any[Colon() for x in size(c)]
    nd=ndims(domain(inp))
    colons[[Iterators.filter(d->d<=nd, r.dims)...]] .= 1
    dmn = c[colons...]
    d = reduce(domain(inp), dims=r.dims)
    ds = reduce(domainchunks(inp), dims=r.dims)
    DArray(eltype(inp), d, ds, thunks)
end
