
import Base: reduce, map

export reducebykey

#### Map
immutable Map <: Computation
    f::Function
    inputs::Tuple
end

function stage(ctx, node::Map)
    inputs = Any[cached_stage(ctx, n) for n in node.inputs]
    primary = inputs[1] # all others will align to this guy
    domains = domain(primary).children
    thunks = similar(domains, Any)
    f = node.f
    for i=eachindex(domains)
        inps = map(inp->sub(inp, domains[i]), inputs)
        thunks[i] = Thunk((args...) -> map(f, args...), (inps...))
    end
    Cat(partition(primary), Any, domain(primary), thunks)
end

map(f, xs::Computation...) = Map(f, xs)
map(f, x::AbstractPart) = map(f, Computed(x))
map(f, x::Thunk) = Thunk(x->map(f,x), x)

#### Reduce

import Base: reduce, sum, prod, mean

immutable Reduce <: Computation
    op::Function
    op_master::Function
    input::Computation
    get_result::Bool
end

function stage(ctx, r::Reduce)
    inp = stage(ctx, r.input)
    reduced_parts = map(x -> Thunk(r.op, (x,); get_result=r.get_result), inp.parts)
    Thunk(r.op_master, (reduced_parts...); meta=true)
end

reduceblock(f, x::Computation; get_result=true) = Reduce(f, f, x, get_result)
reduceblock(f, g::Function, x::Computation; get_result=true) = Reduce(f, g, x, get_result)

sum(x::Computation) = reduceblock(sum, (xs...)->sum(xs), x)
sum(f::Function, x::Computation) = reduceblock(a->sum(f, a), sum, x)
prod(x::Computation) = reduceblock(prod, x)
prod(f::Function, x::Computation) = reduceblock(a->prod(f, a), prod, x)

length(x::Computation) = reduceblock(length, sum, x)

mean(x::Computation) = sum(x) / length(x)


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
    (xs...) -> reduce((d,x) -> reducebykey_seq(op, x, d), Dict(), xs)
end
reducebykey_seq(op, itr,dict=Dict()) = mapreducebykey_seq(Base.IdFun(), op, itr, dict)
reducebykey(op, input) = reduceblock(itr->reducebykey_seq(op, itr), merge_reducebykey(op), input)
