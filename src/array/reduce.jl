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

reduce(f, x::Computation; get_result=true) = Reduce(f, f, x, get_result)
reduce(f, g::Function, x::Computation; get_result=true) = Reduce(f, g, x, get_result)

sum(x::Computation) = reduce(sum, (xs...)->sum(xs), x)
sum(f, x::Computation) = reduce(a->sum(f, a), sum, x)
prod(x::Computation) = reduce(prod, x)

length(x::Computation) = reduce(length, sum, x)

mean(x::Computation) = sum(x) / length(x)
