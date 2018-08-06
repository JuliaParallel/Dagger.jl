struct GetIndex{T,N} <: ArrayOp{T,N}
    input::ArrayOp
    idx::Tuple
end

GetIndex(input::ArrayOp, idx::Tuple) =
    GetIndex{eltype(input), ndims(input)}(input, idx)

function stage(ctx, gidx::GetIndex)
    inp = cached_stage(ctx, gidx.input)

    dmn = domain(inp)
    idxs = [if isa(gidx.idx[i], Colon)
        indexes(dmn)[i]
    else
        gidx.idx[i]
    end for i in 1:length(gidx.idx)]

    # Figure out output dimension
    view(inp, ArrayDomain(idxs))
end

function size(x::GetIndex)
    map(a -> a[2] isa Colon ?
        size(x.input, a[1]) : length(a[2]),
        enumerate(x.idx)) |> Tuple
end

struct GetIndexScalar <: Computation
    input::ArrayOp
    idx::Tuple
end

function stage(ctx, gidx::GetIndexScalar)
    inp = cached_stage(ctx, gidx.input)
    s = view(inp, ArrayDomain(gidx.idx))
    delayed(identity)(collect(s)[1])
end

Base.getindex(c::ArrayOp, idx::ArrayDomain) = GetIndex(c, indexes(idx))
Base.getindex(c::ArrayOp, idx...)           = GetIndex(c, idx)
Base.getindex(c::ArrayOp, idx::Integer...)  = compute(GetIndexScalar(c, idx))
