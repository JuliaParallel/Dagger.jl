export getindex_async

immutable GetIndex{T,N} <: LazyArray{T,N}
    input::LazyArray
    idx::Tuple
end

function GetIndex(input::LazyArray, idx::Tuple)
    GetIndex{eltype(input), ndims(input)}(input, idx)
end

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

size(x::GetIndex) = Base.index_shape(x.input, x.idx...)

immutable GetIndexScalar <: Computation
    input::LazyArray
    idx::Tuple
end

function stage(ctx, gidx::GetIndexScalar)
    inp = cached_stage(ctx, gidx.input)
    s = view(inp, ArrayDomain(gidx.idx))
    Thunk(x->x[1], (s,), get_result=true)
end

Base.getindex(c::LazyArray, idx::ArrayDomain) = GetIndex(c, indexes(idx))
Base.getindex(c::LazyArray, idx...) = GetIndex(c, idx)
Base.getindex(c::LazyArray, idx::Integer...) =
   compute(GetIndexScalar(c, idx))
