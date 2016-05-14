immutable GetIndex <: Computation
    input::Computation
    idx::Tuple
end

Base.getindex(c::Computation, idx...) = GetIndex(c, idx)

Base.size(c::Computed, i::Int) = size(domain(c.result),i)
Base.size(c::Computed) = size(domain(c.result))

function stage(ctx, gidx::GetIndex)
    inp = cached_stage(ctx, gidx.input)
    dmn = domain(inp)
 
    idxs = [if isa(gidx.idx[i], Colon)
        indexes(dmn)[i]
    elseif isa(gidx.idx[i], Integer)
        Int[gidx.idx[i]]
    else
        gidx.idx[i]
    end for i in 1:length(gidx.idx)]

    sub(inp, DenseDomain(idxs))
end
