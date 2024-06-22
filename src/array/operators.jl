export mappart, mapchunk

struct MapChunk{F, Ni, T, Nd} <: ArrayOp{T, Nd}
    f::F
    input::NTuple{Ni, ArrayOp{T,Nd}}
end

mapchunk(f::Function, xs::ArrayOp...) = MapChunk(f, xs)
Base.@deprecate mappart(args...) mapchunk(args...)
function stage(ctx::Context, node::MapChunk)
    inputs = map(x->stage(ctx, x), node.input)
    thunks = map(map(chunks, inputs)...) do ps...
        Dagger.spawn(node.f, map(p->nothing=>p, ps)...)
    end

    # TODO: Concrete type
    DArray(Any, domain(inputs[1]), domainchunks(inputs[1]), thunks)
end

# Basic indexing helpers

Base.first(A::DArray) = A[begin]
Base.last(A::DArray) = A[end]
