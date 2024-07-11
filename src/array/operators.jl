import Base: broadcast
import Base: Broadcast
import Base.Broadcast: Broadcasted, BroadcastStyle, combine_eltypes

"""
This is a way of suggesting that stage should call
stage_operand with the operation and other arguments.
"""
struct PromotePartition{T,N} <: ArrayOp{T,N}
    data::AbstractArray{T,N}
end

size(p::PromotePartition) = size(domain(p.data))

struct BCast{B, T, Nd} <: ArrayOp{T, Nd}
    bcasted::B
end

BCast(b::Broadcasted) = BCast{typeof(b), combine_eltypes(b.f, b.args), length(axes(b))}(b)

size(x::BCast) = map(length, axes(x.bcasted))

function stage_operands(ctx::Context, ::BCast, xs::ArrayOp...)
    map(x->stage(ctx, x), xs)
end

function stage_operands(ctx::Context, ::BCast, x::ArrayOp, y::PromotePartition)
    stg_x = stage(ctx, x)
    y1 = Distribute(domain(stg_x), y.data)
    stg_x, stage(ctx, y1)
end

function stage_operands(ctx::Context, ::BCast, x::PromotePartition, y::ArrayOp)
    stg_y = stage(ctx, y)
    x1 = Distribute(domain(stg_y), x.data)
    stage(ctx, x1), stg_y
end

struct DaggerBroadcastStyle <: BroadcastStyle end

BroadcastStyle(::Type{<:ArrayOp}) = DaggerBroadcastStyle()
BroadcastStyle(::DaggerBroadcastStyle, ::BroadcastStyle) = DaggerBroadcastStyle()
BroadcastStyle(::BroadcastStyle, ::DaggerBroadcastStyle) = DaggerBroadcastStyle()

function Base.copy(b::Broadcast.Broadcasted{<:DaggerBroadcastStyle})
    return _to_darray(BCast(b))
end

function stage(ctx::Context, node::BCast{B,T,N}) where {B,T,N}
    bc = Broadcast.flatten(node.bcasted)
    args = bc.args
    args1 = map(args) do x
        x isa ArrayOp ? stage(ctx, x) : x
    end
    ds = map(x->x isa DArray ? domainchunks(x) : nothing, args1)
    sz = size(node)
    dss = filter(x->x !== nothing, collect(ds))
    # TODO: Use a more intelligent scheme
    part = args1[findfirst(arg->arg isa DArray && ndims(arg) == N, args1)].partitioning
    cumlengths = ntuple(ndims(node)) do i
        idx = findfirst(d -> i <= length(d.cumlength), dss)
        if idx === nothing
            [sz[i]] # just one slice
        end
        dss[idx].cumlength[i]
    end

    args2 = map(args1) do arg
        if arg isa AbstractArray
            s = size(arg)
            splits = map(enumerate(s)) do dim
                i, n = dim
                if n == 1
                    return [1]
                else
                    cumlengths[i]
                end
            end |> Tuple
            dmn = DomainBlocks(ntuple(_->1, length(s)), splits)
            stage(ctx, Distribute(dmn, part, arg)).chunks
        else
            arg
        end
    end
    blcks = DomainBlocks(map(_->1, size(node)), cumlengths)

    thunks = broadcast((args3...)->Dagger.spawn((args...)->broadcast(bc.f, args...), args3...), args2...)
    DArray(eltype(node), domain(node), blcks, thunks, part)
end

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

# In-place operations

function Base.map!(f, a::DArray{T}) where T
    Dagger.spawn_datadeps() do
        for ca in chunks(a)
            Dagger.@spawn map!(f, InOut(ca), ca)
        end
    end
    return a
end

function Base.map!(f, a::DArray{T}, b::AbstractArray{U}) where {T, U}
    b2 = view(b, a.partitioning)
    Dagger.spawn_datadeps() do
        for (c_a, c_b2) in zip(chunks(a), chunks(b2))
            Dagger.@spawn map!(f, InOut(c_a), c_b2)
        end
    end
    return a
end
