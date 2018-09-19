
import Base: exp, expm1, log, log10, log1p, sqrt, cbrt, exponent,
             significand, sin, sinpi, cos, cospi, tan, sec, cot, csc,
             sinh, cosh, tanh, coth, sech, csch,
             asin, acos, atan, acot, asec, acsc,
             asinh, acosh, atanh, acoth, asech, acsch, sinc, cosc,
             +, -, %, &, *

import Base: broadcast
import Base: Broadcast
import Base.Broadcast: Broadcasted, BroadcastStyle, combine_eltypes

"""
This is a way of suggesting that stage should call
stage_operand with the operation and other arguments
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

function stage_operands(ctx, ::BCast, xs::ArrayOp...)
    map(x->cached_stage(ctx, x), xs)
end

function stage_operands(ctx, ::BCast, x::ArrayOp, y::PromotePartition)
    stg_x = cached_stage(ctx, x)
    y1 = Distribute(domain(stg_x), y.data)
    stg_x, cached_stage(ctx, y1)
end

function stage_operands(ctx, ::BCast, x::PromotePartition, y::ArrayOp)
    stg_y = cached_stage(ctx, y)
    x1 = Distribute(domain(stg_y), x.data)
    cached_stage(ctx, x1), stg_y
end

struct DaggerBroadcastStyle <: BroadcastStyle end

BroadcastStyle(::Type{<:ArrayOp}) = DaggerBroadcastStyle()
BroadcastStyle(::DaggerBroadcastStyle, ::BroadcastStyle) = DaggerBroadcastStyle()
BroadcastStyle(::BroadcastStyle, ::DaggerBroadcastStyle) = DaggerBroadcastStyle()

function Base.copy(b::Broadcast.Broadcasted{<:DaggerBroadcastStyle})
    BCast(b)
end

function stage(ctx, node::BCast)
    bc = Broadcast.flatten(node.bcasted)
    args = bc.args
    args1 = map(args) do x
        x isa ArrayOp ? cached_stage(ctx, x) : x
    end
    ds = map(x->x isa DArray ? domainchunks(x) : nothing, args1)
    sz = size(node)
    dss = filter(x->x !== nothing, collect(ds))
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
            cached_stage(ctx, Distribute(dmn, arg)).chunks
        else
            arg
        end
    end
    blcks = DomainBlocks(map(_->1, size(node)), cumlengths)

    thunks = broadcast(delayed((args...)->broadcast(bc.f, args...); ),
                       args2...)
    DArray(eltype(node), domain(node), blcks, thunks)
end

export mappart, mapchunk

struct MapChunk{F, Ni, T, Nd} <: ArrayOp{T, Nd}
    f::F
    input::NTuple{Ni, ArrayOp{T,Nd}}
end

mapchunk(f, xs::ArrayOp...) = MapChunk(f, xs)
Base.@deprecate mappart(args...) mapchunk(args...)
function stage(ctx, node::MapChunk)
    inputs = map(x->cached_stage(ctx, x), node.input)
    thunks = map(map(chunks, inputs)...) do ps...
        Thunk(node.f, ps...)
    end

    DArray(Any, domain(inputs[1]), domainchunks(inputs[1]), thunks)
end
