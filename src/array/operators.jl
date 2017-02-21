
import Base: exp, expm1, log, log10, log1p, sqrt, cbrt, exponent,
             significand, sin, sinpi, cos, cospi, tan, sec, cot, csc,
             sinh, cosh, tanh, coth, sech, csch,
             asin, acos, atan, acot, asec, acsc,
             asinh, acosh, atanh, acoth, asech, acsch, sinc, cosc,
             +, -, %, (.*), (.+), (.-), (.%), (./), (.^),
             $, &, (.!=), (.<), (.<=), (.==), (.>),
             (.>=), (.\), (.//), (.>>), (.<<), *

import Base: broadcast

blockwise_unary = [:exp, :expm1, :log, :log10, :log1p, :sqrt, :cbrt, :exponent, :significand,
         :(-),
         :sin, :sinpi, :cos, :cospi, :tan, :sec, :cot, :csc,
         :sinh, :cosh, :tanh, :coth, :sech, :csch,
         :asin, :acos, :atan, :acot, :asec, :acsc,
         :asinh, :acosh, :atanh, :acoth, :asech, :acsch, :sinc, :cosc]

blockwise_binary = [:+, :-, :%,]

broadcast_ops = [ :.*, :.+, :.-, :.%, :./, :.^,
         :$, :&, :.!=, :.<, :.<=, :.==, :.>,
         :.>=, :.\, :.//, :.>>, :.<<]

"""
This is a way of suggesting that stage should call
stage_operand with the operation and other arguments
"""
immutable PromotePartition{T,N} <: LazyArray{T,N}
    data::AbstractArray{T,N}
end

size(p::PromotePartition) = size(domain(p.data))

immutable BCast{F, Ni, T, Nd} <: LazyArray{T, Nd}
    f::F
    input::NTuple{Ni, LazyArray}
end

function BCast{F}(f::F, input::Tuple)
    T = promote_type(map(eltype, input)...)
    Nd = reduce(max, map(ndims, input))
    BCast{F, length(input), T, Nd}(f, input)
end

size(x::BCast) = size(x.input[1])

for fn in blockwise_unary
    @eval begin
        $fn(x::LazyArray) = BCast($fn, (x,))
    end
end

### Appease ambiguity warnings on Julia 0.4
for fn in [:+, :-]
    @eval begin
        $fn(x::Bool, y::LazyArray{Bool}) = BCast(z -> $fn(x, z), (y,))
        $fn(x::LazyArray{Bool}, y::Bool) = BCast(z -> $fn(z, y), (x,))
    end
end

function stage_operands(ctx, ::BCast, xs::LazyArray...)
    map(x->cached_stage(ctx, x), xs)
end

function stage_operands(ctx, ::BCast, x::LazyArray, y::PromotePartition)
    stg_x = cached_stage(ctx, x)
    y1 = Distribute(domain(stg_x), y.data)
    stg_x, cached_stage(ctx, y1)
end

function stage_operands(ctx, ::BCast, x::PromotePartition, y::LazyArray)
    stg_y = cached_stage(ctx, y)
    x1 = Distribute(domain(stg_y), x.data)
    cached_stage(ctx, x1), stg_y
end

for fn in blockwise_binary
    @eval begin
        $fn(x::LazyArray, y::LazyArray) = BCast($fn, (x, y))
        $fn(x::AbstractArray, y::LazyArray) = BCast($fn, (PromotePartition(x), y))
        $fn(x::LazyArray, y::AbstractArray) = BCast($fn, (x, PromotePartition(y)))
        $fn(x::Number, y::LazyArray) = BCast(z -> $fn(x, z), (y,))
        $fn(x::LazyArray, y::Number) = BCast(z -> $fn(z, y), (x,))
    end
end

if VERSION < v"0.6.0-dev"
    eval(:((.^)(x::Irrational{:e}, y::LazyArray) = BCast(z -> x.^z, (y,))))
    for fn in broadcast_ops
        @eval begin
            $fn(x::LazyArray, y::LazyArray) = BCast($fn, (x, y))
            $fn(x::AbstractArray, y::LazyArray) = BCast($fn, (PromotePartition(x), y))
            $fn(x::LazyArray, y::AbstractArray) = BCast($fn, (x, PromotePartition(y)))
            $fn(x::Number, y::LazyArray) = BCast(z -> $fn(x, z), (y,))
            $fn(x::LazyArray, y::Number) = BCast(z -> $fn(z, y), (x,))
        end
    end
end

Base.broadcast(fn::Function, x::LazyArray, xs::LazyArray...) = BCast(fn, (x, xs...))
Base.broadcast(fn::Function, x::AbstractArray, y::LazyArray) = BCast(fn, (PromotePartition(x), y))
Base.broadcast(fn::Function, x::LazyArray, y::AbstractArray) = BCast(fn, (x, PromotePartition(y)))
Base.broadcast(fn::Function, x::Number, y::LazyArray) = BCast(z -> fn(x, z), (y,))
Base.broadcast(fn::Function, x::LazyArray, y::Number) = BCast(z -> fn(z, y), (x,))

(*)(x::Number, y::LazyArray) = BCast(z -> x*z, (y,))
(*)(x::LazyArray, y::Number) = BCast(z -> z*y, (x,))

function curry_broadcast(f)
    (xs...) -> broadcast(f, xs...)
end
function stage(ctx, node::BCast)
    inputs = stage_operands(ctx, node, node.input...)
    broadcast_f = curry_broadcast(node.f)
    thunks,d, dchunks = if length(inputs) == 2
        a, b = domain(inputs[1]), domain(inputs[2])
        ac, bc = domainchunks(inputs[1]), domainchunks(inputs[2])
        if length(a) == 1 && length(b) != 1
            map(chunks(inputs[2])) do p
                Thunk(broadcast_f, chunks(inputs[1])[1], p)
            end, b, bc
        elseif length(a) != 1 && length(b) == 1
            map(chunks(inputs[1])) do p
                Thunk(broadcast_f, p, chunks(inputs[2])[1])
            end, a, ac
        else
            @assert domain(a) == domain(b)
            map(map(chunks, inputs)...) do x, y
                Thunk(broadcast_f, x, y)
            end, a, ac
        end
    else
        # TODO: include broadcast semantics in this.
        map(map(chunks, inputs)...) do ps...
            Thunk(broadcast_f, ps...)
        end, domain(inputs[1]), domainchunks(inputs[1])
    end

    Cat(Any, d, dchunks, thunks)
end

export mappart, mapchunk

immutable MapChunk{F, Ni, T, Nd} <: LazyArray{T, Nd}
    f::F
    input::NTuple{Ni, LazyArray{T,Nd}}
end

mapchunk(f, xs::LazyArray...) = MapChunk(f, xs)
Base.@deprecate mappart(args...) mapchunk(args...)
function stage(ctx, node::MapChunk)
    inputs = map(x->cached_stage(ctx, x), node.input)
    thunks = map(map(chunks, inputs)...) do ps...
        Thunk(node.f, ps...)
    end

    Cat(Any, domain(inputs[1]), domainchunks(inputs[1]), thunks)
end
