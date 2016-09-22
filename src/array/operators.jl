
import Base: exp, expm1, log, log10, log1p, sqrt, cbrt, exponent,
             significand, sin, sinpi, cos, cospi, tan, sec, cot, csc,
             sinh, cosh, tanh, coth, sech, csch,
             asin, acos, atan, acot, asec, acsc,
             asinh, acosh, atanh, acoth, asech, acsch, sinc, cosc,
             +, -, %, (.*), (.+), (.-), (.%), (./), (.^),
             $, &, (.!=), (.<), (.<=), (.==), (.>),
             (.>=), (.\), (.//), (.>>), (.<<), *

blockwise_unary = [:exp, :expm1, :log, :log10, :log1p, :sqrt, :cbrt, :exponent, :significand,
         :(-),
         :sin, :sinpi, :cos, :cospi, :tan, :sec, :cot, :csc,
         :sinh, :cosh, :tanh, :coth, :sech, :csch,
         :asin, :acos, :atan, :acot, :asec, :acsc,
         :asinh, :acosh, :atanh, :acoth, :asech, :acsch, :sinc, :cosc]

blockwise_binary =
        [:+, :-, :%, :.*, :.+, :.-, :.%, :./, :.^,
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

immutable BlockwiseOp{F, Ni, T, Nd} <: LazyArray{T, Nd}
    f::F
    input::NTuple{Ni, LazyArray}
end

function BlockwiseOp{F}(f::F, input::Tuple)
    T = promote_type(map(eltype, input)...)
    Nd = ndims(input[1])
    BlockwiseOp{F, length(input), T, Nd}(f, input)
end

size(x::BlockwiseOp) = size(x.input[1])

for fn in blockwise_unary
    @eval begin
        $fn(x::LazyArray) = BlockwiseOp($fn, (x,))
    end
end

### Appease ambiguity warnings on Julia 0.4
for fn in [:+, :-]
    @eval begin
        $fn(x::Bool, y::LazyArray{Bool}) = BlockwiseOp(z -> $fn(x, z), (y,))
        $fn(x::LazyArray{Bool}, y::Bool) = BlockwiseOp(z -> $fn(z, y), (x,))
    end
end
(.^)(x::Irrational{:e}, y::LazyArray) = BlockwiseOp(z -> x.^z, (y,))

for fn in blockwise_binary
    @eval begin
        $fn(x::LazyArray, y::LazyArray) = BlockwiseOp($fn, (x, y))
        $fn(x::Number, y::LazyArray) = BlockwiseOp(z -> $fn(x, z), (y,))
        $fn(x::LazyArray, y::Number) = BlockwiseOp(z -> $fn(z, y), (x,))
    end
end

(*)(x::Number, y::LazyArray) = BlockwiseOp(z -> x*z, (y,))
(*)(x::LazyArray, y::Number) = BlockwiseOp(z -> z*y, (x,))

function stage(ctx, node::BlockwiseOp)
    inputs = Any[cached_stage(ctx, n) for n in node.input]
    primary = inputs[1] # all others will align to this guy
    domains = parts(domain(primary))
    thunks = map(map(parts, inputs)...) do args...
        Thunk(node.f, args)
    end

    Cat(Any, domain(primary), thunks)
end

export mappart
mappart(f, xs::LazyArray...) = BlockwiseOp(f, xs)
