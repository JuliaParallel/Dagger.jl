
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

function stage_operands(ctx, ::BlockwiseOp, xs::LazyArray...)
    map(x->cached_stage(ctx, x), xs)
end

function stage_operands(ctx, ::BlockwiseOp, x::LazyArray, y::PromotePartition)
    stg_x = cached_stage(ctx, x)
    y1 = Distribute(domain(stg_x), y.data)
    stg_x, cached_stage(ctx, y1)
end

function stage_operands(ctx, ::BlockwiseOp, x::PromotePartition, y::LazyArray)
    stg_y = cached_stage(ctx, y)
    x1 = Distribute(domain(stg_y), x.data)
    cached_stage(ctx, x1), stg_y
end

for fn in blockwise_binary
    @eval begin
        $fn(x::LazyArray, y::LazyArray) = BlockwiseOp($fn, (x, y))
        $fn(x::AbstractArray, y::LazyArray) = BlockwiseOp($fn, (PromotePartition(x), y))
        $fn(x::LazyArray, y::AbstractArray) = BlockwiseOp($fn, (x, PromotePartition(y)))
        $fn(x::Number, y::LazyArray) = BlockwiseOp(z -> $fn(x, z), (y,))
        $fn(x::LazyArray, y::Number) = BlockwiseOp(z -> $fn(z, y), (x,))
    end
end

(*)(x::Number, y::LazyArray) = BlockwiseOp(z -> x*z, (y,))
(*)(x::LazyArray, y::Number) = BlockwiseOp(z -> z*y, (x,))

function stage(ctx, node::BlockwiseOp)
    inputs = stage_operands(ctx, node, node.input...)
    thunks,d = if length(inputs) == 2
        a, b = domain(inputs[1]), domain(inputs[2])
        if length(a) == 1 && length(b) != 1
            map(parts(inputs[2])) do p
                Thunk(node.f, (parts(inputs[1])[1], p))
            end, b
        elseif length(a) != 1 && length(b) == 1
            map(parts(inputs[1])) do p
                Thunk(node.f, (p, parts(inputs[2])[1]))
            end, a
        else
            @assert domain(a) == domain(b)
            map(map(parts, inputs)...) do x, y
                Thunk(node.f, (x, y))
            end, a
        end
    else
        # TODO: include broadcast semantics in this.
        map(map(parts, inputs)...) do ps...
            Thunk(node.f, (ps...,))
        end, domain(inputs[1])
    end

    Cat(Any, d, thunks)
end

export mappart
mappart(f, xs::LazyArray...) = BlockwiseOp(f, xs)
