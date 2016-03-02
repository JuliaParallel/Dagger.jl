
import Base: exp, expm1, log, log10, log1p, sqrt, cbrt, exponent,
             significand, sin, sinpi, cos, cospi, tan, sec, cot, csc,
             sinh, cosh, tanh, coth, sech, csch,
             asin, acos, atan, acot, asec, acsc,
             asinh, acosh, atanh, acoth, asech, acsch, sinc, cosc,
             +, -, %, (.*), (.+), (.-), (.%), (./), (.^),
             $, &, (.!=), (.<), (.<=), (.==), (.>),
             (.>=), (.\), (.^), (//), (.<), (.>>), (.<<)

blockwise_unary = [:exp, :expm1, :log, :log10, :log1p, :sqrt, :cbrt, :exponent, :significand,
         :(-),
         :sin, :sinpi, :cos, :cospi, :tan, :sec, :cot, :csc, 
         :sinh, :cosh, :tanh, :coth, :sech, :csch,
         :asin, :acos, :atan, :acot, :asec, :acsc,
         :asinh, :acosh, :atanh, :acoth, :asech, :acsch, :sinc, :cosc]

blockwise_binary =
        [:+, :-, :%, :(.*), :(.+), :(.-), :(.%), :(./), :(.^),
         :$, :&, :(.!=), :(.<), :(.<=), :(.==), :(.>),
         :(.>=), :(.\), :(.^), :(//), :(.<), :(.>>), :(.<<)]


immutable BlockwiseOp{F, N} <: Computation
    f::F
    input::NTuple{N, Computation}
end

for fn in blockwise_unary
    @eval begin
        $fn(x::Computation) = BlockwiseOp($fn, (x,))
    end
end

for fn in blockwise_binary
    @eval begin
        $fn(x::Computation, y::Computation) = BlockwiseOp($fn, (x, y))
        $fn(x::Number, y::Computation) = BlockwiseOp(z -> $fn(x, z), (y,))
        $fn(x::Computation, y::Number) = BlockwiseOp(z -> $fn(z, y), (x,))
    end
end

function stage(ctx, node::BlockwiseOp)
    inputs = Any[cached_stage(ctx, n) for n in node.input]
    primary = inputs[1] # all others will align to this guy
    domains = domain(primary).children
    thunks = similar(domains, Any)
    f = node.f
    for i=eachindex(domains)
        inps = map(inp->sub(inp, domains[i]), inputs)
        thunks[i] = Thunk(f, inps)
    end
    Cat(partition(primary), Any, domain(primary), thunks)
end
