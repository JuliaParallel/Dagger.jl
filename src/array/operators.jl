
import Base: exp, expm1, log, log10, log1p, sqrt, cbrt, exponent,
             significand, sin, sinpi, cos, cospi, tan, sec, cot, csc,
             sinh, cosh, tanh, coth, sech, csch,
             asin, acos, atan, acot, asec, acsc,
             asinh, acosh, atanh, acoth, asech, acsch, sinc, cosc,
             +, -, %, (.*), (.+), (.-), (.%), (./), (.^),
             $, &, *, (.!=), (.<), (.<=), (.==), (.>),
             (.>=), (.\), (.^), (//), (.<), (.>>), (.<<)

elementwise_unary = [:exp, :expm1, :log, :log10, :log1p, :sqrt, :cbrt, :exponent, :significand,
         :sin, :sinpi, :cos, :cospi, :tan, :sec, :cot, :csc, 
         :sinh, :cosh, :tanh, :coth, :sech, :csch,
         :asin, :acos, :atan, :acot, :asec, :acsc,
         :asinh, :acosh, :atanh, :acoth, :asech, :acsch, :sinc, :cosc]

elementwise_binary =
        [:+, :-, :%, :(.*), :(.+), :(.-), :(.%), :(./), :(.^),
         :$, :&, :*, :(.!=), :(.<), :(.<=), :(.==), :(.>),
         :(.>=), :(.\), :(.^), :(//), :(.<), :(.>>), :(.<<)]


immutable ElementwiseOp{F, N} <: Computation
    f::F
    input::NTuple{N, Computation}
end

for fn in elementwise_unary
    @eval begin
        $fn(x::Computation) = ElementwiseOp($fn, (x,))
    end
end

for fn in elementwise_binary
    @eval begin
        $fn(x::Computation, y::Computation) = ElementwiseOp($fn, (x, y))
        $fn(x::Number, y::Computation) = ElementwiseOp($fn, (broadcast(x), y))
        $fn(x::Computation, y::Number) = ElementwiseOp($fn, (x, broadcast(y)))
    end
end

function stage(ctx, node::ElementwiseOp)
    inputs = Any[cached_stage(ctx, n) for n in node.input]
    primary = inputs[1] # all others will align to this guy
    domains = domain(primary).children
    thunks = similar(domains, Any)
    for i=eachindex(domains)
        inps = map(inp->sub(inp, domains[i]), inputs)
        thunks[i] = Thunk(node.f, inps)
    end
    Cat(partition(primary), Any, domain(primary), thunks)
end
