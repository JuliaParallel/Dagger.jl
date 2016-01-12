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


immutable ElementwiseOp{F, N} <: ComputeNode
    f::F
    input::NTuple{N, AbstractNode}
end

function compute(ctx, node::ElementwiseOp)
    inputs = [compute(ctx, n) for n in node.input]
   #if !iscompatible(inputs...)
   #    # TODO: try to make them compatible
   #    error("Inputs to $(node.f) are incompatible. They must have the same layout and distribution")
   #end
    compute(ctx, mappart(node.f, inputs...);
        output_layout=layout(inputs[1]),
        output_metadata=metadata(inputs[1]))
end

for fn in elementwise_unary
    @eval begin
        $fn(x::AbstractNode) = ElementwiseOp($fn, (x,))
    end
end

for fn in elementwise_binary
    @eval begin
        $fn(x::AbstractNode, y::AbstractNode) = ElementwiseOp($fn, (x, y))
        $fn(x::Number, y::AbstractNode) = ElementwiseOp($fn, (broadcast(x), y))
        $fn(x::AbstractNode, y::Number) = ElementwiseOp($fn, (x, broadcast(y)))
    end
end
