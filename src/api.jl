using MacroTools, Lazy

import Base: eltype, ndims

export CFArray, @cf

@forward Computed.result eltype, ndims
ndims(x::AbstractPart) = ndims(domain(x))

type CFArray{T,N} <: AbstractArray{T,N}
  head::Computation
end
@forward CFArray.head size

function CFArray(xs::Computation)
  xs = compute(xs)
  CFArray{eltype(xs),ndims(xs)}(xs)
end

macro cf(ex)
  @capture(shortdef(ex), name_(arg_) = exs__) ||
    error("invalid @cf function")
  quote
    $ex
    $name($arg::ComputeFramework.CFArray) = ComputeFramework.CFArray($name($arg.head))
  end |> esc
end

# x = CFArray(rand(BlockPartition(100), 10^3))

Base.getindex(xs::CFArray, args::Integer...) = @> getindex(xs.head, args...) gather first

for f in :[getindex map reduce exp log].args
  @eval $f(xs::CFArray, args...) = CFArray($f(xs.head, args...))
end

for f in :[(+) (*)].args
  @eval $f(xs::CFArray, ys::CFArray) = $f(xs, ys)
end

for f in :[(.+) (.*)].args
  @eval $f(xs::CFArray, ys::Number) = $f(xs, ys)
  @eval $f(xs::Number, ys::CFArray) = $f(xs, ys)
  @eval $f(xs::CFArray, ys::CFArray) = $f(xs, ys)
end
