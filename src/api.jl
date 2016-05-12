using MacroTools, Lazy

import Base: eltype, ndims

export CFArray, @cf

@forward Computed.result eltype, ndims
@forward Cat.parttype eltype, ndims

type CFArray{T,N} <: AbstractArray{T,N}
  head::Computation
end

function CFArray(xs::Computation)
  xs = compute(xs)
  CFArray{eltype(xs),ndims(xs)}(xs)
end

macro cf(ex)
  @capture(shortdef(ex), name_(arg_) = exs__) ||
    error("invalid @cf function")
  quote
    ex
    $name($arg::ComputeFramework) = ComputeFramework.CFArray($name($arg.head))
  end |> esc
end

Base.getindex(xs::CFArray, args::Integer...) = compute($f($xs.head, args...))

for f in :[getindex map reduce exp log].args
  @eval $f(xs::CFArray, args...) = CFArray($f(xs.head, args...))
end

for f in :[(+) (*)].args
  @eval $f(xs::CFArray, ys::CFArray) = $f(xs, ys)
end

for f in :[(.+) (.*) ].args
  @eval $f(xs::CFArray, ys::Number) = $f(xs, ys)
  @eval $f(xs::Number, ys::CFArray) = $f(xs, ys)
  @eval $f(xs::CFArray, ys::CFArray) = $f(xs, ys)
end
