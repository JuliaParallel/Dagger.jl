addprocs(1)

using ComputeFramework
using Base.Test

x = rand(100, 100)
@test gather(Context(), map(-, distribute(x))) == -x


# Type-Layout tests
@everywhere type testtype
    a::Vector
    b::Vector{Vector}
    c::AbstractArray{Int, 2}
end
isequal(x::testtype, y::testtype) = x.a == y.a && x.b == y.b && x.c == y.c
obj = testtype(ones(10), [ones(10) for i in 1:10], ones(Int, 10, 10))
layout = typelayout(testtype, [cutdim(1), cutdim(1), cutdim(2)])
dobj = compute(Context(), distribute(obj, layout))
nobj = gather(Context(), dobj)
@test isequal(obj, nobj)
