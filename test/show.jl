a = rand(10,10)
b = distribute(a)
c = ComputeFramework.map(x -> x^2, b)
d = reduce(+, 0.0, c)

@test show(IOBuffer(), b) == 1
@test show(IOBuffer(), c) == 1
@test show(IOBuffer(), d) == 1
