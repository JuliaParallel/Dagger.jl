addprocs(1)

using ComputeFramework
using Base.Test

x = rand(100, 100)
@test gather(Context(), map(-, distribute(x))) == -x

numbers = [1, 2, 3 ,4, 5, 6, 7, 8, 9, 10]

# Test for reduce
@test compute(Context(), reduce(+, 0, distribute(numbers))) == 55

# Test for filter
@test gather(Context(), filter(x -> x>3, distribute(numbers))) == [4,5,6,7,8,9,10]
