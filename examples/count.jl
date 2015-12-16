addprocs(2)

# Demonstration of accumulator
# Simple example of summing squares

using ComputeFramework

acc = Accumulator(+, 0.0)
@par for i=distribute(1:10^2)
    accumulate!(acc, i*i)
end

@show get(acc)
