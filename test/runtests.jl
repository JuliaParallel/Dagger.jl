addprocs(1)

using ComputeFramework
using Base.Test

x = rand(100, 100)
@test gather(Context(), map(-, distribute(x))) == -x
