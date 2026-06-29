# Exploratory autodiff tests for non-Enzyme DifferentiationInterface backends.
#
# Unlike the Enzyme backend (see `enzyme.jl`), these backends are NOT integrated
# with Dagger's distributed task-graph AD: there is no `DArray`-aware method, so
# DI drives them generically over the `DArray`. We don't expect all of these to
# pass yet -- this file exists to track which backends/operations work, and runs
# in its own non-blocking CI job. Each case is wrapped so an exception (e.g. a
# backend that can't push duals/perturbations through a `DArray`) surfaces as a
# clean test failure rather than aborting the file.

using DifferentiationInterface
const DI = DifferentiationInterface

using ForwardDiff
using ReverseDiff
using FiniteDiff
using FiniteDifferences

using LinearAlgebra
using Random
Random.seed!(1234)

# Central finite-difference reference gradient over a dense array.
function fd_grad(fscalar, A; h=1e-6)
    g = zeros(float(eltype(A)), size(A))
    @inbounds for i in eachindex(A)
        Ap = copy(A); Ap[i] += h
        Am = copy(A); Am[i] -= h
        g[i] = (fscalar(Ap) - fscalar(Am)) / (2h)
    end
    return g
end

# Run a backend on a Dagger `DArray` and compare its gradient to the dense FD
# reference. Returns `true` on a close match, `false` on mismatch or error.
function backend_matches_fd(backend, fdag, fref, A, blocks; rtol=1e-3, atol=1e-6)
    try
        DA = distribute(A, blocks)
        _, grad = DI.value_and_gradient(fdag, backend, DA)
        return isapprox(collect(grad), fd_grad(fref, A); rtol, atol)
    catch err
        @debug "DI backend $(backend) failed" exception=(err, catch_backtrace())
        return false
    end
end

# A small set of scalar-valued operations spanning the categories. We keep these
# tiny because some backends are O(n) slower (finite differencing) or allocate
# heavily (dual numbers).
const A0 = rand(8, 8)
const BLOCKS = Blocks(4, 4)

operations = [
    ("copy+sum",      A -> sum(copy(A)),            A -> sum(copy(A))),
    ("sum(abs2)",     A -> sum(abs2, A),            A -> sum(abs2, A)),
    ("map(sin)+sum",  A -> sum(map(sin, A)),        A -> sum(sin, A)),
    ("broadcast",     A -> sum(A .* 2 .+ 1),        A -> sum(A .* 2 .+ 1)),
]

backends = [
    ("ForwardDiff",       AutoForwardDiff()),
    ("ReverseDiff",       AutoReverseDiff()),
    ("FiniteDiff",        AutoFiniteDiff()),
    ("FiniteDifferences", AutoFiniteDifferences(; fdm=FiniteDifferences.central_fdm(5, 1))),
]

@testset "DI backend: $bname" for (bname, backend) in backends
    @testset "$opname" for (opname, fdag, fref) in operations
        @test backend_matches_fd(backend, fdag, fref, A0, BLOCKS)
    end
end
