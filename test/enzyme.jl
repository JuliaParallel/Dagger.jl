# Autodiff (Enzyme) tests.
#
# These exercise `value_and_gradient(f, AutoEnzyme(), x::DArray)`, which
# differentiates arbitrary Dagger task graphs (functional *and* mutating/
# DataDeps regions) by sitting an `EnzymeTaskQueue` below the user's code and
# reading the In/Out/InOut access metadata Dagger records into task options.
#
# Enzyme is only a weak dependency and is slow to load, so this file runs in its
# own CI job (which `Pkg.add`s Enzyme + DifferentiationInterface on demand) and
# is gated behind `CI_ENZYME=1` in `runtests.jl`.

using Enzyme
using DifferentiationInterface
# Load the extension (and Enzyme's rules) on every worker so reverse-pass VJP
# kernels can run wherever Dagger schedules them.
@everywhere using Enzyme
@everywhere using DifferentiationInterface

using LinearAlgebra
using Random
import Dagger: @stencil, Pad
Random.seed!(1234)

# Central finite-difference gradient of a scalar function, used as the reference.
function fd_grad(fscalar, A; h=1e-6)
    g = zeros(float(eltype(A)), size(A))
    @inbounds for i in eachindex(A)
        Ap = copy(A); Ap[i] += h
        Am = copy(A); Am[i] -= h
        g[i] = (fscalar(Ap) - fscalar(Am)) / (2h)
    end
    return g
end

# Compare the Dagger/Enzyme gradient to finite differences.
#
# `fdag` runs on a `DArray`. When it returns a scalar (a reduction), the gradient
# is of that scalar. When it returns a `DArray`/factorization (a linear-algebra
# result), `value_and_gradient` seeds its outputs with ones, so the gradient is
# of `sum(outputs)`. `fref` is the matching *dense scalar* reference.
function grad_matches_fd(fdag, fref, A, blocks; rtol=1e-3, atol=1e-6)
    DA = distribute(A, blocks)
    _, grad = value_and_gradient(fdag, AutoEnzyme(), DA)
    return isapprox(collect(grad), fd_grad(fref, A); rtol, atol)
end

# Also check that the returned primal value is correct, for scalar reductions.
function value_matches(fdag, fref, A, blocks; rtol=1e-8)
    DA = distribute(A, blocks)
    val, _ = value_and_gradient(fdag, AutoEnzyme(), DA)
    return isapprox(val, fref(A); rtol)
end

@testset "Allocation and copies" begin
    A = rand(8, 8)
    @test grad_matches_fd(x -> sum(copy(x)), x -> sum(copy(x)), A, Blocks(4, 4))
    # Allocates a fresh array then reduces it.
    @test grad_matches_fd(x -> sum(map(t -> t + 1.0, x)),
                          x -> sum(map(t -> t + 1.0, x)), A, Blocks(4, 4))
    @test grad_matches_fd(x -> sum(2 .* x), x -> sum(2 .* x), A, Blocks(4, 4))
end

@testset "Reductions: sum / prod / mapreduce" begin
    A = rand(8, 8)
    @test value_matches(x -> sum(x), x -> sum(x), A, Blocks(4, 4))
    @test grad_matches_fd(x -> sum(x), x -> sum(x), A, Blocks(4, 4))
    @test grad_matches_fd(x -> sum(abs2, x), x -> sum(abs2, x), A, Blocks(4, 4))

    # `prod` over a modest vector keeps the product (and its gradient) O(1) so
    # finite differences stay well-conditioned.
    v = rand(6) .+ 0.8
    @test value_matches(x -> prod(x), x -> prod(x), v, Blocks(3))
    @test grad_matches_fd(x -> prod(x), x -> prod(x), v, Blocks(3))

    # User-specified reduction function via `mapreduce`.
    @test grad_matches_fd(x -> mapreduce(t -> t^2, +, x),
                          x -> mapreduce(t -> t^2, +, x), A, Blocks(4, 4))
end

@testset "User-specified map functions" begin
    A = rand(8, 8)
    @test grad_matches_fd(x -> sum(map(sin, x)), x -> sum(map(sin, x)), A, Blocks(4, 4))
    @test grad_matches_fd(x -> sum(map(t -> t^3 - 2t, x)),
                          x -> sum(map(t -> t^3 - 2t, x)), A, Blocks(4, 4))
end

@testset "Broadcasting" begin
    A = rand(8, 8)
    @test grad_matches_fd(x -> sum(x .* x .+ 1), x -> sum(x .* x .+ 1), A, Blocks(4, 4))
    @test grad_matches_fd(x -> sum(sin.(x)), x -> sum(sin.(x)), A, Blocks(4, 4))
    @test grad_matches_fd(x -> sum((x .- 1) .^ 2), x -> sum((x .- 1) .^ 2), A, Blocks(4, 4))
end

@testset "GEMM (matrix-matrix multiply)" begin
    A = rand(8, 8)
    Bm = rand(8, 8)
    Bd = distribute(Bm, Blocks(4, 4))
    # Differentiate sum(x*B) and sum(B*x) w.r.t. x (primal output seeded ones).
    @test grad_matches_fd(x -> x * Bd, x -> sum(x * Bm), A, Blocks(4, 4))
    @test grad_matches_fd(x -> Bd * x, x -> sum(Bm * x), A, Blocks(4, 4))
end

@testset "GEMV (matrix-vector multiply)" begin
    A = rand(8, 8)
    vm = rand(8)
    vd = distribute(vm, Blocks(4))
    @test grad_matches_fd(x -> x * vd, x -> sum(x * vm), A, Blocks(4, 4))
end

@testset "LU factorization (NoPivot)" begin
    # Well-conditioned so unpivoted LU is stable; custom Enzyme rule for
    # `generic_lufact!` drives the per-block factorization.
    A = rand(8, 8) + 8I
    fref(x) = (F = copy(x); LinearAlgebra.generic_lufact!(F, NoPivot(); check=false); sum(F))
    @test grad_matches_fd(x -> lu(x, NoPivot()), fref, A, Blocks(4, 4); rtol=1e-3)
end

@testset "Cholesky factorization" begin
    A = rand(8, 8)
    SPD = Matrix(A'A + 8I)
    fref(x) = (F = copy(x); LinearAlgebra._chol!(F, UpperTriangular); sum(UpperTriangular(F)))
    # TODO: needs an Enzyme reverse rule for the per-block `potrf!` kernel (and
    # differentiable `syrk!`/`herk!`), analogous to the `generic_lufact!` rule.
    @test_broken grad_matches_fd(x -> cholesky(x), fref, SPD, Blocks(4, 4); rtol=1e-3)
end

@testset "QR factorization" begin
    A = rand(8, 8)
    # TODO: Householder-based QR kernels currently differentiate to an incorrect
    # adjoint; needs dedicated reverse rules.
    @test_broken grad_matches_fd(x -> qr(x), x -> sum(qr(x).R), A, Blocks(4, 4); rtol=1e-3)
end

@testset "Linear solve" begin
    A = rand(8, 8)
    SPD = Matrix(A'A + 8I)
    bm = rand(8)
    bd = distribute(bm, Blocks(4))
    # TODO: the triangular-solve / factorization kernels backing `\` don't yet
    # have working reverse rules (fails in the reverse VJP); needs dedicated
    # rules like the `generic_lufact!` one.
    @test_broken grad_matches_fd(x -> x \ bd, x -> sum(x \ bm), SPD, Blocks(4, 4); rtol=1e-3)
end

@testset "Stencils" begin
    # A simple linear stencil (sum of a Pad(0) neighborhood) written through the
    # `@stencil` DataDeps macro; differentiate sum of the output w.r.t. input.
    function stencil_apply(A)
        B = zeros(Blocks(4, 4), eltype(A), size(A)...)
        Dagger.spawn_datadeps() do
            @stencil B[idx] = sum(@neighbors(A[idx], 1, Pad(0)))
        end
        return B
    end
    function stencil_ref(A)
        n, m = size(A)
        s = 0.0
        for i in 1:n, j in 1:m
            for di in -1:1, dj in -1:1
                ni, nj = i + di, j + dj
                if 1 <= ni <= n && 1 <= nj <= m
                    s += A[ni, nj]
                end
            end
        end
        return s
    end
    A = rand(8, 8)
    # TODO: verify the @stencil kernel differentiates; marked broken pending
    # confirmation in CI.
    @test_broken grad_matches_fd(stencil_apply, stencil_ref, A, Blocks(4, 4); rtol=1e-3)
end
