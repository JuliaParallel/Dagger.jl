# Distributed stencil (@stencil) benchmark suite.
#
# Exercises Dagger's @stencil macro over DArrays: simple assignments, neighborhood
# reductions with various boundary conditions, functional allocation syntax, update
# operators, and multi-expression blocks. Operands are allocated inside each
# benchmark's `setup` (and freed in `teardown`) so only the running size is
# resident; sizes whose estimated peak allocation exceeds the memory budget are
# skipped.
#
# Sparse variants of the neighborhood kernels run the same stencil over sparse
# tiles, and are gated the same way.

import Dagger: @stencil, Wrap, Pad, Reflect, Clamp

@everywhere using SparseArrays

# Fraction of stored entries in a sparse stencil operand. Chosen below the point
# where restricting the sweep to the dilated nonzero pattern stops paying --
# roughly `(2*neigh_dist+1)^-ndims`, about 11% for the 3x3 2-D stencils here.
const stencil_density = parse(Float64, get(ENV, "BENCHMARK_STENCIL_DENSITY", "0.01"))

# BenchmarkTools cannot parse @stencil inside @benchmarkable, so each kernel is
# wrapped in a plain function.
function stencil_assign!(B, ::Type{T}) where {T}
    @stencil B[idx] = one(T)
    return B
end

function stencil_neighbors_wrap!(A, B)
    @stencil B[idx] = sum(@neighbors(A[idx], 1, Wrap()))
    return B
end

function stencil_neighbors_pad!(A, B)
    @stencil B[idx] = sum(@neighbors(A[idx], 1, Pad(0)))
    return B
end

function stencil_neighbors_clamp!(A, B)
    @stencil B[idx] = sum(@neighbors(A[idx], 1, Clamp()))
    return B
end

function stencil_neighbors_reflect!(A, B)
    @stencil B[idx] = sum(@neighbors(A[idx], 1, Reflect(true)))
    return B
end

function stencil_alloc_neighbors_wrap(A)
    return @stencil sum(@neighbors(A[idx], 1, Wrap()))
end

function stencil_alloc_neighbors_pad(A)
    return @stencil sum(@neighbors(A[idx], 1, Pad(0)))
end

function stencil_update_plus!(A, B)
    @stencil B[idx] = B[idx] + A[idx]
    return B
end

function stencil_multi_expr!(A, B, ::Type{T}) where {T}
    @stencil begin
        A[idx] = one(T)
        B[idx] = A[idx] * 2
    end
    return B
end

# The `zero_preserving=true` option is newer than `@stencil` itself, and an
# unrecognized option is a *macroexpansion* error -- which `supported` cannot
# catch, since it would abort this file's load rather than a benchmark's run. So
# define these kernels through `@eval` and treat a failure as "baseline lacks the
# option", the same way `supported` treats a failing probe.
const zero_preserving_ok = try
    @eval function stencil_neighbors_pad_zp!(A, B)
        @stencil zero_preserving=true B[idx] = sum(@neighbors(A[idx], 1, Pad(0)))
        return B
    end
    @eval function stencil_alloc_neighbors_pad_zp(A)
        return @stencil zero_preserving=true sum(@neighbors(A[idx], 1, Pad(0)))
    end
    true
catch err
    @warn "Skipping unsupported benchmark(s): stencil/sparse (zero_preserving)" exception = (err, catch_backtrace())
    false
end

# Sparse operands, built on the driver and distributed -- mirroring the sparse
# linalg suite. `sprand`/`spzeros` have `Blocks`-aware methods, but going through
# `distribute` keeps this runnable on a baseline revision that lacks them.
sparse_rand(T, N, b, density) = distribute(sprand(T, N, N, density), Blocks(b, b))
sparse_zeros(T, N, b) = distribute(spzeros(T, N, N), Blocks(b, b))

function stencil_suite(ctx; method, accels)
    @assert method == "dagger" "Stencil suite only supports `dagger` execution"
    accel = isempty(accels) ? "cpu" : only(accels)
    @assert accel == "cpu" "Stencil suite only supports CPU execution"

    T = Float64
    suite = BenchmarkGroup()

    # Capability probes (run once, at a tiny size). @stencil is a relatively new
    # Dagger feature: a baseline revision in an AirspeedVelocity comparison may
    # lack it entirely, in which case running the kernel would abort the whole
    # benchmark run.
    assign_ok = supported("stencil/assign (const)") do
        B = zeros(Blocks(2, 2), T, 8, 8)
        stencil_assign!(B, T)
    end
    wrap_ok = supported("stencil/neighbors (Wrap)") do
        A = ones(Blocks(2, 2), T, 8, 8)
        B = zeros(Blocks(2, 2), T, 8, 8)
        stencil_neighbors_wrap!(A, B)
    end
    pad_ok = supported("stencil/neighbors (Pad)") do
        A = ones(Blocks(2, 2), T, 8, 8)
        B = zeros(Blocks(2, 2), T, 8, 8)
        stencil_neighbors_pad!(A, B)
    end
    clamp_ok = supported("stencil/neighbors (Clamp)") do
        A = ones(Blocks(2, 2), T, 8, 8)
        B = zeros(Blocks(2, 2), T, 8, 8)
        stencil_neighbors_clamp!(A, B)
    end
    reflect_ok = supported("stencil/neighbors (Reflect)") do
        A = ones(Blocks(2, 2), T, 8, 8)
        B = zeros(Blocks(2, 2), T, 8, 8)
        stencil_neighbors_reflect!(A, B)
    end
    alloc_ok = supported("stencil/alloc (neighbors Wrap)") do
        A = ones(Blocks(2, 2), T, 8, 8)
        wait(stencil_alloc_neighbors_wrap(A))
    end
    update_ok = supported("stencil/update (+)") do
        A = ones(Blocks(2, 2), T, 8, 8)
        B = zeros(Blocks(2, 2), T, 8, 8)
        stencil_update_plus!(A, B)
    end
    multi_ok = supported("stencil/multi-expr") do
        A = zeros(Blocks(2, 2), T, 8, 8)
        B = zeros(Blocks(2, 2), T, 8, 8)
        stencil_multi_expr!(A, B, T)
    end

    sparse_ok = supported("stencil/sparse (neighbors Pad)") do
        A = sparse_rand(T, 8, 4, 0.5)
        B = sparse_zeros(T, 8, 4)
        stencil_neighbors_pad!(A, B)
    end
    sparse_zp_ok = zero_preserving_ok && supported("stencil/sparse (zero_preserving)") do
        A = sparse_rand(T, 8, 4, 0.5)
        B = sparse_zeros(T, 8, 4)
        stencil_neighbors_pad_zp!(A, B)
    end
    sparse_alloc_ok = supported("stencil/sparse alloc (Pad)") do
        A = sparse_rand(T, 8, 4, 0.5)
        wait(stencil_alloc_neighbors_pad(A))
    end
    sparse_zp_alloc_ok = zero_preserving_ok && supported("stencil/sparse alloc (zero_preserving)") do
        A = sparse_rand(T, 8, 4, 0.5)
        wait(stencil_alloc_neighbors_pad_zp(A))
    end

    for N in scales
        b = square_block(N)
        sub = BenchmarkGroup()

        # In-place stencils hold at most the input plus a same-size result.
        if fits_budget(dense_bytes(N; nmats=2, T=T))
            if assign_ok
                sub["assign (const)"] = @benchmarkable(stencil_assign!(B, $T),
                    setup = (B = zeros(Blocks($b, $b), $T, $N, $N); wait(B)),
                    teardown = (B = nothing; @everywhere GC.gc()))
            end

            if wrap_ok
                sub["neighbors (Wrap)"] = @benchmarkable(stencil_neighbors_wrap!(A, B),
                    setup = (A = ones(Blocks($b, $b), $T, $N, $N); B = zeros(Blocks($b, $b), $T, $N, $N); wait(A)),
                    teardown = (A = nothing; B = nothing; @everywhere GC.gc()))
            end

            if pad_ok
                sub["neighbors (Pad)"] = @benchmarkable(stencil_neighbors_pad!(A, B),
                    setup = (A = ones(Blocks($b, $b), $T, $N, $N); B = zeros(Blocks($b, $b), $T, $N, $N); wait(A)),
                    teardown = (A = nothing; B = nothing; @everywhere GC.gc()))
            end

            if clamp_ok
                sub["neighbors (Clamp)"] = @benchmarkable(stencil_neighbors_clamp!(A, B),
                    setup = (A = ones(Blocks($b, $b), $T, $N, $N); B = zeros(Blocks($b, $b), $T, $N, $N); wait(A)),
                    teardown = (A = nothing; B = nothing; @everywhere GC.gc()))
            end

            if reflect_ok
                sub["neighbors (Reflect)"] = @benchmarkable(stencil_neighbors_reflect!(A, B),
                    setup = (A = ones(Blocks($b, $b), $T, $N, $N); B = zeros(Blocks($b, $b), $T, $N, $N); wait(A)),
                    teardown = (A = nothing; B = nothing; @everywhere GC.gc()))
            end

            if update_ok
                sub["update (+)"] = @benchmarkable(stencil_update_plus!(A, B),
                    setup = (A = ones(Blocks($b, $b), $T, $N, $N); B = zeros(Blocks($b, $b), $T, $N, $N); wait(A)),
                    teardown = (A = nothing; B = nothing; @everywhere GC.gc()))
            end

            if multi_ok
                sub["multi-expr"] = @benchmarkable(stencil_multi_expr!(A, B, $T),
                    setup = (A = zeros(Blocks($b, $b), $T, $N, $N);
                             B = zeros(Blocks($b, $b), $T, $N, $N); wait(A)),
                    teardown = (A = nothing; B = nothing; @everywhere GC.gc()))
            end
        end

        # Sparse tiles, same kernel and same blocking as the dense entries above,
        # so the two are directly comparable at a given N. Budgeted against
        # *dense* bytes despite the sparse storage: an unrestricted sweep visits
        # every index of every tile, so its cost tracks N^2 rather than the
        # nonzero count, and this keeps that work bounded on the same ladder the
        # dense entries stop at.
        if fits_budget(dense_bytes(N; nmats=2, T=T))
            if sparse_ok
                sub["sparse neighbors (Pad)"] = @benchmarkable(stencil_neighbors_pad!(A, B),
                    setup = (A = sparse_rand($T, $N, $b, $stencil_density);
                             B = sparse_zeros($T, $N, $b); wait(A)),
                    teardown = (A = nothing; B = nothing; @everywhere GC.gc()))
            end

            # Same kernel and same result as the entry above; the option asserts
            # the kernel is zero-preserving, which lets the sweep skip every index
            # outside the operands' nonzero pattern dilated by `neigh_dist`.
            if sparse_zp_ok
                sub["sparse neighbors (Pad, zero_preserving)"] = @benchmarkable(stencil_neighbors_pad_zp!(A, B),
                    setup = (A = sparse_rand($T, $N, $b, $stencil_density);
                             B = sparse_zeros($T, $N, $b); wait(A)),
                    teardown = (A = nothing; B = nothing; @everywhere GC.gc()))
            end
        end

        # Out-of-place sparse: allocates its sparse output as well as filling it.
        if fits_budget(dense_bytes(N; nmats=3, T=T))
            if sparse_alloc_ok
                sub["sparse alloc (Pad)"] = @benchmarkable(wait(stencil_alloc_neighbors_pad(A)),
                    setup = (A = sparse_rand($T, $N, $b, $stencil_density); wait(A)),
                    teardown = (A = nothing; @everywhere GC.gc()))
            end

            if sparse_zp_alloc_ok
                sub["sparse alloc (Pad, zero_preserving)"] = @benchmarkable(wait(stencil_alloc_neighbors_pad_zp(A)),
                    setup = (A = sparse_rand($T, $N, $b, $stencil_density); wait(A)),
                    teardown = (A = nothing; @everywhere GC.gc()))
            end
        end

        # Functional allocation syntax also materializes an output DArray.
        if alloc_ok && fits_budget(dense_bytes(N; nmats=3, T=T))
            sub["alloc (neighbors Wrap)"] = @benchmarkable(wait(stencil_alloc_neighbors_wrap(A)),
                setup = (A = ones(Blocks($b, $b), $T, $N, $N); wait(A)),
                teardown = (A = nothing; @everywhere GC.gc()))
        end

        isempty(sub) || (suite["N=$N (block $b)"] = sub)
    end

    suite
end

stencil_suite
