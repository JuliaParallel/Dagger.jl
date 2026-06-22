# Distributed array (DArray) benchmark suite.
#
# Exercises Dagger's eager elementwise/reduction DArray operations (linear
# algebra lives in the `linalg`/`sparse` suites). Operands are allocated inside
# each benchmark's `setup` (and freed in `teardown`) so only the running size is
# resident; sizes whose estimated peak allocation exceeds the memory budget are
# skipped.

function array_suite(ctx; method, accels)
    @assert method == "dagger" "Array suite only supports `dagger` execution"
    accel = isempty(accels) ? "cpu" : only(accels)
    @assert accel == "cpu" "Array suite only supports CPU execution"

    T = Float64
    suite = BenchmarkGroup()

    for N in scales
        # Elementwise ops hold at most the input plus a same-size result.
        fits_budget(dense_bytes(N; nmats=2, T=T)) || continue
        b = square_block(N)
        sub = BenchmarkGroup()

        sub["alloc (rand)"] = @benchmarkable(wait(rand(Blocks($b, $b), $T, $N, $N)),
            teardown = (@everywhere GC.gc()))

        sub["broadcast (X .+ 1)"] = @benchmarkable(wait(X .+ 1),
            setup = (X = rand(Blocks($b, $b), $T, $N, $N); wait(X)),
            teardown = (X = nothing; @everywhere GC.gc()))

        sub["add (X + X)"] = @benchmarkable(wait(X + X),
            setup = (X = rand(Blocks($b, $b), $T, $N, $N); wait(X)),
            teardown = (X = nothing; @everywhere GC.gc()))

        sub["map (sin.(X))"] = @benchmarkable(wait(sin.(X)),
            setup = (X = rand(Blocks($b, $b), $T, $N, $N); wait(X)),
            teardown = (X = nothing; @everywhere GC.gc()))

        sub["transpose (permutedims)"] = @benchmarkable(wait(permutedims(X)),
            setup = (X = rand(Blocks($b, $b), $T, $N, $N); wait(X)),
            teardown = (X = nothing; @everywhere GC.gc()))

        sub["reduce (sum)"] = @benchmarkable(sum(X),
            setup = (X = rand(Blocks($b, $b), $T, $N, $N); wait(X)),
            teardown = (X = nothing; @everywhere GC.gc()))

        sub["norm"] = @benchmarkable(norm(X),
            setup = (X = rand(Blocks($b, $b), $T, $N, $N); wait(X)),
            teardown = (X = nothing; @everywhere GC.gc()))

        suite["N=$N (block $b)"] = sub
    end

    suite
end

array_suite
