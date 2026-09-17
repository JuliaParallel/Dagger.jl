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
    # The named cyclic grids are currently backed by Distributed processors;
    # an MPI worker has no Distributed workers and must use the MPI-aware
    # arbitrary allocator instead.
    fixture_assignment = length(procs()) > 1 ? :cyclicrow : :arbitrary

    for N in scales
        # Elementwise ops hold at most the input plus a same-size result.
        fits_budget(dense_bytes(N; nmats=2, T=T)) || continue
        for b in blocks_for(N)
            sub = BenchmarkGroup()

            # Multi-process BenchmarkTools only observes allocations made on
            # the driver process. Arbitrary placement therefore turns each
            # driver-owned tile payload into benchmark noise (a 1024² result
            # can vary by several MiB between otherwise-identical revisions).
            # A cyclic grid keeps the Distributed workload balanced and gives
            # both revisions the same share of driver-local tiles.
            sub["alloc (rand)"] = @benchmarkable(wait(rand(Blocks($b, $b), $T, $N, $N; assignment=$fixture_assignment)),
                teardown = (@everywhere GC.gc()))

            sub["broadcast (X .+ 1)"] = @benchmarkable(wait(X .+ 1),
                setup = (X = rand(Blocks($b, $b), $T, $N, $N; assignment=$fixture_assignment); wait(X)),
                teardown = (X = nothing; @everywhere GC.gc()))

            sub["add (X + X)"] = @benchmarkable(wait(X + X),
                setup = (X = rand(Blocks($b, $b), $T, $N, $N; assignment=$fixture_assignment); wait(X)),
                teardown = (X = nothing; @everywhere GC.gc()))

            sub["map (sin.(X))"] = @benchmarkable(wait(sin.(X)),
                setup = (X = rand(Blocks($b, $b), $T, $N, $N; assignment=$fixture_assignment); wait(X)),
                teardown = (X = nothing; @everywhere GC.gc()))

            sub["transpose (permutedims)"] = @benchmarkable(wait(permutedims(X)),
                setup = (X = rand(Blocks($b, $b), $T, $N, $N; assignment=$fixture_assignment); wait(X)),
                teardown = (X = nothing; @everywhere GC.gc()))

            sub["reduce (sum)"] = @benchmarkable(sum(X),
                setup = (X = rand(Blocks($b, $b), $T, $N, $N; assignment=$fixture_assignment); wait(X)),
                teardown = (X = nothing; @everywhere GC.gc()))

            sub["norm"] = @benchmarkable(norm(X),
                setup = (X = rand(Blocks($b, $b), $T, $N, $N; assignment=$fixture_assignment); wait(X)),
                teardown = (X = nothing; @everywhere GC.gc()))

            suite["N=$N (block $b)"] = sub
        end
    end

    suite
end

array_suite
