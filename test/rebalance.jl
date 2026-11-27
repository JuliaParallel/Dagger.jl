# Dagger.rebalance_plan: a rank-consistent assignment of chunks from costs.
using Random

# The per-rank load after `plan` has been applied
function plan_loads(owners, costs, nranks, plan)
    o = collect(owners)
    for (i, r) in plan
        o[i] = r
    end
    loads = zeros(Float64, nranks)
    for i in eachindex(o)
        loads[o[i] + 1] += costs[i]
    end
    return loads
end

own_after(own, plan, i) = (j = findfirst(p -> first(p) == i, plan); j === nothing ? own[i] : last(plan[j]))

@testset "rebalance_plan: pure, deterministic, sticky" begin
    # balanced: nothing moves
    @test isempty(Dagger.rebalance_plan([0, 0, 1, 1], [1.0, 1.0, 1.0, 1.0], 2))
    # all the work on rank 0: half of it moves, and which chunks is fixed (ties go to the lowest index)
    @test Dagger.rebalance_plan([0, 0, 0, 0], [1.0, 1.0, 1.0, 1.0], 2) == [1 => 1, 2 => 1]
    @test sort(last.(Dagger.rebalance_plan([0, 0, 0, 0, 0, 0], fill(1.0, 6), 3))) == [1, 1, 2, 2]
    # the chunk closest to half of the gap moves (not the first, not the biggest)
    @test Dagger.rebalance_plan([0, 0, 1], [5.0, 3.0, 1.0], 2) == [2 => 1]
    @test Dagger.rebalance_plan([0, 0, 0, 0], [1.0, 4.0, 6.0, 10.0], 2) == [4 => 1]
    # a chunk that is not smaller than the gap never moves, however much the ranks differ
    @test isempty(Dagger.rebalance_plan([0], [10.0], 2))                           # one indivisible chunk: nothing to gain
    @test length(Dagger.rebalance_plan([0, 0], [10.0, 10.0], 2; max_moves=5)) == 1   # one 10 moves, then it is balanced
    # a chunk without cost does not move (it would change nothing)
    @test Dagger.rebalance_plan([0, 0, 0], [0.0, 1.0, 1.0], 2) == [2 => 1]
    @test isempty(Dagger.rebalance_plan([0, 0], [0.0, 0.1], 2))
    @test isempty(Dagger.rebalance_plan([0, 0, 1], [0.0, 0.0, 0.0], 2))            # nothing to balance
    @test isempty(Dagger.rebalance_plan(Int[], Float64[], 3))
    @test isempty(Dagger.rebalance_plan([0, 1], [10.0, 1.0], 2; tolerance=100))    # within the (huge) tolerance
    # tolerance: loads 6 and 2 (mean 4, ratio 1.5); the move is made at 1.4 and not at 1.6, and at exactly 1.5 it is not
    @test Dagger.rebalance_plan([0, 0, 0, 1, 1], [2.0, 2.0, 2.0, 1.0, 1.0], 2; tolerance=1.4) == [1 => 1]
    @test isempty(Dagger.rebalance_plan([0, 0, 0, 1, 1], [2.0, 2.0, 2.0, 1.0, 1.0], 2; tolerance=1.6))
    @test isempty(Dagger.rebalance_plan([0, 0, 0, 1, 1], [2.0, 2.0, 2.0, 1.0, 1.0], 2; tolerance=1.5))
    # max_moves
    @test length(Dagger.rebalance_plan(zeros(Int, 6), fill(1.0, 6), 3; max_moves=1)) == 1
    @test length(Dagger.rebalance_plan(zeros(Int, 6), fill(1.0, 6), 3; max_moves=3)) == 3
    @test isempty(Dagger.rebalance_plan(zeros(Int, 6), fill(1.0, 6), 3; max_moves=0))
    # integer costs and owners of any integer type
    @test Dagger.rebalance_plan(Int8[0, 0, 0, 0], [1, 1, 1, 1], 2) == [1 => 1, 2 => 1]
    # pure: the arguments are not touched
    owners = [0, 0, 0, 0]; costs = [1.0, 1.0, 1.0, 1.0]
    Dagger.rebalance_plan(owners, costs, 2)
    @test owners == [0, 0, 0, 0] && costs == [1.0, 1.0, 1.0, 1.0]
    # deterministic, also where several ranks tie for the least loaded one
    first_plan = Dagger.rebalance_plan(zeros(Int, 9), fill(1.0, 9), 4)
    @test all(_ -> Dagger.rebalance_plan(zeros(Int, 9), fill(1.0, 9), 4) == first_plan, 1:100)
    # a chunk is listed once, at its final rank, and only if that is not where it is; the most loaded rank never gets more
    # loaded; the plan is the same when asked again; the number of moves stays within `max_moves`
    rng = MersenneTwister(1)
    for trial in 1:300
        nr = rand(rng, 1:6); n = rand(rng, 0:25)
        own = rand(rng, 0:(nr - 1), n)
        cost = rand(rng, (0.0, 0.5, 1.0, 2.0, 3.7, 10.0), n)
        mm = rand(rng, (0, 1, 3, n))
        plan = Dagger.rebalance_plan(own, cost, nr; max_moves=mm)
        @test plan == Dagger.rebalance_plan(own, cost, nr; max_moves=mm)
        idx = first.(plan)
        ok = allunique(idx) && all(p -> 0 <= last(p) < nr && last(p) != own[first(p)], plan) && length(plan) <= mm
        before = zeros(nr); for i in 1:n; before[own[i] + 1] += cost[i]; end
        after = plan_loads(own, cost, nr, plan)
        ok &= maximum(after; init=0.0) <= maximum(before; init=0.0) + 1e-9 && sum(after) ≈ sum(before)
        # without a limit the most loaded rank ends within the tolerance, or it cannot be improved by moving one chunk
        if mm == n && n > 0 && sum(before) > 0
            hi = argmax(after)
            if after[hi] > 1.05 * sum(after) / nr
                lo = argmin(after); gap = after[hi] - after[lo]
                ok &= !any(i -> own_after(own, plan, i) == hi - 1 && 0 < cost[i] < gap, 1:n)
            end
        end
        @test ok
    end
    # arguments are checked
    @test_throws ArgumentError Dagger.rebalance_plan([0, 3], [1.0, 1.0], 2)       # owner outside the ranks
    @test_throws ArgumentError Dagger.rebalance_plan([0, -1], [1.0, 1.0], 2)
    @test_throws ArgumentError Dagger.rebalance_plan([0, 1], [1.0, -1.0], 2)      # negative cost
    @test_throws ArgumentError Dagger.rebalance_plan([0, 1], [1.0, NaN], 2)
    @test_throws ArgumentError Dagger.rebalance_plan([0, 1], [1.0, Inf], 2)
    @test_throws ArgumentError Dagger.rebalance_plan([0, 1], [1.0], 2)            # lengths differ
    @test_throws ArgumentError Dagger.rebalance_plan(Int[], Float64[], 0)          # no ranks
    @test_throws ArgumentError Dagger.rebalance_plan([0, 1], [1.0, 1.0], 0)
end

@testset "rebalance: needs MPI" begin
    @test_throws ArgumentError Dagger.rebalance(Dagger.Chunk[], Float64[])
    @test_throws ArgumentError Dagger.rebalance([Dagger.tochunk(1)], [1.0]; tolerance=1.1)
    @test Dagger.owner_rank(Dagger.tochunk(1)) == 0
end
