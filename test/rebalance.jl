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

# Ranks of different speed, moves that must pay for themselves, and the loads a plan predicts.
@testset "rebalance_plan: speeds" begin
    # work 12 on a rank of half the speed (rank 0), none on rank 1: rank 1 ends with twice the work of rank 0
    own = zeros(Int, 12); cost = fill(1.0, 12)
    plan = Dagger.rebalance_plan(own, cost, 2; speeds=[0.5, 1.0])
    work = plan_loads(own, cost, 2, plan)
    @test work == [4.0, 8.0]
    @test Dagger.rebalance_loads(own, cost, 2; speeds=[0.5, 1.0], plan) == [8.0, 8.0]   # both need the same time
    # only the ratios matter
    @test Dagger.rebalance_plan(own, cost, 2; speeds=[5.0, 10.0]) == plan
    # equal speeds are the same as no speeds
    @test Dagger.rebalance_plan(own, cost, 2; speeds=[3.0, 3.0]) == Dagger.rebalance_plan(own, cost, 2)
    # the fast rank is the one that starts with everything: it keeps twice the work of the slow one
    work = plan_loads(own, cost, 2, Dagger.rebalance_plan(own, cost, 2; speeds=[2.0, 1.0]))
    @test work == [8.0, 4.0]
    # three ranks of speed 1, 2, 3: work follows the speed
    work = plan_loads(zeros(Int, 24), fill(1.0, 24), 3, Dagger.rebalance_plan(zeros(Int, 24), fill(1.0, 24), 3; speeds=[1.0, 2.0, 3.0]))
    @test all(abs.(work ./ [1.0, 2.0, 3.0] .- 4.0) .<= 0.5 * 1.05 * 4.0)
    @test work[1] < work[2] < work[3]
    # a balanced-in-time placement is left alone, and the same placement without speeds is not
    @test isempty(Dagger.rebalance_plan([0, 0, 1, 1, 1, 1], fill(1.0, 6), 2; speeds=[1.0, 2.0]))
    @test !isempty(Dagger.rebalance_plan([0, 0, 1, 1, 1, 1], fill(1.0, 6), 2))
    # a slow rank gives up a chunk that an equally fast one would keep
    @test isempty(Dagger.rebalance_plan([0, 0, 1, 1], fill(1.0, 4), 2))
    @test Dagger.rebalance_plan([0, 0, 1, 1], fill(1.0, 4), 2; speeds=[1.0, 3.0]) |> length >= 1
    # validation
    @test_throws ArgumentError Dagger.rebalance_plan([0, 1], [1.0, 1.0], 2; speeds=[1.0])
    @test_throws ArgumentError Dagger.rebalance_plan([0, 1], [1.0, 1.0], 2; speeds=[1.0, 1.0, 1.0])
    @test_throws ArgumentError Dagger.rebalance_plan([0, 1], [1.0, 1.0], 2; speeds=[1.0, 0.0])
    @test_throws ArgumentError Dagger.rebalance_plan([0, 1], [1.0, 1.0], 2; speeds=[1.0, -2.0])
    @test_throws ArgumentError Dagger.rebalance_plan([0, 1], [1.0, 1.0], 2; speeds=[1.0, NaN])
    @test_throws ArgumentError Dagger.rebalance_plan([0, 1], [1.0, 1.0], 2; speeds=[Inf, 1.0])
    @test_throws ArgumentError Dagger.rebalance_loads([0, 1], [1.0, 1.0], 2; speeds=[1.0, 0.0])
    @test_throws ArgumentError Dagger.rebalance_loads([0, 1], [1.0, 1.0], 2; speeds=[1.0])
    # deterministic
    p1 = Dagger.rebalance_plan(zeros(Int, 9), fill(1.0, 9), 4; speeds=[1.0, 2.0, 2.0, 0.5])
    @test all(_ -> Dagger.rebalance_plan(zeros(Int, 9), fill(1.0, 9), 4; speeds=[1.0, 2.0, 2.0, 0.5]) == p1, 1:100)
end

@testset "rebalance_plan: a move must pay for itself (move_cost, horizon)" begin
    own = [0, 0]; cost = [1.0, 1.0]       # moving one chunk takes 1 off the slower rank (2 -> 1)
    @test Dagger.rebalance_plan(own, cost, 2) == [1 => 1]                       # free move: made
    @test Dagger.rebalance_plan(own, cost, 2; move_cost=0.9) == [1 => 1]        # saves 1, costs 0.9: made
    @test isempty(Dagger.rebalance_plan(own, cost, 2; move_cost=1.1))           # saves 1, costs 1.1: not made
    @test isempty(Dagger.rebalance_plan(own, cost, 2; move_cost=1.0))           # exactly break even: not made
    # the saving counts for every step the placement lasts
    @test Dagger.rebalance_plan(own, cost, 2; move_cost=1.1, horizon=2) == [1 => 1]   # 2 > 1.1
    @test isempty(Dagger.rebalance_plan(own, cost, 2; move_cost=2.0, horizon=2))      # 2 > 2 is false
    @test isempty(Dagger.rebalance_plan(own, cost, 2; move_cost=0.4, horizon=0.25))   # 0.25 < 0.4
    # one cost per chunk: the chunk that is cheap to move is the one that is chosen... or nothing moves
    @test Dagger.rebalance_plan(own, cost, 2; move_cost=[0.5, 5.0]) == [1 => 1]
    @test isempty(Dagger.rebalance_plan(own, cost, 2; move_cost=[5.0, 0.5]))   # the preferred chunk (index 1) is too dear: the plan stops
    # the plan stops at the first move that does not pay, after the ones that did
    own6 = zeros(Int, 6); cost6 = [4.0, 3.0, 2.0, 1.0, 1.0, 1.0]
    free = Dagger.rebalance_plan(own6, cost6, 3)
    @test length(free) == 3
    # the savings of the moves shrink (7, 3 and 1 ... ), so a higher cost allows fewer of them, always a prefix of the free plan
    counts = [length(Dagger.rebalance_plan(own6, cost6, 3; move_cost=m)) for m in (0.5, 1.5, 3.5, 100.0)]
    @test counts == [3, 2, 1, 0]
    @test Dagger.rebalance_plan(own6, cost6, 3; move_cost=1.5) == free[1:2]
    @test length(Dagger.rebalance_plan(own6, cost6, 3; move_cost=100.0, horizon=1000)) == length(free)
    # with speeds the saving is a time: a move to a fast rank saves more than to a slow one
    @test Dagger.rebalance_plan([0, 0], [1.0, 1.0], 2; speeds=[1.0, 4.0], move_cost=0.9) |> length == 1
    # validation
    @test_throws ArgumentError Dagger.rebalance_plan(own, cost, 2; move_cost=-1.0)
    @test_throws ArgumentError Dagger.rebalance_plan(own, cost, 2; move_cost=[1.0])
    @test_throws ArgumentError Dagger.rebalance_plan(own, cost, 2; move_cost=NaN)
    @test_throws ArgumentError Dagger.rebalance_plan(own, cost, 2; horizon=0)
    @test_throws ArgumentError Dagger.rebalance_plan(own, cost, 2; horizon=-1)
    @test_throws ArgumentError Dagger.rebalance_plan(own, cost, 2; horizon=Inf)
end

@testset "rebalance_loads: the predicted per-rank times" begin
    own = [0, 0, 1]; cost = [2.0, 3.0, 1.0]
    @test Dagger.rebalance_loads(own, cost, 2) == [5.0, 1.0]
    @test Dagger.rebalance_loads(own, cost, 2; speeds=[1.0, 2.0]) == [5.0, 0.5]
    plan = Dagger.rebalance_plan(own, cost, 2)
    @test plan == [1 => 1]
    @test Dagger.rebalance_loads(own, cost, 2; plan) == [3.0, 3.0]
    @test Dagger.rebalance_loads(own, cost, 2; plan, speeds=[1.0, 2.0]) == [3.0, 1.5]
    @test Dagger.rebalance_loads(Int[], Float64[], 3) == zeros(3)
    @test own == [0, 0, 1]                                      # not touched
    # what the plan predicts is what plan_loads computes by hand
    rng = MersenneTwister(7)
    for _ in 1:100
        nr = rand(rng, 1:5); n = rand(rng, 0:15)
        o = rand(rng, 0:(nr - 1), n); c = rand(rng, (0.0, 1.0, 2.5, 4.0), n); s = rand(rng, (0.5, 1.0, 2.0), nr)
        pl = Dagger.rebalance_plan(o, c, nr; speeds=s)
        @test Dagger.rebalance_loads(o, c, nr; speeds=s, plan=pl) ≈ plan_loads(o, c, nr, pl) ./ s
    end
    @test_throws ArgumentError Dagger.rebalance_loads([0, 1], [1.0], 2)
    @test_throws ArgumentError Dagger.rebalance_loads([0, 2], [1.0, 1.0], 2)
    @test_throws ArgumentError Dagger.rebalance_loads([0, 1], [1.0, 1.0], 2; plan=[1 => 5])
end

# Groups are plain entries of the plan (one owner and one cost per group): a group moves as a whole. The members' placement is tested
# under MPI; here: the plan of groups is the plan of their summed costs, and the loads are in groups' costs.
@testset "rebalance_plan: groups are single entries with the cost of the group" begin
    members = [[1.0, 2.0, 0.5, 0.5, 1.0], [3.0, 1.0, 1.0, 0.5, 0.5], [2.0, 2.0, 1.0, 1.0, 1.0], [1.0, 1.0, 1.0, 1.0, 1.0]]
    gcost = sum.(members)
    plan = Dagger.rebalance_plan([0, 0, 0, 0], gcost, 2)
    @test !isempty(plan)
    after = plan_loads([0, 0, 0, 0], gcost, 2, plan)
    @test sum(after) ≈ sum(gcost) && maximum(after) < sum(gcost)
    @test all(p -> 1 <= first(p) <= 4, plan)
end

# A randomized check against a brute-force bound: for small instances the best assignment is found by trying them all. The greedy plan
# never makes the slowest rank slower, and ends within `tolerance` of the mean or within one chunk of the optimum.
@testset "rebalance_plan: random instances against the optimum" begin
    rng = MersenneTwister(11)
    function best_time(cost, spd)
        nr = length(spd); n = length(cost); best = Inf
        for code in 0:(nr^n - 1)
            t = zeros(nr); c = code
            for i in 1:n
                t[c % nr + 1] += cost[i]; c ÷= nr
            end
            best = min(best, maximum(t ./ spd))
        end
        return best
    end
    for trial in 1:150
        nr = rand(rng, 2:3); n = rand(rng, 1:7)
        own = rand(rng, 0:(nr - 1), n)
        cost = rand(rng, (0.5, 1.0, 2.0, 3.0, 5.0), n)
        spd = rand(rng, (1.0, 2.0, 4.0), nr)
        plan = Dagger.rebalance_plan(own, cost, nr; speeds=spd)
        before = maximum(Dagger.rebalance_loads(own, cost, nr; speeds=spd))
        after = maximum(Dagger.rebalance_loads(own, cost, nr; speeds=spd, plan))
        mean_t = sum(cost) / sum(spd)
        opt = best_time(cost, spd)
        @test after <= before + 1e-9
        @test opt <= after + 1e-9
        @test after <= max(1.05 * mean_t, opt + maximum(cost) / minimum(spd)) + 1e-9
    end
end
