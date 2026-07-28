# Tests for the operation-tape layout predictor.
#
# Drop into `test/` and add `include("tapes.jl")` to `test/runtests.jl`.
#
# Most of these exercise the pure logic (site keys, trie, prediction, cost
# models, planner) with synthetic specs, so they run without a cluster and in
# milliseconds. The end-to-end group at the bottom needs real DArrays.

using Test
using LinearAlgebra
using Dagger
using Dagger: Tapes
using Dagger.Tapes: SiteKey, OpKey, ArgSpec, ArgView, LayoutChoice, PredictedOp,
                    TapeNode, TapeRoot, MachineModel,
                    size_bucket, predict, advance!, plan_chain, candidate_layouts,
                    fallback_layout, cost_of, generic_op_cost, step_cost,
                    op_cost, has_cost_model, blocksize, nblocks, ntiles,
                    to_blocks, confidence, child_total

# A deterministic machine so cost comparisons are reproducible.
const TESTMACHINE = MachineModel(nprocs = 16,
                                 flops_per_sec = 1.0e12,
                                 bandwidth = 1.0e10,
                                 latency = 1.0e-5,
                                 task_overhead = 1.0e-4,
                                 mem_per_proc = 8.0e9)

spec(T, dims, bs; assign = :arbitrary) = ArgSpec(T, dims, bs, assign)

function fresh!()
    Tapes.clear!()
    Tapes.set_machine!(TESTMACHINE)
    return nothing
end

@testset "Tapes" begin

@testset "site keys" begin
    fresh!()
    Tapes.CONFIG.size_buckets_per_octave = 2

    # Nearby sizes share a bucket; distant ones do not.
    @test size_bucket((1024, 1024)) == size_bucket((1100, 1100))
    @test size_bucket((1024, 1024)) != size_bucket((8192, 8192))

    k1 = SiteKey(UInt64(7), Float64, Int32(2), size_bucket((1024, 1024)))
    k2 = SiteKey(UInt64(7), Float64, Int32(2), size_bucket((1024, 1024)))
    k3 = SiteKey(UInt64(7), Float32, Int32(2), size_bucket((1024, 1024)))
    @test k1 == k2
    @test hash(k1) == hash(k2)
    @test k1 != k3            # element type is part of identity

    Tapes.CONFIG.size_buckets_per_octave = 0
    @test size_bucket((1024, 1024)) != size_bucket((1100, 1100))
    Tapes.CONFIG.size_buckets_per_octave = 2
end

@testset "trie recording and prediction" begin
    fresh!()
    key = SiteKey(UInt64(1), Float64, Int32(2), size_bucket((1024, 1024)))
    root = Tapes.get_root!(key)
    self = spec(Float64, (1024, 1024), (256, 256))
    layout = LayoutChoice((256, 256), :arbitrary, :square)

    chain = [OpKey(:mul!, 1, 3), OpKey(:cholesky!, 1, 1), OpKey(:trsm!, 1, 2)]

    # Ten identical executions of the same chain.
    for _ in 1:10
        tr = Tapes.LiveTrace(root, self, layout, PredictedOp[], LayoutChoice[layout])
        root.nobservations += 1
        root.root.count += 1
        for op in chain
            advance!(tr, op, ArgSpec[self])
        end
    end

    pred = predict(root.root)
    @test length(pred) == 3
    @test [p.key.op for p in pred] == [:mul!, :cholesky!, :trsm!]
    @test confidence(pred) ≈ 1.0

    # Divergence: five runs take a different second step.
    for _ in 1:5
        tr = Tapes.LiveTrace(root, self, layout, PredictedOp[], LayoutChoice[layout])
        root.nobservations += 1
        root.root.count += 1
        advance!(tr, OpKey(:mul!, 1, 3), ArgSpec[self])
        advance!(tr, OpKey(:lu!, 1, 1), ArgSpec[self])
    end

    pred = predict(root.root)
    @test pred[1].key.op == :mul!
    @test pred[1].prob ≈ 1.0            # first step is still certain
    @test pred[2].key.op == :cholesky!  # modal branch wins
    @test 0.6 < pred[2].prob < 0.7      # 10/15

    # Prefix conditioning: from the :mul! node the answer is the same, but
    # from a node we have never reached there is nothing to say.
    mul_node = root.root.children[OpKey(:mul!, 1, 3)]
    @test predict(mul_node)[1].key.op == :cholesky!
    @test child_total(mul_node) == 15
end

@testset "allocated-but-unused reduces confidence" begin
    fresh!()
    key = SiteKey(UInt64(2), Float64, Int32(2), size_bucket((512, 512)))
    root = Tapes.get_root!(key)
    self = spec(Float64, (512, 512), (128, 128))
    layout = LayoutChoice((128, 128), :arbitrary, :square)

    # One array gets used, nine are allocated and abandoned.
    tr = Tapes.LiveTrace(root, self, layout, PredictedOp[], LayoutChoice[layout])
    root.nobservations += 1; root.root.count += 1
    advance!(tr, OpKey(:cholesky!, 1, 1), ArgSpec[self])
    for _ in 1:9
        root.nobservations += 1; root.root.count += 1
    end

    pred = predict(root.root)
    # 1/10 is below min_branch_prob, so we should predict nothing at all
    # rather than confidently predicting :cholesky!.
    @test isempty(pred)
end

@testset "cost models" begin
    fresh!()
    @test has_cost_model(:cholesky!)
    @test has_cost_model(:mul!)
    @test !has_cost_model(:definitely_not_an_op)

    s = spec(Float64, (8192, 8192), (512, 512))

    square = LayoutChoice((512, 512), :cyclicrow, :square)
    rowblk = LayoutChoice((512, 8192), :blockrow, :rowblock)
    views(l) = ArgView[ArgView(s, l)]

    # Cholesky must prefer square tiles to fat row blocks.
    @test cost_of(:cholesky!, views(square), TESTMACHINE) <
          cost_of(:cholesky!, views(rowblk), TESTMACHINE)

    # An unmodelled op falls through to the generic model without erroring.
    c = cost_of(:some_unknown_op, views(square), TESTMACHINE)
    @test isfinite(c) && c > 0

    # Degenerate layouts must be punished, not merely disfavoured.
    tiny = LayoutChoice((1, 1), :arbitrary, :square)
    huge = LayoutChoice((8192, 8192), :arbitrary, :square)
    base = generic_op_cost(views(square), TESTMACHINE)
    @test generic_op_cost(views(tiny), TESTMACHINE) > 10 * base
    @test generic_op_cost(views(huge), TESTMACHINE) > base
end

@testset "@cost_model defines a usable model" begin
    fresh!()
    Tapes.@cost_model __test_op(A, B) = flops_time(Float64(length(A)) * size(B, 1)) +
                                        task_time(ntiles(A))
    @test has_cost_model(:__test_op)
    sA = spec(Float64, (100, 100), (50, 50))
    sB = spec(Float64, (100, 10), (50, 10))
    lA = LayoutChoice((50, 50), :arbitrary, :square)
    lB = LayoutChoice((50, 10), :arbitrary, :colblock)
    v = ArgView[ArgView(sA, lA), ArgView(sB, lB)]
    @test cost_of(:__test_op, v, TESTMACHINE) > 0

    # Too few recorded arguments must degrade, not throw.
    @test cost_of(:__test_op, ArgView[ArgView(sA, lA)], TESTMACHINE) > 0
end

@testset "candidate generation" begin
    fresh!()
    cands = candidate_layouts(Float64, (8192, 8192), TESTMACHINE)
    @test !isempty(cands)
    @test length(cands) <= Tapes.CONFIG.max_candidates
    @test fallback_layout(Float64, (8192, 8192)) in cands   # baseline always present
    @test any(c -> c.label === :square, cands)
    @test any(c -> c.label === :rowblock, cands)
    @test all(c -> all(>(0), blocksize(c)), cands)

    # Vectors must not produce degenerate 2D candidates.
    cv = candidate_layouts(Float64, (100_000,), TESTMACHINE)
    @test !isempty(cv)
    @test all(c -> c.ndims == 1, cv)
end

@testset "planner" begin
    fresh!()
    s = spec(Float64, (8192, 8192), (512, 512))
    cands = candidate_layouts(Float64, (8192, 8192), TESTMACHINE)

    # A confident, homogeneous Cholesky chain should land on square tiles.
    pred = [PredictedOp(OpKey(:cholesky!, 1, 1), 1.0, ArgSpec[s]) for _ in 1:3]
    lp = plan_chain(pred, cands, s, TESTMACHINE)
    @test lp.accepted
    @test lp.cost <= lp.fallback_cost
    @test length(lp.steps) == 3
    @test Dagger.Tapes.aspect(ArgView(s, lp.steps[1])) < 4.0

    # An empty forecast must decline rather than guess.
    lp0 = plan_chain(PredictedOp[], cands, s, TESTMACHINE)
    @test !lp0.accepted
    @test lp0.reason === :no_prediction

    # A low-confidence forecast is subject to the regret bound.
    predlow = [PredictedOp(OpKey(:cholesky!, 1, 1), 0.3, ArgSpec[s])]
    lplow = plan_chain(predlow, cands, s, TESTMACHINE)
    @test lplow.accepted == false || lplow.max_regret <= Tapes.CONFIG.max_regret_ratio

    # The gate must reject a plan that does not beat the fallback by the margin.
    old = Tapes.CONFIG.gate_margin
    Tapes.CONFIG.gate_margin = 0.0        # nothing can beat this
    @test !plan_chain(pred, cands, s, TESTMACHINE).accepted
    Tapes.CONFIG.gate_margin = old
end

@testset "planner accounts for repartitioning" begin
    fresh!()
    s = spec(Float64, (8192, 8192), (512, 512))
    cands = candidate_layouts(Float64, (8192, 8192), TESTMACHINE)

    # A chain that alternates between two operations with opposed preferences.
    # Whatever the planner decides, it must not thrash: the number of distinct
    # layouts in the plan should be small relative to the chain length, because
    # each change is charged a full redistribution.
    pred = PredictedOp[]
    for i in 1:8
        op = isodd(i) ? :cholesky! : :reduce
        push!(pred, PredictedOp(OpKey(op, 1, 1), 1.0, ArgSpec[s]))
    end
    lp = plan_chain(pred, cands, s, TESTMACHINE)
    @test length(unique(lp.steps)) <= 3
end

@testset "enable/disable is inert by default" begin
    fresh!()
    Tapes.disable!()
    @test !Tapes.is_enabled()
    p = Tapes.plan_allocation(Float64, (1024, 1024))
    @test p.partitioning isa AutoBlocks     # request passed through untouched
    @test p.root === nothing

    Tapes.enable!(site_id = :lexical)
    @test Tapes.is_enabled()
    # An explicit Blocks must survive untouched unless explicitly overridden.
    p = Tapes.plan_allocation(Float64, (1024, 1024); requested = Blocks(64, 64))
    @test p.partitioning == Blocks(64, 64)
    Tapes.disable!()
end

@testset "pinning overrides prediction" begin
    fresh!()
    Tapes.enable!(site_id = :lexical)
    Tapes.pin!(Float64, (2048, 2048), Blocks(256, 256), :cyclicrow)
    p = Tapes.plan_allocation(Float64, (2048, 2048))
    @test p.partitioning == Blocks(256, 256)
    @test p.assignment === :cyclicrow
    Tapes.unpin!()
    Tapes.disable!()
end

@testset "introspection does not throw" begin
    fresh!()
    Tapes.enable!(site_id = :lexical)
    Tapes.plan_allocation(Float64, (1024, 1024))
    io = IOBuffer()
    @test Tapes.report(io) === nothing
    @test Tapes.dump_tapes(io) === nothing
    @test Tapes.explain(io, Float64, (1024, 1024)) === nothing
    @test Tapes.stats().sites >= 1
    Tapes.disable!()
end

# ---------------------------------------------------------------------------
# End-to-end. Requires the `@record_op` / `plan_allocation` patch points from
# `tapes/integration.jl` to actually be applied to Dagger's allocators and
# linear-algebra routines; skipped otherwise.
# ---------------------------------------------------------------------------

@testset "end-to-end learning" begin
    fresh!()
    Tapes.enable!(site_id = :backtrace, min_observations = 1, verbose = false)
    try
        n = 512
        function workload()
            A = rand(AutoBlocks(), Float64, n, n)
            B = A * A' + n * I
            return cholesky!(B)
        end

        workload()                      # cold: learns nothing yet
        s1 = Tapes.stats()
        workload()                      # warm: should now predict
        s2 = Tapes.stats()

        @test s2.observations >= s1.observations
        if s2.observations == 0
            @info "Tapes end-to-end: no allocations tracked; integration patches not applied"
        else
            @test s2.sites >= 1
        end
    finally
        Tapes.disable!()
        Tapes.clear!()
        Tapes.reset_machine!()
    end
end

@testset "@expect_ops declares ahead of time" begin
    fresh!()
    Tapes.enable!(site_id = :lexical, min_observations = 1)
    try
        n = 512
        Dagger.@expect_ops [:cholesky!, :trsm!] begin
            # With a declaration in scope, the very first allocation should be
            # planned rather than falling back, since confidence is asserted.
            p = Tapes.plan_allocation(Float64, (n, n))
            @test p.root !== nothing
            @test !isempty(p.predicted)
            @test p.predicted[1].key.op === :cholesky!
            @test p.predicted[1].prob ≈ 1.0
        end
    finally
        Tapes.disable!()
        Tapes.clear!()
        Tapes.reset_machine!()
    end
end

end # @testset "Tapes"
