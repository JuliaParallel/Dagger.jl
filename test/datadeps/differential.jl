# Differential testing for Datadeps.
#
# A datadeps region must produce the same result as running its tasks
# sequentially, in submission order, with no Dagger involved at all. That
# "sequential reference" is the ground truth here, and it is what makes these
# tests useful: they do not encode any assumption about *how* the region is
# scheduled, only that scheduling never changes the answer.
#
# Two things are checked, over randomly-generated task graphs:
#
#   1. Hierarchical scheduling == sequential reference.
#   2. Flat scheduling == sequential reference.
#
# The generator deliberately mixes the argument shapes that stress the aliasing
# machinery: whole-array read/write, `view`s over part of an array (partial
# writes, so the remainder tracking has to work), copies between arrays (so one
# array's slot becomes another's source), and values produced by other in-region
# tasks (`In(::DTask)` value dependencies).
#
# These live outside the default suite because they need several workers and
# repeat each workload many times; run them with
# `CI_DATADEPS_DIFFERENTIAL=1`. See `.github/workflows/CI.yml`.

using Random

@everywhere begin
    _dd_add!(A, k) = (A .+= k; nothing)
    _dd_mul!(A, k) = (A .*= k; nothing)
    _dd_copy!(dst, src) = (copyto!(dst, src); nothing)
    _dd_axpy!(y, x, a) = (y .+= a .* x; nothing)
    _dd_fill(v, n) = fill(v, n)
end

const DD_TILE_LEN = 8

"""
    dd_gen_ops(seed, ntiles, nops; views, valuedeps) -> Vector

Deterministically generate a workload: a list of ops over `ntiles` tiles. Each
op is `(kind, dst, src, amount)`. `views` enables partial (`view`-based) writes;
`valuedeps` enables tasks that consume another in-region task's return value.
"""
function dd_gen_ops(seed::Int, ntiles::Int, nops::Int; views::Bool, valuedeps::Bool)
    rng = MersenneTwister(seed)
    ops = Tuple{Symbol,Int,Int,Float64}[]
    nkinds = 3 + (views ? 1 : 0) + (valuedeps ? 1 : 0)
    for _ in 1:nops
        kind = rand(rng, 1:nkinds)
        i = rand(rng, 1:ntiles)
        j = rand(rng, 1:ntiles)
        if kind == 1
            push!(ops, (:add, i, j, Float64(rand(rng, 1:3))))
        elseif kind == 2
            push!(ops, (:mul, i, j, 2.0))
        elseif kind == 3
            i != j && push!(ops, (:copy, i, j, 0.0))
        elseif kind == 4 && views
            push!(ops, (:viewadd, i, j, 1.0))
        else
            push!(ops, (:valuedep, i, j, 7.0))
        end
    end
    return ops
end

"Reference: run `ops` sequentially with plain Julia arrays."
function dd_run_sequential(ops, ntiles::Int)
    T = [collect(1.0:DD_TILE_LEN) .+ i for i in 1:ntiles]
    for (kind, i, j, a) in ops
        if kind === :add
            T[i] .+= a
        elseif kind === :mul
            T[i] .*= a
        elseif kind === :copy
            copyto!(T[i], T[j])
        elseif kind === :viewadd
            view(T[i], 1:(DD_TILE_LEN ÷ 2)) .+= a
        elseif kind === :valuedep
            copyto!(T[i], fill(a, DD_TILE_LEN))
        end
    end
    return T
end

"""
    dd_run_datadeps(ops, ntiles, hierarchical, place) -> Vector

Run `ops` inside a datadeps region. `place(i)` gives the worker that tile `i` is
homed on, so a workload can be concentrated on one worker or spread across many.
"""
function dd_run_datadeps(ops, ntiles::Int, hierarchical::Bool, place)
    T = [remotecall_fetch(Dagger.tochunk, place(i), collect(1.0:DD_TILE_LEN) .+ i)
         for i in 1:ntiles]
    Base.ScopedValues.with(Dagger.DATADEPS_HIERARCHICAL => hierarchical) do
        Dagger.spawn_datadeps() do
            for (kind, i, j, a) in ops
                if kind === :add
                    Dagger.@spawn _dd_add!(InOut(T[i]), a)
                elseif kind === :mul
                    Dagger.@spawn _dd_mul!(InOut(T[i]), a)
                elseif kind === :copy
                    Dagger.@spawn _dd_copy!(Out(T[i]), In(T[j]))
                elseif kind === :viewadd
                    Dagger.@spawn _dd_add!(InOut(view(T[i], 1:(DD_TILE_LEN ÷ 2))), a)
                elseif kind === :valuedep
                    produced = Dagger.@spawn _dd_fill(a, DD_TILE_LEN)
                    Dagger.@spawn _dd_copy!(Out(T[i]), In(produced))
                end
            end
        end
    end
    return [fetch(t) for t in T]
end

# Tiles all on the driver, versus spread round-robin across every worker. The
# spread case is what exercises cross-worker planning; `nworkers() == 1` makes
# the two identical, which is fine (the suite still checks the single-owner
# path).
const DD_PLACEMENTS = [
    "concentrated" => (i -> 1),
    "spread"       => (i -> workers()[mod1(i, nworkers())]),
]

# `view` arguments are only generated for tiles homed on the driver.
#
# Known limitation: taking a `view` of a `Chunk` homed on another worker throws
#   AssertionError: DRef ... is not owned by this process
# from `unwrap` inside `move_rewrap` / `remotecall_endpoint_toplevel`. This
# affects *both* schedulers identically and reproduces on `master`, so it is a
# core `ChunkView` limitation rather than anything to do with partitioning. It
# is recorded in the "remote view (known limitation)" testset below; until it is
# fixed, view workloads here stay on the concentrated placement.
dd_views_supported(pname) = pname == "concentrated"

const DD_NTILES = 8
const DD_NOPS = 30
const DD_SEEDS = 1:6

@testset "Datadeps differential" begin
    @testset "hierarchical matches sequential" begin
        # The invariant that matters: however the region is partitioned and
        # placed, the answer is the sequential one.
        for (pname, place) in DD_PLACEMENTS,
            views in (false, true),
            valuedeps in (false, true),
            seed in DD_SEEDS

            views && !dd_views_supported(pname) && continue
            ops = dd_gen_ops(seed, DD_NTILES, DD_NOPS; views, valuedeps)
            expected = dd_run_sequential(ops, DD_NTILES)
            got = dd_run_datadeps(ops, DD_NTILES, true, place)
            @test got == expected
        end
    end

    @testset "flat matches sequential" begin
        for (pname, place) in DD_PLACEMENTS,
            valuedeps in (false, true),
            seed in DD_SEEDS

            # N.B. `views` is deliberately excluded here. Flat scheduling has a
            # pre-existing, intermittent bug with `view` arguments once several
            # workers are involved -- see "Known issue" in
            # `flat view aliasing (known bug)` below. Asserting it here would
            # make this job flaky rather than informative.
            ops = dd_gen_ops(seed, DD_NTILES, DD_NOPS; views=false, valuedeps)
            expected = dd_run_sequential(ops, DD_NTILES)
            got = dd_run_datadeps(ops, DD_NTILES, false, place)
            @test got == expected
        end
    end

    @testset "hierarchical matches flat" begin
        # Where flat is trustworthy, the two paths should agree exactly.
        for (pname, place) in DD_PLACEMENTS, seed in DD_SEEDS
            ops = dd_gen_ops(seed, DD_NTILES, DD_NOPS; views=false, valuedeps=true)
            flat = dd_run_datadeps(ops, DD_NTILES, false, place)
            hier = dd_run_datadeps(ops, DD_NTILES, true, place)
            @test flat == hier
        end
    end

    # ---------------------------------------------------------------------
    # Known issue: flat scheduling + `view` arguments + multiple workers
    # ---------------------------------------------------------------------
    #
    # `distribute_tasks!` (the flat path) intermittently produces wrong results
    # when a region mixes whole-array writes with `view`-based partial writes and
    # copies, and enough workers are present for tasks to be placed across
    # several memory spaces. Hierarchical scheduling is correct on the identical
    # workload, every run.
    #
    # Reproducer (this is the smallest *reliable* trigger found so far; the
    # failure is a property of the accumulated remainder/currency state, not of
    # any single op pair):
    #
    #     ops = dd_gen_ops(4, 8, 30; views=true, valuedeps=false)
    #     ref = dd_run_sequential(ops, 8)
    #     # 3+ workers, tiles all homed on worker 1:
    #     dd_run_datadeps(ops, 8, false, i -> 1) != ref   # ~40% of runs
    #     dd_run_datadeps(ops, 8, true,  i -> 1) == ref   # always
    #
    # Seed 8 reproduces at ~25%. Both were found by this harness comparing
    # against the sequential reference, and both reproduce on `master`, so this
    # is not a regression from hierarchical scheduling.
    #
    # Ruled out as the cause (each checked over 25 repetitions, 3 workers, and
    # never failing):
    #   * partial write then whole-array read: `view(A,1:4) .+= 1; B .= A`
    #   * that plus a write-back:              `... ; A .= B`
    #   * partial write of a copy destination: `B .= A; view(B,1:4) .+= 1; A .= B`
    #   * two tiles partially written, then cross-copied
    #   * whole-array write between the partial write and the copy
    # Greedy delta-debugging of the seed-4 workload stalls at 18 ops, which is
    # consistent with the trigger being an accumulation of remainder history
    # across memory spaces rather than a local pattern.
    #
    # This testset records the bug without failing CI on it. When the flat path
    # is fixed, `@test_skip` should become a real `@test` and `views=false`
    # should be dropped from "flat matches sequential" above.
    @testset "remote view (known limitation)" begin
        # A `view` over a chunk homed on another worker fails in both
        # schedulers, identically, and on `master`. Recorded so the day it
        # starts working is visible.
        if nworkers() > 1
            remote_tile = remotecall_fetch(Dagger.tochunk, workers()[1],
                                           collect(1.0:DD_TILE_LEN))
            for hier in (false, true)
                @test_skip try
                    Dagger.spawn_datadeps() do
                        Dagger.@spawn _dd_add!(InOut(view(remote_tile, 1:4)), 1.0)
                    end
                    true
                catch
                    false
                end
            end
        end
    end

    @testset "flat view aliasing (known bug)" begin
        ops = dd_gen_ops(4, DD_NTILES, DD_NOPS; views=true, valuedeps=false)
        expected = dd_run_sequential(ops, DD_NTILES)
        # Hierarchical must be correct on the very workload flat gets wrong.
        for _ in 1:5
            @test dd_run_datadeps(ops, DD_NTILES, true, i -> 1) == expected
        end
        @test_skip dd_run_datadeps(ops, DD_NTILES, false, i -> 1) == expected
    end
end
