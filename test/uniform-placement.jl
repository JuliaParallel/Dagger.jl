using Dagger, Random, Test

# A rank-uniform acceleration with synthetic processors, so the uniform
# placement helpers can be checked without MPI.
struct UniformPlacementAccel <: Dagger.Acceleration end
struct RankProc <: Dagger.Processor
    rank::Int
    tid::Int
end
Dagger.uniform_execution(::UniformPlacementAccel) = true
Dagger.fire_order_key(p::RankProc) = (p.rank, p.rank)
Dagger.short_name(p::RankProc) = "(rank $(p.rank), thread $(p.tid))"
# The "state" argument carries the preferred rank in these tests
Dagger.task_processor_preference(::UniformPlacementAccel, ::Dagger.Thunk, pref) = pref

# The original implementation: a comparison sort whose key (with its
# `short_name` string) is rebuilt per comparison, then the same rotation.
function reference_select_uniform!(procs, accel, task, pref)
    sort!(procs, by=p -> (
        pref !== nothing && Dagger.processor_owner_rank(accel, p) != pref,
        Dagger.processor_order_key(accel, p)...,
    ))
    n = length(procs)
    if pref !== nothing
        n = 0
        for proc in procs
            Dagger.processor_owner_rank(accel, proc) == pref || break
            n += 1
        end
        n == 0 && (n = length(procs))
    end
    if n > 1
        offset = mod(task.id - 1, n)
        reverse!(procs, 1, offset)
        reverse!(procs, offset + 1, n)
        reverse!(procs, 1, n)
    end
    return procs
end

@testset "Uniform placement" begin
    accel = UniformPlacementAccel()
    rng = MersenneTwister(1)
    noop() = nothing

    @testset "canonical order groups processors by owner rank" begin
        procs = Dagger.Processor[RankProc(r, t) for r in 3:-1:0 for t in 1:3]
        shuffle!(rng, procs)
        @test Dagger.sort_processors_canonical!(procs, accel) === procs
        @test [p.rank for p in procs] == sort([p.rank for p in procs])
        @test issorted(procs; by=p -> Dagger.processor_order_key(accel, p))
    end

    @testset "matches the reference algorithm" begin
        for nranks in (1, 2, 5), nthreads in (1, 3, 8), trial in 1:20
            candidates = Dagger.Processor[RankProc(r, t) for r in 0:nranks-1 for t in 1:nthreads]
            # A scope may admit only some processors of a rank, or skip a rank
            keep = rand(rng, length(candidates)) .< 0.8
            any(keep) || (keep[1] = true)
            candidates = candidates[keep]
            # Includes a rank that owns none of the candidates
            for pref in (nothing, 0, nranks - 1, nranks + 7), tid in (1, 2, 17, 1000)
                task = Dagger.Thunk(noop; id=tid)
                want = reference_select_uniform!(shuffle(rng, candidates), accel, task, pref)
                # Per-task path from a cached canonical order
                canonical = Dagger.sort_processors_canonical!(shuffle(rng, candidates), accel)
                got = Dagger.place_processors_uniform!(copy(canonical), accel, task, pref)
                @test got == want
                # The one-shot entry point agrees as well
                got2 = Dagger.select_processors_uniform!(shuffle(rng, candidates), accel, task, pref)
                @test got2 == want
            end
        end
    end

    @testset "no task keeps the canonical order" begin
        procs = Dagger.Processor[RankProc(r, t) for r in 0:2 for t in 1:2]
        shuffled = shuffle(rng, procs)
        @test Dagger.select_processors_uniform!(shuffled, accel) == procs
        @test Dagger.place_processors_uniform!(copy(procs), accel, nothing, nothing) == procs
    end

    @testset "empty and single candidate" begin
        task = Dagger.Thunk(noop; id=5)
        for pref in (nothing, 0)
            @test Dagger.place_processors_uniform!(Dagger.Processor[], accel, task, pref) == Dagger.Processor[]
            @test Dagger.place_processors_uniform!(Dagger.Processor[RankProc(0, 1)], accel, task, pref) == Dagger.Processor[RankProc(0, 1)]
        end
    end

    @testset "non-uniform accelerations are untouched" begin
        procs = Dagger.Processor[RankProc(2, 1), RankProc(0, 1)]
        want = copy(procs)
        @test Dagger.select_processors_uniform!(procs, Dagger.DistributedAcceleration(), Dagger.Thunk(noop; id=3), nothing) == want
    end
end

# A cached (accel, scope, procs) lookup whose processor list changed without a cache
# flush must be replaced, not served stale: `BasicLFUCache.get!` keeps the old entry for
# a present key, which made every later lookup miss and recompute (and re-sort) the set.
Dagger.compatible_processors(::UniformPlacementAccel, ::Dagger.AbstractScope, procs::Vector{<:Dagger.Processor}) =
    Set{Dagger.Processor}(procs)
@testset "compat cache replaces a stale entry" begin
    accel = UniformPlacementAccel()
    scope = Dagger.AnyScope()
    Dagger.Sch.invalidate_compat_procs_cache!()
    procs = Dagger.Processor[RankProc(r, 1) for r in 0:1]
    e1 = Dagger.Sch.compat_entry(accel, scope, procs)
    @test e1.compat == Set(procs)
    @test Dagger.Sch.compat_entry(accel, scope, procs) === e1
    grown = Dagger.Processor[RankProc(r, 1) for r in 0:2]
    e2 = Dagger.Sch.compat_entry(accel, scope, grown)
    @test e2.compat == Set(grown)
    # The new entry is what the cache now holds: the next lookup hits
    @test Dagger.Sch.compat_entry(accel, scope, grown) === e2
    @test Dagger.Sch.compatible_processors_ordered(accel, scope, grown) == grown
    Dagger.Sch.invalidate_compat_procs_cache!()
end
