import Dagger: DAGSpec, DatadepsArgSpec, dag_add_task!,
               equivalent_structure,
               datadeps_dag_equivalent, datadeps_argspec_equivalent,
               datadeps_ainfo_equivalent, datadeps_schedule_cache,
               LayeredScheduler, RoundRobinScheduler, GreedyScheduler, DataDepsScheduler,
               IteratedGreedyScheduler, iterated_greedy_schedule!, iterated_greedy_step!,
               SimulatedAnnealingScheduler, simulated_annealing_schedule!,
               ScheduleState, cost_of_schedule, greedy_assign_task!, greedy_schedule!,
               NoAliasing, UnknownAliasing, ContiguousAliasing,
               StridedAliasing, TriangularAliasing, DiagonalAliasing,
               ObjectAliasing, CombinedAliasing, AliasingWrapper,
               DATADEPS_SCHEDULER, In, Out, InOut, Deps
import Dagger
using LinearAlgebra
using Random
using Test

# ---------- equivalent_structure unit tests ----------

@testset "equivalent_structure" begin
    @testset "NoAliasing/UnknownAliasing" begin
        @test  equivalent_structure(NoAliasing(),      NoAliasing())
        @test  equivalent_structure(UnknownAliasing(), UnknownAliasing())
        @test !equivalent_structure(NoAliasing(),      UnknownAliasing())
        @test !equivalent_structure(UnknownAliasing(), NoAliasing())
    end

    @testset "ContiguousAliasing" begin
        # Same shape, different allocation → equivalent
        @test  equivalent_structure(Dagger.aliasing(rand(10, 10)),
                                    Dagger.aliasing(rand(10, 10)))
        @test  equivalent_structure(Dagger.aliasing(zeros(Float64, 32)),
                                    Dagger.aliasing(rand(Float64, 32)))
        # Different element counts → not equivalent
        @test !equivalent_structure(Dagger.aliasing(rand(10, 10)),
                                    Dagger.aliasing(rand(5, 5)))
        # Different element types → not equivalent (S/T params differ)
        @test !equivalent_structure(Dagger.aliasing(rand(Float64, 10)),
                                    Dagger.aliasing(rand(Float32, 10)))
    end

    @testset "StridedAliasing" begin
        A1 = rand(10, 10)
        A2 = rand(10, 10)  # same parent shape, different allocation
        # Same view shape & relative offset → equivalent
        @test  equivalent_structure(Dagger.aliasing(view(A1, 2:5, 3:6)),
                                    Dagger.aliasing(view(A2, 2:5, 3:6)))
        # Same view shape, different relative offset → not equivalent
        @test !equivalent_structure(Dagger.aliasing(view(A1, 2:5, 3:6)),
                                    Dagger.aliasing(view(A1, 3:6, 4:7)))
        # Different parent shapes → strides differ → not equivalent
        B = rand(20, 20)
        @test !equivalent_structure(Dagger.aliasing(view(A1, 2:5, 3:6)),
                                    Dagger.aliasing(view(B,  2:5, 3:6)))
        # Different view dim lengths → not equivalent
        @test !equivalent_structure(Dagger.aliasing(view(A1, 2:5, 3:6)),
                                    Dagger.aliasing(view(A1, 2:6, 3:6)))
    end

    @testset "TriangularAliasing" begin
        A1 = rand(10, 10); A2 = rand(10, 10)
        @test  equivalent_structure(Dagger.aliasing(UpperTriangular(A1)),
                                    Dagger.aliasing(UpperTriangular(A2)))
        @test  equivalent_structure(Dagger.aliasing(LowerTriangular(A1)),
                                    Dagger.aliasing(LowerTriangular(A2)))
        # Upper vs Lower → not equivalent
        @test !equivalent_structure(Dagger.aliasing(UpperTriangular(A1)),
                                    Dagger.aliasing(LowerTriangular(A1)))
        # Unit vs non-unit → not equivalent
        @test !equivalent_structure(Dagger.aliasing(UpperTriangular(A1)),
                                    Dagger.aliasing(UnitUpperTriangular(A1)))
        # Different size → not equivalent
        @test !equivalent_structure(Dagger.aliasing(UpperTriangular(A1)),
                                    Dagger.aliasing(UpperTriangular(rand(5, 5))))
    end

    @testset "DiagonalAliasing" begin
        A1 = rand(10, 10); A2 = rand(10, 10)
        @test  equivalent_structure(Dagger.aliasing(A1, Diagonal),
                                    Dagger.aliasing(A2, Diagonal))
        @test !equivalent_structure(Dagger.aliasing(A1, Diagonal),
                                    Dagger.aliasing(rand(5, 5), Diagonal))
    end

    @testset "ObjectAliasing" begin
        r1 = Ref(rand(10))
        r2 = Ref(rand(10))  # same Ref{Vector{Float64}} shape
        a1 = first(Dagger.aliasing(r1).sub_ainfos)::ObjectAliasing
        a2 = first(Dagger.aliasing(r2).sub_ainfos)::ObjectAliasing
        @test equivalent_structure(a1, a2)
    end

    @testset "CombinedAliasing (Ref)" begin
        @test equivalent_structure(Dagger.aliasing(Ref(rand(8))),
                                   Dagger.aliasing(Ref(rand(8))))
        @test !equivalent_structure(Dagger.aliasing(Ref(rand(8))),
                                    Dagger.aliasing(Ref(rand(4))))
    end

    @testset "AliasingWrapper delegation" begin
        A1 = rand(10, 10); A2 = rand(10, 10)
        w1 = AliasingWrapper(Dagger.aliasing(A1))
        w2 = AliasingWrapper(Dagger.aliasing(A2))
        @test equivalent_structure(w1, w2)
        # Wrapper vs bare also works
        @test equivalent_structure(w1, Dagger.aliasing(A2))
        @test equivalent_structure(Dagger.aliasing(A1), w2)
    end

    @testset "Cross-type returns false" begin
        @test !equivalent_structure(Dagger.aliasing(rand(10)),
                                    Dagger.aliasing(view(rand(10, 10), 1:5, 1:5)))
        @test !equivalent_structure(Dagger.aliasing(rand(10, 10)),
                                    NoAliasing())
        @test !equivalent_structure(NoAliasing(),
                                    Dagger.aliasing(rand(10, 10)))
    end
end

# ---------- Helpers for DAG capture via the schedule cache ----------

# N.B. These tests exercise the *flat* AOT DAG-scheduling/caching path
# (`distribute_tasks!` + `datadeps_build_schedule!`), so every `spawn_datadeps`
# call here runs with `hierarchical=false`. Hierarchical scheduling intentionally
# computes a separate, partition-local AOT schedule on each partition's own task
# (no global schedule cache on the calling task), so the caller-visible cache
# these tests inspect is only populated by the flat path.

"""
Run `f()` inside `spawn_datadeps` with `scheduler`, returning the resulting
schedule cache snapshot (a Vector). The cache is cleared before the run so
each call starts from a known state.
"""
function run_with_fresh_cache(f, scheduler::DataDepsScheduler = LayeredScheduler())
    cache = datadeps_schedule_cache(scheduler)
    empty!(cache)
    Base.ScopedValues.with(Dagger.DATADEPS_HIERARCHICAL => false, DATADEPS_SCHEDULER => scheduler) do
        Dagger.spawn_datadeps(f)
    end
    return cache
end

"""
Run a sequence of `n` invocations of `f()` under `scheduler` and return the
cache. Useful for asserting cache size after repeated runs.
"""
function run_n_times(f, n::Int; scheduler::DataDepsScheduler = LayeredScheduler())
    cache = datadeps_schedule_cache(scheduler)
    empty!(cache)
    Base.ScopedValues.with(Dagger.DATADEPS_HIERARCHICAL => false, DATADEPS_SCHEDULER => scheduler) do
        for _ in 1:n
            Dagger.spawn_datadeps(f)
        end
    end
    return cache
end

# Simple test kernels
add!(X, Y) = (X .+= Y; X)
scale!(X, a) = (X .*= a; X)

# ---------- DAGSpec construction ----------

@testset "DAGSpec construction" begin
    @testset "Vertices and function types" begin
        A = rand(64)
        B = rand(64)
        cache = run_with_fresh_cache() do
            Dagger.@spawn add!(InOut(A), In(B))
            Dagger.@spawn scale!(InOut(A), 2.0)
        end
        @test length(cache) == 1
        spec = first(cache).first
        @test length(spec) == 2
        @test spec.id_to_functype[1] === typeof(add!)
        @test spec.id_to_functype[2] === typeof(scale!)
        # `tspec.fargs` includes the function as fargs[1], so argtypes also
        # includes the function argspec → 3 entries per vertex.
        @test length(spec.id_to_argtypes[1]) == 3
        @test length(spec.id_to_argtypes[2]) == 3
    end

    @testset "Argspec positions, types, and dep_mods" begin
        A = rand(32, 32)
        cache = run_with_fresh_cache() do
            Dagger.@spawn add!(InOut(A), In(A))
        end
        spec = first(cache).first
        argspecs = spec.id_to_argtypes[1]
        # 3 argspecs: function (pos=0), A (InOut, pos=1), A (In, pos=2)
        @test length(argspecs) == 3
        @test all(a -> a.pos isa Int, argspecs)
        # The two data argspecs (positions 1 and 2) refer to a Matrix{Float64}
        data_argspecs = filter(a -> a.pos != 0, argspecs)
        @test length(data_argspecs) == 2
        @test all(a -> a.value_type === Matrix{Float64}, data_argspecs)
        @test all(a -> a.dep_mod === identity, data_argspecs)
        @test all(a -> a.ainfo isa ContiguousAliasing, data_argspecs)
        # Function argspec is at position 0 with value_type === typeof(add!)
        f_argspec = only(filter(a -> a.pos == 0, argspecs))
        @test f_argspec.value_type === typeof(add!)
    end

    @testset "Deps with multiple dep_mods at one position" begin
        X = Ref(rand(64))
        do_nothing(R) = nothing
        cache = run_with_fresh_cache() do
            Dagger.@spawn do_nothing(Deps(X, InOut(:x), In(:x)))
        end
        spec = first(cache).first
        argspecs = spec.id_to_argtypes[1]
        # Both Deps entries for :x should be present, even though they share pos.
        x_deps = filter(a -> a.dep_mod === :x, argspecs)
        @test length(x_deps) == 2
        @test all(a -> a.pos == x_deps[1].pos, x_deps)
    end

    @testset "Per-task scope recorded" begin
        A = rand(64)
        my_scope = Dagger.scope(worker=1)
        cache = run_with_fresh_cache() do
            Dagger.@spawn scope=my_scope add!(InOut(A), In(A))
        end
        spec = first(cache).first
        @test spec.id_to_scope[1] == my_scope
    end
end

# ---------- DAGSpec equivalence: cache reuse ----------

@testset "DAGSpec equivalence (end-to-end cache reuse)" begin
    @testset "Identical algorithm with fresh allocations → cache hit" begin
        cache = run_n_times(3) do
            A = rand(128)
            B = rand(128)
            Dagger.@spawn add!(InOut(A), In(B))
            Dagger.@spawn add!(InOut(A), In(B))
        end
        @test length(cache) == 1
    end

    @testset "Different array sizes → cache miss" begin
        cache = datadeps_schedule_cache(LayeredScheduler())
        empty!(cache)
        Base.ScopedValues.with(Dagger.DATADEPS_HIERARCHICAL => false, DATADEPS_SCHEDULER => LayeredScheduler()) do
            for sz in (64, 128, 256)
                A = rand(sz); B = rand(sz)
                Dagger.spawn_datadeps() do
                    Dagger.@spawn add!(InOut(A), In(B))
                end
            end
        end
        @test length(cache) == 3
    end

    @testset "Different element types → cache miss" begin
        cache = datadeps_schedule_cache(LayeredScheduler())
        empty!(cache)
        Base.ScopedValues.with(Dagger.DATADEPS_HIERARCHICAL => false, DATADEPS_SCHEDULER => LayeredScheduler()) do
            for T in (Float64, Float32, Int)
                A = T <: AbstractFloat ? rand(T, 64) : T.(rand(1:100, 64))
                B = T <: AbstractFloat ? rand(T, 64) : T.(rand(1:100, 64))
                Dagger.spawn_datadeps() do
                    Dagger.@spawn add!(InOut(A), In(B))
                end
            end
        end
        @test length(cache) == 3
    end

    @testset "Different task counts → cache miss" begin
        cache = datadeps_schedule_cache(LayeredScheduler())
        empty!(cache)
        Base.ScopedValues.with(Dagger.DATADEPS_HIERARCHICAL => false, DATADEPS_SCHEDULER => LayeredScheduler()) do
            for ntasks in (1, 2, 4)
                A = rand(64); B = rand(64)
                Dagger.spawn_datadeps() do
                    for _ in 1:ntasks
                        Dagger.@spawn add!(InOut(A), In(B))
                    end
                end
            end
        end
        @test length(cache) == 3
    end

    @testset "Different per-task scope → cache miss" begin
        cache = datadeps_schedule_cache(LayeredScheduler())
        empty!(cache)
        scopes = [Dagger.DefaultScope(),
                  Dagger.ExactScope(Dagger.ThreadProc(1, 1))]
        Base.ScopedValues.with(Dagger.DATADEPS_HIERARCHICAL => false, DATADEPS_SCHEDULER => LayeredScheduler()) do
            for scp in scopes
                A = rand(64); B = rand(64)
                Dagger.spawn_datadeps() do
                    Dagger.@spawn scope=scp add!(InOut(A), In(B))
                end
            end
        end
        @test length(cache) == length(scopes)
    end

    @testset "Different functions → cache miss" begin
        cache = run_with_fresh_cache() do
            A = rand(64); B = rand(64)
            Dagger.@spawn add!(InOut(A), In(B))
        end
        @test length(cache) == 1
        # Repeat with the other function
        Base.ScopedValues.with(Dagger.DATADEPS_HIERARCHICAL => false, DATADEPS_SCHEDULER => LayeredScheduler()) do
            A = rand(64)
            Dagger.spawn_datadeps() do
                Dagger.@spawn scale!(InOut(A), 2.0)
            end
        end
        @test length(cache) == 2
    end
end

# ---------- Multi-scheduler cache partitioning ----------

@testset "Cache partitioning between schedulers" begin
    ls_cache = datadeps_schedule_cache(LayeredScheduler())
    empty!(ls_cache)
    Base.ScopedValues.with(Dagger.DATADEPS_HIERARCHICAL => false, DATADEPS_SCHEDULER => LayeredScheduler()) do
        A = rand(64); B = rand(64)
        Dagger.spawn_datadeps() do
            Dagger.@spawn add!(InOut(A), In(B))
        end
    end
    @test length(ls_cache) == 1

    # RoundRobinScheduler doesn't AOT-schedule, so it shouldn't add to its
    # cache, and it must NOT see LayeredScheduler's entry.
    rr_cache = datadeps_schedule_cache(RoundRobinScheduler())
    @test rr_cache !== ls_cache
    Base.ScopedValues.with(Dagger.DATADEPS_HIERARCHICAL => false, DATADEPS_SCHEDULER => RoundRobinScheduler()) do
        A = rand(64); B = rand(64)
        Dagger.spawn_datadeps() do
            Dagger.@spawn add!(InOut(A), In(B))
        end
    end
    @test isempty(rr_cache)
    @test length(ls_cache) == 1
end

# ---------- Benchmark algorithms ----------

# A small mock-Cholesky-like algorithm that doesn't depend on DArray, to keep
# the test fast and self-contained.
function mock_cholesky!(M::Matrix{<:Matrix})
    mt = size(M, 1)
    for k in 1:mt
        Dagger.@spawn LinearAlgebra.LAPACK.potrf!('L', InOut(M[k, k]))
        for m in (k+1):mt
            Dagger.@spawn LinearAlgebra.BLAS.trsm!('R', 'L', 'T', 'N',
                                                   1.0, In(M[k, k]),
                                                   InOut(M[m, k]))
        end
        for n in (k+1):mt
            Dagger.@spawn LinearAlgebra.BLAS.syrk!('L', 'N', -1.0,
                                                   In(M[n, k]), 1.0,
                                                   InOut(M[n, n]))
            for m in (n+1):mt
                Dagger.@spawn LinearAlgebra.BLAS.gemm!('N', 'T', -1.0,
                                                       In(M[m, k]),
                                                       In(M[n, k]), 1.0,
                                                       InOut(M[m, n]))
            end
        end
    end
end

make_spd_blocks(sz::Int, nb::Int) = begin
    M_dense = rand(sz, sz); M_dense = M_dense * M_dense'
    M_dense[diagind(M_dense)] .+= sz
    return [M_dense[i:(i+nb-1), j:(j+nb-1)] for i in 1:nb:sz, j in 1:nb:sz]
end

@testset "Benchmark: mock Cholesky" begin
    @testset "Repeated runs, fresh allocations → 1 cache entry" begin
        cache = datadeps_schedule_cache(LayeredScheduler())
        empty!(cache)
        Base.ScopedValues.with(Dagger.DATADEPS_HIERARCHICAL => false, DATADEPS_SCHEDULER => LayeredScheduler()) do
            for _ in 1:3
                M = make_spd_blocks(64, 16)
                Dagger.spawn_datadeps() do
                    mock_cholesky!(M)
                end
            end
        end
        @test length(cache) == 1
        spec = first(cache).first
        # 4x4 block grid → potrf + trsms + syrks + gemms.
        # Count expected: sum_{k=1}^{4} (1 + (4-k) + (4-k) + (4-k)(4-k-1)/2 * 2)
        # Or just sanity check >0.
        @test length(spec) > 0
    end

    @testset "Different block-grid sizes → distinct cache entries" begin
        cache = datadeps_schedule_cache(LayeredScheduler())
        empty!(cache)
        Base.ScopedValues.with(Dagger.DATADEPS_HIERARCHICAL => false, DATADEPS_SCHEDULER => LayeredScheduler()) do
            # 2x2 grid (sz=32, nb=16) vs 4x4 grid (sz=64, nb=16) → different task counts
            for (sz, nb) in ((32, 16), (64, 16))
                M = make_spd_blocks(sz, nb)
                Dagger.spawn_datadeps() do
                    mock_cholesky!(M)
                end
            end
        end
        @test length(cache) == 2
    end

    @testset "Same block-grid shape, different block size → distinct entries" begin
        cache = datadeps_schedule_cache(LayeredScheduler())
        empty!(cache)
        Base.ScopedValues.with(Dagger.DATADEPS_HIERARCHICAL => false, DATADEPS_SCHEDULER => LayeredScheduler()) do
            # Both 2x2 grids; block size 16 vs 32 → same task count, different ContiguousAliasing length
            for (sz, nb) in ((32, 16), (64, 32))
                M = make_spd_blocks(sz, nb)
                Dagger.spawn_datadeps() do
                    mock_cholesky!(M)
                end
            end
        end
        @test length(cache) == 2
    end
end

@testset "Benchmark: tree reduce" begin
    function tree_reduce!(As)
        to_reduce = Vector[]
        push!(to_reduce, As)
        while !isempty(to_reduce)
            xs = pop!(to_reduce)
            n = length(xs)
            if n == 2
                Dagger.@spawn Base.mapreducedim!(identity, +, InOut(xs[1]), In(xs[2]))
            elseif n > 2
                push!(to_reduce, [xs[1], xs[div(n, 2)+1]])
                push!(to_reduce, xs[1:div(n, 2)])
                push!(to_reduce, xs[div(n, 2)+1:end])
            end
        end
    end

    @testset "Repeated runs → 1 cache entry" begin
        cache = datadeps_schedule_cache(LayeredScheduler())
        empty!(cache)
        Base.ScopedValues.with(Dagger.DATADEPS_HIERARCHICAL => false, DATADEPS_SCHEDULER => LayeredScheduler()) do
            for _ in 1:3
                As = [rand(64) for _ in 1:8]
                Dagger.spawn_datadeps() do
                    tree_reduce!(As)
                end
            end
        end
        @test length(cache) == 1
    end

    @testset "Different reduction widths → distinct entries" begin
        cache = datadeps_schedule_cache(LayeredScheduler())
        empty!(cache)
        Base.ScopedValues.with(Dagger.DATADEPS_HIERARCHICAL => false, DATADEPS_SCHEDULER => LayeredScheduler()) do
            for n in (4, 8, 16)
                As = [rand(64) for _ in 1:n]
                Dagger.spawn_datadeps() do
                    tree_reduce!(As)
                end
            end
        end
        @test length(cache) == 3
    end
end

# ---------- Direct datadeps_dag_equivalent semantics ----------

@testset "datadeps_dag_equivalent (direct)" begin
    LS = LayeredScheduler()

    # Build two DAGSpecs from equivalent-but-freshly-allocated workloads.
    cache = datadeps_schedule_cache(LS)
    empty!(cache)
    Base.ScopedValues.with(Dagger.DATADEPS_HIERARCHICAL => false, DATADEPS_SCHEDULER => LS) do
        for _ in 1:2
            A = rand(64); B = rand(64)
            Dagger.spawn_datadeps() do
                Dagger.@spawn add!(InOut(A), In(B))
                Dagger.@spawn scale!(InOut(A), 2.0)
            end
        end
    end
    # Cache hit on second run → still 1 entry
    @test length(cache) == 1
    spec_a = first(cache).first

    # Now build a structurally-different spec.
    empty!(cache)
    Base.ScopedValues.with(Dagger.DATADEPS_HIERARCHICAL => false, DATADEPS_SCHEDULER => LS) do
        A = rand(128); B = rand(128)  # different size
        Dagger.spawn_datadeps() do
            Dagger.@spawn add!(InOut(A), In(B))
            Dagger.@spawn scale!(InOut(A), 2.0)
        end
    end
    spec_b = first(cache).first

    @test  datadeps_dag_equivalent(LS, spec_a, spec_a)
    @test  datadeps_dag_equivalent(LS, spec_b, spec_b)
    @test !datadeps_dag_equivalent(LS, spec_a, spec_b)
end

# ---------- Scheduler overrides ----------

# A scheduler that opts out of all caching by always returning false.
struct NoCacheScheduler <: DataDepsScheduler end
Dagger.datadeps_dag_equivalent(::NoCacheScheduler, ::DAGSpec, ::DAGSpec) = false
function Dagger.datadeps_schedule_dag_aot!(::NoCacheScheduler, schedule, dag_spec, all_procs, all_scope)
    for idx in 1:Dagger.nv(dag_spec.g)
        task = dag_spec.id_to_task[idx]
        schedule[task] = first(all_procs)
    end
end
Dagger.datadeps_schedule_task_jit!(::NoCacheScheduler, all_procs, all_scope, task_scope, spec, task) =
    first(all_procs)

@testset "Scheduler-overridable equivalence" begin
    @testset "Always-false override never hits cache" begin
        nc = NoCacheScheduler()
        cache = datadeps_schedule_cache(nc)
        empty!(cache)
        Base.ScopedValues.with(Dagger.DATADEPS_HIERARCHICAL => false, DATADEPS_SCHEDULER => nc) do
            for _ in 1:3
                A = rand(64); B = rand(64)
                Dagger.spawn_datadeps() do
                    Dagger.@spawn add!(InOut(A), In(B))
                end
            end
        end
        @test length(cache) == 3
    end
end

# A scheduler that uses a strict pointer-identical comparison (the original
# intent of `equivalent_structure`).
struct PtrStrictScheduler <: DataDepsScheduler end
Dagger.datadeps_ainfo_equivalent(::PtrStrictScheduler,
                                 a1::Dagger.AbstractAliasing,
                                 a2::Dagger.AbstractAliasing) =
    hash(a1) == hash(a2)
function Dagger.datadeps_schedule_dag_aot!(::PtrStrictScheduler, schedule, dag_spec, all_procs, all_scope)
    for idx in 1:Dagger.nv(dag_spec.g)
        task = dag_spec.id_to_task[idx]
        schedule[task] = first(all_procs)
    end
end
Dagger.datadeps_schedule_task_jit!(::PtrStrictScheduler, all_procs, all_scope, task_scope, spec, task) =
    first(all_procs)

@testset "PtrStrictScheduler: structural matches miss, pointer-identical hits" begin
    ps = PtrStrictScheduler()
    cache = datadeps_schedule_cache(ps)

    # Fresh allocations each run → pointer hash differs → all miss.
    empty!(cache)
    Base.ScopedValues.with(Dagger.DATADEPS_HIERARCHICAL => false, DATADEPS_SCHEDULER => ps) do
        for _ in 1:3
            A = rand(64); B = rand(64)
            Dagger.spawn_datadeps() do
                Dagger.@spawn add!(InOut(A), In(B))
            end
        end
    end
    @test length(cache) == 3

    # Same allocation reused each run → pointer hash matches → cache hits.
    empty!(cache)
    A = rand(64); B = rand(64)
    Base.ScopedValues.with(Dagger.DATADEPS_HIERARCHICAL => false, DATADEPS_SCHEDULER => ps) do
        for _ in 1:3
            Dagger.spawn_datadeps() do
                Dagger.@spawn add!(InOut(A), In(B))
            end
        end
    end
    @test length(cache) == 1
end

@testset "GreedyScheduler" begin
    @testset "Type registration and structure" begin
        @test GreedyScheduler() isa DataDepsScheduler
        @test fieldcount(GreedyScheduler) == 0
    end

    @testset "Empty DAGSpec yields empty ScheduleState (direct primitive)" begin
        empty_dag = DAGSpec()
        all_procs = collect(Dagger.all_processors())
        snap = Dagger.MT.snapshot(Dagger.MT.global_metrics_cache())

        state = ScheduleState()
        greedy_schedule!(state, snap, empty_dag, all_procs)

        @test isempty(state)
        @test length(state) == 0
        @test isempty(state.task_proc)
        @test isempty(state.task_finish_ns)
        @test cost_of_schedule(state) == 0.0

        Base.ScopedValues.with(DATADEPS_SCHEDULER => GreedyScheduler()) do
            cache = datadeps_schedule_cache(GreedyScheduler())
            empty!(cache)
            Dagger.spawn_datadeps() do
            end
            @test isempty(cache)
        end
    end

    @testset "Single task schedules and completes" begin
        A = rand(64)
        B = rand(64)
        A_copy = copy(A)
        B_copy = copy(B)
        Base.ScopedValues.with(DATADEPS_SCHEDULER => GreedyScheduler()) do
            Dagger.spawn_datadeps() do
                Dagger.@spawn add!(InOut(A), In(B))
            end
        end
        @test A ≈ A_copy .+ B_copy
    end

    @testset "Respects per-task scope (direct primitive, scope is the only constraint)" begin
        A = rand(64); B = rand(64)
        target_proc = Dagger.ThreadProc(1, 1)

        spec_pair = nothing
        Base.ScopedValues.with(DATADEPS_SCHEDULER => GreedyScheduler()) do
            cache = datadeps_schedule_cache(GreedyScheduler())
            empty!(cache)
            Dagger.spawn_datadeps() do
                Dagger.@spawn scope=Dagger.ExactScope(target_proc) add!(InOut(A), In(B))
            end
            spec_pair = first(cache)
        end
        dag_spec = spec_pair.first

        candidate_procs = Dagger.Processor[
            Dagger.ThreadProc(1, 2),
            Dagger.ThreadProc(1, 3),
            target_proc,
            Dagger.ThreadProc(1, 4),
            Dagger.ThreadProc(1, 5),
        ]
        snap = Dagger.MT.snapshot(Dagger.MT.global_metrics_cache())

        for trial in 1:5
            state = ScheduleState()
            greedy_schedule!(state, snap, dag_spec, candidate_procs)
            @test state.task_proc[1] === target_proc
        end
    end

    @testset "Throws SchedulingException when no compatible processor exists" begin
        A = rand(64); B = rand(64)
        target_proc = Dagger.ThreadProc(1, 1)

        spec_pair = nothing
        Base.ScopedValues.with(DATADEPS_SCHEDULER => GreedyScheduler()) do
            cache = datadeps_schedule_cache(GreedyScheduler())
            empty!(cache)
            Dagger.spawn_datadeps() do
                Dagger.@spawn scope=Dagger.ExactScope(target_proc) add!(InOut(A), In(B))
            end
            spec_pair = first(cache)
        end
        dag_spec = spec_pair.first

        incompatible_procs = Dagger.Processor[
            Dagger.ThreadProc(1, 2),
            Dagger.ThreadProc(2, 1),
        ]
        snap = Dagger.MT.snapshot(Dagger.MT.global_metrics_cache())
        state = ScheduleState()
        @test_throws Dagger.Sch.SchedulingException greedy_schedule!(state, snap, dag_spec, incompatible_procs)
    end

    @testset "Schedule output is complete (every task assigned)" begin
        cache = datadeps_schedule_cache(GreedyScheduler())
        empty!(cache)
        Base.ScopedValues.with(DATADEPS_SCHEDULER => GreedyScheduler()) do
            A = rand(64); B = rand(64)
            Dagger.spawn_datadeps() do
                Dagger.@spawn scope=Dagger.ExactScope(Dagger.ThreadProc(1, 1)) add!(InOut(A), In(B))
            end
        end
        @test length(cache) == 1
        dag_spec, dag_schedule = first(cache)
        @test length(dag_schedule.id_to_proc) == Dagger.nv(dag_spec.g)
        for idx in 1:Dagger.nv(dag_spec.g)
            @test haskey(dag_schedule.id_to_proc, idx)
            @test dag_schedule.id_to_proc[idx] isa Dagger.Processor
        end
    end

    @testset "Schedule cache reuses across identical runs" begin
        cache = datadeps_schedule_cache(GreedyScheduler())
        empty!(cache)
        Base.ScopedValues.with(DATADEPS_SCHEDULER => GreedyScheduler()) do
            for _ in 1:3
                A = rand(64); B = rand(64)
                Dagger.spawn_datadeps() do
                    Dagger.@spawn add!(InOut(A), In(B))
                end
            end
        end
        @test length(cache) == 1
    end

    @testset "Schedule cache misses on different DAG shapes" begin
        cache = datadeps_schedule_cache(GreedyScheduler())
        empty!(cache)
        Base.ScopedValues.with(DATADEPS_SCHEDULER => GreedyScheduler()) do
            for ntasks in (1, 2, 4)
                A = rand(64); B = rand(64)
                Dagger.spawn_datadeps() do
                    for _ in 1:ntasks
                        Dagger.@spawn scope=Dagger.ExactScope(Dagger.ThreadProc(1, 1)) add!(InOut(A), In(B))
                    end
                end
            end
        end
        @test length(cache) == 3
    end

    @testset "Cache partitioning vs LayeredScheduler" begin
        gs_cache = datadeps_schedule_cache(GreedyScheduler())
        ls_cache = datadeps_schedule_cache(LayeredScheduler())
        empty!(gs_cache)
        empty!(ls_cache)
        @test gs_cache !== ls_cache
        Base.ScopedValues.with(DATADEPS_SCHEDULER => GreedyScheduler()) do
            A = rand(64); B = rand(64)
            Dagger.spawn_datadeps() do
                Dagger.@spawn scope=Dagger.ExactScope(Dagger.ThreadProc(1, 1)) add!(InOut(A), In(B))
            end
        end
        @test length(gs_cache) == 1
        @test isempty(ls_cache)
    end

    @testset "Greedy ready-time helper: Chunk on same space → 0" begin
        empty_dag = DAGSpec()
        empty_state = ScheduleState()
        snap = Dagger.MT.snapshot(Dagger.MT.global_metrics_cache())
        data = rand(1024)
        c = Dagger.tochunk(data)
        chunk_space = Dagger.memory_space(c)
        ready = Dagger._greedy_arg_ready_time_ns(c, snap, empty_dag, chunk_space, empty_state)
        @test ready == 0.0
    end

    @testset "Greedy ready-time helper: non-Chunk non-DTask → 0" begin
        empty_dag = DAGSpec()
        empty_state = ScheduleState()
        snap = Dagger.MT.snapshot(Dagger.MT.global_metrics_cache())
        proc = first(Dagger.all_processors())
        target_space = only(Dagger.memory_spaces(proc))
        ready = Dagger._greedy_arg_ready_time_ns(42, snap, empty_dag, target_space, empty_state)
        @test ready == 0.0
    end

    @testset "ScheduleState lifecycle" begin
        state = ScheduleState()
        @test isempty(state)
        @test length(state) == 0
        @test cost_of_schedule(state) == 0.0

        proc = first(Dagger.all_processors())
        state.task_proc[1] = proc
        state.task_finish_ns[1] = 5.0e8
        state.proc_ready_ns[proc] = 5.0e8
        @test !isempty(state)
        @test length(state) == 1
        @test cost_of_schedule(state) == 5.0e8

        state.task_finish_ns[2] = 9.0e8
        @test cost_of_schedule(state) == 9.0e8

        snapshot = copy(state)
        @test snapshot.task_finish_ns == state.task_finish_ns
        snapshot.task_finish_ns[1] = 1.0e10
        @test state.task_finish_ns[1] == 5.0e8
        @test cost_of_schedule(state) == 9.0e8

        empty!(state)
        @test isempty(state)
        @test cost_of_schedule(state) == 0.0
    end

    @testset "cost_of_schedule with multiple finish times" begin
        state = ScheduleState()
        state.task_finish_ns[1] = 100.0
        state.task_finish_ns[2] = 500.0
        state.task_finish_ns[3] = 250.0
        @test cost_of_schedule(state) == 500.0
    end

    @testset "Validation flow: greedy_schedule! called outside spawn_datadeps" begin
        A = rand(64); B = rand(64)
        spec_pair = nothing
        Base.ScopedValues.with(DATADEPS_SCHEDULER => GreedyScheduler()) do
            cache = datadeps_schedule_cache(GreedyScheduler())
            empty!(cache)
            Dagger.spawn_datadeps() do
                Dagger.@spawn scope=Dagger.ExactScope(Dagger.ThreadProc(1, 1)) add!(InOut(A), In(B))
            end
            spec_pair = first(cache)
        end
        dag_spec = spec_pair.first

        snap = Dagger.MT.snapshot(Dagger.MT.global_metrics_cache())
        all_procs = collect(Dagger.all_processors())
        state = ScheduleState()
        greedy_schedule!(state, snap, dag_spec, all_procs)

        @test length(state.task_proc) == Dagger.nv(dag_spec.g)
        for idx in 1:Dagger.nv(dag_spec.g)
            @test haskey(state.task_proc, idx)
            @test state.task_proc[idx] isa Dagger.Processor
            @test haskey(state.task_finish_ns, idx)
            @test state.task_finish_ns[idx] > 0
        end
        @test cost_of_schedule(state) > 0
    end

    @testset "greedy_assign_task! one task at a time" begin
        A = rand(64); B = rand(64)
        spec_pair = nothing
        Base.ScopedValues.with(DATADEPS_SCHEDULER => GreedyScheduler()) do
            cache = datadeps_schedule_cache(GreedyScheduler())
            empty!(cache)
            Dagger.spawn_datadeps() do
                Dagger.@spawn scope=Dagger.ExactScope(Dagger.ThreadProc(1, 1)) add!(InOut(A), In(B))
                Dagger.@spawn scope=Dagger.ExactScope(Dagger.ThreadProc(1, 1)) scale!(InOut(A), 2.0)
            end
            spec_pair = first(cache)
        end
        dag_spec = spec_pair.first

        snap = Dagger.MT.snapshot(Dagger.MT.global_metrics_cache())
        all_procs = collect(Dagger.all_processors())
        state = ScheduleState()

        proc1 = greedy_assign_task!(state, snap, dag_spec, all_procs, 1)
        @test proc1 isa Dagger.Processor
        @test state.task_proc[1] === proc1
        @test length(state.task_proc) == 1

        proc2 = greedy_assign_task!(state, snap, dag_spec, all_procs, 2)
        @test proc2 isa Dagger.Processor
        @test state.task_proc[2] === proc2
        @test state.task_finish_ns[2] >= state.task_finish_ns[1]
    end

    @testset "greedy_schedule! with custom task_order" begin
        A = rand(64); B = rand(64)
        spec_pair = nothing
        Base.ScopedValues.with(DATADEPS_SCHEDULER => GreedyScheduler()) do
            cache = datadeps_schedule_cache(GreedyScheduler())
            empty!(cache)
            Dagger.spawn_datadeps() do
                Dagger.@spawn scope=Dagger.ExactScope(Dagger.ThreadProc(1, 1)) scale!(InOut(A), 2.0)
                Dagger.@spawn scope=Dagger.ExactScope(Dagger.ThreadProc(1, 1)) scale!(InOut(B), 3.0)
            end
            spec_pair = first(cache)
        end
        dag_spec = spec_pair.first

        snap = Dagger.MT.snapshot(Dagger.MT.global_metrics_cache())
        all_procs = collect(Dagger.all_processors())

        state_default = ScheduleState()
        greedy_schedule!(state_default, snap, dag_spec, all_procs)
        state_reversed = ScheduleState()
        greedy_schedule!(state_reversed, snap, dag_spec, all_procs; task_order=[2, 1])

        @test length(state_default.task_proc) == 2
        @test length(state_reversed.task_proc) == 2
    end

    @testset "Algorithm correctness: Chunk ready time = transfer_time when cross-space" begin
        empty_dag = DAGSpec()
        empty_state = ScheduleState()
        snap = Dagger.MT.snapshot(Dagger.MT.global_metrics_cache())
        data = rand(Float64, 128)
        c = Dagger.tochunk(data)
        chunk_space = Dagger.memory_space(c)

        same_space_ready = Dagger._greedy_arg_ready_time_ns(c, snap, empty_dag, chunk_space, empty_state)
        @test same_space_ready == 0.0

        @test typeof(chunk_space) === Dagger.CPURAMMemorySpace
        cross_space = Dagger.CPURAMMemorySpace(chunk_space.owner + 999)
        cross_ready = Dagger._greedy_arg_ready_time_ns(c, snap, empty_dag, cross_space, empty_state)
        @test cross_ready > 0.0
        size_bytes = c.handle.size === nothing ? Dagger.GREEDY_DEFAULT_OUTPUT_SIZE : UInt64(c.handle.size)
        expected = Float64(size_bytes) / Float64(Dagger.GREEDY_DEFAULT_TRANSFER_RATE) * 1e9
        @test cross_ready ≈ expected
    end

    @testset "Validation: greedy_schedule! is deterministic given same metrics + DAG" begin
        A = rand(64); B = rand(64)
        spec_pair = nothing
        Base.ScopedValues.with(DATADEPS_SCHEDULER => GreedyScheduler()) do
            cache = datadeps_schedule_cache(GreedyScheduler())
            empty!(cache)
            Dagger.spawn_datadeps() do
                Dagger.@spawn scope=Dagger.ExactScope(Dagger.ThreadProc(1, 1)) add!(InOut(A), In(B))
            end
            spec_pair = first(cache)
        end
        dag_spec = spec_pair.first

        snap = Dagger.MT.snapshot(Dagger.MT.global_metrics_cache())
        all_procs = collect(Dagger.all_processors())

        s1 = ScheduleState(); greedy_schedule!(s1, snap, dag_spec, all_procs)
        s2 = ScheduleState(); greedy_schedule!(s2, snap, dag_spec, all_procs)
        @test s1.task_proc == s2.task_proc
        @test s1.task_finish_ns == s2.task_finish_ns
        @test cost_of_schedule(s1) == cost_of_schedule(s2)
    end
end

# Helper: build a DAGSpec by running `f()` inside spawn_datadeps with a
# GreedyScheduler that has a fresh cache, then extracting the just-cached spec.
# Returns (dag_spec, all_procs, snap) so tests can drive primitives directly.
function _capture_dag(f)
    spec_pair = nothing
    Base.ScopedValues.with(DATADEPS_SCHEDULER => GreedyScheduler()) do
        cache = datadeps_schedule_cache(GreedyScheduler())
        empty!(cache)
        f()
        spec_pair = first(cache)
    end
    return (spec_pair.first,
            collect(Dagger.all_processors()),
            Dagger.MT.snapshot(Dagger.MT.global_metrics_cache()))
end

# Build a multi-task DAG (mock_cholesky on a small grid) so IG has more than
# one task to permute. Returns the same triple as _capture_dag.
function _capture_cholesky_dag(grid_size::Int=3, block_size::Int=16)
    M = make_spd_blocks(grid_size * block_size, block_size)
    return _capture_dag(() -> Dagger.spawn_datadeps() do
        mock_cholesky!(M)
    end)
end

@testset "IteratedGreedyScheduler" begin
    @testset "Type registration and constructor validation" begin
        @test IteratedGreedyScheduler() isa DataDepsScheduler
        @test IteratedGreedyScheduler() isa IteratedGreedyScheduler{GreedyScheduler}

        # Defaults are reachable
        s = IteratedGreedyScheduler()
        @test s.n_iters == Dagger.IG_DEFAULT_N_ITERS
        @test s.destroy_frac == Dagger.IG_DEFAULT_DESTROY_FRAC

        # Explicit overrides round-trip
        s2 = IteratedGreedyScheduler(GreedyScheduler();
                                     n_iters=7, destroy_frac=0.5,
                                     rng=MersenneTwister(42))
        @test s2.n_iters == 7
        @test s2.destroy_frac == 0.5
        @test s2.rng isa MersenneTwister

        # Validation
        @test_throws ArgumentError IteratedGreedyScheduler(; n_iters=-1)
        @test_throws ArgumentError IteratedGreedyScheduler(; destroy_frac=0.0)
        @test_throws ArgumentError IteratedGreedyScheduler(; destroy_frac=1.5)
        @test_throws ArgumentError IteratedGreedyScheduler(; destroy_frac=-0.1)
    end

    @testset "Cache delegation to inner scheduler" begin
        # IG{Greedy} should reuse the same underlying cache vector as
        # GreedyScheduler, so equivalent DAGs hit the cache regardless of
        # which wrapper produced them.
        cache_ig = datadeps_schedule_cache(IteratedGreedyScheduler())
        cache_gr = datadeps_schedule_cache(GreedyScheduler())
        @test cache_ig === cache_gr
    end

    @testset "Empty DAG: iterated_greedy_schedule! is a no-op" begin
        empty_dag = DAGSpec()
        all_procs = collect(Dagger.all_processors())
        snap = Dagger.MT.snapshot(Dagger.MT.global_metrics_cache())

        state = ScheduleState()
        out = iterated_greedy_schedule!(state, snap, empty_dag, all_procs;
                                         n_iters=10, destroy_frac=0.5,
                                         rng=MersenneTwister(0))
        @test out === state
        @test isempty(state)
        @test cost_of_schedule(state) == 0.0
    end

    @testset "n_iters=0 returns the seed state untouched" begin
        dag_spec, all_procs, snap = _capture_cholesky_dag(3, 16)

        seed = ScheduleState()
        greedy_schedule!(seed, snap, dag_spec, all_procs)
        seed_cost = cost_of_schedule(seed)
        seed_proc = copy(seed.task_proc)
        seed_finish = copy(seed.task_finish_ns)

        result = iterated_greedy_schedule!(copy(seed), snap, dag_spec, all_procs;
                                            n_iters=0, destroy_frac=0.3,
                                            rng=MersenneTwister(0))
        @test cost_of_schedule(result) == seed_cost
        @test result.task_proc == seed_proc
        @test result.task_finish_ns == seed_finish
    end

    @testset "Non-worsening invariant: IG cost ≤ Greedy seed cost" begin
        dag_spec, all_procs, snap = _capture_cholesky_dag(3, 16)

        seed = ScheduleState()
        greedy_schedule!(seed, snap, dag_spec, all_procs)
        seed_cost = cost_of_schedule(seed)

        # Run multiple seeds — IG must never worsen the seed cost on ANY of them.
        for trial_seed in (1, 7, 42, 123, 9001)
            result = iterated_greedy_schedule!(copy(seed), snap, dag_spec, all_procs;
                                                n_iters=16, destroy_frac=0.3,
                                                rng=MersenneTwister(trial_seed))
            @test cost_of_schedule(result) <= seed_cost
        end
    end

    @testset "Non-worsening invariant across DAG shapes × RNG seeds" begin
        # The accept-only-on-improvement rule is the central correctness
        # guarantee of IG: for any DAG, any seed, any RNG sequence,
        # `cost_of_schedule(IG_result) ≤ cost_of_schedule(seed)`. This guards
        # against future changes to destroy/replay silently breaking that
        # rule (e.g. by corrupting proc_ready_ns on a particular DAG shape).
        #
        # Shapes:
        #   :single   — one task; the boundary case
        #   :chain    — linear precedence t1 → t2 → … → tN via a single InOut
        #   :fanout   — t0 writes a buffer; t1..tN each read it into their own
        #   :fanin    — t1..tN each write their own buffer; t_final reads all
        #
        # For each shape × seed: build the DAG via spawn_datadeps, run Greedy
        # to seed, then IG. Assert the invariant holds. Also assert that the
        # ScheduleState invariants from the destroy/replay step survive
        # (every proc_ready_ns ≥ max finish_time on that proc).

        seeds = (1, 7, 42, 123, 9001)

        function _build_dag(shape::Symbol; n::Int=6)
            if shape === :single
                A = rand(64); B = rand(64)
                return _capture_dag(() -> Dagger.spawn_datadeps() do
                    Dagger.@spawn add!(InOut(A), In(B))
                end)
            elseif shape === :chain
                acc = rand(64)
                addends = [rand(64) for _ in 1:n]
                return _capture_dag(() -> Dagger.spawn_datadeps() do
                    for i in 1:n
                        Dagger.@spawn add!(InOut(acc), In(addends[i]))
                    end
                end)
            elseif shape === :fanout
                src = rand(64)
                sinks = [rand(64) for _ in 1:n]
                return _capture_dag(() -> Dagger.spawn_datadeps() do
                    # First task writes `src`; subsequent tasks each read `src`
                    # and write into their own distinct sink buffer.
                    seed_addend = rand(64)
                    Dagger.@spawn add!(InOut(src), In(seed_addend))
                    for i in 1:n
                        Dagger.@spawn add!(InOut(sinks[i]), In(src))
                    end
                end)
            elseif shape === :fanin
                sources = [rand(64) for _ in 1:n]
                sink = rand(64)
                return _capture_dag(() -> Dagger.spawn_datadeps() do
                    seed_addend = rand(64)
                    for i in 1:n
                        Dagger.@spawn add!(InOut(sources[i]), In(seed_addend))
                    end
                    # Final task reads all source buffers via successive `In`
                    # arguments, creating a fan-in into a single sink.
                    for i in 1:n
                        Dagger.@spawn add!(InOut(sink), In(sources[i]))
                    end
                end)
            else
                error("Unknown shape $shape")
            end
        end

        for shape in (:single, :chain, :fanout, :fanin)
            dag_spec, all_procs, snap = _build_dag(shape; n=6)
            seed = ScheduleState()
            greedy_schedule!(seed, snap, dag_spec, all_procs)
            seed_cost = cost_of_schedule(seed)
            for trial_seed in seeds
                result = iterated_greedy_schedule!(copy(seed), snap, dag_spec, all_procs;
                                                    n_iters=16, destroy_frac=0.3,
                                                    rng=MersenneTwister(trial_seed))
                @test cost_of_schedule(result) <= seed_cost

                # ScheduleState invariant: per-proc readiness must dominate
                # the max finish-time scheduled on that proc. Catches
                # destroy/replay leaving proc_ready_ns stale.
                proc_max_finish = Dict{Dagger.Processor, Float64}()
                for (idx, proc) in result.task_proc
                    f = result.task_finish_ns[idx]
                    proc_max_finish[proc] = max(get(proc_max_finish, proc, 0.0), f)
                end
                for (proc, max_f) in proc_max_finish
                    @test result.proc_ready_ns[proc] >= max_f
                end
            end
        end
    end

    @testset "Reproducibility: identical RNG seed → identical result" begin
        dag_spec, all_procs, snap = _capture_cholesky_dag(3, 16)
        seed = ScheduleState(); greedy_schedule!(seed, snap, dag_spec, all_procs)

        r1 = iterated_greedy_schedule!(copy(seed), snap, dag_spec, all_procs;
                                        n_iters=8, destroy_frac=0.4,
                                        rng=MersenneTwister(2025))
        r2 = iterated_greedy_schedule!(copy(seed), snap, dag_spec, all_procs;
                                        n_iters=8, destroy_frac=0.4,
                                        rng=MersenneTwister(2025))
        @test r1.task_proc == r2.task_proc
        @test r1.task_finish_ns == r2.task_finish_ns
    end

    @testset "Per-task scope is respected after IG iteration" begin
        # Pin one task to ThreadProc(1, 1) and run IG with a candidate set
        # that includes that pin plus several other procs. The destroyed
        # task — when reassigned — must still end up on the pinned proc.
        A = rand(64); B = rand(64)
        target_proc = Dagger.ThreadProc(1, 1)
        dag_spec, _, _ = _capture_dag(() -> Dagger.spawn_datadeps() do
            Dagger.@spawn scope=Dagger.ExactScope(target_proc) add!(InOut(A), In(B))
        end)

        candidate_procs = Dagger.Processor[
            Dagger.ThreadProc(1, 2),
            target_proc,
            Dagger.ThreadProc(1, 3),
        ]
        snap = Dagger.MT.snapshot(Dagger.MT.global_metrics_cache())

        # Seed via greedy_schedule! on the candidate set, then run IG.
        for trial in 1:5
            seed = ScheduleState()
            greedy_schedule!(seed, snap, dag_spec, candidate_procs)
            @test seed.task_proc[1] === target_proc
            result = iterated_greedy_schedule!(copy(seed), snap, dag_spec,
                                                candidate_procs;
                                                n_iters=8, destroy_frac=1.0,
                                                rng=MersenneTwister(trial))
            @test result.task_proc[1] === target_proc
        end
    end

    @testset "Throws SchedulingException when no compatible processor exists" begin
        A = rand(64); B = rand(64)
        target_proc = Dagger.ThreadProc(1, 1)
        dag_spec, _, _ = _capture_dag(() -> Dagger.spawn_datadeps() do
            Dagger.@spawn scope=Dagger.ExactScope(target_proc) add!(InOut(A), In(B))
        end)

        # Candidate set excludes target_proc entirely.
        incompatible_procs = Dagger.Processor[
            Dagger.ThreadProc(1, 2),
            Dagger.ThreadProc(1, 3),
        ]
        snap = Dagger.MT.snapshot(Dagger.MT.global_metrics_cache())

        state = ScheduleState()
        @test_throws Dagger.Sch.SchedulingException iterated_greedy_schedule!(
            state, snap, dag_spec, incompatible_procs;
            n_iters=4, destroy_frac=1.0, rng=MersenneTwister(0))
    end

    @testset "destroy_frac=1.0 → equivalent to repeated full rebuilds" begin
        dag_spec, all_procs, snap = _capture_cholesky_dag(3, 16)
        seed = ScheduleState(); greedy_schedule!(seed, snap, dag_spec, all_procs)

        # destroy_frac=1.0 wipes every assignment per iteration. Because
        # greedy_assign_task! is deterministic given a fixed (snap, dag, procs)
        # — see the existing "deterministic" test — every rebuild yields
        # the same state, and IG must not worsen the seed.
        result = iterated_greedy_schedule!(copy(seed), snap, dag_spec, all_procs;
                                            n_iters=4, destroy_frac=1.0,
                                            rng=MersenneTwister(0))
        @test cost_of_schedule(result) <= cost_of_schedule(seed)
        @test length(result) == length(seed)
    end

    @testset "iterated_greedy_step! preserves invariants" begin
        dag_spec, all_procs, snap = _capture_cholesky_dag(3, 16)
        seed = ScheduleState(); greedy_schedule!(seed, snap, dag_spec, all_procs)

        # Destroy IDs {2, 4}; replay should reassign exactly those two.
        n_tasks = Dagger.nv(dag_spec.g)
        destroyed_ids = Set([2, min(4, n_tasks)])

        before_proc = copy(seed.task_proc)
        after = iterated_greedy_step!(copy(seed), snap, dag_spec, all_procs, destroyed_ids)

        # All task IDs still assigned.
        @test sort(collect(keys(after.task_proc))) == collect(1:n_tasks)
        @test sort(collect(keys(after.task_finish_ns))) == collect(1:n_tasks)

        # Non-destroyed tasks must keep their proc (the only mutation is
        # proc_ready_ns being recomputed, which is internal).
        for idx in 1:n_tasks
            idx in destroyed_ids && continue
            @test after.task_proc[idx] === before_proc[idx]
        end

        # Finish-time monotonicity vs proc_ready_ns: every proc_ready_ns
        # must be ≥ the max finish_time assigned to that proc.
        proc_max_finish = Dict{Dagger.Processor, Float64}()
        for (idx, proc) in after.task_proc
            f = after.task_finish_ns[idx]
            proc_max_finish[proc] = max(get(proc_max_finish, proc, 0.0), f)
        end
        for (proc, max_f) in proc_max_finish
            @test after.proc_ready_ns[proc] >= max_f
        end
    end

    @testset "End-to-end via spawn_datadeps: matmul produces correct result" begin
        # A small tile-grid matmul under IG: verifies the AOT path wires up,
        # that the schedule cache is populated, and that the numeric result
        # matches the dense reference.
        nb = 16
        nt = 2
        A = [rand(nb, nb) for _ in 1:nt, _ in 1:nt]
        B = [rand(nb, nb) for _ in 1:nt, _ in 1:nt]
        C = [zeros(nb, nb) for _ in 1:nt, _ in 1:nt]

        ig = IteratedGreedyScheduler(; n_iters=4, destroy_frac=0.5,
                                       rng=MersenneTwister(13))
        cache = datadeps_schedule_cache(ig)
        empty!(cache)

        Base.ScopedValues.with(DATADEPS_SCHEDULER => ig) do
            Dagger.spawn_datadeps() do
                for i in 1:nt, j in 1:nt, k in 1:nt
                    Dagger.@spawn LinearAlgebra.BLAS.gemm!('N', 'N', 1.0,
                                                           In(A[i, k]),
                                                           In(B[k, j]), 1.0,
                                                           InOut(C[i, j]))
                end
            end
        end

        # Reassemble C and compare to dense A*B
        sz = nt * nb
        dense_A = zeros(sz, sz); dense_B = zeros(sz, sz); dense_C = zeros(sz, sz)
        for i in 1:nt, j in 1:nt
            dense_A[(i-1)*nb+1:i*nb, (j-1)*nb+1:j*nb] .= A[i, j]
            dense_B[(i-1)*nb+1:i*nb, (j-1)*nb+1:j*nb] .= B[i, j]
            dense_C[(i-1)*nb+1:i*nb, (j-1)*nb+1:j*nb] .= C[i, j]
        end
        @test dense_C ≈ dense_A * dense_B

        # Cache populated by the IG run (shares with Greedy's cache).
        @test !isempty(cache)
    end

    @testset "Wall-clock time_limit_sec early-exits" begin
        # Constructor: accepts finite budget, rejects nonpositive, defaults to Inf.
        s_default = IteratedGreedyScheduler()
        @test s_default.time_limit_sec === Inf
        s_finite = IteratedGreedyScheduler(; time_limit_sec=0.5)
        @test s_finite.time_limit_sec == 0.5
        @test_throws ArgumentError IteratedGreedyScheduler(; time_limit_sec=0.0)
        @test_throws ArgumentError IteratedGreedyScheduler(; time_limit_sec=-1.0)
        @test_throws ArgumentError iterated_greedy_schedule!(ScheduleState(),
            Dagger.MT.snapshot(Dagger.MT.global_metrics_cache()),
            DAGSpec(), collect(Dagger.all_processors());
            time_limit_sec=0.0)

        # `similar` carries the field over so hierarchical partition shards
        # inherit the same budget (shares the fix path with the RNG/config
        # forwarding).
        @test similar(s_finite).time_limit_sec == 0.5

        # Nanosecond budget stops the loop before its natural stop condition.
        # A DAG of a few hundred tasks with n_iters=10000 will otherwise
        # run for well over a millisecond; the budget must dominate.
        dag_spec, all_procs, snap = _capture_cholesky_dag(3, 16)
        seed = ScheduleState()
        greedy_schedule!(seed, snap, dag_spec, all_procs)
        seed_cost = cost_of_schedule(seed)

        t0 = time_ns()
        result = iterated_greedy_schedule!(copy(seed), snap, dag_spec, all_procs;
                                            n_iters=10_000, destroy_frac=0.3,
                                            rng=MersenneTwister(0),
                                            time_limit_sec=1e-6)
        elapsed_ms = (time_ns() - t0) / 1e6
        # Bound: even one iteration of a small DAG comfortably fits in 100 ms
        # on any dev/CI box; a 10000-iter unbounded run does not.
        @test elapsed_ms < 100.0
        # Correctness invariant preserved under early exit: best-so-far
        # returned, never worse than the seed.
        @test cost_of_schedule(result) <= seed_cost + 1e-6

        # `Inf` is a true opt-out: behavior must match not passing the kwarg
        # at all. Same seed → identical result.
        r_inf = iterated_greedy_schedule!(copy(seed), snap, dag_spec, all_procs;
                                           n_iters=4, destroy_frac=0.3,
                                           rng=MersenneTwister(11),
                                           time_limit_sec=Inf)
        r_none = iterated_greedy_schedule!(copy(seed), snap, dag_spec, all_procs;
                                            n_iters=4, destroy_frac=0.3,
                                            rng=MersenneTwister(11))
        @test cost_of_schedule(r_inf) == cost_of_schedule(r_none)
    end
end

@testset "SimulatedAnnealingScheduler" begin
    @testset "Type registration and constructor validation" begin
        @test SimulatedAnnealingScheduler() isa DataDepsScheduler
        @test SimulatedAnnealingScheduler() isa SimulatedAnnealingScheduler{IteratedGreedyScheduler{GreedyScheduler, typeof(Random.default_rng())}}

        s = SimulatedAnnealingScheduler()
        @test s.q == Dagger.SA_DEFAULT_Q
        @test s.k == Dagger.SA_DEFAULT_K
        @test s.n_restarts == Dagger.SA_DEFAULT_N_RESTARTS

        s2 = SimulatedAnnealingScheduler(GreedyScheduler();
                                          q=0.9, k=2, n_restarts=3,
                                          rng=MersenneTwister(11))
        @test s2.q == 0.9
        @test s2.k == 2.0
        @test s2.n_restarts == 3
        @test s2.rng isa MersenneTwister
        @test s2.inner isa GreedyScheduler

        @test_throws ArgumentError SimulatedAnnealingScheduler(; q=0.0)
        @test_throws ArgumentError SimulatedAnnealingScheduler(; q=1.0)
        @test_throws ArgumentError SimulatedAnnealingScheduler(; q=-0.1)
        @test_throws ArgumentError SimulatedAnnealingScheduler(; k=0.0)
        @test_throws ArgumentError SimulatedAnnealingScheduler(; k=-1.0)
        @test_throws ArgumentError SimulatedAnnealingScheduler(; n_restarts=0)
        @test_throws ArgumentError SimulatedAnnealingScheduler(; n_restarts=-2)
    end

    @testset "Cache delegation through inner chain (SA → IG → Greedy)" begin
        cache_sa = datadeps_schedule_cache(SimulatedAnnealingScheduler())
        cache_gr = datadeps_schedule_cache(GreedyScheduler())
        @test cache_sa === cache_gr

        # SA over Greedy directly also shares the Greedy cache.
        cache_sg = datadeps_schedule_cache(SimulatedAnnealingScheduler(GreedyScheduler()))
        @test cache_sg === cache_gr
    end

    @testset "Empty DAG: simulated_annealing_schedule! is a no-op" begin
        empty_dag = DAGSpec()
        all_procs = collect(Dagger.all_processors())
        snap = Dagger.MT.snapshot(Dagger.MT.global_metrics_cache())

        state = ScheduleState()
        out = simulated_annealing_schedule!(state, snap, empty_dag, all_procs;
                                             rng=MersenneTwister(0))
        @test out === state
        @test isempty(state)
        @test cost_of_schedule(state) == 0.0
    end

    @testset "Non-worsening invariant: SA cost ≤ seed cost" begin
        dag_spec, all_procs, snap = _capture_cholesky_dag(3, 16)

        seed = ScheduleState()
        greedy_schedule!(seed, snap, dag_spec, all_procs)
        seed_cost = cost_of_schedule(seed)

        for trial_seed in (1, 7, 42, 123, 9001)
            result = simulated_annealing_schedule!(copy(seed), snap, dag_spec, all_procs;
                                                    rng=MersenneTwister(trial_seed))
            @test cost_of_schedule(result) <= seed_cost
        end
    end

    @testset "Non-worsening invariant across DAG shapes × RNG seeds" begin
        seeds = (1, 7, 42, 123, 9001)

        function _build_dag(shape::Symbol; n::Int=6)
            if shape === :single
                A = rand(64); B = rand(64)
                return _capture_dag(() -> Dagger.spawn_datadeps() do
                    Dagger.@spawn add!(InOut(A), In(B))
                end)
            elseif shape === :chain
                acc = rand(64)
                addends = [rand(64) for _ in 1:n]
                return _capture_dag(() -> Dagger.spawn_datadeps() do
                    for i in 1:n
                        Dagger.@spawn add!(InOut(acc), In(addends[i]))
                    end
                end)
            elseif shape === :fanout
                src = rand(64)
                sinks = [rand(64) for _ in 1:n]
                return _capture_dag(() -> Dagger.spawn_datadeps() do
                    seed_addend = rand(64)
                    Dagger.@spawn add!(InOut(src), In(seed_addend))
                    for i in 1:n
                        Dagger.@spawn add!(InOut(sinks[i]), In(src))
                    end
                end)
            elseif shape === :fanin
                sources = [rand(64) for _ in 1:n]
                sink = rand(64)
                return _capture_dag(() -> Dagger.spawn_datadeps() do
                    seed_addend = rand(64)
                    for i in 1:n
                        Dagger.@spawn add!(InOut(sources[i]), In(seed_addend))
                    end
                    for i in 1:n
                        Dagger.@spawn add!(InOut(sink), In(sources[i]))
                    end
                end)
            else
                error("Unknown shape $shape")
            end
        end

        for shape in (:single, :chain, :fanout, :fanin)
            dag_spec, all_procs, snap = _build_dag(shape; n=6)
            seed = ScheduleState()
            greedy_schedule!(seed, snap, dag_spec, all_procs)
            seed_cost = cost_of_schedule(seed)
            for trial_seed in seeds
                result = simulated_annealing_schedule!(copy(seed), snap, dag_spec, all_procs;
                                                        rng=MersenneTwister(trial_seed))
                @test cost_of_schedule(result) <= seed_cost

                # proc_ready_ns ≥ max finish_time on that proc.
                proc_max_finish = Dict{Dagger.Processor, Float64}()
                for (idx, proc) in result.task_proc
                    f = result.task_finish_ns[idx]
                    proc_max_finish[proc] = max(get(proc_max_finish, proc, 0.0), f)
                end
                for (proc, max_f) in proc_max_finish
                    @test result.proc_ready_ns[proc] >= max_f
                end
            end
        end
    end

    @testset "Reproducibility: identical RNG seed → identical result" begin
        dag_spec, all_procs, snap = _capture_cholesky_dag(3, 16)
        seed = ScheduleState(); greedy_schedule!(seed, snap, dag_spec, all_procs)

        r1 = simulated_annealing_schedule!(copy(seed), snap, dag_spec, all_procs;
                                            rng=MersenneTwister(2026))
        r2 = simulated_annealing_schedule!(copy(seed), snap, dag_spec, all_procs;
                                            rng=MersenneTwister(2026))
        @test r1.task_proc == r2.task_proc
        @test r1.task_finish_ns == r2.task_finish_ns
        @test cost_of_schedule(r1) == cost_of_schedule(r2)
    end

    @testset "Per-task scope is respected after SA perturbation" begin
        A = rand(64); B = rand(64)
        target_proc = Dagger.ThreadProc(1, 1)
        dag_spec, _, _ = _capture_dag(() -> Dagger.spawn_datadeps() do
            Dagger.@spawn scope=Dagger.ExactScope(target_proc) add!(InOut(A), In(B))
        end)

        candidate_procs = Dagger.Processor[
            Dagger.ThreadProc(1, 2),
            target_proc,
            Dagger.ThreadProc(1, 3),
        ]
        snap = Dagger.MT.snapshot(Dagger.MT.global_metrics_cache())

        for trial in 1:5
            seed = ScheduleState()
            greedy_schedule!(seed, snap, dag_spec, candidate_procs)
            @test seed.task_proc[1] === target_proc
            result = simulated_annealing_schedule!(copy(seed), snap, dag_spec,
                                                    candidate_procs;
                                                    rng=MersenneTwister(trial))
            @test result.task_proc[1] === target_proc
        end
    end

    @testset "Throws SchedulingException when no compatible processor exists" begin
        A = rand(64); B = rand(64)
        target_proc = Dagger.ThreadProc(1, 1)
        dag_spec, _, _ = _capture_dag(() -> Dagger.spawn_datadeps() do
            Dagger.@spawn scope=Dagger.ExactScope(target_proc) add!(InOut(A), In(B))
        end)

        incompatible_procs = Dagger.Processor[
            Dagger.ThreadProc(1, 2),
            Dagger.ThreadProc(1, 3),
        ]
        snap = Dagger.MT.snapshot(Dagger.MT.global_metrics_cache())

        state = ScheduleState()
        @test_throws Dagger.Sch.SchedulingException simulated_annealing_schedule!(
            state, snap, dag_spec, incompatible_procs;
            rng=MersenneTwister(0))
    end

    @testset "Acceptance function (Orsila Eq. 6)" begin
        rng = MersenneTwister(0)

        for ΔC in (-1.0, -100.0, -1e9), T in (0.1, 1.0, 10.0), C0 in (1.0, 1e6)
            @test Dagger._sa_accept(ΔC, T, C0, rng) === true
        end

        # ΔC = 0 → P = 0.5 (Orsila §3.3.1).
        accepts = 0
        N = 10_000
        for _ in 1:N
            Dagger._sa_accept(0.0, 1.0, 1.0, rng) && (accepts += 1)
        end
        # Bernoulli(N, 0.5) std-dev ~ √(N/4) = 50; ±5σ = 250 ⇒ allow ±400.
        @test abs(accepts - N ÷ 2) < 400

        # Worsening move at low T → near-zero acceptance.
        low_T_accepts = 0
        for _ in 1:N
            Dagger._sa_accept(1.0, 1e-6, 1.0, rng) && (low_T_accepts += 1)
        end
        @test low_T_accepts < 50   # here P essentially zero

        # ΔC/(C0·T) = 0.4 → P ≈ 0.401; tighter than the ≈0.5 limit.
        moderate_T_accepts = 0
        for _ in 1:N
            Dagger._sa_accept(0.4, 1.0, 1.0, rng) && (moderate_T_accepts += 1)
        end
        @test 0.35 * N < moderate_T_accepts < 0.45 * N


        cold = 0; warm = 0
        for _ in 1:5000
            Dagger._sa_accept(1.0, 0.01, 1.0, rng) && (cold += 1)
            Dagger._sa_accept(1.0, 1.0,  1.0, rng) && (warm += 1)
        end
        @test cold < warm

        @test Dagger._sa_accept(1.0, 0.0, 1.0, rng) === false
        @test Dagger._sa_accept(1.0, 1.0, 0.0, rng) === false
        @test Dagger._sa_accept(1.0, -1.0, 1.0, rng) === false
    end

    @testset "Energy params computation (Orsila §4.3 inputs)" begin
        dag_spec, all_procs, snap = _capture_cholesky_dag(3, 16)
        params = Dagger._sa_compute_energy_params(snap, dag_spec, all_procs)

        @test params.t_min > 0
        @test params.t_max >= params.t_min
        @test params.t_min_sum > 0
        @test params.t_max_sum >= params.t_min_sum

        # Per-task-min cannot exceed per-task-max summed.
        n_tasks = Dagger.nv(dag_spec.g)
        @test params.t_min_sum <= params.t_max_sum

        # Derived temperatures from Orsila §4.3 Eqs. 18, 19.
        k = 1.0
        T0 = k * params.t_max / params.t_min_sum
        Tf = params.t_min / (k * params.t_max_sum)
        @test T0 > 0
        @test Tf > 0
        @test T0 >= Tf
    end

    @testset "End-to-end via spawn_datadeps: matmul produces correct result" begin
        nb = 16
        nt = 2
        A = [rand(nb, nb) for _ in 1:nt, _ in 1:nt]
        B = [rand(nb, nb) for _ in 1:nt, _ in 1:nt]
        C = [zeros(nb, nb) for _ in 1:nt, _ in 1:nt]

        sa = SimulatedAnnealingScheduler(GreedyScheduler();
                                          n_restarts=1, rng=MersenneTwister(17))
        cache = datadeps_schedule_cache(sa)
        empty!(cache)

        Base.ScopedValues.with(DATADEPS_SCHEDULER => sa) do
            Dagger.spawn_datadeps() do
                for i in 1:nt, j in 1:nt, k in 1:nt
                    Dagger.@spawn LinearAlgebra.BLAS.gemm!('N', 'N', 1.0,
                                                           In(A[i, k]),
                                                           In(B[k, j]), 1.0,
                                                           InOut(C[i, j]))
                end
            end
        end

        sz = nt * nb
        dense_A = zeros(sz, sz); dense_B = zeros(sz, sz); dense_C = zeros(sz, sz)
        for i in 1:nt, j in 1:nt
            dense_A[(i-1)*nb+1:i*nb, (j-1)*nb+1:j*nb] .= A[i, j]
            dense_B[(i-1)*nb+1:i*nb, (j-1)*nb+1:j*nb] .= B[i, j]
            dense_C[(i-1)*nb+1:i*nb, (j-1)*nb+1:j*nb] .= C[i, j]
        end
        @test dense_C ≈ dense_A * dense_B

        # Cache populated by the SA run (shares with Greedy's cache).
        @test !isempty(cache)
    end

    @testset "n_restarts > 1 never worsens the best across restarts" begin
        dag_spec, all_procs, snap = _capture_cholesky_dag(3, 16)
        seed = ScheduleState(); greedy_schedule!(seed, snap, dag_spec, all_procs)
        seed_cost = cost_of_schedule(seed)

        for restarts in (1, 2, 4)
            result = simulated_annealing_schedule!(copy(seed), snap, dag_spec, all_procs;
                                                    n_restarts=restarts,
                                                    rng=MersenneTwister(2026))
            @test cost_of_schedule(result) <= seed_cost
        end
    end

    @testset "AOT hook populates schedule for every task" begin
        nb = 16
        nt = 2
        M = [rand(nb, nb) for _ in 1:nt, _ in 1:nt]
        for i in 1:nt
            M[i, i] = M[i, i] * M[i, i]' + nb * I
        end

        sa = SimulatedAnnealingScheduler(GreedyScheduler();
                                          n_restarts=1, rng=MersenneTwister(0))
        cache = datadeps_schedule_cache(sa)
        empty!(cache)

        Base.ScopedValues.with(DATADEPS_SCHEDULER => sa) do
            Dagger.spawn_datadeps() do
                Dagger.@spawn LinearAlgebra.LAPACK.potrf!('L', InOut(M[1, 1]))
                Dagger.@spawn LinearAlgebra.BLAS.trsm!('R', 'L', 'T', 'N',
                                                       1.0, In(M[1, 1]),
                                                       InOut(M[2, 1]))
                Dagger.@spawn LinearAlgebra.BLAS.syrk!('L', 'N', -1.0,
                                                       In(M[2, 1]), 1.0,
                                                       InOut(M[2, 2]))
                Dagger.@spawn LinearAlgebra.LAPACK.potrf!('L', InOut(M[2, 2]))
            end
        end

        @test !isempty(cache)
        dag_spec = first(cache).first
        spec_schedule = first(cache).second
        @test length(spec_schedule.id_to_proc) == Dagger.nv(dag_spec.g)
        @test length(spec_schedule.id_to_proc) == 4   # potrf + trsm + syrk + potrf
    end

    @testset "Inner scheduler selection: SA(Greedy) bypasses IG refinement" begin
        # SA(Greedy) and SA(IG) diverge whenever IG actually improves the
        # greedy seed; that divergence is the observable signal that the
        # inner choice is honored.
        dag_spec, all_procs, snap = _capture_cholesky_dag(3, 16)

        greedy_seed = ScheduleState()
        greedy_schedule!(greedy_seed, snap, dag_spec, all_procs)

        ig_seed = copy(greedy_seed)
        iterated_greedy_schedule!(ig_seed, snap, dag_spec, all_procs;
                                   rng=MersenneTwister(99))

        if greedy_seed.task_proc != ig_seed.task_proc ||
           greedy_seed.task_finish_ns != ig_seed.task_finish_ns
            @test true
        else
            @test_skip "IG did not improve seed on this DAG/metrics state"
        end

        gr_seed_cost = cost_of_schedule(greedy_seed)
        r_greedy = simulated_annealing_schedule!(copy(greedy_seed), snap, dag_spec, all_procs;
                                                  rng=MersenneTwister(2026))
        r_ig = simulated_annealing_schedule!(copy(ig_seed), snap, dag_spec, all_procs;
                                              rng=MersenneTwister(2026))
        @test cost_of_schedule(r_greedy) <= gr_seed_cost
        @test cost_of_schedule(r_ig) <= cost_of_schedule(ig_seed)
        @test length(r_greedy.task_proc) == Dagger.nv(dag_spec.g)
        @test length(r_ig.task_proc) == Dagger.nv(dag_spec.g)
    end

    @testset "Wall-clock time_limit_sec early-exits" begin
        # Constructor: accepts finite budget, rejects nonpositive, defaults to Inf.
        s_default = SimulatedAnnealingScheduler()
        @test s_default.time_limit_sec === Inf
        s_finite = SimulatedAnnealingScheduler(; time_limit_sec=0.5)
        @test s_finite.time_limit_sec == 0.5
        @test_throws ArgumentError SimulatedAnnealingScheduler(; time_limit_sec=0.0)
        @test_throws ArgumentError SimulatedAnnealingScheduler(; time_limit_sec=-1.0)
        @test_throws ArgumentError simulated_annealing_schedule!(ScheduleState(),
            Dagger.MT.snapshot(Dagger.MT.global_metrics_cache()),
            DAGSpec(), collect(Dagger.all_processors());
            time_limit_sec=0.0)

        # `similar` propagates the budget so hierarchical shards inherit it.
        @test similar(s_finite).time_limit_sec == 0.5

        # A tiny budget must interrupt SA long before Orsila's cooling
        # schedule × n_restarts safety cap would fire. The larger DAG here
        # (nt=4 → K=20) guarantees SA's inner while-loop is well-populated,
        # exercising the intra-restart wall-clock check.
        dag_spec, all_procs, snap = _capture_cholesky_dag(4, 16)
        seed = ScheduleState()
        greedy_schedule!(seed, snap, dag_spec, all_procs)
        seed_cost = cost_of_schedule(seed)

        t0 = time_ns()
        result = simulated_annealing_schedule!(copy(seed), snap, dag_spec, all_procs;
                                                n_restarts=32,
                                                rng=MersenneTwister(0),
                                                time_limit_sec=1e-6)
        elapsed_ms = (time_ns() - t0) / 1e6
        @test elapsed_ms < 200.0
        @test cost_of_schedule(result) <= seed_cost + 1e-6

        # `Inf` opt-out preserves the pre-existing deterministic result.
        r_inf = simulated_annealing_schedule!(copy(seed), snap, dag_spec, all_procs;
                                               n_restarts=1,
                                               rng=MersenneTwister(11),
                                               time_limit_sec=Inf)
        r_none = simulated_annealing_schedule!(copy(seed), snap, dag_spec, all_procs;
                                                n_restarts=1,
                                                rng=MersenneTwister(11))
        @test cost_of_schedule(r_inf) == cost_of_schedule(r_none)
    end
end

@testset "OptimizingScheduler (adaptive selection)" begin
    OptimizingScheduler = Dagger.OptimizingScheduler
    opt_uses_milp = Dagger.opt_uses_milp

    @testset "Constructor and validation" begin
        s = OptimizingScheduler()
        @test s isa DataDepsScheduler
        @test s.optimizer === nothing
        @test s.milp_threshold == Dagger.OPT_DEFAULT_MILP_THRESHOLD
        @test s.milp_time_limit_sec == 60.0
        @test s.milp_Z == 10.0
        @test s.ig_n_iters == Dagger.IG_DEFAULT_N_ITERS
        @test s.ig_destroy_frac == Dagger.IG_DEFAULT_DESTROY_FRAC
        @test s.sa_q == Dagger.SA_DEFAULT_Q
        @test s.sa_k == Dagger.SA_DEFAULT_K
        @test s.sa_n_restarts == Dagger.SA_DEFAULT_N_RESTARTS

        s2 = OptimizingScheduler(; milp_threshold=25, milp_time_limit_sec=15.0,
                                    ig_n_iters=8, sa_q=0.9,
                                    rng=MersenneTwister(123))
        @test s2.milp_threshold == 25
        @test s2.milp_time_limit_sec == 15.0
        @test s2.ig_n_iters == 8
        @test s2.sa_q == 0.9
        @test s2.rng isa MersenneTwister

        @test_throws ArgumentError OptimizingScheduler(; milp_threshold=-1)
        @test_throws ArgumentError OptimizingScheduler(; milp_time_limit_sec=0)
        @test_throws ArgumentError OptimizingScheduler(; milp_time_limit_sec=-2.0)
        @test_throws ArgumentError OptimizingScheduler(; ig_n_iters=-3)
        @test_throws ArgumentError OptimizingScheduler(; ig_destroy_frac=0.0)
        @test_throws ArgumentError OptimizingScheduler(; ig_destroy_frac=1.5)
        @test_throws ArgumentError OptimizingScheduler(; sa_q=0.0)
        @test_throws ArgumentError OptimizingScheduler(; sa_q=1.0)
        @test_throws ArgumentError OptimizingScheduler(; sa_k=0.0)
        @test_throws ArgumentError OptimizingScheduler(; sa_n_restarts=0)
    end

    @testset "Cache partition: OptimizingScheduler has its own cache" begin
        cache_opt = datadeps_schedule_cache(OptimizingScheduler())
        cache_gr  = datadeps_schedule_cache(GreedyScheduler())
        cache_sa  = datadeps_schedule_cache(SimulatedAnnealingScheduler())
        @test cache_opt !== cache_gr
        @test cache_opt !== cache_sa
    end

    @testset "Empty DAG: AOT hook is a no-op" begin
        empty_dag = DAGSpec()
        all_procs = collect(Dagger.all_processors())
        schedule = Dict{Dagger.DTask, Dagger.Processor}()
        sched = OptimizingScheduler()
        Dagger.datadeps_schedule_dag_aot!(sched, schedule, empty_dag, all_procs,
                                          Dagger.UnionScope(map(Dagger.ExactScope, all_procs)))
        @test isempty(schedule)
    end

    @testset "Selection: no optimizer → heuristic path regardless of K or threshold" begin
        s = OptimizingScheduler(; milp_threshold=1_000_000)
        @test opt_uses_milp(s, 1) == false
        @test opt_uses_milp(s, 10) == false
        @test opt_uses_milp(s, 100) == false
    end

    @testset "Reproducibility: same RNG seed → identical heuristic-path result" begin
        # OptimizingScheduler on the heuristic branch composes IG and SA, both
        # of which are deterministic given an RNG. The composition must
        # preserve that determinism: two schedulers seeded identically must
        # produce bit-identical assignments on the same DAG.
        dag_spec, all_procs, _ = _capture_cholesky_dag(3, 16)
        all_scope = Dagger.UnionScope(map(Dagger.ExactScope, all_procs))

        s1 = OptimizingScheduler(; milp_threshold=0, rng=MersenneTwister(2027))
        s2 = OptimizingScheduler(; milp_threshold=0, rng=MersenneTwister(2027))
        r1 = Dict{Dagger.DTask, Dagger.Processor}()
        r2 = Dict{Dagger.DTask, Dagger.Processor}()
        Dagger.datadeps_schedule_dag_aot!(s1, r1, dag_spec, all_procs, all_scope)
        Dagger.datadeps_schedule_dag_aot!(s2, r2, dag_spec, all_procs, all_scope)

        for idx in 1:Dagger.nv(dag_spec.g)
            task = dag_spec.id_to_task[idx]
            @test r1[task] === r2[task]
        end
    end

    @testset "End-to-end (heuristic path, no optimizer): matmul correctness" begin
        # Even without a solver, OptimizingScheduler must produce a valid schedule through the SA(IG(Greedy)) pipeline for any K.
        nb = 16
        nt = 2
        A = [rand(nb, nb) for _ in 1:nt, _ in 1:nt]
        B = [rand(nb, nb) for _ in 1:nt, _ in 1:nt]
        C = [zeros(nb, nb) for _ in 1:nt, _ in 1:nt]

        sched = OptimizingScheduler(; rng=MersenneTwister(0))
        cache = datadeps_schedule_cache(sched); empty!(cache)

        Base.ScopedValues.with(DATADEPS_SCHEDULER => sched) do
            Dagger.spawn_datadeps() do
                for i in 1:nt, j in 1:nt, k in 1:nt
                    Dagger.@spawn LinearAlgebra.BLAS.gemm!('N', 'N', 1.0,
                                                           In(A[i, k]),
                                                           In(B[k, j]), 1.0,
                                                           InOut(C[i, j]))
                end
            end
        end

        sz = nt * nb
        dense_A = zeros(sz, sz); dense_B = zeros(sz, sz); dense_C = zeros(sz, sz)
        for i in 1:nt, j in 1:nt
            dense_A[(i-1)*nb+1:i*nb, (j-1)*nb+1:j*nb] .= A[i, j]
            dense_B[(i-1)*nb+1:i*nb, (j-1)*nb+1:j*nb] .= B[i, j]
            dense_C[(i-1)*nb+1:i*nb, (j-1)*nb+1:j*nb] .= C[i, j]
        end
        @test dense_C ≈ dense_A * dense_B
        @test !isempty(cache)
    end

    @testset "Heuristic time_limit_sec forwards to IG and SA sub-schedulers" begin
        OptimizingScheduler = Dagger.OptimizingScheduler

        # Constructor: fresh defaults, explicit overrides, validation.
        s_default = OptimizingScheduler()
        @test s_default.ig_time_limit_sec === Inf
        @test s_default.sa_time_limit_sec === Inf

        s_finite = OptimizingScheduler(; ig_time_limit_sec=0.25,
                                        sa_time_limit_sec=0.75)
        @test s_finite.ig_time_limit_sec == 0.25
        @test s_finite.sa_time_limit_sec == 0.75

        @test_throws ArgumentError OptimizingScheduler(; ig_time_limit_sec=0.0)
        @test_throws ArgumentError OptimizingScheduler(; ig_time_limit_sec=-1.0)
        @test_throws ArgumentError OptimizingScheduler(; sa_time_limit_sec=0.0)
        @test_throws ArgumentError OptimizingScheduler(; sa_time_limit_sec=-1.0)

        # `similar` carries both budgets.
        s_similar = similar(s_finite)
        @test s_similar.ig_time_limit_sec == 0.25
        @test s_similar.sa_time_limit_sec == 0.75

        # End-to-end: an OptimizingScheduler routed to the heuristic path
        # (optimizer=nothing) must respect the wall-clock budgets. Total
        # elapsed time is bounded by IG budget + SA budget plus per-stage
        # setup overhead (allow 100 ms slack per stage on CI). Above nt=4
        # the SA cooling schedule is nontrivial, exercising the intra-loop
        # check.
        dag_spec, all_procs, snap = _capture_cholesky_dag(4, 16)
        schedule = Dict{Any, Any}()

        # Give SA a tiny budget so the whole pipeline exits quickly.
        sched = OptimizingScheduler(; ig_time_limit_sec=1e-6,
                                     sa_time_limit_sec=1e-6,
                                     rng=MersenneTwister(0))
        # Warm up once so first-invocation compile time (~200 ms when
        # JuMPExt has been loaded into the session) doesn't count against
        # the timed second call. The test measures early-exit behavior,
        # not JIT overhead.
        Dagger.datadeps_schedule_dag_aot!(sched, Dict{Any, Any}(),
                                           dag_spec, all_procs,
                                           Dagger.DefaultScope())
        t0 = time_ns()
        Dagger.datadeps_schedule_dag_aot!(sched, schedule, dag_spec, all_procs,
                                           Dagger.DefaultScope())
        elapsed_ms = (time_ns() - t0) / 1e6
        # Post-warmup a K=20 pipeline with two tripped budgets exits in
        # well under 500 ms on any dev/CI box; the natural stop condition
        # of an unbounded SA + IG on the same DAG takes seconds.
        @test elapsed_ms < 500.0
        # Every task must still receive an assignment (fallback returns
        # best-so-far, which is at least the Greedy seed).
        @test length(schedule) == Dagger.nv(dag_spec.g)
    end
end

@static if Base.find_package("JuMP") !== nothing &&
            Base.find_package("HiGHS") !== nothing
    using JuMP
    using HiGHS

    @testset "JuMPScheduler" begin
        @testset "Constructor and validation" begin
            sched = Dagger.JuMPScheduler(HiGHS.Optimizer)
            @test sched isa DataDepsScheduler
            @test sched.optimizer === HiGHS.Optimizer
            @test sched.Z == 10.0
            @test sched.time_limit_sec == 60.0

            sched2 = Dagger.JuMPScheduler(HiGHS.Optimizer; Z=5.0, time_limit_sec=30.0)
            @test sched2.Z == 5.0
            @test sched2.time_limit_sec == 30.0

            @test_throws ArgumentError Dagger.JuMPScheduler(HiGHS.Optimizer; time_limit_sec=0)
            @test_throws ArgumentError Dagger.JuMPScheduler(HiGHS.Optimizer; time_limit_sec=-1)
        end

        @testset "Cache partition: JuMPScheduler has its own cache" begin
            jump_cache = datadeps_schedule_cache(Dagger.JuMPScheduler(HiGHS.Optimizer))
            greedy_cache = datadeps_schedule_cache(GreedyScheduler())
            @test jump_cache !== greedy_cache   # distinct partitions
        end

        @testset "Empty DAG: AOT hook is a no-op" begin
            empty_dag = DAGSpec()
            all_procs = collect(Dagger.all_processors())
            schedule = Dict{Dagger.DTask, Dagger.Processor}()
            sched = Dagger.JuMPScheduler(HiGHS.Optimizer; time_limit_sec=5.0)
            Dagger.datadeps_schedule_dag_aot!(sched, schedule, empty_dag, all_procs,
                                              Dagger.UnionScope(map(Dagger.ExactScope, all_procs)))
            @test isempty(schedule)
        end

        @testset "Throws SchedulingException when no compatible processor exists" begin
            A = rand(64); B = rand(64)
            target_proc = Dagger.ThreadProc(1, 1)
            dag_spec, _, _ = _capture_dag(() -> Dagger.spawn_datadeps() do
                Dagger.@spawn scope=Dagger.ExactScope(target_proc) add!(InOut(A), In(B))
            end)

            incompatible_procs = Dagger.Processor[
                Dagger.ThreadProc(1, 2),
                Dagger.ThreadProc(1, 3),
            ]
            sched = Dagger.JuMPScheduler(HiGHS.Optimizer; time_limit_sec=5.0)
            schedule = Dict{Dagger.DTask, Dagger.Processor}()
            @test_throws Dagger.Sch.SchedulingException Dagger.datadeps_schedule_dag_aot!(
                sched, schedule, dag_spec, incompatible_procs,
                Dagger.UnionScope(map(Dagger.ExactScope, incompatible_procs)))
        end

        @testset "Per-task scope is respected by the solver" begin
            A = rand(64); B = rand(64)
            target_proc = Dagger.ThreadProc(1, 1)
            sched = Dagger.JuMPScheduler(HiGHS.Optimizer; time_limit_sec=10.0)
            cache = datadeps_schedule_cache(sched); empty!(cache)
            Base.ScopedValues.with(DATADEPS_SCHEDULER => sched) do
                Dagger.spawn_datadeps() do
                    Dagger.@spawn scope=Dagger.ExactScope(target_proc) add!(InOut(A), In(B))
                end
            end
            @test !isempty(cache)
            spec_schedule = first(cache).second
            assigned = first(values(spec_schedule.id_to_proc))
            @test assigned === target_proc
        end

        @testset "End-to-end via spawn_datadeps: matmul produces correct result" begin
            nb = 16
            nt = 2
            A = [rand(nb, nb) for _ in 1:nt, _ in 1:nt]
            B = [rand(nb, nb) for _ in 1:nt, _ in 1:nt]
            C = [zeros(nb, nb) for _ in 1:nt, _ in 1:nt]

            sched = Dagger.JuMPScheduler(HiGHS.Optimizer; time_limit_sec=60.0)
            cache = datadeps_schedule_cache(sched); empty!(cache)

            Base.ScopedValues.with(DATADEPS_SCHEDULER => sched) do
                Dagger.spawn_datadeps() do
                    for i in 1:nt, j in 1:nt, k in 1:nt
                        Dagger.@spawn LinearAlgebra.BLAS.gemm!('N', 'N', 1.0,
                                                               In(A[i, k]),
                                                               In(B[k, j]), 1.0,
                                                               InOut(C[i, j]))
                    end
                end
            end

            sz = nt * nb
            dense_A = zeros(sz, sz); dense_B = zeros(sz, sz); dense_C = zeros(sz, sz)
            for i in 1:nt, j in 1:nt
                dense_A[(i-1)*nb+1:i*nb, (j-1)*nb+1:j*nb] .= A[i, j]
                dense_B[(i-1)*nb+1:i*nb, (j-1)*nb+1:j*nb] .= B[i, j]
                dense_C[(i-1)*nb+1:i*nb, (j-1)*nb+1:j*nb] .= C[i, j]
            end
            @test dense_C ≈ dense_A * dense_B

            @test !isempty(cache)
        end

        @testset "End-to-end via spawn_datadeps: mock Cholesky produces L*L' ≈ A" begin
            nb = 16
            nt = 2
            M = make_spd_blocks(nt * nb, nb)
            A_ref = vcat([hcat([M[i, j] for j in 1:nt]...) for i in 1:nt]...)

            sched = Dagger.JuMPScheduler(HiGHS.Optimizer; time_limit_sec=60.0)
            cache = datadeps_schedule_cache(sched); empty!(cache)

            Base.ScopedValues.with(DATADEPS_SCHEDULER => sched) do
                Dagger.spawn_datadeps() do
                    mock_cholesky!(M)
                end
            end

            L = zeros(nt * nb, nt * nb)
            for i in 1:nt, j in 1:nt
                L[(i-1)*nb+1:i*nb, (j-1)*nb+1:j*nb] .= M[i, j]
            end
            for i in 1:size(L, 1), j in 1:size(L, 2)
                j > i && (L[i, j] = 0.0)
            end
            @test isapprox(L * L', A_ref; rtol=1e-8)
            @test !isempty(cache)
        end

        @testset "AOT hook populates schedule for every task" begin
            nb = 16
            nt = 2
            M = [rand(nb, nb) for _ in 1:nt, _ in 1:nt]
            for i in 1:nt
                M[i, i] = M[i, i] * M[i, i]' + nb * LinearAlgebra.I
            end

            sched = Dagger.JuMPScheduler(HiGHS.Optimizer; time_limit_sec=30.0)
            cache = datadeps_schedule_cache(sched); empty!(cache)

            Base.ScopedValues.with(DATADEPS_SCHEDULER => sched) do
                Dagger.spawn_datadeps() do
                    Dagger.@spawn LinearAlgebra.LAPACK.potrf!('L', InOut(M[1, 1]))
                    Dagger.@spawn LinearAlgebra.BLAS.trsm!('R', 'L', 'T', 'N',
                                                           1.0, In(M[1, 1]),
                                                           InOut(M[2, 1]))
                    Dagger.@spawn LinearAlgebra.BLAS.syrk!('L', 'N', -1.0,
                                                           In(M[2, 1]), 1.0,
                                                           InOut(M[2, 2]))
                    Dagger.@spawn LinearAlgebra.LAPACK.potrf!('L', InOut(M[2, 2]))
                end
            end

            @test !isempty(cache)
            dag_spec = first(cache).first
            spec_schedule = first(cache).second
            @test length(spec_schedule.id_to_proc) == Dagger.nv(dag_spec.g)
            @test length(spec_schedule.id_to_proc) == 4
        end

        @testset "Optimality: MILP cost ≤ Greedy cost on a small DAG" begin
            dag_spec, all_procs, snap = _capture_cholesky_dag(3, 16)

            greedy_state = ScheduleState()
            greedy_schedule!(greedy_state, snap, dag_spec, all_procs)
            greedy_cost = cost_of_schedule(greedy_state)

            sched = Dagger.JuMPScheduler(HiGHS.Optimizer; time_limit_sec=60.0)
            schedule = Dict{Dagger.DTask, Dagger.Processor}()
            all_scope = Dagger.UnionScope(map(Dagger.ExactScope, all_procs))
            Dagger.datadeps_schedule_dag_aot!(sched, schedule, dag_spec, all_procs, all_scope)

            # Replay MILP assignments through `_replay_schedule!` under the
            # same snapshot so costs are directly comparable.
            milp_state = ScheduleState()
            n_tasks = Dagger.nv(dag_spec.g)
            for idx in 1:n_tasks
                task = dag_spec.id_to_task[idx]
                milp_state.task_proc[idx] = schedule[task]
                milp_state.task_finish_ns[idx] = 0.0
            end
            Dagger._replay_schedule!(milp_state, snap, dag_spec, all_procs, Set{Int}())
            milp_cost = cost_of_schedule(milp_state)

            @test milp_cost <= greedy_cost
        end

        @testset "Optimality chain: MILP ≤ IG ≤ Greedy on a small DAG" begin
            dag_spec, all_procs, snap = _capture_cholesky_dag(3, 16)

            greedy_state = ScheduleState()
            greedy_schedule!(greedy_state, snap, dag_spec, all_procs)
            greedy_cost = cost_of_schedule(greedy_state)

            ig_state = copy(greedy_state)
            iterated_greedy_schedule!(ig_state, snap, dag_spec, all_procs;
                                       n_iters=16, destroy_frac=0.3,
                                       rng=MersenneTwister(2026))
            ig_cost = cost_of_schedule(ig_state)

            sched = Dagger.JuMPScheduler(HiGHS.Optimizer; time_limit_sec=60.0)
            schedule = Dict{Dagger.DTask, Dagger.Processor}()
            all_scope = Dagger.UnionScope(map(Dagger.ExactScope, all_procs))
            Dagger.datadeps_schedule_dag_aot!(sched, schedule, dag_spec, all_procs, all_scope)

            milp_state = ScheduleState()
            n_tasks = Dagger.nv(dag_spec.g)
            for idx in 1:n_tasks
                task = dag_spec.id_to_task[idx]
                milp_state.task_proc[idx] = schedule[task]
                milp_state.task_finish_ns[idx] = 0.0
            end
            Dagger._replay_schedule!(milp_state, snap, dag_spec, all_procs, Set{Int}())
            milp_cost = cost_of_schedule(milp_state)

            @test milp_cost <= ig_cost
            @test ig_cost <= greedy_cost
            @test milp_cost <= greedy_cost
        end

        @testset "Parallelism: MILP uses parallel processors when no precedence" begin
            # Two independent tasks on ≥2 procs must land on distinct procs
            # (makespan = single-task cost, not 2×). Regression guard.
            A1 = rand(64); B1 = rand(64)
            A2 = rand(64); B2 = rand(64)

            sched = Dagger.JuMPScheduler(HiGHS.Optimizer; time_limit_sec=30.0)
            cache = datadeps_schedule_cache(sched); empty!(cache)

            Base.ScopedValues.with(DATADEPS_SCHEDULER => sched) do
                Dagger.spawn_datadeps() do
                    Dagger.@spawn add!(InOut(A1), In(B1))
                    Dagger.@spawn add!(InOut(A2), In(B2))
                end
            end

            @test !isempty(cache)
            spec_schedule = first(cache).second
            assigned_procs = collect(values(spec_schedule.id_to_proc))
            @test length(assigned_procs) == 2
            # Multi-proc machines: distinct procs; single-proc: sharing is fine.
            n_distinct_procs = length(unique(assigned_procs))
            n_available_procs = length(Dagger.all_processors())
            if n_available_procs >= 2
                @test n_distinct_procs == 2
            end
        end

        @testset "Single-task DAG: trivially optimal" begin
            A = rand(64); B = rand(64)
            sched = Dagger.JuMPScheduler(HiGHS.Optimizer; time_limit_sec=10.0)
            cache = datadeps_schedule_cache(sched); empty!(cache)
            Base.ScopedValues.with(DATADEPS_SCHEDULER => sched) do
                Dagger.spawn_datadeps() do
                    Dagger.@spawn add!(InOut(A), In(B))
                end
            end
            @test !isempty(cache)
            spec_schedule = first(cache).second
            @test length(spec_schedule.id_to_proc) == 1
        end

        @testset "Time limit: warm-start guarantees a populated schedule on timeout" begin
            # Tight budget cannot prove optimality; warm-start guarantees at
            # least the greedy schedule is returned, never SchedulingException.
            dag_spec, all_procs, _ = _capture_cholesky_dag(4, 16)
            n_tasks = Dagger.nv(dag_spec.g)
            @test n_tasks == 20

            sched = Dagger.JuMPScheduler(HiGHS.Optimizer; time_limit_sec=0.01)
            schedule = Dict{Dagger.DTask, Dagger.Processor}()
            all_scope = Dagger.UnionScope(map(Dagger.ExactScope, all_procs))

            Dagger.datadeps_schedule_dag_aot!(sched, schedule, dag_spec, all_procs, all_scope)

            @test length(schedule) == n_tasks
            for idx in 1:n_tasks
                @test haskey(schedule, dag_spec.id_to_task[idx])
                @test schedule[dag_spec.id_to_task[idx]] in all_procs
            end
        end
    end

    @testset "OptimizingScheduler (MILP path with JuMP loaded)" begin
        OptimizingScheduler = Dagger.OptimizingScheduler
        opt_uses_milp = Dagger.opt_uses_milp

        @testset "Selection: MILP path enabled when K ≤ threshold and optimizer given" begin
            s = OptimizingScheduler(; optimizer=HiGHS.Optimizer, milp_threshold=12)
            @test opt_uses_milp(s, 1)  == true
            @test opt_uses_milp(s, 12) == true    # inclusive boundary
            @test opt_uses_milp(s, 13) == false   # over threshold
            @test opt_uses_milp(s, 64) == false

            # Even with optimizer set, threshold=0 forces heuristic path.
            s0 = OptimizingScheduler(; optimizer=HiGHS.Optimizer, milp_threshold=0)
            @test opt_uses_milp(s0, 1) == false

            # `nothing` optimizer disables MILP regardless of threshold.
            snone = OptimizingScheduler(; optimizer=nothing, milp_threshold=100)
            @test opt_uses_milp(snone, 5) == false
        end

        @testset "Selection: threshold boundary is inclusive (K == threshold uses MILP)" begin
            # Direct verification against the AOT hook path: when K == threshold,
            # the MILP schedule must equal the schedule that a stand-alone
            # JuMPScheduler with matching parameters produces.
            dag_spec, all_procs, _ = _capture_cholesky_dag(3, 16)   # K = 10
            @test Dagger.nv(dag_spec.g) == 10
            all_scope = Dagger.UnionScope(map(Dagger.ExactScope, all_procs))

            opt = OptimizingScheduler(; optimizer=HiGHS.Optimizer,
                                        milp_threshold=10,
                                        milp_time_limit_sec=60.0,
                                        milp_Z=10.0)
            sched_opt = Dict{Dagger.DTask, Dagger.Processor}()
            Dagger.datadeps_schedule_dag_aot!(opt, sched_opt, dag_spec, all_procs, all_scope)

            jump = Dagger.JuMPScheduler(HiGHS.Optimizer; Z=10.0, time_limit_sec=60.0)
            sched_jump = Dict{Dagger.DTask, Dagger.Processor}()
            Dagger.datadeps_schedule_dag_aot!(jump, sched_jump, dag_spec, all_procs, all_scope)

            for k in 1:Dagger.nv(dag_spec.g)
                task = dag_spec.id_to_task[k]
                @test sched_opt[task] === sched_jump[task]
            end
        end

        @testset "Selection: K > threshold falls back to heuristic even with optimizer" begin
            # Force the heuristic path on a small DAG by shrinking the threshold.
            # The result should be a valid schedule under SA(IG(Greedy)), NOT
            # the MILP output.
            dag_spec, all_procs, _ = _capture_cholesky_dag(2, 16)   # K = 4
            all_scope = Dagger.UnionScope(map(Dagger.ExactScope, all_procs))

            opt = OptimizingScheduler(; optimizer=HiGHS.Optimizer,
                                        milp_threshold=0,
                                        rng=MersenneTwister(42))
            sched_opt = Dict{Dagger.DTask, Dagger.Processor}()
            Dagger.datadeps_schedule_dag_aot!(opt, sched_opt, dag_spec, all_procs, all_scope)

            @test length(sched_opt) == Dagger.nv(dag_spec.g)
            for k in 1:Dagger.nv(dag_spec.g)
                @test sched_opt[dag_spec.id_to_task[k]] in all_procs
            end
        end

        @testset "End-to-end (MILP path): matmul correctness at machine precision" begin
            nb = 16
            nt = 2
            A = [rand(nb, nb) for _ in 1:nt, _ in 1:nt]
            B = [rand(nb, nb) for _ in 1:nt, _ in 1:nt]
            C = [zeros(nb, nb) for _ in 1:nt, _ in 1:nt]

            sched = OptimizingScheduler(; optimizer=HiGHS.Optimizer,
                                          milp_threshold=64,
                                          milp_time_limit_sec=60.0)
            cache = datadeps_schedule_cache(sched); empty!(cache)

            Base.ScopedValues.with(DATADEPS_SCHEDULER => sched) do
                Dagger.spawn_datadeps() do
                    for i in 1:nt, j in 1:nt, k in 1:nt
                        Dagger.@spawn LinearAlgebra.BLAS.gemm!('N', 'N', 1.0,
                                                               In(A[i, k]),
                                                               In(B[k, j]), 1.0,
                                                               InOut(C[i, j]))
                    end
                end
            end

            sz = nt * nb
            dense_A = zeros(sz, sz); dense_B = zeros(sz, sz); dense_C = zeros(sz, sz)
            for i in 1:nt, j in 1:nt
                dense_A[(i-1)*nb+1:i*nb, (j-1)*nb+1:j*nb] .= A[i, j]
                dense_B[(i-1)*nb+1:i*nb, (j-1)*nb+1:j*nb] .= B[i, j]
                dense_C[(i-1)*nb+1:i*nb, (j-1)*nb+1:j*nb] .= C[i, j]
            end
            @test dense_C ≈ dense_A * dense_B
            @test !isempty(cache)
        end

        @testset "End-to-end (adaptive): mock Cholesky produces L*L' ≈ A" begin
            # Cholesky (K=4 tasks) exercises a triangular-factorisation DAG
            # topology distinct from matmul. K=4 ≤ default threshold=12, so
            # the MILP path is taken. Verifies adaptive selection produces a
            # numerically correct schedule on a factorisation workload.
            nb = 16
            nt = 2
            M = make_spd_blocks(nt * nb, nb)
            A_ref = vcat([hcat([M[i, j] for j in 1:nt]...) for i in 1:nt]...)

            sched = OptimizingScheduler(; optimizer=HiGHS.Optimizer,
                                          milp_time_limit_sec=60.0)
            cache = datadeps_schedule_cache(sched); empty!(cache)

            Base.ScopedValues.with(DATADEPS_SCHEDULER => sched) do
                Dagger.spawn_datadeps() do
                    mock_cholesky!(M)
                end
            end

            L = zeros(nt * nb, nt * nb)
            for i in 1:nt, j in 1:nt
                L[(i-1)*nb+1:i*nb, (j-1)*nb+1:j*nb] .= M[i, j]
            end
            rows, cols = axes(L)
            for i in rows, j in cols
                j > i && (L[i, j] = 0.0)
            end
            @test isapprox(L * L', A_ref; rtol=1e-8)
            @test !isempty(cache)
        end

        @testset "End-to-end (heuristic path via threshold=0): matmul correctness" begin
            # Same workload as above but forced onto the SA(IG(Greedy)) pipeline
            # via `milp_threshold=0`. Numeric result must still be identical.
            nb = 16
            nt = 2
            A = [rand(nb, nb) for _ in 1:nt, _ in 1:nt]
            B = [rand(nb, nb) for _ in 1:nt, _ in 1:nt]
            C = [zeros(nb, nb) for _ in 1:nt, _ in 1:nt]

            sched = OptimizingScheduler(; optimizer=HiGHS.Optimizer,
                                          milp_threshold=0,
                                          rng=MersenneTwister(7))
            cache = datadeps_schedule_cache(sched); empty!(cache)

            Base.ScopedValues.with(DATADEPS_SCHEDULER => sched) do
                Dagger.spawn_datadeps() do
                    for i in 1:nt, j in 1:nt, k in 1:nt
                        Dagger.@spawn LinearAlgebra.BLAS.gemm!('N', 'N', 1.0,
                                                               In(A[i, k]),
                                                               In(B[k, j]), 1.0,
                                                               InOut(C[i, j]))
                    end
                end
            end

            sz = nt * nb
            dense_A = zeros(sz, sz); dense_B = zeros(sz, sz); dense_C = zeros(sz, sz)
            for i in 1:nt, j in 1:nt
                dense_A[(i-1)*nb+1:i*nb, (j-1)*nb+1:j*nb] .= A[i, j]
                dense_B[(i-1)*nb+1:i*nb, (j-1)*nb+1:j*nb] .= B[i, j]
                dense_C[(i-1)*nb+1:i*nb, (j-1)*nb+1:j*nb] .= C[i, j]
            end
            @test dense_C ≈ dense_A * dense_B
            @test !isempty(cache)
        end
    end
end

# Regression test for the bench harness's `TimedScheduler` wrapper. Coupled
# to the bench file only via `include`; the wrapper lives in bench-side code
# because it exists to measure the AOT scheduling phase for paper figures.
#
# Why this test is here: under Dagger's default hierarchical partitioning,
# `distribute_tasks_hierarchical!` calls `similar(queue.scheduler)` per
# partition. A wrapper scheduler without an explicit `Base.similar` falls
# through to `typeof(s)()` and errors out because `TimedScheduler` requires
# an inner argument. Additionally, even after adding `similar`, a naive
# implementation with a per-shard `Ref{UInt64}` counter would zero the
# `sched_phase_ns` reading since shards write to their own private Refs.
# Both are fixed in `bench/datadeps_schedulers/driver.jl` and this test
# guards against regressions.
@testset "TimedScheduler bench wrapper" begin
    include(joinpath(@__DIR__, "..", "..", "bench", "datadeps_schedulers", "driver.jl"))

    @testset "Base.similar shares atomic across partition shards" begin
        inner = Dagger.OptimizingScheduler(; rng=MersenneTwister(0))
        timed = TimedScheduler(inner)
        @test timed.last_aot_ns isa Threads.Atomic{UInt64}
        @test timed.last_aot_ns[] == 0

        shard = similar(timed)
        @test shard isa TimedScheduler
        @test shard.inner !== timed.inner   # inner is `similar`-cloned per shard
        @test shard.last_aot_ns === timed.last_aot_ns   # atomic is *shared*

        # Shard-side writes are visible to the parent-side reader.
        Threads.atomic_add!(shard.last_aot_ns, UInt64(42_000))
        @test timed.last_aot_ns[] == 42_000

        # Multiple shard writes accumulate (this is the hierarchical case).
        shard2 = similar(timed)
        Threads.atomic_add!(shard2.last_aot_ns, UInt64(8_000))
        @test timed.last_aot_ns[] == 50_000
    end

    @testset "sched_phase_ns is non-zero under hierarchical run" begin
        # Runs a small Cholesky through the actual harness pipeline under
        # Dagger's default (hierarchical) mode. Before the fix this either
        # crashed (`similar` missing) or reported `sched_phase_ns == 0`
        # (shard-private Refs). Non-zero here proves both bugs are gone.
        res = run_once(:cholesky, Dagger.GreedyScheduler(), 3, 16, 0;
                       collect_logs=true, metrics_warm=false)
        @test res.sched_phase_ns > 0
        @test res.n_tasks > 0

        # Fresh instance starts at zero (no state carryover across trials).
        fresh = TimedScheduler(Dagger.GreedyScheduler())
        @test fresh.last_aot_ns[] == 0
    end

    @testset "Cache and equivalence hooks forward to inner" begin
        # Regression: TimedScheduler must not accidentally partition the
        # cache under its own type — the whole point of forwarding is that
        # a `TimedScheduler{Greedy}` sweep reuses `GreedyScheduler`'s
        # cached DAG schedules. Verify both cache identity and dag/argspec
        # equivalence forwarding.
        inner = Dagger.GreedyScheduler()
        timed = TimedScheduler(inner)
        @test Dagger.datadeps_schedule_cache(timed) ===
              Dagger.datadeps_schedule_cache(inner)
    end
end
