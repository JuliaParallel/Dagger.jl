import Dagger: DAGSpec, DatadepsArgSpec, dag_add_task!,
               equivalent_structure,
               datadeps_dag_equivalent, datadeps_argspec_equivalent,
               datadeps_ainfo_equivalent, datadeps_schedule_cache,
               LayeredScheduler, RoundRobinScheduler, DataDepsScheduler,
               NoAliasing, UnknownAliasing, ContiguousAliasing,
               StridedAliasing, TriangularAliasing, DiagonalAliasing,
               ObjectAliasing, CombinedAliasing, AliasingWrapper,
               DATADEPS_SCHEDULER, In, Out, InOut, Deps
import Dagger
using LinearAlgebra
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
