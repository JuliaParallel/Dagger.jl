# Unified CPU MPI test suite (datadeps + linalg smokes).
#
# Acceleration-independent logic — aliasing algebra (will_alias, span math),
# dependency-ordering semantics (In/Out/InOut/Deps), remainder computation —
# is covered by the Distributed suite (test/datadeps.jl) and is intentionally
# NOT re-tested here. This suite covers only MPI-exclusive machinery:
#
#   - SPMD-uniform chunk creation (deterministic placement, MPIRef handles)
#   - check_uniform / compare_all agreement
#   - acceleration-aware wrapping of non-Chunk arguments (rank-0 ownership)
#   - the SPMD-symmetric endpoint (MPIWireValue move_rewrap, aliased-object
#     cache sharing, rank-stamped aliasing spans)  [mirrors "Aliased Object
#     Copying"]
#   - cross-rank copies at execution time (identity, view/remainder, and
#     non-identity dep_mod transfers)
#   - collect/fetch under uniform execution
#   - result-type broadcast for untyped task results
#   - Cholesky / LU DArray smokes
#
# GPU coverage lives in test/mpi_cuda.jl (CUDA) and test/mpi_rocm.jl (ROCm).
#
# Known gaps (not tested): NaiveScheduler/UltraScheduler use rank-local cost
# measurements and are not deterministic across ranks.
#
# Run: mpiexec -n 4 julia --project --threads=2 test/mpi.jl

using Dagger, MPI, LinearAlgebra, Random, Test
using Dagger: In, Out, InOut, Deps

# MPI-specific types live in the MPIExt extension (loaded via `using MPI`),
# not in Dagger core. Reference them through the extension module.
const MPIExt = Base.get_extension(Dagger, :MPIExt)

include(joinpath(@__DIR__, "util.jl"))

Dagger.accelerate!(:mpi)
Dagger.check_uniformity!(true)
const comm = MPI.COMM_WORLD
const rank = MPI.Comm_rank(comm)
const nranks = MPI.Comm_size(comm)
const accel = Dagger.current_acceleration()

mpi_procs() = sort(collect(Dagger.get_processors(MPIExt.MPIClusterProc(comm)));
                   by=p->(p.rank, Dagger.short_name(p)))
proc_for_rank(r) = first(filter(p->p.rank == r, mpi_procs()))
space_for_rank(r) = MPIExt.MPIMemorySpace(Dagger.CPURAMMemorySpace(1), comm, r)
rank_scope(r) = Dagger.ExactScope(proc_for_rank(r))

# Rank pairs for slot tests (mirrors Distributed's worker pairs), including a
# same-rank pair to check that origin slots alias the original data
const rank_pairs = if nranks >= 3
    [(0, 1), (1, 0), (1, 2), (1, 1)]
else
    [(0, nranks-1), (nranks-1, 0), (0, 0)]
end

inc!(X) = (X .+= 1; nothing)
add1!(X) = (X .+= 1; nothing)
scale2!(X) = (X .*= 2; nothing)
sum_into!(r, X) = (r[] = Int(sum(X)); nothing)
function upper_double!(X)
    for j in axes(X, 2), i in 1:j
        X[i, j] *= 2
    end
    return nothing
end
function diag_inc!(X)
    for i in 1:minimum(size(X))
        X[i, i] += 1
    end
    return nothing
end
function strict_upper_double!(X)
    for j in axes(X, 2), i in 1:j-1
        X[i, j] *= 2
    end
    return nothing
end
function lower_double!(X)
    for j in axes(X, 2), i in j:size(X, 1)
        X[i, j] *= 2
    end
    return nothing
end
function strict_lower_double!(X)
    for j in axes(X, 2), i in j+1:size(X, 1)
        X[i, j] *= 2
    end
    return nothing
end
untyped_result(x) = x > 0 ? Int(x) : Float64(x)
concrete_fail(x) = x > 0 ? error("Test concrete") : x + 1 # inferred Int, throws at runtime
nothrow_add(x) = x + 1 # concrete Int, proven nothrow → execute! skips status bcast
mut_ref!(R) = (R[] .= 0; nothing)
exec_rank() = MPI.Comm_rank(MPI.COMM_WORLD) # rank actually executing a task

function fresh_cache(dst_space)
    backing = Dagger._with_default_acceleration() do
        Dagger.tochunk(Dagger.AliasedObjectCacheStore(accel))
    end
    return Dagger.AliasedObjectCache(accel, dst_space, backing)
end

# Generate a slot for `obj` (a Chunk in src space) in dst space, like
# generate_slot! does, and return it
function make_slot(cache, src_rank, dst_rank, obj)
    src_space = space_for_rank(src_rank)
    dst_space = space_for_rank(dst_rank)
    return Dagger.remotecall_endpoint_toplevel(Dagger.move_rewrap, accel, cache,
                                               proc_for_rank(src_rank), proc_for_rank(dst_rank),
                                               src_space, dst_space, obj)
end

@testset "MPI" begin

@testset "accelerate! idempotency and switch" begin
    @test Dagger.current_acceleration() isa MPIExt.MPIAcceleration
    Dagger.accelerate!(:mpi)
    @test Dagger.current_acceleration() isa MPIExt.MPIAcceleration
    Dagger.accelerate!(:distributed)
    @test Dagger.current_acceleration() isa Dagger.DistributedAcceleration
    # Diverge default RNGs across ranks; re-init must re-sync them.
    Random.seed!(UInt64(rank + 1))
    Dagger.accelerate!(:mpi)
    @test Dagger.current_acceleration() isa MPIExt.MPIAcceleration
    probe = rand(UInt64)
    @test Dagger.check_uniform(probe)
    MPI.Barrier(comm)
end

@testset "Datadeps" begin

@testset "Uniform chunk creation" begin
    Random.seed!(1)
    A = rand(16, 16)
    # distribute path
    DA = DArray(A, Blocks(4, 4))
    owners = Int[]
    for t in vec(DA.chunks)
        c = fetch(t; raw=true)
        h = c.handle
        @test h isa MPIExt.MPIRef
        @test Dagger.check_uniform(h)
        @test Dagger.check_uniform(c.space)
        @test Dagger.check_uniform(c.processor)
        # The owner (and only the owner) holds the payload
        @test (h.rank == rank) == (h.innerRef !== nothing)
        push!(owners, h.rank)
    end
    # Deterministic round-robin placement covers all ranks
    @test sort(unique(owners)) == collect(0:nranks-1)
    @test Dagger.check_uniform(hash(owners))

    # alloc path
    DZ = zeros(Blocks(4, 4), Float64, 16, 16)
    for t in vec(DZ.chunks)
        c = fetch(t; raw=true)
        @test c.handle isa MPIExt.MPIRef
        @test Dagger.check_uniform(c.handle)
    end
end

@testset "check_uniform" begin
    @test Dagger.check_uniform(42)
    @test Dagger.check_uniform(hash((1, :a, "x")))
    # Rank-dependent values must be detected on every rank
    @test_throws ArgumentError Dagger.check_uniform(rank)
    # The compare stream stays aligned after a detected failure
    @test Dagger.check_uniform(7)
end

@testset "Non-Chunk argument wrapping" begin
    # Non-Chunk mutable arguments wrap as rank-0-owned MPIRef chunks
    x = Ref(11)
    c = Dagger.tochunk(x)
    @test c.handle isa MPIExt.MPIRef
    @test c.space isa MPIExt.MPIMemorySpace
    @test c.space.rank == 0
    @test Dagger.check_uniform(c.handle)
    @test (rank == 0) == (c.handle.innerRef !== nothing)

    # As datadeps arguments: rank 0's copy is the source of truth and is
    # written back at region end, even when the task runs on another rank
    r = Ref(0)
    A = ones(4, 4)
    exec_rank = min(1, nranks-1)
    Dagger.spawn_datadeps() do
        Dagger.@spawn scope=rank_scope(exec_rank) sum_into!(InOut(r), In(A))
    end
    if rank == 0
        @test r[] == 16
    end
end

# Mirrors test/datadeps.jl "Aliased Object Copying", via the SPMD endpoint
@testset "Aliased Object Copying" begin
    for (src, dst) in rank_pairs
        @testset "Rank $src -> $dst" begin
            @testset "Array" begin
                Random.seed!(100 + 10*src + dst)
                A = rand(4, 4) # replicated construction; owner rank holds the chunk payload
                obj = Dagger.tochunk(A, proc_for_rank(src), space_for_rank(src))
                cache = fresh_cache(space_for_rank(dst))
                slot = make_slot(cache, src, dst, obj)
                @test Dagger.memory_space(slot) == space_for_rank(dst)
                @test Dagger.check_uniform(slot.handle)
                @test Dagger.chunktype(slot) == Matrix{Float64}

                src_ainfo = Dagger.aliasing(accel, obj, identity)
                dst_ainfo = Dagger.aliasing(accel, slot, identity)
                ss, ds = Dagger.memory_spans(src_ainfo), Dagger.memory_spans(dst_ainfo)
                @test length(ss) == length(ds)
                @test all(Dagger.span_len(a) == Dagger.span_len(b) for (a, b) in zip(ss, ds))
                if src == dst
                    # Origin slots wrap (alias) the original data
                    @test Dagger.will_alias(src_ainfo, dst_ainfo)
                else
                    # Cross-rank spans are stamped with their owner rank and
                    # must never alias, even with coinciding SPMD addresses
                    @test !Dagger.will_alias(src_ainfo, dst_ainfo)
                end
                # The slot holds a copy of the source data on the destination
                if rank == dst
                    @test Dagger.unwrap(slot) == A
                end
            end

            @testset "SubArray + parent sharing" begin
                Random.seed!(200 + 10*src + dst)
                A = rand(8, 8)
                vA = view(A, 1:4, 1:8)
                vB = view(A, 5:8, 1:8)
                objA = Dagger.tochunk(vA, proc_for_rank(src), space_for_rank(src))
                objB = Dagger.tochunk(vB, proc_for_rank(src), space_for_rank(src))
                cache = fresh_cache(space_for_rank(dst))
                slotA = make_slot(cache, src, dst, objA)
                slotB = make_slot(cache, src, dst, objB)
                @test Dagger.check_uniform(slotA.handle)
                @test Dagger.check_uniform(slotB.handle)
                @test Dagger.chunktype(slotA) <: SubArray

                aA = Dagger.aliasing(accel, slotA, identity)
                aB = Dagger.aliasing(accel, slotB, identity)
                # Both views must share one parent allocation in the
                # destination space (aliased-object cache reuse)
                @test aA isa Dagger.StridedAliasing
                @test aB isa Dagger.StridedAliasing
                @test aA.base_ptr == aB.base_ptr
                # ... while the views themselves remain disjoint
                @test !Dagger.will_alias(aA, aB)
                if rank == dst
                    @test Dagger.unwrap(slotA) == vA
                    @test Dagger.unwrap(slotB) == vB
                    @test parent(Dagger.unwrap(slotA)) === parent(Dagger.unwrap(slotB))
                end
            end

            @testset "UpperTriangular wrapper" begin
                Random.seed!(300 + 10*src + dst)
                A = rand(4, 4)
                U = UpperTriangular(A)
                obj = Dagger.tochunk(U, proc_for_rank(src), space_for_rank(src))
                cache = fresh_cache(space_for_rank(dst))
                slot = make_slot(cache, src, dst, obj)
                @test Dagger.check_uniform(slot.handle)
                @test Dagger.chunktype(slot) <: UpperTriangular
                if rank == dst
                    @test Dagger.unwrap(slot) == U
                end
            end

            @testset "ChunkView" begin
                Random.seed!(400 + 10*src + dst)
                A = rand(8, 8)
                obj = Dagger.tochunk(A, proc_for_rank(src), space_for_rank(src))
                cvA = view(obj, 1:4, 1:8)
                cvB = view(obj, 5:8, 1:8)
                @test cvA isa Dagger.ChunkView
                cache = fresh_cache(space_for_rank(dst))
                slotA = make_slot(cache, src, dst, cvA)
                slotB = make_slot(cache, src, dst, cvB)
                @test Dagger.memory_space(slotA) == space_for_rank(dst)
                @test Dagger.check_uniform(slotA.handle)
                @test Dagger.check_uniform(slotB.handle)
                @test Dagger.chunktype(slotA) <: SubArray

                src_ainfo = Dagger.aliasing(accel, cvA, identity)
                dst_ainfo = Dagger.aliasing(accel, slotA, identity)
                ss, ds = Dagger.memory_spans(src_ainfo), Dagger.memory_spans(dst_ainfo)
                @test length(ss) == length(ds)
                @test all(Dagger.span_len(a) == Dagger.span_len(b) for (a, b) in zip(ss, ds))
                if src == dst
                    @test Dagger.will_alias(src_ainfo, dst_ainfo)
                else
                    @test !Dagger.will_alias(src_ainfo, dst_ainfo)
                end

                # Both views share one backing-chunk allocation on the
                # destination (aliased-object cache reuse), but don't overlap
                aA = Dagger.aliasing(accel, slotA, identity)
                aB = Dagger.aliasing(accel, slotB, identity)
                @test aA isa Dagger.StridedAliasing
                @test aA.base_ptr == aB.base_ptr
                @test !Dagger.will_alias(aA, aB)
                if rank == dst
                    @test Dagger.unwrap(slotA) == view(A, 1:4, 1:8)
                    @test Dagger.unwrap(slotB) == view(A, 5:8, 1:8)
                    @test parent(Dagger.unwrap(slotA)) === parent(Dagger.unwrap(slotB))
                end

                # Nested ChunkView flattens to the same slices as a direct view
                cv_nested = view(cvA, 2:3, 1:4)
                cv_direct = view(obj, 2:3, 1:4)
                @test cv_nested isa Dagger.ChunkView
                @test cv_nested.chunk === obj
                @test cv_nested.slices == cv_direct.slices
                slot_nested = make_slot(fresh_cache(space_for_rank(dst)), src, dst, cv_nested)
                slot_direct = make_slot(fresh_cache(space_for_rank(dst)), src, dst, cv_direct)
                @test Dagger.chunktype(slot_nested) <: SubArray
                if rank == dst
                    @test Dagger.unwrap(slot_nested) == Dagger.unwrap(slot_direct)
                    @test Dagger.unwrap(slot_nested) == view(A, 2:3, 1:4)
                end
            end

            @testset "HaloArray" begin
                H = Dagger.HaloArray{Int,2}((4, 4), (1, 1))
                fill!(H.center, 7)
                foreach(h->fill!(h, -1), H.halos)
                obj = Dagger.tochunk(H, proc_for_rank(src), space_for_rank(src))
                cache = fresh_cache(space_for_rank(dst))
                slot = make_slot(cache, src, dst, obj)
                @test Dagger.check_uniform(slot.handle)
                @test Dagger.chunktype(slot) <: Dagger.HaloArray

                src_ainfo = Dagger.aliasing(accel, obj, identity)
                dst_ainfo = Dagger.aliasing(accel, slot, identity)
                ss, ds = Dagger.memory_spans(src_ainfo), Dagger.memory_spans(dst_ainfo)
                @test length(ss) == length(ds)
                @test all(Dagger.span_len(a) == Dagger.span_len(b) for (a, b) in zip(ss, ds))
                if src != dst
                    @test !Dagger.will_alias(src_ainfo, dst_ainfo)
                end
                if rank == dst
                    H2 = Dagger.unwrap(slot)
                    @test H2.center == H.center
                    @test all(h2 == h for (h2, h) in zip(H2.halos, H.halos))
                end
            end
        end
    end
end

@testset "Cross-rank aliasing isolation" begin
    # Same-shaped arrays owned by different ranks must never alias
    Random.seed!(4)
    A = rand(4, 4)
    B = rand(4, 4)
    r2 = min(1, nranks-1)
    cA = Dagger.tochunk(A, proc_for_rank(0), space_for_rank(0))
    cB = Dagger.tochunk(B, proc_for_rank(r2), space_for_rank(r2))
    aA = Dagger.aliasing(accel, cA, identity)
    aB = Dagger.aliasing(accel, cB, identity)
    @test Dagger.check_uniform(aA)
    @test Dagger.check_uniform(aB)
    @test !Dagger.will_alias(aA, aB)
end

# Numeric dataflow across ranks: full-array, view (remainder copies), and
# triangular-dep_mod (non-identity cross-rank move!) writes, with sequential
# (program-order) semantics
@testset "Cross-rank dataflow" begin
    Random.seed!(7)
    A = rand(8, 8)
    ref = copy(A)
    vA = view(A, 3:6, 3:6)

    r1 = min(1, nranks-1)
    r2 = min(2, nranks-1)
    Dagger.spawn_datadeps() do
        Dagger.@spawn scope=rank_scope(r1) add1!(InOut(A))
        Dagger.@spawn scope=rank_scope(r2) scale2!(InOut(vA))
        Dagger.@spawn scope=rank_scope(0) upper_double!(Deps(A, InOut(UpperTriangular)))
    end

    # Serial reference, in program order
    ref .+= 1
    view(ref, 3:6, 3:6) .*= 2
    upper_double!(ref)

    # Rank 0 owns the original (non-Chunk) argument and receives the write-back
    if rank == 0
        @test A ≈ ref
    end
end

@testset "ChunkView dataflow" begin
    # Views over a DArray block as datadeps arguments, mixed with whole-chunk
    # access (exercises cross-rank remainder copies through the shared backing
    # allocation), mirroring the Distributed "ChunkView -> Aliasing" testset
    Random.seed!(8)
    A = rand(8, 8)
    DA = DArray(A, Blocks(4, 4))
    c = fetch(DA.chunks[1, 1]; raw=true)
    @test c.handle.rank == 0 # deterministic placement puts block (1,1) on rank 0
    cv_top = view(c, 1:2, 1:4)
    cv_bot = view(c, 3:4, 1:4)
    @test cv_top isa Dagger.ChunkView

    r1 = min(1, nranks-1)
    r2 = min(2, nranks-1)
    Dagger.spawn_datadeps() do
        Dagger.@spawn scope=rank_scope(r1) add1!(InOut(cv_top))
        Dagger.@spawn scope=rank_scope(r2) scale2!(InOut(cv_bot))
        Dagger.@spawn scope=rank_scope(0) add1!(InOut(c))
    end

    ref_blk = A[1:4, 1:4]
    ref_blk[1:2, :] .+= 1
    ref_blk[3:4, :] .*= 2
    ref_blk .+= 1
    # Collective uniform fetches: identical on every rank
    @test fetch(c) ≈ ref_blk
    @test fetch(cv_top) ≈ ref_blk[1:2, :]
end

@testset "DTask arguments" begin
    # DTasks as datadeps arguments resolve to their (rank-local) result chunks
    Random.seed!(9)
    t = Dagger.@spawn rand(4, 4)
    # Collective fetch; copy since the owner's fetch aliases the stored result
    B = copy(fetch(t))
    exec_rank = min(1, nranks-1)
    Dagger.spawn_datadeps() do
        Dagger.@spawn scope=rank_scope(exec_rank) add1!(InOut(t))
    end
    @test fetch(t) ≈ B .+ 1
end

@testset "Dep-mod coverage" begin
    # Diagonal dep_mod with cross-rank placement
    Random.seed!(10)
    A = rand(6, 6)
    ref = copy(A)
    r1 = min(1, nranks-1)
    r2 = min(2, nranks-1)
    Dagger.spawn_datadeps() do
        Dagger.@spawn scope=rank_scope(r1) diag_inc!(Deps(A, InOut(Diagonal)))
        Dagger.@spawn scope=rank_scope(r2) add1!(InOut(A))
        Dagger.@spawn scope=rank_scope(0) diag_inc!(Deps(A, InOut(Diagonal)))
    end
    diag_inc!(ref); ref .+= 1; diag_inc!(ref)
    if rank == 0
        @test A ≈ ref
    end

    # Full triangular wrapper matrix: each dep_mod narrows the cross-rank
    # move! to its region (mirrors test/datadeps.jl's (Unit)Upper/Lower set)
    Random.seed!(11)
    B = rand(6, 6)
    refB = copy(B)
    Dagger.spawn_datadeps() do
        Dagger.@spawn scope=rank_scope(r1) upper_double!(Deps(B, InOut(UpperTriangular)))
        Dagger.@spawn scope=rank_scope(r2) strict_lower_double!(Deps(B, InOut(UnitLowerTriangular)))
        Dagger.@spawn scope=rank_scope(0) strict_upper_double!(Deps(B, InOut(UnitUpperTriangular)))
        Dagger.@spawn scope=rank_scope(r1) lower_double!(Deps(B, InOut(LowerTriangular)))
        Dagger.@spawn scope=rank_scope(r2) diag_inc!(Deps(B, InOut(Diagonal)))
    end
    upper_double!(refB); strict_lower_double!(refB); strict_upper_double!(refB)
    lower_double!(refB); diag_inc!(refB)
    if rank == 0
        @test B ≈ refB
    end
end

@testset "Field aliasing" begin
    # Symbol dep_mods narrow the dependency (and cross-rank move!) to one
    # field of a wrapped object (mirrors test/datadeps.jl "Field aliasing")
    Random.seed!(12)
    X = Ref(rand(100))
    r1 = min(1, nranks-1)
    t = Dagger.spawn_datadeps() do
        Dagger.@spawn scope=rank_scope(r1) mut_ref!(Deps(X, InOut(:x)))
        Dagger.@spawn scope=rank_scope(0) getfield(Deps(X, In(:x)), :x)
    end
    # Collective fetch of the read task's result: program-order RAW means it
    # observed the mutation
    @test all(==(0), fetch(t))
    # Rank 0 owns the wrapped Ref and receives the write-back
    if rank == 0
        @test all(==(0), X[])
    end
end

@testset "Scope semantics" begin
    # Outer scope: get_compute_scope() restricts where datadeps tasks run
    target = min(1, nranks-1)
    ts = Dagger.with_options(;scope=rank_scope(target)) do
        Dagger.spawn_datadeps() do
            [Dagger.@spawn exec_rank() for _ in 1:4]
        end
    end
    @test all(==(target), fetch.(ts))

    # Inner scope: an unsatisfiable task scope throws uniformly on all ranks
    @test_throws Dagger.Sch.SchedulingException Dagger.spawn_datadeps() do
        Dagger.@spawn scope=Dagger.ExactScope(Dagger.ThreadProc(1, 5000)) 1+1
    end
    # The system remains usable afterwards
    A = ones(4, 4)
    Dagger.spawn_datadeps() do
        Dagger.@spawn scope=rank_scope(target) add1!(InOut(A))
    end
    if rank == 0
        @test A == fill(2.0, 4, 4)
    end
end

# Reduced dataflow verification (mirrors test/datadeps.jl's logging-based
# test_dataflow suite): under SPMD every rank plans the same copy tasks, so
# each rank checks its own logs for the exact set of `move!` tasks inserted
function with_logs(f)
    Dagger.enable_logging!(;tasknames=true, all_task_deps=false)
    try
        f()
        return Dagger.fetch_logs!()
    finally
        Dagger.disable_logging!()
    end
end
function count_move_tasks(logs)
    n = 0
    for w in keys(logs), name in logs[w][:tasknames]
        if name isa String && occursin("move!", name)
            n += 1
        end
    end
    return n
end
read_only(X) = (sum(X); nothing)

@testset "Copy planning" begin
    r1 = min(1, nranks-1)
    cross = r1 != 0 # single-rank runs degenerate to zero copies

    # Read-only cross-rank arg: one copy-in, no writeback
    A = ones(4, 4)
    logs = with_logs() do
        Dagger.spawn_datadeps() do
            Dagger.@spawn scope=rank_scope(r1) read_only(In(A))
        end
    end
    @test count_move_tasks(logs) == (cross ? 1 : 0)
    @test Dagger.check_uniform(count_move_tasks(logs))

    # Same-rank InOut: the origin slot aliases the original, zero copies
    B = ones(4, 4)
    logs = with_logs() do
        Dagger.spawn_datadeps() do
            Dagger.@spawn scope=rank_scope(0) add1!(InOut(B))
        end
    end
    @test count_move_tasks(logs) == 0

    # Cross-rank InOut: exactly one copy-in plus one writeback
    C = ones(4, 4)
    logs = with_logs() do
        Dagger.spawn_datadeps() do
            Dagger.@spawn scope=rank_scope(r1) add1!(InOut(C))
        end
    end
    @test count_move_tasks(logs) == (cross ? 2 : 0)

    # RAW chain on one rank: the intermediate copy is elided (slot up-to-date)
    D = ones(4, 4)
    logs = with_logs() do
        Dagger.spawn_datadeps() do
            Dagger.@spawn scope=rank_scope(r1) add1!(InOut(D))
            Dagger.@spawn scope=rank_scope(r1) scale2!(InOut(D))
        end
    end
    @test count_move_tasks(logs) == (cross ? 2 : 0)
    if rank == 0
        @test D == fill(4.0, 4, 4)
    end
end

@testset "Error propagation" begin
    # A throwing task poisons its result-status broadcast, so every rank
    # rethrows and SPMD control flow stays aligned
    @test_throws_unwrap (Dagger.DTaskFailedException, ErrorException) Dagger.spawn_datadeps() do
        Dagger.@spawn error("Test")
    end
    # Concrete inferred return type still needs a status broadcast when the
    # compiler does not prove :nothrow (type skip alone cannot silence errors)
    @test_throws Exception Dagger.spawn_datadeps() do
        Dagger.@spawn concrete_fail(1)
    end
    t = Dagger.@spawn concrete_fail(1)
    @test_throws Exception fetch(t)
    # Concrete + nothrow: no status/type traffic; all ranks still agree
    r = Dagger.spawn_datadeps() do
        Dagger.@spawn nothrow_add(41)
    end
    @test fetch(r) == 42
    # The system remains usable afterwards
    A = ones(4, 4)
    Dagger.spawn_datadeps() do
        Dagger.@spawn scope=rank_scope(min(1, nranks-1)) add1!(InOut(A))
    end
    if rank == 0
        @test A == fill(2.0, 4, 4)
    end
end

@testset "collect/fetch SPMD" begin
    Random.seed!(3)
    A = rand(12, 12)
    DA = DArray(A, Blocks(3, 3))
    # Datadeps tree collect: identical on every rank
    @test collect(DA) == A

    # fetch resolves chunk records locally (owners keep their payloads)
    fa = fetch(DA)
    for c in vec(fa.chunks)
        @test c isa Dagger.Chunk
        h = c.handle
        @test (h.rank == rank) == (h.innerRef !== nothing)
    end

    # Mutation round-trip through a datadeps region
    Dagger.spawn_datadeps() do
        for c in vec(DA.chunks)
            Dagger.@spawn inc!(InOut(c))
        end
    end
    @test collect(DA) == A .+ 1
end

@testset "Untyped task results" begin
    # Non-concrete inferred return types rely on the owner's result-status
    # broadcast carrying the actual result type to all ranks
    t1 = Dagger.@spawn untyped_result(3.0)
    @test fetch(t1) === 3
    t2 = Dagger.@spawn untyped_result(-3.0)
    @test fetch(t2) === -3.0
end

@testset "Nothing task results" begin
    # Void results must stay typed as Nothing (not rewritten to Any)
    t = Dagger.@spawn (() -> nothing)()
    @test fetch(t) === nothing
    @test Dagger.chunktype(fetch(t; raw=true)) === Nothing
end

@testset "Production mode (uniformity checks off)" begin
    # With checks disabled, ranks run planning and execution fully
    # asynchronously: no compare_all broadcasts keep them in lockstep, so this
    # exercises tag matching and task-overlap under maximal divergence
    # (a serialized-execution regression here deadlocks in copy cycles)
    Dagger.check_uniformity!(false)
    try
        Random.seed!(40)
        N, B = 256, 64
        A = rand(N, N); A = A * A'; A[diagind(A)] .+= N
        local U
        for _ in 1:2
            DA = DArray(A, Blocks(B, B))
            U = collect(cholesky(DA).U)
        end
        @test U ≈ cholesky(A).U
    finally
        Dagger.check_uniformity!(true)
    end
end

end # @testset "Datadeps"

@testset "Cholesky" begin
    Random.seed!(42)
    A = rand(Float64, 64, 64)
    A = A * A'
    A[diagind(A)] .+= size(A, 1)
    B = copy(A)

    DA = zeros(Blocks(16, 16), Float64, 64, 64)
    copyto!(DA, A)
    LinearAlgebra._chol!(DA, UpperTriangular)
    U_dist = UpperTriangular(collect(DA))

    C = cholesky(B)
    err = norm(U_dist - C.U) / norm(C.U)
    recon = norm(U_dist' * U_dist - B) / norm(B)
    @test err < 1e-12
    @test recon < 1e-12
end

@testset "LU" begin
    Random.seed!(1234)
    A = randn(100, 100)
    orig_A = copy(A)
    DA = DArray(A, Blocks(25, 25))

    F = lu!(DA, RowMaximum(); check=false)
    lu!(A, RowMaximum())

    @test norm(collect(A) - collect(DA)) / norm(collect(A)) < 1e-12
    DAc = collect(DA)
    p = LinearAlgebra.ipiv2perm(collect(F.ipiv), size(DAc, 1))
    LtU = UnitLowerTriangular(DAc) * UpperTriangular(DAc)
    @test norm(LtU - orig_A[p, :]) / norm(orig_A) < 1e-12
end

end # @testset "MPI"

MPI.Barrier(comm)
Core.println("[$rank] MPI suite OK")
