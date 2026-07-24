# Shared MPI × GPU datadeps test suite.
#
# Every backend (CUDA, ROCm, OpenCL, Metal, oneAPI) drives the exact same SPMD
# datadeps logic; only the array/device/memory-space names and the set of
# supported features differ. Rather than copy this suite per backend, each
# `test/mpi_<backend>.jl` loads its backend, then calls `run_mpi_gpu_suite`
# with a small config describing those differences.
#
# Data plane is host-staged through P2P (no GPU-aware MPI required).
#
# Config fields (NamedTuple; feature fields default to off / absent):
#   name              :: String   backend label used in testset names / logs
#   DeviceProc        :: Type     the backend's device Processor type
#   elt               :: Type     element type for host arrays (Float32/Float64)
#   subarray_depmod   :: Bool     run SubArray + dep_mod / dep-mod coverage
#   matmul            :: Bool     run the DArray matmul smoke
#   cholesky          :: Bool     run the DArray cholesky smoke
#   remap             :: NamedTuple or absent — MPI remap / memory-kind hooks:
#       make_space    :: Function () -> a VRAM memory space (device 1)
#       device_field  :: Symbol   field naming the device index (:device/:device_id)
#       kind          :: Symbol   expected gpu_memory_kind
#       test_ipc      :: Bool     also assert !ipc_eligible(space, space)
#       extra         :: Function or absent — extra backend-specific checks

using Dagger, MPI, LinearAlgebra, Random, Test
using Dagger: In, Out, InOut, Deps

const MPIExt = Base.get_extension(Dagger, :MPIExt)

# Broadcast-only mutation helpers (scalar indexing is illegal on GPU arrays)
add1!(X) = (X .+= 1; nothing)
scale2!(X) = (X .*= 2; nothing)
function upper_double!(X)
    for d in 0:size(X, 1)-1
        view(X, diagind(X, d)) .*= 2
    end
    return nothing
end
function diag_inc!(X)
    view(X, diagind(X)) .+= 1
    return nothing
end
function strict_upper_double!(X)
    for d in 1:size(X, 1)-1
        view(X, diagind(X, d)) .*= 2
    end
    return nothing
end
function lower_double!(X)
    for d in 0:size(X, 1)-1
        view(X, diagind(X, -d)) .*= 2
    end
    return nothing
end
function strict_lower_double!(X)
    for d in 1:size(X, 1)-1
        view(X, diagind(X, -d)) .*= 2
    end
    return nothing
end

function run_mpi_gpu_suite(cfg)
    DeviceProc = cfg.DeviceProc
    elt = cfg.elt

    # Init MPI (via the MPI acceleration) before any MPI routine is called.
    Dagger.accelerate!(:mpi)
    Dagger.check_uniformity!(true)

    comm = MPI.COMM_WORLD
    rank = MPI.Comm_rank(comm)
    nranks = MPI.Comm_size(comm)

    mpi_procs() = sort(collect(Dagger.get_processors(MPIExt.MPIClusterProc(comm)));
                       by=p->(p.rank, Dagger.short_name(p)))
    gpu_proc_for_rank(r) = first(filter(p->p.rank == r && p.innerProc isa DeviceProc, mpi_procs()))
    cpu_proc_for_rank(r) = first(filter(p->p.rank == r && p.innerProc isa Dagger.ThreadProc, mpi_procs()))
    gpu_scope(r) = Dagger.ExactScope(gpu_proc_for_rank(r))
    cpu_scope(r) = Dagger.ExactScope(cpu_proc_for_rank(r))
    all_gpu_scope() = Dagger.UnionScope([gpu_scope(r) for r in 0:nranks-1]...)
    # GPU processors are not default-enabled; regions mixing CPU and GPU tasks
    # must opt in via an outer scope that includes both kinds
    mixed_scope() = Dagger.UnionScope(vcat([cpu_scope(r) for r in 0:nranks-1],
                                           [gpu_scope(r) for r in 0:nranks-1])...)

@testset "MPI $(cfg.name) Datadeps" begin

@testset "GPU processors visible per rank" begin
    for r in 0:nranks-1
        p = gpu_proc_for_rank(r)
        @test p.innerProc isa DeviceProc
        @test Dagger.check_uniform(p)
    end
end

@testset "Cross-rank GPU dataflow" begin
    # CPU-origin data mutated by GPU tasks on different ranks, in program
    # order, with write-back to the rank-0 CPU original
    Random.seed!(20)
    A = rand(elt, 8, 8)
    ref = copy(A)
    r1 = min(1, nranks-1)
    Dagger.with_options(;scope=mixed_scope()) do
        Dagger.spawn_datadeps() do
            Dagger.@spawn scope=gpu_scope(r1) add1!(InOut(A))
            Dagger.@spawn scope=gpu_scope(0) scale2!(InOut(A))
        end
    end
    ref .+= 1
    ref .*= 2
    if rank == 0
        @test A ≈ ref
    end
end

@testset "Mixed CPU/GPU dataflow" begin
    # The same slot chain crosses CPU and GPU spaces (HtoD and DtoH inside
    # one region), across ranks
    Random.seed!(21)
    A = rand(elt, 8, 8)
    ref = copy(A)
    r1 = min(1, nranks-1)
    Dagger.with_options(;scope=mixed_scope()) do
        Dagger.spawn_datadeps() do
            Dagger.@spawn scope=gpu_scope(r1) add1!(InOut(A))
            Dagger.@spawn scope=cpu_scope(0) scale2!(InOut(A))
            Dagger.@spawn scope=gpu_scope(0) add1!(InOut(A))
        end
    end
    ref .+= 1; ref .*= 2; ref .+= 1
    if rank == 0
        @test A ≈ ref
    end
end

if get(cfg, :subarray_depmod, false)
    @testset "GPU SubArray + dep_mod dataflow" begin
        # View remainders and non-identity dep_mods exercise multi-span KA pack
        # / copy paths across ranks (mirrors test/mpi.jl Cross-rank dataflow)
        Random.seed!(24)
        A = rand(elt, 8, 8)
        ref = copy(A)
        vA = view(A, 3:6, 3:6)
        r1 = min(1, nranks-1)
        r2 = min(2, nranks-1) % nranks
        Dagger.with_options(;scope=mixed_scope()) do
            Dagger.spawn_datadeps() do
                Dagger.@spawn scope=gpu_scope(r1) add1!(InOut(A))
                Dagger.@spawn scope=gpu_scope(r2) scale2!(InOut(vA))
                Dagger.@spawn scope=gpu_scope(0) upper_double!(Deps(A, InOut(UpperTriangular)))
            end
        end
        ref .+= 1
        view(ref, 3:6, 3:6) .*= 2
        upper_double!(ref)
        if rank == 0
            @test A ≈ ref
        end
    end

    @testset "GPU dep-mod coverage" begin
        Random.seed!(25)
        A = rand(elt, 6, 6)
        ref = copy(A)
        r1 = min(1, nranks-1)
        r2 = min(2, nranks-1) % nranks
        Dagger.with_options(;scope=mixed_scope()) do
            Dagger.spawn_datadeps() do
                Dagger.@spawn scope=gpu_scope(r1) diag_inc!(Deps(A, InOut(Diagonal)))
                Dagger.@spawn scope=gpu_scope(r2) add1!(InOut(A))
                Dagger.@spawn scope=gpu_scope(0) diag_inc!(Deps(A, InOut(Diagonal)))
            end
        end
        diag_inc!(ref); ref .+= 1; diag_inc!(ref)
        if rank == 0
            @test A ≈ ref
        end

        Random.seed!(26)
        B = rand(elt, 6, 6)
        refB = copy(B)
        Dagger.with_options(;scope=mixed_scope()) do
            Dagger.spawn_datadeps() do
                Dagger.@spawn scope=gpu_scope(r1) upper_double!(Deps(B, InOut(UpperTriangular)))
                Dagger.@spawn scope=gpu_scope(r2) strict_lower_double!(Deps(B, InOut(UnitLowerTriangular)))
                Dagger.@spawn scope=gpu_scope(0) strict_upper_double!(Deps(B, InOut(UnitUpperTriangular)))
                Dagger.@spawn scope=gpu_scope(r1) lower_double!(Deps(B, InOut(LowerTriangular)))
                Dagger.@spawn scope=gpu_scope(r2) diag_inc!(Deps(B, InOut(Diagonal)))
            end
        end
        upper_double!(refB); strict_lower_double!(refB); strict_upper_double!(refB)
        lower_double!(refB); diag_inc!(refB)
        if rank == 0
            @test B ≈ refB
        end
    end
end

if get(cfg, :matmul, false)
    @testset "GPU matmul (DArray)" begin
        Random.seed!(22)
        A = rand(elt, 8, 8)
        B = rand(elt, 8, 8)
        DA = DArray(A, Blocks(4, 4))
        DB = DArray(B, Blocks(4, 4))
        DC = zeros(Blocks(4, 4), elt, 8, 8)
        Dagger.with_options(;scope=all_gpu_scope()) do
            mul!(DC, DA, DB)
        end
        C = collect(DC)
        @test C ≈ A * B
        @test Dagger.check_uniform(hash(C))
    end
end

if get(cfg, :cholesky, false)
    @testset "GPU cholesky (DArray)" begin
        Random.seed!(23)
        A = rand(elt, 8, 8)
        A = A * A'
        A[diagind(A)] .+= size(A, 1)
        DA = DArray(A, Blocks(4, 4))
        Dagger.with_options(;scope=all_gpu_scope()) do
            @test collect(cholesky(DA).U) ≈ cholesky(A).U
        end
    end
end

remap = get(cfg, :remap, nothing)
if remap !== nothing
    @testset "MPI remap / memory kind hooks" begin
        space = remap.make_space()
        remapped = Dagger.mpi_remap_space(space, 3)
        @test remapped.owner == 3
        @test getfield(remapped, remap.device_field) == 1
        @test Dagger.gpu_memory_kind(space) === remap.kind
        if get(remap, :test_ipc, false)
            @test !Dagger.ipc_eligible(space, space)
        end
        extra = get(remap, :extra, nothing)
        extra !== nothing && extra()
    end
end

end # @testset "MPI $(cfg.name) Datadeps"

    MPI.Barrier(comm)
    Core.println("[$rank] MPI $(cfg.name) suite OK")
end
