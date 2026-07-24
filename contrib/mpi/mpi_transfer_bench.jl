# MPI data-plane benchmark: transfer bandwidth and datadeps macro timings.
#
# Run (CPU):  mpiexec -n 2 julia --project --threads=2 benchmarks/mpi_transfer_bench.jl
# Run (GPU):  mpiexec -n 2 julia --project=../gpuenv --threads=2 benchmarks/mpi_transfer_bench.jl
#
# Note on GPU-direct validation: the default MPICH_jll is not CUDA-aware
# (MPI.has_cuda() == false), so device buffers host-stage. To validate the
# device-direct path for real, switch to a CUDA-aware library, e.g.:
#   using MPIPreferences; MPIPreferences.use_system_binary()   # UCX/CUDA OpenMPI
# and force the path on with DAGGER_MPI_GPU_DIRECT=1.

using Dagger, MPI, LinearAlgebra, Random, Printf

Dagger.accelerate!(:mpi)
const comm = MPI.COMM_WORLD
const rank = MPI.Comm_rank(comm)
const nranks = MPI.Comm_size(comm)
@assert nranks == 2 "transfer bench expects exactly 2 ranks"

const HAVE_CUDA = try
    @eval using CUDA
    CUDA.functional()
catch
    false
end

function bench_transfer(label, make; iters=10)
    if rank == 0
        @printf("%-14s %10s %12s\n", label, "size", "bandwidth")
    end
    for p in 16:2:26
        nbytes = 2^p
        n = nbytes ÷ sizeof(Float64)
        src = make(n)
        dst = make(n)
        tag = 20_000 + p
        # Warmup (includes compile and any pool/pinning setup)
        for _ in 1:3
            if rank == 0
                Dagger.send_yield!(src, comm, 1, tag)
                Dagger.recv_yield!(dst, comm, 1, tag)
            else
                Dagger.recv_yield!(dst, comm, 0, tag)
                Dagger.send_yield!(src, comm, 0, tag)
            end
        end
        MPI.Barrier(comm)
        t0 = time_ns()
        for _ in 1:iters
            if rank == 0
                Dagger.send_yield!(src, comm, 1, tag)
                Dagger.recv_yield!(dst, comm, 1, tag)
            else
                Dagger.recv_yield!(dst, comm, 0, tag)
                Dagger.send_yield!(src, comm, 0, tag)
            end
        end
        dt = (time_ns() - t0) / 1e9
        # Each iteration moves nbytes in each direction (round trip)
        bw = 2 * iters * nbytes / dt / 1e9
        if rank == 0
            @printf("%-14s %7.2f MiB %9.3f GB/s\n", "", nbytes / 2^20, bw)
        end
        MPI.Barrier(comm)
    end
end

function time_region(f; reps=2)
    f() # warmup (compile)
    best = Inf
    for _ in 1:reps
        MPI.Barrier(comm)
        t0 = time_ns()
        f()
        dt = (time_ns() - t0) / 1e9
        best = min(best, dt)
    end
    return best
end

function bench_macro_cpu()
    Random.seed!(1)
    N, B = 512, 128
    A = rand(N, N); A = A * A'; A[diagind(A)] .+= N
    t_chol = time_region() do
        DA = DArray(A, Blocks(B, B))
        cholesky(DA)
        nothing
    end
    M1 = rand(N, N); M2 = rand(N, N)
    t_mul = time_region() do
        DA = DArray(M1, Blocks(B, B))
        DB = DArray(M2, Blocks(B, B))
        DC = zeros(Blocks(B, B), N, N)
        mul!(DC, DA, DB)
        nothing
    end
    rank == 0 && @printf("macro CPU:    cholesky %7.3f s   mul! %7.3f s  (N=%d, B=%d)\n", t_chol, t_mul, N, B)
end

function bench_macro_gpu()
    CUDAExt = Base.get_extension(Dagger, :CUDAExt)
    CuProc = CUDAExt.CuArrayDeviceProc
    procs = sort(collect(Dagger.get_processors(Dagger.MPIClusterProc(comm)));
                 by=p->(p.rank, Dagger.short_name(p)))
    gpu_scope = Dagger.UnionScope([Dagger.ExactScope(p) for p in procs if p.innerProc isa CuProc]...)

    Random.seed!(2)
    N, B = 512, 128
    A = rand(N, N); A = A * A'; A[diagind(A)] .+= N
    t_chol = time_region() do
        DA = DArray(A, Blocks(B, B))
        Dagger.with_options(;scope=gpu_scope) do
            cholesky(DA)
        end
        nothing
    end
    M1 = rand(N, N); M2 = rand(N, N)
    t_mul = time_region() do
        DA = DArray(M1, Blocks(B, B))
        DB = DArray(M2, Blocks(B, B))
        DC = zeros(Blocks(B, B), N, N)
        Dagger.with_options(;scope=gpu_scope) do
            mul!(DC, DA, DB)
        end
        nothing
    end
    rank == 0 && @printf("macro GPU:    cholesky %7.3f s   mul! %7.3f s  (N=%d, B=%d)\n", t_chol, t_mul, N, B)
end

rank == 0 && println("== MPI transfer bench (nranks=$nranks, has_cuda=$(MPI.has_cuda()), CUDA=$(HAVE_CUDA)) ==")

bench_transfer("Array", n->rand(n))
if HAVE_CUDA
    bench_transfer("CuArray", n->CUDA.rand(Float64, n))
end

# Same-node device IPC protocol round trip (handle + DtoD, no host hop)
function bench_ipc(; iters=10)
    rank == 0 && @printf("%-14s %10s %12s\n", "CuArray-IPC", "size", "bandwidth")
    for p in 16:2:26
        nbytes = 2^p
        n = nbytes ÷ sizeof(Float64)
        src = CUDA.rand(Float64, n)
        dst = CUDA.rand(Float64, n)
        tag = 21_000 + p
        for _ in 1:3
            if rank == 0
                Dagger.mpi_ipc_send(src, comm, 1, tag)
                Dagger.mpi_ipc_recv!(dst, comm, 1, tag)
            else
                Dagger.mpi_ipc_recv!(dst, comm, 0, tag)
                Dagger.mpi_ipc_send(src, comm, 0, tag)
            end
        end
        MPI.Barrier(comm)
        t0 = time_ns()
        for _ in 1:iters
            if rank == 0
                Dagger.mpi_ipc_send(src, comm, 1, tag)
                Dagger.mpi_ipc_recv!(dst, comm, 1, tag)
            else
                Dagger.mpi_ipc_recv!(dst, comm, 0, tag)
                Dagger.mpi_ipc_send(src, comm, 0, tag)
            end
        end
        dt = (time_ns() - t0) / 1e9
        bw = 2 * iters * nbytes / dt / 1e9
        rank == 0 && @printf("%-14s %7.2f MiB %9.3f GB/s\n", "", nbytes / 2^20, bw)
        MPI.Barrier(comm)
    end
end
if HAVE_CUDA && Dagger.same_node(Dagger.current_acceleration(), 0, 1)
    bench_ipc()
end

bench_macro_cpu()
if HAVE_CUDA
    bench_macro_gpu()
end

MPI.Barrier(comm)
rank == 0 && println("== done ==")
