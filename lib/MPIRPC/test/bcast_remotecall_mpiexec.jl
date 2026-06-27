using Test
using MPI
using MPIRPC

MPI.Init(; threadlevel=:multiple)
backend = MPIRPC.select_mpi_rpc_backend!(UniformMPIRPCBackend(MPI.COMM_WORLD))
const COMM = backend.comm
const RANK = MPI.Comm_rank(COMM)
const NPROC = MPI.Comm_size(COMM)

NPROC >= 3 || error("bcast_remotecall test expects at least 3 ranks (got $NPROC)")

function rpc_barrier_local()
    MPIRPC.rpc_barrier(backend)
end

function pump_until(pred; timeout::Real = 30.0)
    t0 = time()
    while !pred()
        MPIRPC.rpc_progress!(backend)
        time() - t0 > timeout && return false
        yield()
    end
    return true
end

rpc_barrier_local()
if RANK == 0
    println("--- bcast_remotecall / all other ranks ---")
    flush(stdout)
end
rpc_barrier_local()

@testset "bcast_remotecall skips caller" begin
    futs = MPIRPC.bcast_remotecall(backend, +, RANK, 100)
    @test length(futs) == NPROC - 1
    @test pump_until(() -> all(Base.isready, futs))
    @test all(f -> MPIRPC.fetch(f) == RANK + 100, futs)
end

rpc_barrier_local()
if RANK == 0
    println("--- bcast_remotecall / explicit ranks vector ---")
    flush(stdout)
end
rpc_barrier_local()

@testset "bcast_remotecall explicit ranks vector" begin
    peer = mod(RANK + 1, NPROC)
    futs = MPIRPC.bcast_remotecall(backend, +, Int[peer], 10, 5)
    @test length(futs) == 1
    @test pump_until(() -> Base.isready(futs[1]))
    @test MPIRPC.fetch(futs[1]) == 15
end

rpc_barrier_local()
MPIRPC.shutdown!()
