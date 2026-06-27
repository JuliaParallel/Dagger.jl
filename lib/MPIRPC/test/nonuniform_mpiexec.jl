using Test
using MPI
using MPIRPC
using Random

Random.seed!(0xC0FFEE)

MPI.Init(; threadlevel=:multiple)

const WORLD_RANK = MPI.Comm_rank(MPI.COMM_WORLD)
const NPROC      = MPI.Comm_size(MPI.COMM_WORLD)

NPROC >= 4 || error("non-uniform tests need at least 4 ranks (got $NPROC); " *
                    "rerun with `mpiexec -n 4 ...`")

# Roughly half the ranks are listeners, the rest are clients only.
const HALF             = max(1, NPROC ÷ 2)
const LISTENER_RANKS   = collect(0:(HALF - 1))
const CLIENT_RANKS     = collect(HALF:(NPROC - 1))
const IS_LISTENER      = WORLD_RANK in LISTENER_RANKS

backend = MPIRPC.select_mpi_rpc_backend!(
    NonUniformMPIRPCBackend(MPI.COMM_WORLD; listener_ranks = LISTENER_RANKS))
const COMM = backend.comm

@assert is_listener(backend) == IS_LISTENER
@assert listener_ranks(backend) == LISTENER_RANKS

# A progress-pumping barrier and an announce on rank 0 — same idea as the
# uniform suite. Critical for non-uniform: clients pump replies and listeners
# pump requests, so a `rpc_barrier` after each phase prevents a listener
# from "going quiet" before clients have collected pending replies.
function _phase!(name::AbstractString)
    rpc_barrier()
    if WORLD_RANK == 0
        println("--- ", name, " ---")
        flush(stdout)
    end
    rpc_barrier()
end

function pump_until(pred; timeout::Real = 30.0,
                    backend = MPIRPC.current_mpi_rpc_backend())
    t0 = time()
    while !pred()
        rpc_progress!(backend)
        time() - t0 > timeout && return false
        yield()
    end
    return true
end

# ---------------------------------------------------------------------------

_phase!("non-uniform / role checks")
@testset "non-uniform / role checks" begin
    @test is_listener(backend) == (WORLD_RANK in LISTENER_RANKS)
    if !IS_LISTENER && length(CLIENT_RANKS) >= 2
        # Clients cannot remotecall to other clients; only listeners service RPC.
        another_client = first(c for c in CLIENT_RANKS if c != WORLD_RANK)
        @test_throws ArgumentError remotecall(+, another_client, 1, 2)
    else
        @test true
    end
end

_phase!("non-uniform / clients hammer listeners")
@testset "non-uniform / clients hammer listeners" begin
    if IS_LISTENER
        # Listeners do not initiate RPC in this test — they just service.
        @test true
    else
        K = 16
        futs = MPIFuture[]
        for ℓ in LISTENER_RANKS
            for k in 1:K
                push!(futs, remotecall(+, ℓ, WORLD_RANK, k))
            end
        end
        @test pump_until(() -> all(isready, futs); timeout = 60.0)
        # Each call computed `WORLD_RANK + k` (in unspecified ℓ).
        for ℓ in LISTENER_RANKS, k in 1:K
            idx = findfirst(==(WORLD_RANK + k), [fetch(f) for f in futs])
            @test idx !== nothing
        end
    end
end

_phase!("non-uniform / handler observes its own rank")
@testset "non-uniform / handler observes its own rank" begin
    if !IS_LISTENER
        peer = first(LISTENER_RANKS)
        got = remotecall_fetch(peer) do
            return MPI.Comm_rank(MPI.COMM_WORLD)
        end
        @test got == peer
    else
        @test true
    end
end

_phase!("non-uniform / many OIDs from one client to one listener")
@testset "non-uniform / many OIDs from one client to one listener" begin
    if !IS_LISTENER
        peer = first(LISTENER_RANKS)
        K = 64
        futs = [remotecall(+, peer, WORLD_RANK * 1000, k) for k in 1:K]
        @test pump_until(() -> all(isready, futs); timeout = 30.0)
        perm = randperm(K)
        for i in perm
            @test fetch(futs[i]) == WORLD_RANK * 1000 + i
        end
    else
        @test true
    end
end

_phase!("non-uniform / multi-threaded handler-spawn-fetch")
@testset "non-uniform / listener handler that Threads.@spawn-then-fetches" begin
    # Regression test for the multi-threaded handler deadlock. The listener
    # receiving the call dispatches the handler on `Threads.@spawn`; the
    # handler in turn spawns a task that calls back into another listener
    # via `remotecall_fetch`, then `fetch`es it. This worked under the
    # synchronous-handler model only because of `ReentrantLock` re-entry
    # on the same task; under multi-threading it required removing
    # `progress_lock` and running handlers on their own tasks.
    if Threads.nthreads() >= 2 && length(LISTENER_RANKS) >= 2 && !IS_LISTENER
        peer    = LISTENER_RANKS[1]
        helper  = LISTENER_RANKS[2]
        result = remotecall_fetch(peer, helper, WORLD_RANK) do helper_rank, origin
            t = Threads.@spawn MPIRPC.remotecall_fetch(+, helper_rank, origin, 5)
            return fetch(t) + 1000
        end
        @test result == WORLD_RANK + 5 + 1000
    else
        @test true
    end
end

_phase!("non-uniform / concurrent client threads")
@testset "non-uniform / multiple client threads issuing concurrent RPC" begin
    if Threads.nthreads() >= 2 && !IS_LISTENER
        K = 16
        peer = first(LISTENER_RANKS)
        results = Vector{Vector{Int}}(undef, Threads.nthreads())
        ts = Task[]
        for tid in 1:Threads.nthreads()
            t = Threads.@spawn begin
                local got = Vector{Int}(undef, K)
                for k in 1:K
                    got[k] = MPIRPC.remotecall_fetch(+, peer, WORLD_RANK * 100 + tid, k)
                end
                results[tid] = got
            end
            push!(ts, t)
        end
        for t in ts
            wait(t)
        end
        for tid in 1:Threads.nthreads()
            @test results[tid] == [WORLD_RANK * 100 + tid + k for k in 1:K]
        end
    else
        @test true
    end
end

_phase!("non-uniform / world barrier coexists with subcomm RPC")
@testset "non-uniform / world barrier coexists with subcomm RPC" begin
    if !IS_LISTENER
        peer = first(LISTENER_RANKS)
        f = remotecall(+, peer, WORLD_RANK, 1)
        # Every rank in COMM_WORLD must reach this barrier so collectives
        # stay aligned. Listeners reach it from the listener phase below.
        MPI.Barrier(MPI.COMM_WORLD)
        @test pump_until(() -> isready(f); timeout = 30.0)
        @test fetch(f) == WORLD_RANK + 1
    else
        # Listener: pump while clients post their RPC, then barrier with them.
        t0 = time()
        while time() - t0 < 0.5
            rpc_progress!(backend)
            yield()
        end
        MPI.Barrier(MPI.COMM_WORLD)
        # Pump a bit more so any pending replies clear out before _phase!.
        t0 = time()
        while time() - t0 < 0.5
            rpc_progress!(backend)
            yield()
        end
        @test true
    end
end

_phase!("non-uniform / remote exception path")
@testset "non-uniform / remote exception path" begin
    if !IS_LISTENER
        peer = first(LISTENER_RANKS)
        @test_throws MPIRemoteException remotecall_fetch(peer) do
            error("planned failure")
        end
    else
        @test true
    end
end

_phase!("non-uniform / remote_do is fire-and-forget")
@testset "non-uniform / remote_do returns nothing immediately" begin
    if !IS_LISTENER
        peer = first(LISTENER_RANKS)
        @test remote_do(peer) do; nothing end === nothing
    else
        @test true
    end
end

_phase!("non-uniform / set-then-read via remotecall_wait")
@testset "non-uniform / set-then-read with remotecall_wait" begin
    if !IS_LISTENER
        peer = first(LISTENER_RANKS)
        sentinel = Symbol("_NONUNI_SET_FROM_$(WORLD_RANK)")
        # Under multi-threading the spawned-handler model does not preserve
        # FIFO of handler *execution* on the same (src, dest, tag) — only
        # MPI delivery. `remotecall_wait` provides the explicit ack we need
        # before reading back the side effect.
        remotecall_wait(peer, WORLD_RANK, sentinel) do origin, name
            @eval Main const $name = $origin
        end
        v = remotecall_fetch(peer, sentinel) do name
            return getfield(Main, name)
        end
        @test v == WORLD_RANK
    else
        @test true
    end
end

_phase!("non-uniform / shutdown")
shutdown!()
rpc_barrier()
MPI.Finalize()
