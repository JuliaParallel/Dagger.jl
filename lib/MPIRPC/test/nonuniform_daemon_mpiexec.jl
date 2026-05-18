using Test
using MPI
using MPIRPC

# Non-uniform companion to `uniform_daemon_mpiexec.jl`. Listeners receive
# inbound requests via the daemon, clients receive their own replies via
# the daemon (or via `wait`/`fetch` self-pumping — both paths must work).
# No testset in this script calls `rpc_progress!` or `serve_listener`.

MPI.Init(; threadlevel = :multiple)

const WORLD_RANK = MPI.Comm_rank(MPI.COMM_WORLD)
const NPROC      = MPI.Comm_size(MPI.COMM_WORLD)

NPROC >= 4 || error("non-uniform daemon tests need at least 4 ranks (got $NPROC); " *
                    "rerun with `mpiexec -n 4 ...`")

const HALF           = max(1, NPROC ÷ 2)
const LISTENER_RANKS = collect(0:(HALF - 1))
const CLIENT_RANKS   = collect(HALF:(NPROC - 1))
const IS_LISTENER    = WORLD_RANK in LISTENER_RANKS

backend = MPIRPC.select_mpi_rpc_backend!(
    NonUniformMPIRPCBackend(MPI.COMM_WORLD;
                            listener_ranks = LISTENER_RANKS,
                            daemon         = true),
)
const COMM = backend.comm

@assert backend.daemon "daemon flag did not stick"
@assert backend.daemon_task isa Task "daemon task should have been spawned"
@assert !istaskdone(backend.daemon_task)

# Daemon must be on the `:interactive` pool so it cannot be starved by
# CPU-bound user code on the `:default` pool. See uniform_daemon_mpiexec.jl
# for the rationale.
if Threads.nthreads(:interactive) > 0
    @assert Threads.threadpool(backend.daemon_task) === :interactive (
        "daemon task expected on :interactive, got $(Threads.threadpool(backend.daemon_task))")
end

# `_phase!` uses `rpc_barrier`; see the uniform daemon script for why this
# is safe alongside the daemon (per-backend `mpi_lock` serializes raw MPI
# calls, the barrier request is task-local).
function _phase!(name::AbstractString)
    rpc_barrier()
    if WORLD_RANK == 0
        println("--- ", name, " ---")
        flush(stdout)
    end
    rpc_barrier()
end

# ---------------------------------------------------------------------------

_phase!("non-uniform-daemon / smoke (no manual progress, no serve_listener)")
@testset "non-uniform-daemon / client-listener round trip" begin
    if !IS_LISTENER
        peer = first(LISTENER_RANKS)
        # Critically, this script never has the listener call
        # `serve_listener` or `rpc_progress!`. The daemon on the listener
        # is doing all inbound draining.
        got = remotecall_fetch(+, peer, WORLD_RANK, 1)
        @test got == WORLD_RANK + 1
    else
        @test true
    end
end

_phase!("non-uniform-daemon / each client fans out to all listeners")
@testset "non-uniform-daemon / each client fans out to all listeners" begin
    if !IS_LISTENER
        results = Vector{Int}(undef, length(LISTENER_RANKS))
        for (i, l) in enumerate(LISTENER_RANKS)
            results[i] = remotecall_fetch(+, l, WORLD_RANK, l)
        end
        @test results == [WORLD_RANK + l for l in LISTENER_RANKS]
    else
        @test true
    end
end

_phase!("non-uniform-daemon / multiple client threads concurrent")
@testset "non-uniform-daemon / multiple client threads concurrent" begin
    if Threads.nthreads() >= 2 && !IS_LISTENER
        K = 16
        peer = first(LISTENER_RANKS)
        results = Vector{Vector{Int}}(undef, Threads.nthreads())
        ts = Task[]
        for tid in 1:Threads.nthreads()
            t = Threads.@spawn begin
                local got = Vector{Int}(undef, K)
                for k in 1:K
                    got[k] = MPIRPC.remotecall_fetch(
                        +, peer, WORLD_RANK * 100 + tid, k)
                end
                results[tid] = got
            end
            push!(ts, t)
        end
        foreach(wait, ts)
        for tid in 1:Threads.nthreads()
            @test results[tid] == [WORLD_RANK * 100 + tid + k for k in 1:K]
        end
    else
        @test true
    end
end

_phase!("non-uniform-daemon / handler that spawns and fetches")
@testset "non-uniform-daemon / nested fetch from spawned handler subtask" begin
    if Threads.nthreads() >= 2 && length(LISTENER_RANKS) >= 2 && !IS_LISTENER
        peer   = LISTENER_RANKS[1]
        helper = LISTENER_RANKS[2]
        result = remotecall_fetch(peer, helper, WORLD_RANK) do helper_rank, origin
            t = Threads.@spawn MPIRPC.remotecall_fetch(+, helper_rank, origin, 5)
            return fetch(t) + 1000
        end
        @test result == WORLD_RANK + 5 + 1000
    else
        @test true
    end
end

_phase!("non-uniform-daemon / shutdown joins the daemon")
@testset "non-uniform-daemon / shutdown! cleanly joins the daemon" begin
    @test backend.daemon_task isa Task
    @test !istaskdone(backend.daemon_task)
    shutdown!()
    @test backend.running[] == false
    @test backend.daemon_task === nothing
end

# Final sync on the duplicate communicator. As in the uniform daemon
# script, every rank has called `shutdown!` so no rank is pumping
# progress; a plain `MPI.Barrier` on the RPC subcomm is the right tool.
MPI.Barrier(backend.comm)
MPI.Finalize()
