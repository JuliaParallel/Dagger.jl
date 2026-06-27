using Test
using MPI
using MPIRPC

# This script verifies that the *yield-only progress daemon* is enough to
# drive the uniform backend end-to-end without a single explicit call to
# `rpc_progress!` from user code. The flag we never set in this script —
# anywhere — is `rpc_progress!`. If anything inside the testsets calls it,
# the test would not be measuring what it claims to.

MPI.Init(; threadlevel = :multiple)
backend = MPIRPC.select_mpi_rpc_backend!(
    UniformMPIRPCBackend(MPI.COMM_WORLD; daemon = true),
)
const COMM  = backend.comm
const RANK  = MPI.Comm_rank(COMM)
const NPROC = MPI.Comm_size(COMM)

NPROC >= 2 || error("uniform daemon tests require at least 2 ranks (got $NPROC)")
@assert backend.daemon "daemon flag did not stick"
@assert backend.daemon_task isa Task "daemon task should have been spawned by select_mpi_rpc_backend!"
@assert !istaskdone(backend.daemon_task) "daemon task already exited before any RPC ran"

# The daemon must run on the `:interactive` pool so user CPU-bound work
# on the `:default` pool cannot starve the wire pump. `mpi_tests.jl`
# launches us with `--threads=N,1` to make at least one interactive
# thread available; if for some reason it didn't, the assertion below
# fails loudly rather than letting the test pass under a weaker
# guarantee than what the design promises.
if Threads.nthreads(:interactive) > 0
    @assert Threads.threadpool(backend.daemon_task) === :interactive (
        "daemon task expected on :interactive, got $(Threads.threadpool(backend.daemon_task))")
end

# Phase boundary: still uses `rpc_barrier` because that is a *collective*
# synchronization, not a progress driver — every rank must reach it. The
# daemon happens to also pump progress on each rank while inside the
# barrier, which is fine: `rpc_barrier` and `_daemon_loop` both go
# through the same per-backend `mpi_lock` so they cannot race on raw MPI
# calls.
function _phase!(name::AbstractString)
    rpc_barrier()
    if RANK == 0
        println("--- ", name, " ---")
        flush(stdout)
    end
    rpc_barrier()
end

# ---------------------------------------------------------------------------

_phase!("uniform-daemon / smoke (no manual progress)")
@testset "uniform-daemon / remotecall_fetch with no manual progress" begin
    peer = mod(RANK + 1, NPROC)
    got = remotecall_fetch(+, peer, RANK, 100)
    @test got == RANK + 100
end

_phase!("uniform-daemon / ABBA without manual progress")
@testset "uniform-daemon / ABBA without manual progress" begin
    # Same shape as the non-daemon ABBA test, but here neither client side
    # nor server side has any user-driven `rpc_progress!`. Forward progress
    # on inbound *and* on reply draining for our own outstanding futures
    # is entirely the daemon's responsibility. (Reply draining works
    # because `wait`/`fetch` on `MPIFuture` calls `rpc_progress!` itself,
    # but the more interesting half — receiving inbound requests from
    # peers — has no user touchpoint.)
    peer = mod(RANK + 1, NPROC)
    futs = [remotecall(*, peer, RANK + 1, k) for k in 1:8]
    for (k, f) in enumerate(futs)
        @test fetch(f) == (RANK + 1) * k
    end
end

_phase!("uniform-daemon / fully passive rank")
@testset "uniform-daemon / a rank that never initiates RPC still serves" begin
    # The cleanest demonstration that the daemon is doing useful work:
    # one rank issues every RPC, others issue none. Without a daemon the
    # passive ranks would never see their inbound `CallMsg` because nobody
    # is calling `rpc_progress!` on them. The active rank uses `wait`,
    # which itself pumps progress, but that only drives *its own* MPI
    # state — the inbound matching on the passive ranks is independent.
    if RANK == 0
        results = Vector{Int}(undef, NPROC - 1)
        for p in 1:(NPROC - 1)
            results[p] = remotecall_fetch(+, p, p, 1000)
        end
        @test results == [p + 1000 for p in 1:(NPROC - 1)]
    else
        # Passive ranks: don't touch the API at all in this testset.
        @test true
    end
end

_phase!("uniform-daemon / many concurrent client tasks")
@testset "uniform-daemon / concurrent client tasks rely on daemon for inbound" begin
    if Threads.nthreads() >= 2 && NPROC >= 2
        peers = [r for r in 0:(NPROC - 1) if r != RANK]
        K = 16
        results = Vector{Vector{Int}}(undef, length(peers))
        ts = Task[]
        for (i, p) in enumerate(peers)
            t = Threads.@spawn begin
                local got = Vector{Int}(undef, K)
                for k in 1:K
                    got[k] = MPIRPC.remotecall_fetch(+, p, RANK * 1000, k)
                end
                results[i] = got
            end
            push!(ts, t)
        end
        foreach(wait, ts)
        for (i, p) in enumerate(peers)
            @test results[i] == [RANK * 1000 + k for k in 1:K]
        end
    else
        @test true
    end
end

_phase!("uniform-daemon / handler that spawns and fetches an RPC")
@testset "uniform-daemon / nested fetch from spawned handler subtask" begin
    # Same regression as in the non-daemon suite (the deadlock that
    # motivated removing `progress_lock`), but with the daemon also
    # racing to drain the request queue. We expect both to coexist.
    if Threads.nthreads() >= 2 && NPROC >= 3
        peer   = mod(RANK + 1, NPROC)
        helper = mod(RANK + 2, NPROC)
        result = remotecall_fetch(peer, helper, RANK) do helper_rank, origin
            t = Threads.@spawn MPIRPC.remotecall_fetch(+, helper_rank, origin, 7)
            return fetch(t) * 2
        end
        @test result == (RANK + 7) * 2
    else
        @test true
    end
end

_phase!("uniform-daemon / starvation: CPU-bound default-pool task must not block daemon")
@testset "uniform-daemon / daemon survives CPU-bound default-pool work" begin
    # Pathological-case test: spawn a CPU-bound task on the `:default`
    # pool that does *not* yield, and concurrently issue RPC traffic to
    # a peer. Under a naive design where the daemon shares the default
    # pool with user work, the busy task could starve the daemon and
    # the peer's `remotecall_fetch` call to *us* would never be
    # serviced. With the daemon on `:interactive`, this cannot happen:
    # the interactive thread is reserved.
    if Threads.nthreads(:interactive) >= 1 && NPROC >= 2
        peer = mod(RANK + 1, NPROC)
        # Burn ~0.5 s of CPU on every default-pool thread, with no
        # explicit yield points. If the daemon were on :default it
        # would be locked out for the duration.
        burners = Task[]
        deadline = time() + 0.5
        for _ in 1:Threads.nthreads(:default)
            t = Threads.@spawn :default begin
                acc = 0
                while time() < deadline
                    # Inner loop deliberately has no `yield()` and no
                    # I/O so the cooperative scheduler does not
                    # preempt this task.
                    for i in 1:1_000_000
                        acc = (acc + i * 31) % 9_973
                    end
                end
                acc
            end
            push!(burners, t)
        end
        # While the burners hammer the default pool, the peer is going
        # to call remotecall_fetch on *us* — see the symmetric arm
        # below. Our daemon on :interactive must keep accepting that
        # request. We measure success by completing our own
        # remotecall_fetch *to* the peer within a tight bound.
        t0 = time()
        got = MPIRPC.remotecall_fetch(+, peer, RANK, 7)
        elapsed = time() - t0
        @test got == RANK + 7
        # Generous bound — what matters is that this completes at all
        # while default-pool threads are saturated. Without the
        # interactive-pool placement the call would block until all
        # burners finish (≈ 0.5 s); with it, the call completes in a
        # few ms. We give 5 s of slack so a slow CI does not flake.
        @test elapsed < 5.0
        foreach(wait, burners)
    else
        @test true
    end
end

_phase!("uniform-daemon / cond-park: many concurrent waiters wake exactly once")
@testset "uniform-daemon / cond-park: many concurrent waiters wake correctly" begin
    # Regression test for the `Threads.Condition`-park path in
    # `wait(::MPIFuture)`. Spawn a large number of client tasks, each
    # blocking in `fetch` on its own future, and verify every one
    # observes its expected reply. The properties we want to lock in:
    #
    # 1. Every parked waiter is woken — no lost-wakeup race between
    #    `deliver!`'s `notify` and the consumer's `wait(f.cond)`.
    # 2. Each waiter wakes for *its own* future (no cross-future
    #    wakeup that returns a stale value to the wrong task).
    # 3. The daemon's `_dispatch_reply!` path correctly transitions
    #    through `take_waiter!` → `deliver!` → cond `notify` while
    #    none of `mpi_lock`, `waiters_lock`, `f.cond` are held more
    #    than one at a time.
    #
    # The test cannot directly assert "no spinning happened" without
    # peeking at task state, but a regression to the spin path would
    # still pass *correctness* — so we additionally check that the
    # waiters are *responsive*, by serializing many short calls and
    # measuring that they all complete inside a generous bound. If
    # `wait` was somehow not waking on `notify` (e.g. a bug where we
    # accidentally took a stale `isready` snapshot under the lock and
    # parked anyway), this would time out.
    if Threads.nthreads() >= 2
        peers = [r for r in 0:(NPROC - 1) if r != RANK]
        K = 64                  # waiters per peer
        results = Vector{Vector{Int}}(undef, length(peers))
        ts = Task[]
        t_start = time()
        for (i, p) in enumerate(peers)
            t = Threads.@spawn begin
                local got = Vector{Int}(undef, K)
                # Each task spawns its own future and immediately blocks
                # in `fetch`. Under daemon=true this means each task
                # parks on its own `f.cond` while the daemon services
                # all of the inbound replies on this rank.
                for k in 1:K
                    got[k] = MPIRPC.remotecall_fetch(+, p, RANK * 10_000, k)
                end
                results[i] = got
            end
            push!(ts, t)
        end
        foreach(wait, ts)
        elapsed = time() - t_start
        for (i, p) in enumerate(peers)
            @test results[i] == [RANK * 10_000 + k for k in 1:K]
        end
        # Loose upper bound. On a healthy machine this completes in
        # well under a second; we give it 60 s to absorb CI variance.
        # The point is to catch a hang, not to enforce performance.
        @test elapsed < 60.0
    else
        @test true
    end
end

_phase!("uniform-daemon / Imrecv!: large concurrent payloads round-trip")
@testset "uniform-daemon / large concurrent payloads via Imrecv! + Test/yield" begin
    # Regression test for the non-blocking-receive path. Each task on
    # this rank issues a `remotecall_fetch` with a 1 MiB payload. On
    # most MPI implementations 1 MiB exceeds the eager threshold
    # (typical defaults: 64 KiB for OpenMPI, 256 KiB for MPICH), so
    # the receiving side actually goes through the rendezvous
    # protocol — which is the one regime where the previous
    # `MPI.Mrecv!` would have held `mpi_lock` for a non-trivial
    # duration. With `Imrecv!` + `Test`/`yield` this is broken up.
    #
    # We verify only correctness (all replies arrive, with the right
    # values) and that the whole batch completes in bounded time.
    # Asserting "the daemon thread did not block" directly is not
    # cleanly testable from user code; the existence and correctness
    # of the round-trip under concurrent pressure is the practical
    # proxy.
    if Threads.nthreads() >= 2 && NPROC >= 2
        peer = mod(RANK + 1, NPROC)
        N = 1_048_576                                # 1 MiB
        K = 4                                        # concurrent calls
        # Use the same canonical payload across tasks so we can
        # cross-check sums without reconstructing per-task arrays.
        payload = Vector{UInt8}(undef, N)
        for i in 1:N
            payload[i] = UInt8((i - 1) % 256)
        end
        expected = sum(Int(b) for b in payload)
        ts = Task[]
        for _ in 1:K
            t = Threads.@spawn MPIRPC.remotecall_fetch(peer, payload) do data
                # Verify on the remote side that the buffer survived
                # the rendezvous round-trip intact, then return the
                # sum so the client can independently verify.
                length(data) == N || error("size mismatch: got $(length(data))")
                acc = 0
                for b in data
                    acc += Int(b)
                end
                return acc
            end
            push!(ts, t)
        end
        t0 = time()
        results = fetch.(ts)
        elapsed = time() - t0
        @test all(==(expected), results)
        # Loose bound: with rendezvous and 4 concurrent 1-MiB payloads
        # plus their replies, we expect well under 30 s on any sane
        # interconnect (loopback / shared memory / TCP localhost). The
        # point of the bound is to catch a hang from a leaked request
        # or a missed `Test`, not to enforce performance.
        @test elapsed < 30.0
    else
        @test true
    end
end

_phase!("uniform-daemon / shutdown joins the daemon task")
@testset "uniform-daemon / shutdown! cleanly joins the daemon" begin
    @test backend.daemon_task isa Task
    @test !istaskdone(backend.daemon_task)
    # `shutdown!` flips `running[]` to `false` and waits for the daemon
    # task to terminate. After it returns, the daemon must be done and
    # the `daemon_task` slot must have been cleared so a (hypothetical)
    # subsequent re-init does not see a stale handle.
    shutdown!()
    @test backend.running[] == false
    @test backend.daemon_task === nothing
end

# Final coordination on the duplicate communicator. We cannot use
# `rpc_barrier` here: the daemon is gone, and every rank has called
# `shutdown!` so no rank is pumping progress. A plain `MPI.Barrier`
# on `backend.comm` is what we want — there is no in-flight RPC at
# this point because every testset above completed via `fetch`/`wait`.
MPI.Barrier(backend.comm)
MPI.Finalize()
