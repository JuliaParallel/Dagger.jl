using Test
using MPI
using MPIRPC
using Random

# Each rank sets the *same* seed so any per-rank randomized peer-selection
# decision is reproducible across processes.
Random.seed!(0xC0FFEE)

MPI.Init(; threadlevel=:multiple)
backend = MPIRPC.select_mpi_rpc_backend!(UniformMPIRPCBackend(MPI.COMM_WORLD))
const COMM  = backend.comm
const RANK  = MPI.Comm_rank(COMM)
const NPROC = MPI.Comm_size(COMM)

NPROC >= 2 || error("uniform tests require at least 2 ranks (got $NPROC)")

# Synchronisation aid: a *progress-pumping* barrier on the RPC subcomm and
# announce the next testset on rank 0 so a hang is easy to localise. We
# specifically use `rpc_barrier` rather than `MPI.Barrier` so ranks continue
# servicing inbound RPC while waiting for stragglers — without this, a rank
# whose own primary `remotecall_fetch` completed could exit with a peer's
# nested-RPC request still queued for it.
function _phase!(name::AbstractString)
    rpc_barrier()
    if RANK == 0
        println("--- ", name, " ---")
        flush(stdout)
    end
    rpc_barrier()
end

# Pump progress until `pred()` is true or `timeout` seconds elapse. Returns
# `true` if the predicate became true. Liveness assertion: misuse of the API
# (forgetting to pump progress, holding a lock across fetch, etc.) fails in
# bounded time rather than hanging the whole CI.
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

_phase!("uniform / smoke")
@testset "uniform / smoke" begin
    peer = mod(RANK + 1, NPROC)
    got = remotecall_fetch(+, peer, RANK, 100)
    # `+` ran on `peer` over the args (RANK, 100), so the result is RANK + 100.
    @test got == RANK + 100
end

_phase!("uniform / handler observes its own rank")
@testset "uniform / handler observes its own rank" begin
    peer = mod(RANK + 1, NPROC)
    got = remotecall_fetch(peer) do
        return MPI.Comm_rank(MPI.COMM_WORLD)
    end
    @test got == peer
end

_phase!("uniform / ABBA cross-calls")
@testset "uniform / ABBA: every rank simultaneously calls (i+1) % N" begin
    # Classic ABBA pattern: rank i sends a request to (i+1) and must service
    # the inbound request from (i-1) at the same time. With one tag per kind
    # of message (request vs reply), correlation by RRID, and progress on
    # every rank, this must complete deterministically.
    peer = mod(RANK + 1, NPROC)
    futs = [remotecall(*, peer, RANK + 1, k) for k in 1:8]
    @test pump_until(() -> all(isready, futs); timeout = 30.0)
    if all(isready, futs)
        for (k, f) in enumerate(futs)
            @test fetch(f) == (RANK + 1) * k
        end
    end
end

_phase!("uniform / many OIDs, permuted fetch on one peer")
@testset "uniform / many OIDs, permuted fetch on one peer" begin
    peer = mod(RANK + 1, NPROC)
    K = 64
    expected = [RANK * 1000 + k for k in 1:K]
    futs = [remotecall(+, peer, RANK * 1000, k) for k in 1:K]

    perm = randperm(K)
    @test pump_until(() -> all(isready, futs); timeout = 30.0)
    if all(isready, futs)
        got = Vector{Int}(undef, K)
        for i in perm
            got[i] = fetch(futs[i])
        end
        @test got == expected
    end
end

_phase!("uniform / all-to-all burst")
@testset "uniform / all-to-all burst" begin
    peers = [r for r in 0:(NPROC-1) if r != RANK]
    futs  = [remotecall(+, p, RANK, p) for p in peers]
    @test pump_until(() -> all(isready, futs); timeout = 30.0)
    if all(isready, futs)
        for (p, f) in zip(peers, futs)
            @test fetch(f) == RANK + p
        end
    end
end

_phase!("uniform / same-pair, different correlation ids")
@testset "uniform / same-pair, different correlation ids" begin
    # Two messages from the same (src, dest) at the same time, distinguished
    # only by RRID. MPI guarantees in-order delivery on (src, dest, tag), but
    # the *application-level* matching must use RRID rather than message order
    # to avoid coupling correctness to MPI ordering of unrelated calls.
    peer = mod(RANK + 1, NPROC)
    f1 = remotecall(identity, peer, :first)
    f2 = remotecall(identity, peer, :second)
    @test pump_until(() -> isready(f1) && isready(f2); timeout = 30.0)
    @test fetch(f1) == :first
    @test fetch(f2) == :second
end

_phase!("uniform / nested re-entrant remotecall_fetch")
@testset "uniform / nested re-entrant remotecall_fetch" begin
    if NPROC >= 3
        peer = mod(RANK + 1, NPROC)
        helper = mod(RANK + 2, NPROC)
        # Inside the handler on `peer`, call back to a third rank. Exercises
        # nested progress: while `peer` is servicing rank `RANK`'s request,
        # its handler must be able to issue and await a fresh
        # remotecall_fetch — and the originating rank must keep pumping
        # progress so its outer reply can come back.
        result = remotecall_fetch(peer, helper, RANK) do helper_rank, origin
            inner = MPIRPC.remotecall_fetch(+, helper_rank, origin, 1)
            return inner * 10
        end
        @test result == (RANK + 1) * 10
    else
        @test true  # not enough ranks
    end
end

_phase!("uniform / multi-threaded handler-spawn-fetch")
@testset "uniform / handler that Threads.@spawn-then-fetches an RPC" begin
    # Regression test for the deadlock previously documented in
    # `docs/ARCHITECTURE.md` §6: with synchronous handlers, the handler ran
    # on the calling task while `progress_lock` was held. A handler that
    # spawned its own task and called `fetch` on it would block forever
    # because the spawned task could not acquire `progress_lock` from
    # another OS thread (the lock was held by the handler's task on a
    # different thread). After moving handlers to `Threads.@spawn`, this
    # pattern works.
    if Threads.nthreads() >= 2 && NPROC >= 3
        peer = mod(RANK + 1, NPROC)
        helper = mod(RANK + 2, NPROC)
        result = remotecall_fetch(peer, helper, RANK) do helper_rank, origin
            t = Threads.@spawn MPIRPC.remotecall_fetch(+, helper_rank, origin, 7)
            return fetch(t) * 2
        end
        @test result == (RANK + 7) * 2
    else
        @test true  # need julia -t >=2 and at least 3 ranks to exercise this
    end
end

_phase!("uniform / many handlers, threads concurrent waiters")
@testset "uniform / concurrent client threads pumping the same backend" begin
    # Several client tasks on the same rank issue concurrent
    # `remotecall_fetch` calls. With `progress_lock` removed, every task
    # can drive progress on its own; with handlers spawned on the peer,
    # several of *its* handler tasks can run on different threads. The
    # combined effect is a stress test for the lock geometry under
    # `julia -t >=2`.
    if Threads.nthreads() >= 2
        peers = [r for r in 0:(NPROC-1) if r != RANK]
        K = 16
        results = Vector{Vector{Int}}(undef, length(peers))
        client_tasks = Task[]
        for (i, p) in enumerate(peers)
            t = Threads.@spawn begin
                local got = Vector{Int}(undef, K)
                for k in 1:K
                    got[k] = MPIRPC.remotecall_fetch(+, p, RANK * 1000, k)
                end
                results[i] = got
            end
            push!(client_tasks, t)
        end
        for t in client_tasks
            wait(t)
        end
        for (i, p) in enumerate(peers)
            @test results[i] == [RANK * 1000 + k for k in 1:K]
        end
    else
        @test true
    end
end

_phase!("uniform / remote_do is fire-and-forget")
@testset "uniform / remote_do returns nothing immediately" begin
    peer = mod(RANK + 1, NPROC)
    # `remote_do` is fire-and-forget: it returns `nothing` as soon as the
    # request has been posted, regardless of whether the remote handler
    # has run.
    @test remote_do(peer) do; nothing end === nothing
end

_phase!("uniform / remote_do failure prints to stderr on handler rank")
@testset "uniform / remote_do failure prints to stderr on handler rank" begin
    # Rank 0 issues `remote_do` to rank 1; rank 1 captures stderr while its
    # progress loop (spawned task) services the inbound message so
    # `showerror` from `run_work_thunk(...; print_error=true)` lands in the file.
    cap_mark = "MPIRPC_planned_remote_do_failure"
    cap_path = joinpath(mktempdir(), "mpirpc_remote_do_stderr.txt")
    cap_task = nothing
    if RANK == 1
        ready = Channel{Nothing}(1)
        cap_task = Threads.@spawn begin
            open(cap_path, "w") do f
                redirect_stderr(f) do
                    put!(ready, nothing)
                    t0 = time()
                    while time() - t0 < 30.0
                        rpc_progress!(backend)
                        flush(f)
                        yield()
                        if isfile(cap_path) && filesize(cap_path) > 0
                            s = read(cap_path, String)
                            occursin(cap_mark, s) && break
                        end
                    end
                end
            end
            return read(cap_path, String)
        end
        take!(ready)
    end
    rpc_barrier()
    if RANK == 0
        @test remote_do(1) do
            error(cap_mark)
        end === nothing
    end
    rpc_barrier()
    if RANK == 1
        txt = fetch(cap_task::Task)
        @test occursin(cap_mark, txt)
        rm(cap_path, force=true)
    end
    rpc_barrier()
end

_phase!("uniform / set-then-read via remotecall_wait")
@testset "uniform / set-then-read with remotecall_wait" begin
    peer = mod(RANK + 1, NPROC)
    sentinel_name = Symbol("_MPIRPC_SET_FROM_$(RANK)")
    # `remotecall_wait` blocks the caller until the remote handler
    # acknowledges completion, which is the correct synchronization
    # primitive when a follow-up call needs to observe the side effect.
    # Under multi-threading the spawned-handler model does *not* preserve
    # FIFO of *handler execution* on the same `(src, dest, tag)` — only
    # FIFO of message delivery — so a `remote_do` followed by a sync
    # `remotecall_fetch` would race.
    remotecall_wait(peer, RANK, sentinel_name) do origin, name
        @eval Main const $name = $origin
    end
    val = remotecall_fetch(peer, sentinel_name) do name
        return getfield(Main, name)
    end
    @test val == RANK
end

_phase!("uniform / remote exception is wrapped")
@testset "uniform / remote exception is wrapped" begin
    peer = mod(RANK + 1, NPROC)
    @test_throws MPIRemoteException remotecall_fetch(peer) do
        error("planned failure on remote rank")
    end
end

_phase!("uniform / dest validation")
@testset "uniform / dest validation" begin
    @test_throws ArgumentError remotecall(+, NPROC, 1)
    @test_throws ArgumentError remotecall(+, -1, 1)
end

_phase!("uniform / world barrier coexists with subcomm RPC")
@testset "uniform / collective on world comm coexists with RPC subcomm" begin
    # The uniform backend dups MPI.COMM_WORLD by default, so a Barrier on the
    # original world communicator does not collide with pending RPC traffic.
    peer = mod(RANK + 1, NPROC)
    fut = remotecall(+, peer, RANK, 1)
    MPI.Barrier(MPI.COMM_WORLD)
    @test pump_until(() -> isready(fut); timeout = 30.0)
    if isready(fut)
        @test fetch(fut) == RANK + 1
    end
    MPI.Barrier(MPI.COMM_WORLD)
end

_phase!("uniform / stress: rounds of permuted all-to-all")
@testset "uniform / stress: rounds of permuted all-to-all" begin
    M = 4
    failures = 0
    for round in 1:M
        # Same seed across ranks ⇒ same permutation; identical scheduling
        # decisions on every rank so any failure is reproducible.
        Random.seed!(0xCAFE00 + round)
        peers = collect(0:(NPROC-1))[randperm(NPROC)]
        peers_nonself = [p for p in peers if p != RANK]
        futs = [remotecall(+, p, RANK, round) for p in peers_nonself]
        completed = pump_until(() -> all(isready, futs); timeout = 30.0)
        if completed
            for f in futs
                fetch(f) == RANK + round || (failures += 1)
            end
        else
            failures += length(peers_nonself)
        end
        # Progress-pumping barrier between rounds so any round-N inner
        # request still in flight on a peer is drained before round N+1
        # starts, even if a few ranks raced ahead of others.
        rpc_barrier()
    end
    @test failures == 0
end

_phase!("uniform / shutdown")
shutdown!()
rpc_barrier()
MPI.Finalize()
