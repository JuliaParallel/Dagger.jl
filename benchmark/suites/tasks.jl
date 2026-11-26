# Task-graph overhead benchmark suite.
#
# The array/linalg/sparse/stencil suites measure data-parallel kernels whose cost
# is dominated by tile compute and data movement. This suite isolates the
# *runtime's own* per-task cost -- spawning, placement, cross-owner hops, fan-in
# and an irregular multi-phase step -- using tasks whose bodies take microseconds.
# It is what a latency-bound application (e.g. a tree code with local phases and
# one cross-rank exchange per step) is limited by, and it is the regime in which
# SPMD replication (every rank planning every task) shows up: under MPI each
# rank pays planning for tasks it never runs.
#
# Placement is explicit (`ExactScope`) wherever the benchmark is not *about*
# placement, so a comparison between revisions measures the same task-to-processor
# mapping. "Owner" below means a Distributed worker or an MPI rank, whichever the
# active acceleration uses (`Dagger.processor_owner_rank`).
#
# `BENCHMARK_NTASKS` (default 256) sets the task count of the flat benchmarks.

@everywhere begin
    # Tiny, concrete-typed bodies: dispatch/scheduling dominates, and a concrete
    # return type keeps the MPI execute! on its cheap path.
    bench_noop() = 1
    # Concrete return type but cannot be proven not to throw (like most real task bodies)
    bench_checked(x::Int) = x > 0 ? 1 : error("bench_checked")
    bench_inc(x::Int) = x + 1
    bench_add(a::Int, b::Int) = a + b
    bench_produce(n::Int, seed::Int) = fill(Float64(seed), n)
    bench_stage(v::Vector{Float64}) = (v[1] += 1.0; v)
    bench_consume(a::Vector{Float64}, b::Vector{Float64}) = sum(a) + sum(b)
    bench_combine(x::Float64, y::Float64) = x + y
end

const bench_ntasks = parse(Int, get(ENV, "BENCHMARK_NTASKS", "256"))

# CPU processors of the active acceleration grouped by owner, in a deterministic
# order that is identical on every rank.
function task_bench_layout()
    accel = Dagger.current_acceleration()
    procs = collect(Dagger.compatible_processors(accel))
    sort!(procs; by=p -> Dagger.processor_order_key(accel, p))
    owners = Dict{Any,Vector{Dagger.Processor}}()
    for p in procs
        push!(get!(owners, Dagger.processor_owner_rank(accel, p), Dagger.Processor[]), p)
    end
    owner_ids = sort!(collect(keys(owners)))
    return (; accel, procs, by_owner=[owners[o] for o in owner_ids])
end

pin(p) = Dagger.ExactScope(p)

# `i`-th processor round robin over every processor / over one owner's processors
rr(procs, i) = procs[mod1(i, length(procs))]

function spawn_pinned(procs, n)
    return [Dagger.@spawn scope=pin(rr(procs, i)) bench_noop() for i in 1:n]
end

function spawn_pinned_checked(procs, n)
    return [Dagger.@spawn scope=pin(rr(procs, i)) bench_checked(i) for i in 1:n]
end

function spawn_unscoped_checked(n)
    return [Dagger.@spawn bench_checked(i) for i in 1:n]
end

function spawn_unscoped(n)
    return [Dagger.@spawn bench_noop() for i in 1:n]
end

# Rank-local tasks as a task graph on a communicator of size one (`Dagger.with_rank_local`)
function spawn_comm_self(n)
    return Dagger.with_rank_local() do
        [Dagger.@spawn bench_noop() for i in 1:n]
    end
end

function spawn_local(n)
    local_accel = Dagger.DistributedAcceleration()
    return [Dagger.@spawn acceleration=local_accel bench_noop() for i in 1:n]
end

# A dependent chain alternating between the first processors of the first and
# last owner: every link is a cross-owner hop when there is more than one owner.
function hop_chain(a, b, n)
    x = Dagger.@spawn scope=pin(a) bench_noop()
    for i in 1:n
        x = Dagger.@spawn scope=pin(isodd(i) ? b : a) bench_inc(x)
    end
    return x
end

# Pairwise reduction tree over `n` leaves, every node pinned round robin.
function fan_in(procs, n)
    level = [Dagger.@spawn(scope=pin(rr(procs, i)), bench_inc(i)) for i in 1:n]
    k = n
    while length(level) > 1
        next = []
        for j in 1:2:length(level)-1
            k += 1
            push!(next, Dagger.@spawn scope=pin(rr(procs, k)) bench_add(level[j], level[j+1]))
        end
        isodd(length(level)) && push!(next, level[end])
        level = next
    end
    return level[1]
end

# A step shaped like a distributed tree code: every owner runs `nlocal`
# dependent local stages over `ntiles` tiles, then every owner consumes one tile
# from every other owner (the only cross-owner traffic, one small array per
# pair), then runs one more local stage. Returns the final tasks to wait on.
function exchange_step(by_owner, ntiles, nlocal, len)
    P = length(by_owner)
    tiles = [[Dagger.@spawn(scope=pin(rr(by_owner[o], t)), bench_produce(len, o * 100 + t))
              for t in 1:ntiles] for o in 1:P]
    for _stage in 2:nlocal, o in 1:P, t in 1:ntiles
        tiles[o][t] = Dagger.@spawn scope=pin(rr(by_owner[o], t)) bench_stage(tiles[o][t])
    end
    finals = []
    for o in 1:P
        # P-1 imports per owner (on a single owner: one local consumer)
        acc = nothing
        for q in 1:max(P - 1, 1)
            src = P == 1 ? o : mod1(o + q, P)
            s = Dagger.@spawn scope=pin(rr(by_owner[o], q)) bench_consume(
                tiles[o][mod1(q, ntiles)], tiles[src][mod1(q, ntiles)])
            acc = acc === nothing ? s : Dagger.@spawn(scope=pin(rr(by_owner[o], 1)), bench_combine(acc, s))
        end
        push!(finals, acc)
    end
    return finals
end

waitall_tasks(tasks) = (foreach(wait, tasks); nothing)
waitall_tasks(t::Dagger.DTask) = (wait(t); nothing)

function tasks_suite(_ctx; method, accels)
    @assert method == "dagger" "Tasks suite only supports `dagger` execution"
    isempty(accels) || error("The tasks suite is CPU-only")

    layout = task_bench_layout()
    procs = layout.procs
    by_owner = layout.by_owner
    n = bench_ntasks
    nunscoped = min(n, 64)
    nhops = min(n, 64)
    first_owner = by_owner[1]
    last_owner = by_owner[end]
    mpi = Dagger.accel_kind(layout.accel) == :mpi

    suite = BenchmarkGroup()
    sub = BenchmarkGroup()

    # `wait` does not rethrow a failed task, so a benchmark whose tasks all fail
    # (e.g. an invalid placement) would "run" in microseconds. Check every
    # benchmark's result once, at a tiny size, with `fetch`; a benchmark that does
    # not produce the expected value is skipped with a warning, never timed.
    ok(label, f, expected) = supported(() -> (r = f(); r == expected || error("got $r, expected $expected")), label)

    ok("tasks/pinned", () -> fetch.(spawn_pinned(procs, 4)), fill(1, 4)) &&
        (sub["pinned, round robin (n=$n)"] = @benchmarkable(waitall_tasks(spawn_pinned($procs, $n))))
    # Every task on one owner: all other owners are spectators. Under SPMD they
    # still plan, place and execute-as-no-op every one of these tasks.
    ok("tasks/pinned-one-owner", () -> fetch.(spawn_pinned(first_owner, 4)), fill(1, 4)) &&
        (sub["pinned, one owner (n=$n)"] = @benchmarkable(waitall_tasks(spawn_pinned($first_owner, $n))))
    # Only the last owner's tasks: the benchmark process (rank 0 under MPI) takes no
    # part in them, so this is what a rank that plans foreign tasks pays
    ok("tasks/pinned-last-owner", () -> fetch.(spawn_pinned(last_owner, 4)), fill(1, 4)) &&
        (sub["pinned, last owner (n=$n)"] = @benchmarkable(waitall_tasks(spawn_pinned($last_owner, $n))))
    ok("tasks/pinned-checked", () -> fetch.(spawn_pinned_checked(procs, 4)), fill(1, 4)) &&
        (sub["pinned, round robin, may throw (n=$n)"] = @benchmarkable(waitall_tasks(spawn_pinned_checked($procs, $n))))
    ok("tasks/pinned-checked-last-owner", () -> fetch.(spawn_pinned_checked(last_owner, 4)), fill(1, 4)) &&
        (sub["pinned, last owner, may throw (n=$n)"] = @benchmarkable(waitall_tasks(spawn_pinned_checked($last_owner, $n))))
    # Larger, and with bodies that may throw: unscoped placement is cheap since the
    # candidate order is cached, so the task count can match the pinned benchmarks
    ok("tasks/unscoped-checked", () -> fetch.(spawn_unscoped_checked(4)), fill(1, 4)) &&
        (sub["unscoped, may throw (n=$n)"] = @benchmarkable(waitall_tasks(spawn_unscoped_checked($n))))
    ok("tasks/unscoped", () -> fetch.(spawn_unscoped(4)), fill(1, 4)) &&
        (sub["unscoped (n=$nunscoped)"] = @benchmarkable(waitall_tasks(spawn_unscoped($nunscoped))))
    ok("tasks/hops", () -> fetch(hop_chain(first(first_owner), first(last_owner), 6)), 7) &&
        (sub["dependent hops (n=$nhops)"] = @benchmarkable(
            waitall_tasks(hop_chain($(first(first_owner)), $(first(last_owner)), $nhops))))
    ok("tasks/fan-in", () -> fetch(fan_in(procs, 8)), 44) &&
        (sub["fan-in tree (leaves=$n)"] = @benchmarkable(waitall_tasks(fan_in($procs, $n))))
    # Per owner: sum over its P-1 imports of (tile+tile') plus the combines; the
    # exact value only depends on the layout, so just require equal, finite sums.
    ok("tasks/exchange", () -> (r = fetch.(exchange_step(by_owner, 4, 3, 16)); all(isfinite, r) && length(r) == length(by_owner)), true) &&
        (sub["exchange step (owners=$(length(by_owner)))"] = @benchmarkable(
            waitall_tasks(exchange_step($by_owner, 4, 3, 1024))))
    # Rank-local tasks only exist as a concept under SPMD acceleration, and are
    # only safe where MPI processors refuse them (see `accel_matches_proc`);
    # older revisions place them on MPI processors and lose them, so probe the
    # predicate rather than running tasks that can hang.
    # Not every revision has it: the probe skips the benchmark where `with_rank_local` is missing
    if mpi && ok("tasks/comm-self", () -> fetch.(spawn_comm_self(4)), fill(1, 4))
        sub["rank-local, one-rank communicator (n=$n)"] = @benchmarkable(waitall_tasks(spawn_comm_self($n)))
    end
    if mpi && !Dagger.accel_matches_proc(Dagger.DistributedAcceleration(), first(procs)) &&
       ok("tasks/local", () -> fetch.(spawn_local(4)), fill(1, 4))
        sub["rank-local (n=$n)"] = @benchmarkable(waitall_tasks(spawn_local($n)))
    end

    suite["owners=$(length(by_owner)) procs=$(length(procs))"] = sub
    return suite
end

return tasks_suite
