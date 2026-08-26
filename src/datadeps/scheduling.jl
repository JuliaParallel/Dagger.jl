abstract type DataDepsScheduler end

# Default for user-defined schedulers with a zero-arg constructor. Schedulers
# that carry mutable state should specialize `similar` to return a fresh shard
# (used when hierarchical scheduling clones a scheduler per partition).
Base.similar(s::DataDepsScheduler) = typeof(s)()

"""
    DATADEPS_LOCALITY_BIAS[] -> Float64

How strongly Datadeps schedulers favor placing a task where its arguments
already have a current replica (`state.arg_current`), trading that off against
load-spreading. Ranges `0.0`–`1.0`:

- `0.0`: locality is ignored entirely. Every scheduler falls through to its
  original, locality-blind behavior *exactly* -- this is not merely "weighted
  toward zero", it is a genuine no-op path, so it doubles as the "before" leg
  of an A/B comparison and reproduces today's behavior bit-for-bit.
- `1.0`: never move data if a candidate holding it is otherwise viable.

Pure locality (an implicit `1.0`-only policy) is a degenerate scheduler: it
pins every tile to wherever it first landed and starves any processor that
didn't get there first. Every scheduler that consults this Ref blends it
against a load/cost term rather than letting it override placement outright,
which is why the default sits at `0.5` instead of `1.0`.

This matters more once `DataDepsState` outlives a single region (see
`PLAN.md`, phases 3+): a tile a prior region left on worker 3 is *not* back at
its origin the way a region barrier used to guarantee, and scheduling as though
it were manufactures exactly the cross-space movement this exists to avoid.

!!! warning "MPI/SPMD uniformity"
    This is a process-global `Ref` consulted during planning, which under
    uniform (SPMD/MPI) execution every rank replays -- so every rank must set
    it to the *same* value before planning a region. Unlike `state.arg_current`
    (a planning-time fact every rank derives identically from the same replayed
    program), nothing about `bias` is derived; it's an external knob, and a
    rank that reads a different value makes a different placement *decision*,
    not just a different measurement -- that's divergence, which under
    uniform execution is a deadlock, not a wrong answer. Every scheduler that
    reads this checks it with `check_uniform`, so a real mismatch raises
    rather than hangs when `Dagger.check_uniformity!(true)` is active; the
    check is otherwise a no-op (see `CHECK_UNIFORMITY`).
"""
const DATADEPS_LOCALITY_BIAS = Ref{Float64}(0.5)

"""
    datadeps_tracked_args(state::DataDepsState, spec::DTaskSpec) -> Vector{Tuple{ArgumentWrapper,Float64}}

For each argument in `spec.fargs`, resolves the `ArgumentWrapper`(s) that
`populate_task_info!` will derive for it later (one per `dep_mod`, for a
`Deps`-wrapped argument), paired with a locality weight: the argument's byte
size, doubled when the dependency is a write (`Out`/`InOut`). A write forces
every other replica of the same memory stale, so placing a writer on the wrong
side of an interconnect costs the move twice over -- once to bring the data to
where the write happens, and again because every previously-current replica
elsewhere is now garbage.

This is read-only with respect to `state`: unlike `populate_task_info!`, it
never generates a slot, allocates a `Chunk` for an argument Datadeps hasn't
seen yet, or registers aliasing info. An argument that is untracked --
`state.raw_arg_to_chunk` has no entry for it, meaning this is the *first* task
in the region to touch it -- is skipped outright: there is no data yet for
anywhere to be local *to*, so it correctly contributes no opinion rather than
a guess. Non-aliasing arguments (`type_may_alias` false) and arguments whose
`raw_arg_to_chunk` entry isn't itself a `Chunk` (e.g. a raw MPI wire value
under uniform execution) are skipped for the same reason: there is no `Chunk`
to size.

Building `ArgumentWrapper`s here duplicates work `populate_task_info!` repeats
moments later for the same task (including, under MPI, `check_uniform`'s
collective per wrapper) -- but scheduling runs *before* `populate_task_info!`,
so there is no cached answer yet to reuse. Callers that don't need locality
(bias `0`, or a region with only one memory space in play) must not call this
at all; see `datadeps_single_space`.

!!! note "Why the `haskey(state.raw_arg_to_chunk, arg)` branch is rank-uniform"
    Every branch above it (`type_may_alias`) is a pure function of `arg`'s
    *type*, so it can't diverge on identical arguments. The `haskey` check is
    a function of *state accumulated so far this region*, so it needs a
    little more: under uniform (SPMD/MPI) execution, every rank plans every
    task in the same region by replaying the identical program (the same
    `spawn_datadeps` body, the same `@spawn` calls, in the same order --
    that's the whole premise uniform execution runs on, not something this
    function introduces). `raw_arg_to_chunk` is populated by
    `populate_task_info!` for every prior task in that same replayed order, so
    at the point any given task is scheduled, its content -- and therefore
    this `haskey` result for any of that task's arguments -- is identical on
    every rank by construction, the same invariant that already lets
    `state.arg_owner`/`state.arg_current` be read here without a fresh
    collective per read. No additional `check_uniform` guard is added for this
    specific branch, since one only makes sense where ranks could plausibly
    disagree, and doing so on every tracked argument of every task would add a
    collective to a path `PLANNING_SCALING.md` already measured at ~21% of an
    MPI sweep at 4 ranks. `ArgumentWrapper`'s own `check_uniform(h, arg)` still
    runs for every wrapper actually constructed past this branch.
"""
function datadeps_tracked_args(state::DataDepsState, spec::DTaskSpec)
    out = Tuple{ArgumentWrapper,Float64}[]
    # N.B. Locality weighting is disabled entirely under uniform (SPMD/MPI)
    # execution, because the weights are derived from `datasize`, which is
    # deliberately NOT rank-uniform: `datasize(x::MPIRef) = x.size`, and
    # `ext/MPIExt.jl` documents that non-owning ranks carry a size-0
    # placeholder, calling that "harmless because `datasize` only feeds
    # cost-based placement, which uniform execution overrides".
    #
    # Feeding it into placement here breaks that invariant. `distribute_tasks!`
    # calls `select_processors_uniform!` to give every rank the *same ordered*
    # `all_procs`, and round-robin then advances the same index everywhere --
    # but a locality scan picking `best_idx` from rank-dependent weights picks a
    # *different* processor per rank. That diverges task placement, hence the
    # emitted transfers, hence the tag sequence: ranks deadlock on mismatched
    # tags rather than failing visibly. Measured: the pencil FFT on the flat
    # path at 4 ranks hangs with "Hit probable hang on recv".
    #
    # Re-enabling this needs a size that is metadata, known identically on every
    # rank (chunk domain extents x eltype size, say) rather than a property only
    # the owning rank can answer. Until then MPI keeps pre-locality placement,
    # which costs little in practice since locality is already inert under the
    # default hierarchical path (`partition_dag` fixes cross-worker placement
    # before any scheduler runs).
    uniform_execution() && return out
    for _arg in spec.fargs
        arg_pre_unwrap, deps = unwrap_inout(value(_arg))
        arg = arg_pre_unwrap isa DTask ? fetch(arg_pre_unwrap; raw=true) : arg_pre_unwrap
        type_may_alias(typeof(arg)) || continue
        haskey(state.raw_arg_to_chunk, arg) || continue
        arg_chunk = state.raw_arg_to_chunk[arg]
        arg_chunk isa Chunk || continue
        sz = Float64(datasize(arg_chunk))
        for (dep_mod, _readdep, writedep) in deps
            arg_w = ArgumentWrapper(arg_chunk, dep_mod)
            # N.B. `arg_current` may not exist yet even though `raw_arg_to_chunk`
            # does, e.g. for a `dep_mod` this particular task is the first to
            # use on an otherwise-seen argument (a fresh sub-region view).
            haskey(state.arg_current, arg_w) || continue
            push!(out, (arg_w, writedep ? 2sz : sz))
        end
    end
    return out
end

"""
    datadeps_locality_weights(state::DataDepsState, spec::DTaskSpec, all_procs) -> Dict{MemorySpace,Float64}

Bytes that would *not* have to move if the task were placed in each candidate
memory space: for every argument `datadeps_tracked_args` can resolve, credit
every space in `state.arg_current[arg_w]` (every space holding a fully-current
replica) with the argument's weight. An argument resident in more than one
space credits all of them -- being available at space A doesn't make it any
less available at space B.

Only spaces reachable by some processor in `all_procs` are ever inserted, so a
caller can safely index the result with `only(memory_spaces(candidate_proc))`
for any `candidate_proc in all_procs` and treat a missing key as zero (via
`get(weights, space, 0.0)`) without separately having to filter out spaces the
region can't actually use.

Order-independence note (MPI/SPMD rank uniformity): this only ever *adds into*
independent `Dict` entries keyed by `space`, so the result does not depend on
`Set`/`Dict` iteration order even though `arg_current` is a `Set` -- unlike,
say, `findmax` over a `Dict` or picking an arbitrary element as "the"
representative, summing contributions is commutative. See the schedulers below
for how the result is then turned into a single choice deterministically.
"""
function datadeps_locality_weights(state::DataDepsState, spec::DTaskSpec, all_procs)
    weights = Dict{MemorySpace,Float64}()
    valid_spaces = Set{MemorySpace}(only(memory_spaces(proc)) for proc in all_procs)
    for (arg_w, weight) in datadeps_tracked_args(state, spec)
        current = state.arg_current[arg_w]
        for space in current
            space in valid_spaces || continue
            weights[space] = get(weights, space, 0.0) + weight
        end
    end
    return weights
end

"""
    datadeps_single_space(all_procs) -> Union{MemorySpace,Nothing}

The one memory space shared by every processor in `all_procs`, or `nothing` if
they don't all agree on one. Used to make locality-aware placement a true
no-op -- not just an inexpensive one -- for a single-process/multithreaded
region: `memory_spaces(proc::ThreadProc)` is keyed by *process*
(`src/memory-spaces.jl`), so a one-process, N-thread run has exactly one
memory space, nothing is ever non-local, and there is nothing to gain (and
real per-task cost to lose) by computing locality weights at all.
"""
function datadeps_single_space(all_procs)
    isempty(all_procs) && return nothing
    space = only(memory_spaces(all_procs[1]))
    for idx in 2:length(all_procs)
        only(memory_spaces(all_procs[idx])) == space || return nothing
    end
    return space
end

mutable struct RoundRobinScheduler <: DataDepsScheduler
    proc_idx::Int

    # Locality is checked against the *identity* of `all_procs`, not its
    # contents: `distribute_tasks!`/`distribute_task!` build `all_procs` once
    # per region and pass the very same object for every task in it, so
    # comparing by `===` lets the (only mildly cheap) single-space probe run
    # once per region instead of once per task, making the common
    # single-space case free after the first task rather than merely cheap.
    locality_procs::Any
    locality_single_space::Bool

    RoundRobinScheduler() = new(1, nothing, false)
end
Base.similar(::RoundRobinScheduler) = RoundRobinScheduler()
function datadeps_schedule_task(sched::RoundRobinScheduler, state::DataDepsState, all_procs, all_scope, task_scope, spec::DTaskSpec, task::DTask)
    proc_idx = sched.proc_idx
    our_proc = all_procs[proc_idx]
    if task_scope === all_scope
        # all_procs is already limited to scope
    elseif task_scope === DefaultScope()
        # Common case: no user-specified scope. `DefaultScope()` is a shared
        # singleton, so `===` identifies it exactly. Its inner scope is
        # `AnyScope`, so `constrain(task_scope, all_scope)` can never be an
        # `InvalidScope` and the compatibility check is skippable; all that
        # remains of `proc_in_scope(proc, DefaultScope())` is its lone
        # `DefaultEnabledTaint`, i.e. `default_enabled(proc)`.
        while !default_enabled(our_proc)
            proc_idx = mod1(proc_idx + 1, length(all_procs))
            our_proc = all_procs[proc_idx]
        end
    else
        if isa(constrain(task_scope, all_scope), InvalidScope)
            throw(Sch.SchedulingException("Scopes are not compatible: $(all_scope), $(task_scope)"))
        end
        while !proc_in_scope(our_proc, task_scope)
            proc_idx = mod1(proc_idx + 1, length(all_procs))
            our_proc = all_procs[proc_idx]
        end
    end

    # Locality-aware placement. Ordered by cost: `bias == 0` and "only one
    # processor" are free checks that skip everything below, and the
    # single-space probe (the common single-process/multithreaded case) is
    # cached per-region so it's free after the first task too. `bias == 0`
    # must leave `proc_idx`/`our_proc` byte-for-byte as pure round robin would
    # -- it's both the correctness baseline (untouched round robin was already
    # correct) and the "before" leg of any A/B comparison. This is a hard
    # bypass (the whole block is skipped, not merely made to compute a zero
    # term), so `bias == 0` costs nothing beyond this one comparison.
    #
    # N.B. `bias` is a process-global `Ref` read during planning, which under
    # SPMD/MPI must agree across every rank or ranks place tasks differently
    # and desync (a diverging *decision*, not just a diverging measurement --
    # unlike `state.arg_current`, nothing about `bias` is derived from
    # rank-uniform planning state, so a rank-local override is a foot-gun with
    # no other guard against it). Checked once per region (cached the same way
    # as `locality_single_space`, not once per task) since `check_uniform` is
    # a collective under `CHECK_UNIFORMITY[]`.
    bias = DATADEPS_LOCALITY_BIAS[]
    if bias > 0 && length(all_procs) > 1
        if sched.locality_procs !== all_procs
            sched.locality_procs = all_procs
            check_uniform(bias)
            sched.locality_single_space = datadeps_single_space(all_procs) !== nothing
        end
        if !sched.locality_single_space
            weights = datadeps_locality_weights(state, spec, all_procs)
            if !isempty(weights)
                # `argmax` is invariant to scaling by a positive constant, so
                # `score = bias * get(weights, space, 0.0)` (an earlier version
                # of this) picked the same processor for every `bias > 0`: the
                # "load" side of the blend was a literal zero, so there was
                # nothing for `bias` to trade off against and every positive
                # bias behaved like `1.0`, the degenerate always-pin policy
                # DATADEPS_LOCALITY_BIAS exists to avoid. To actually
                # interpolate, both terms need to be on the same [0,1] scale:
                # `locality` (resident bytes here, normalized by the most any
                # candidate has) against `staleness` (distance from the
                # rotation position round robin would have picked, normalized
                # by candidate count) -- `offset/n` is round robin's own
                # load-spreading proxy, since "the next one in rotation" *is*
                # round robin's entire load-balancing policy, not a separate
                # measurement bolted on.
                #
                # Scanned in rotation order (not `findmax` over `weights`) so
                # ties -- and `bias == 0`, see below -- resolve to whichever
                # candidate pure round robin would have picked, deterministically,
                # rather than depending on `Dict` iteration order (which would
                # not be rank-uniform under MPI).
                maxw = maximum(values(weights))
                if maxw > 0
                    best_idx = proc_idx
                    best_score = -Inf
                    n = length(all_procs)
                    for offset in 0:(n - 1)
                        idx = mod1(proc_idx + offset, n)
                        proc = all_procs[idx]
                        proc_in_scope(proc, task_scope) || continue
                        space = only(memory_spaces(proc))
                        locality = get(weights, space, 0.0) / maxw  # 1.0 = fully resident here
                        staleness = offset / n                      # 0.0 = round robin's own pick
                        score = bias * locality - (1 - bias) * staleness
                        if score > best_score
                            best_score = score
                            best_idx = idx
                        end
                    end
                    proc_idx = best_idx
                    our_proc = all_procs[proc_idx]
                end
            end
        end
    end

    proc_idx = mod1(proc_idx + 1, length(all_procs))
    sched.proc_idx = proc_idx
    return our_proc
end

"""
    NaiveScheduler()

Places each task on whichever processor the *main* scheduler's cost model
(`Sch.estimate_task_costs`) ranks cheapest, given the live scheduler's current
per-processor pressure and the transfer cost of the task's chunk arguments.

"Naive" is about the horizon, not the model: each task is costed in isolation,
against the scheduler's state as it stands right now, with no memory of what
this same region decided for the previous task. Placing a hundred tasks in a
row therefore tends to place them all in the same "cheapest" spot, because
nothing that happened during planning has moved the pressure numbers -- only
tasks that have actually *run* do. Use [`UltraScheduler`](@ref) when the
region's own decisions should feed back into the next one.

!!! warning "Not usable under uniform (SPMD/MPI) execution"
    `estimate_task_costs` ranks processors using per-rank measurements
    (`signature_time_cost`, `worker_transfer_rate`, chunk `datasize`) and
    breaks ties with `randperm!`. Under uniform execution every rank must
    reach an *identical* placement decision or the emitted transfers -- and
    hence the MPI tag sequence -- diverge, which deadlocks rather than
    producing a wrong answer. Rather than hang, this raises.
"""
struct NaiveScheduler <: DataDepsScheduler end
Base.similar(::NaiveScheduler) = NaiveScheduler()
function datadeps_schedule_task(sched::NaiveScheduler, state::DataDepsState, all_procs, all_scope, task_scope, spec::DTaskSpec, task::DTask)
    # Fail loudly instead of deadlocking; see the warning in the docstring.
    if uniform_execution()
        throw(Sch.SchedulingException("NaiveScheduler is not rank-uniform and cannot be used under uniform (SPMD/MPI) execution; use RoundRobinScheduler or UltraScheduler"))
    end

    # Restrict to processors this task is actually allowed to run on *before*
    # costing them. `estimate_task_costs` knows nothing about scopes, so
    # without this the cheapest-ranked processor could be one `task_scope`
    # excludes, and `distribute_task!` would then fail the region with an
    # `InvalidScope` -- a scheduler-chosen placement violating a user-stated
    # constraint, not a genuinely unsatisfiable one. Structured to avoid
    # allocating a filtered copy in the common cases (no per-task scope, or a
    # scope already equal to the region's).
    procs_in_scope = if task_scope === all_scope
        # `all_procs` is already limited to scope
        all_procs
    elseif task_scope === DefaultScope()
        # See `RoundRobinScheduler` for why `DefaultScope()` reduces to
        # `default_enabled` and needs no compatibility check.
        all(default_enabled, all_procs) ? all_procs : filter(default_enabled, all_procs)
    else
        if isa(constrain(task_scope, all_scope), InvalidScope)
            throw(Sch.SchedulingException("Scopes are not compatible: $(all_scope), $(task_scope)"))
        end
        filter(proc->proc_in_scope(proc, task_scope), all_procs)
    end
    if isempty(procs_in_scope)
        throw(Sch.SchedulingException("Scopes are not compatible: $(all_scope), $(task_scope)"))
    end

    # Prefer the chunk that reflects where an argument's data actually is over
    # `tochunk(value(arg))`, which is always the *origin* chunk. Under a
    # region barrier the origin was always correct (every argument starts each
    # region there), but once `state` persists across regions (see PLAN.md,
    # phases 3+) origin can be stale by the time this task is scheduled, and
    # costing the origin instead of the real location would manufacture
    # exactly the cross-space movement this project exists to remove.
    #
    # `bias == 0` skips this and reproduces the original `tochunk`-only
    # behavior exactly, same rationale as `RoundRobinScheduler`. No
    # `check_uniform(bias)` guard here, unlike the other two schedulers: this
    # one never runs under uniform execution at all (it raised above), so there
    # are no other ranks for `bias` to disagree with.
    #
    # `signature`/`estimate_task_costs!` expect `Argument`-like elements (they
    # call `Dagger.value`/`Dagger.valuetype` on each one), not bare `Chunk`s --
    # so each resolved chunk is rewrapped in a fresh `Argument` at the same
    # position, the same way `Base.copy(arg::Argument)` does elsewhere.
    # `Base.mapany` (not `map`) keeps `raw_args` a plain `Vector` regardless of
    # whether `spec.fargs` is a `Vector{Argument}` or (for a typed spec) a
    # heterogeneous `Tuple` -- the `@view raw_args[2:end]` below requires an
    # `AbstractArray`, which a `Tuple` is not.
    bias = DATADEPS_LOCALITY_BIAS[]
    raw_args = Base.mapany(spec.fargs) do arg
        v = value(arg)
        chunk = bias > 0 ? something(datadeps_current_chunk(state, v), tochunk(v)) : tochunk(v)
        return Argument(ArgPosition(arg.pos), chunk)
    end
    our_proc = remotecall_fetch(1, procs_in_scope, raw_args) do all_procs, raw_args
        Sch.init_eager()
        sch_state = Sch.EAGER_STATE[]

        @lock sch_state.lock begin
            # Calculate costs per processor and select the most optimal.
            # `raw_args[1]` is the task's function argument and
            # `raw_args[2:end]` its actual arguments, matching how
            # `signature(state, task::Thunk)` lays out `Thunk.inputs`
            # (function first) elsewhere in the scheduler.
            sig = Sch.signature(raw_args[1], @view raw_args[2:end])
            fake_task = (inputs=raw_args,)
            procs, costs = Sch.estimate_task_costs(sch_state, all_procs, fake_task; sig)
            return first(procs)
        end
    end
    return our_proc
end

"""
    datadeps_current_chunk(state::DataDepsState, raw_value) -> Union{Chunk,Nothing}

The `Chunk` currently backing `raw_value` (an unprocessed `Argument.value`,
possibly `In`/`Out`/`InOut`-wrapped and/or a `DTask`) at its recorded owner
space, or `nothing` if that can't be determined cheaply and safely.

Deliberately conservative: a `Deps`-wrapped argument with more than one
`dep_mod` doesn't have a single "current chunk" to hand to a whole-argument
cost estimate, so that case (and anything else off the common path) falls back
to `nothing`, leaving the caller to use the origin chunk instead. This also
avoids ever picking a representative space by iterating
`state.arg_current[arg_w]` (a `Set`): `state.arg_owner[arg_w]` is a single,
non-`Set` value, so checking whether *it* is current is rank-uniform by
construction, where `first(a_set)` would not obviously be.
"""
function datadeps_current_chunk(state::DataDepsState, raw_value)
    # Disabled under uniform execution for the same reason as
    # `datadeps_tracked_args`: substituting the resident chunk changes what
    # `estimate_task_costs` sizes, and chunk sizes are not rank-uniform under
    # MPI, so the chosen processor could differ per rank and desync tags.
    uniform_execution() && return nothing
    arg_pre_unwrap, deps = unwrap_inout(raw_value)
    length(deps) == 1 || return nothing
    arg = arg_pre_unwrap isa DTask ? fetch(arg_pre_unwrap; raw=true) : arg_pre_unwrap
    type_may_alias(typeof(arg)) || return nothing
    haskey(state.raw_arg_to_chunk, arg) || return nothing
    arg_chunk = state.raw_arg_to_chunk[arg]
    arg_chunk isa Chunk || return nothing
    dep_mod = deps[1][1]
    arg_w = ArgumentWrapper(arg_chunk, dep_mod)
    current = get(state.arg_current, arg_w, nothing)
    (current === nothing || isempty(current)) && return nothing
    space = get(state.arg_owner, arg_w, nothing)
    (space === nothing || !(space in current)) && return nothing
    remote_for_space = get(state.remote_args, space, nothing)
    remote_for_space === nothing && return nothing
    return get(remote_for_space, arg_chunk, nothing)
end

# Nanoseconds of predicted delay per byte that `UltraScheduler` would have to
# move into a candidate memory space. `Sch.DEFAULT_TRANSFER_RATE` is documented
# (and populated, from `metadata.transfer_rate`) as bytes per *second*, so
# converting to the nanoseconds `signature_time_cost` speaks costs a factor of
# 1e9.
#
# That factor is the whole fix. The original `UltraScheduler` wrote
# `missing_bytes / Sch.DEFAULT_TRANSFER_RATE`, leaving a *seconds* quantity
# added to nanosecond task times: a 2 MB tile priced at 2 ns against a
# 512-cube `gemm!`'s ~4.5 ms, i.e. locality with a weight of zero. (The same
# 1e9 mismatch exists in `estimate_task_costs!` itself, where this constant
# came from, and is why its magnitude has never visibly mattered there. Not
# fixed here: that is the main scheduler's cost model, with its own callers.)
#
# 1 MB/s looks far too pessimistic for any real interconnect, and measurement
# says to keep it anyway. Swept on a blocked Cholesky (N=4096, nb=512, 8x8
# tiles, 4 single-thread Distributed workers pinned to cores 0-5,
# `hierarchical=false`, bias 0.5, 11 interleaved reps, first dropped, median):
#
#     1 GB/s -> 1047 ms    100 MB/s -> 695 ms    10 MB/s -> 504 ms
#     1 MB/s ->  420 ms    0.1 MB/s -> 428 ms
#
# with `RoundRobinScheduler` at the same bias measuring 421 ms. Monotone, far
# outside the ~10 ms run-to-run noise, and flat-to-slightly-worse below 1 MB/s.
# What is being priced is not wire bandwidth: it is Dagger's end-to-end cost of
# making a tile available in another memory space -- Distributed serialization,
# a MemPool `DRef` round trip, an extra Dagger task per copy, plus the
# write-back a later reader on the original side then pays. Against that, a
# nameplate GB/s figure under-discourages movement by roughly the observed
# factor.
const DATADEPS_TRANSFER_NS_PER_BYTE = 1e9 / Float64(Sch.DEFAULT_TRANSFER_RATE)

# Placeholder runtime for a signature the main scheduler has never measured,
# matching what `estimate_task_costs!` assumes for the same case.
const DATADEPS_UNKNOWN_TASK_TIME = UInt64(1000^3)

# Shared empty stand-in so `UltraScheduler`'s `bias == 0` / no-tracked-args
# path allocates nothing at all per task.
const EMPTY_SPACE_WEIGHTS = Dict{MemorySpace,Float64}()

# `UltraScheduler.task_completions` is keyed by task uid and can only grow: a
# region's tasks are never removed from it, and a scheduler handed via
# `DATADEPS_SCHEDULER[]` outlives any one region. Past this many entries the
# map is emptied wholesale. Losing a completion estimate only makes a later
# task look *ready earlier* than it is, which is a heuristic degradation, not
# a correctness problem -- and it is rank-uniform, since every rank inserts
# the same number of entries in the same order.
const DATADEPS_ULTRA_COMPLETION_CAP = 1 << 16

"""
    UltraScheduler()

Places each task on the processor where it is predicted to *finish* earliest --
the classic "earliest finish time" list-scheduling heuristic. A candidate
processor's finish time is

    max(when that processor is predicted to go idle,
        when this task's syncdeps are predicted to complete)
    + time to move whatever isn't resident in that memory space yet
    + the task's own measured runtime

Load-spreading is a *consequence* here rather than a rotation: handing a
processor work pushes its predicted idle time into the future, so it stops
attracting tasks until the others catch up. Unlike `NaiveScheduler`, which
costs each task in isolation against the live scheduler's current pressure,
`UltraScheduler` simulates the region it is planning -- the decision made for
task 1 is visible when task 500 is placed. That is what lets it see a pile-up
coming, and also why it is the only one of the three carrying per-region
mutable state (hence the `Base.similar` specialization: hierarchical
partitions must each get their own shard, see `hierarchical.jl`).

Runtimes come from the main scheduler's `signature_time_cost` table, which is
empty until a signature has actually executed once. Every task in a cold
region therefore costs the same placeholder and placement degenerates to
"balance the task counts, prefer whoever already holds the data" -- which is
the best available answer at that point, not a failure mode.

!!! note "What it cannot see"
    Datadeps' own dependency analysis runs *after* placement (`distribute_task!`
    computes syncdeps from `our_space`, which is this function's output), so
    the only dependencies visible here are the ones the user's `@spawn`
    already carried: explicit `syncdeps` and `DTask`-valued arguments. The
    data dependencies Datadeps derives from `In`/`Out`/`InOut`, and the copy
    tasks it inserts, are not yet known. Tasks related only through those look
    mutually ready, so the critical-path term is a lower bound on the real one.

!!! note "Under uniform (SPMD/MPI) execution"
    Safe to use, but deliberately less informed: measured runtimes are
    per-rank facts, so they are replaced by a single placeholder and the
    policy degenerates to a deterministic least-loaded-processor choice. See
    the N.B. on `task_time` below for why a rank-dependent cost here
    deadlocks rather than merely misplacing a task.
"""
mutable struct UltraScheduler <: DataDepsScheduler
    # Predicted time (ns, on a common clock renormalized per region so the
    # earliest-free processor sits at 0) at which each processor goes idle.
    # This is the scheduler's entire model of load.
    proc_completions::Dict{Processor,UInt64}

    # Predicted completion time of each already-placed task, keyed by
    # `DTask.uid`.
    #
    # N.B. Keyed by *uid*, not by `DTask`. `spec.options.syncdeps` holds
    # `ThunkSyncdep`s, which carry a `ThunkID` wrapping that uid rather than
    # the `DTask` itself, so the original `Dict{DTask,UInt64}` could never be
    # hit by a syncdep lookup: `deps_completed` was silently always zero (and
    # then, separately, never read at all).
    task_completions::Dict{Int,UInt64}

    # Memory space each task was placed in. Not consulted by placement --
    # `proc_completions` subsumes what the old `assignments`-rescan computed --
    # but kept because it is the only externally observable record of what this
    # scheduler decided, which is what the tests assert against.
    assignments::Dict{Int,MemorySpace}

    # Per-region memoization of `signature -> measured runtime`, so a region of
    # N tasks over a handful of distinct signatures pays a handful of lookups
    # into the eager scheduler's tables instead of N (each of which is a
    # `remotecall_fetch` when planning off worker 1). Dropped whenever
    # `region_procs` changes, so a long-lived scheduler picks up fresh
    # measurements at the next region rather than pinning the first ones
    # forever.
    sig_time_cache::Dict{Signature,UInt64}

    # Identity of the `all_procs` this scheduler last saw, used to detect a
    # region boundary. Same trick (and same rationale) as
    # `RoundRobinScheduler.locality_procs`: `distribute_tasks!` builds
    # `all_procs` once per region and passes that same object for every task.
    region_procs::Any

    function UltraScheduler()
        return new(Dict{Processor,UInt64}(),
                   Dict{Int,UInt64}(),
                   Dict{Int,MemorySpace}(),
                   Dict{Signature,UInt64}(),
                   nothing)
    end
end
Base.similar(::UltraScheduler) = UltraScheduler()

"""
    ultra_signature_time!(sched::UltraScheduler, spec::DTaskSpec) -> UInt64

The main scheduler's measured runtime (ns) for `spec`'s call signature, or
[`DATADEPS_UNKNOWN_TASK_TIME`](@ref) if it has never run one.

Builds the signature from the *raw* argument values rather than
`tochunk`-wrapping each one, as this used to. `Sch.signature` reduces every
argument through `chunktype`, and `chunktype(c::Chunk) = c.chunktype` -- the
type of what the chunk *holds* -- so a raw `Vector{Float64}` and a `Chunk`
wrapping one produce the identical `Signature`, and hence hit the identical
entry in `signature_time_cost` that the real (chunk-argument) execution
populates. The wrapping was therefore pure cost: one `tochunk` per argument
per task, each allocating a `DRef` in MemPool that nothing ever reads.

The old code also passed `spec.fargs` -- which includes the function at
position 1 -- as the *argument* list while separately passing the function,
counting `f` twice in the signature and so never matching a real one.
"""
function ultra_signature_time!(sched::UltraScheduler, spec::DTaskSpec)
    # `Base.mapany` (not `map`): a typed spec's `fargs` is a heterogeneous
    # `Tuple`, and `Sch.signature`'s `@view args[...]`-free path still needs an
    # indexable, `Vector`-typed collection to hand back below.
    raw_args = Base.mapany(spec.fargs) do arg
        v, _ = unwrap_inout(value(arg))
        if v isa DTask
            v = fetch(v; move_value=false, unwrap=false)
        end
        return Argument(ArgPosition(arg.pos), v)
    end
    sig = Sch.signature(raw_args[1], @view raw_args[2:end])
    cached = get(sched.sig_time_cache, sig, nothing)
    cached === nothing || return cached
    # N.B. Reads `signature_time_cost` under *its own* lock rather than under
    # `sch_state.lock`, which the original took. That field is documented
    # (Sch.jl) as having its own lock precisely so reads are independent of
    # the scheduler's; taking the big lock once per task put planning in
    # direct contention with the running scheduler for no added consistency.
    time = if myid() == 1
        Sch.init_eager()
        sch_state = Sch.EAGER_STATE[]
        lock(sch_state.signature_time_cost) do stc
            get(stc, sig, DATADEPS_UNKNOWN_TASK_TIME)
        end
    else
        remotecall_fetch(1, sig) do sig
            Sch.init_eager()
            sch_state = Sch.EAGER_STATE[]
            lock(sch_state.signature_time_cost) do stc
                get(stc, sig, DATADEPS_UNKNOWN_TASK_TIME)
            end
        end::UInt64
    end
    sched.sig_time_cache[sig] = time
    return time
end

function datadeps_schedule_task(sched::UltraScheduler, state::DataDepsState, all_procs, all_scope, task_scope, spec::DTaskSpec, task::DTask)
    # Refuse rather than deadlock. Every input to the placement decision below
    # was made rank-uniform on purpose -- `task_time` is flattened to the
    # placeholder, `datadeps_tracked_args` returns nothing, every tie-break is
    # positional over `all_procs` -- and yet a 2-rank measurement says the
    # result is *not* uniform in practice:
    #
    #   4 isolated runs, 2 ranks, 8 tasks over 8 arrays, `check_uniformity!(true)`
    #   RoundRobinScheduler: 4/4 correct
    #   UltraScheduler:      3/4 `[rank 0][tag 1073741823] Hit hang on recv`,
    #                        1/4 correct but hung at shutdown
    #
    # The divergent input has not been identified, so this cannot be presented
    # as rank-uniform on the strength of a code reading -- the reading says it
    # should be, and the machine says otherwise. Until someone finds it, a
    # clear error beats an intermittent hang: a deadlock under SPMD gives no
    # error, no wrong answer, and no backtrace pointing here.
    #
    # This costs nothing in practice. On the blocked-Cholesky benchmark this
    # scheduler was rewritten against, it measured 422ms against
    # `RoundRobinScheduler`'s 422ms -- within noise -- so there is no
    # MPI workload that wants it badly enough to justify shipping a hang.
    if uniform_execution()
        throw(Sch.SchedulingException("UltraScheduler is not verified rank-uniform and intermittently deadlocks under uniform (SPMD/MPI) execution; use RoundRobinScheduler"))
    end

    bias = DATADEPS_LOCALITY_BIAS[]

    # Region boundary: `all_procs` is rebuilt once per region, so a change of
    # identity is the cheapest available "a new region started" signal.
    if sched.region_procs !== all_procs
        sched.region_procs = all_procs
        empty!(sched.sig_time_cache)
        # `bias` is a process-global `Ref` that every rank must agree on --
        # a rank-local override changes the *decision*, not just a
        # measurement, and diverging placement under uniform execution
        # deadlocks rather than misbehaving visibly. Checked here, once per
        # region, rather than per task: `check_uniform` is a collective when
        # `CHECK_UNIFORMITY[]` is on. Same reasoning as `RoundRobinScheduler`.
        @check_uniform(bias)
        # Renormalize the clock so the earliest-free processor sits at 0.
        # Without this, `proc_completions` accumulates across every region a
        # long-lived scheduler ever plans: the absolute values are meaningless
        # (only differences drive placement), they drift further from reality
        # every region (under the default `sync=true` every processor really
        # *is* idle at a region boundary), and they grow without bound.
        # Subtracting the minimum preserves relative load while pinning the
        # scale, and is rank-uniform because every rank holds the same values.
        if !isempty(sched.proc_completions)
            base = minimum(values(sched.proc_completions))
            if base > 0
                # `collect(keys(...))` rather than iterating the `Dict`
                # directly: assigning to an existing key doesn't rehash today,
                # but "mutate while iterating" is not a contract Julia's `Dict`
                # offers, and this runs once per region, not per task.
                for proc in collect(keys(sched.proc_completions))
                    sched.proc_completions[proc] -= base
                end
            end
        end
        if length(sched.task_completions) > DATADEPS_ULTRA_COMPLETION_CAP
            empty!(sched.task_completions)
            empty!(sched.assignments)
        end
    end

    # N.B. Under uniform (SPMD/MPI) execution every task is costed at the
    # placeholder instead of its measured runtime. `signature_time_cost` is
    # filled from each rank's *own* completed tasks, so it is the one input
    # here that genuinely differs per rank -- `datadeps_tracked_args` already
    # returns nothing under uniform execution, and every remaining tie-break
    # below is positional. A rank-dependent `task_time` would place tasks
    # differently on different ranks, which desyncs the tag sequence and
    # deadlocks. Flattening it leaves a deterministic least-loaded-processor
    # policy: less informed than the measured version, but rank-uniform, which
    # is the difference between "suboptimal" and "hangs".
    task_time = uniform_execution() ? DATADEPS_UNKNOWN_TASK_TIME :
                                      ultra_signature_time!(sched, spec)

    # Earliest this task could start, given what it already depends on. See the
    # "What it cannot see" note above for why this is a lower bound.
    # N.B. `max` over a `Set` is order-independent, so this stays rank-uniform
    # despite `Set` iteration order not being.
    ready = UInt64(0)
    deps = spec.options.syncdeps
    if deps !== nothing
        for dep in deps
            id = dep.id
            id === nothing && continue
            t = get(sched.task_completions, id.id, nothing)
            t === nothing && continue
            ready = max(ready, t)
        end
    end

    # Bytes that would *not* have to move, per candidate space, plus the total
    # so "would have to move" is a subtraction rather than a second scan.
    # `bias == 0` skips this entirely (a hard bypass, not a zero-weighted
    # term), which is what makes bias 0 a true no-op A/B control;
    # `datadeps_tracked_args` additionally returns nothing at all under uniform
    # execution, because its weights come from `datasize`, which is not
    # rank-uniform by design.
    resident = EMPTY_SPACE_WEIGHTS
    total_bytes = 0.0
    if bias > 0
        tracked = datadeps_tracked_args(state, spec)
        if !isempty(tracked)
            resident = Dict{MemorySpace,Float64}()
            for (arg_w, w) in tracked
                total_bytes += w
                for space in state.arg_current[arg_w]
                    resident[space] = get(resident, space, 0.0) + w
                end
            end
        end
    end

    # Pick the earliest-finishing scope-compatible processor. Scanned in
    # `all_procs` order with a strict `<`, so ties go to the earliest candidate
    # in that (rank-uniform, `select_processors_uniform!`-ordered) list rather
    # than to whatever a `Dict`/`Set` happened to yield first -- the original
    # used `findmin` over a `Dict` and then `rand` among the winning space's
    # processors, both of which pick differently on different ranks and so
    # desync tags under MPI.
    our_proc = nothing
    our_space = nothing
    best_finish = typemax(UInt64)
    # Nanoseconds per byte that has to move, with `bias` folded in; the same
    # for every candidate, so hoisted out of the scan.
    ns_per_byte = bias * DATADEPS_TRANSFER_NS_PER_BYTE
    for proc in all_procs
        proc_in_scope(proc, task_scope) || continue
        space = only(memory_spaces(proc))
        move_time = if total_bytes > 0
            missing_bytes = max(0.0, total_bytes - get(resident, space, 0.0))
            round(UInt64, missing_bytes * ns_per_byte)
        else
            UInt64(0)
        end
        start = max(get(sched.proc_completions, proc, UInt64(0)), ready)
        finish = start + move_time + task_time
        if finish < best_finish
            best_finish = finish
            our_proc = proc
            our_space = space
        end
    end
    if our_proc === nothing
        throw(Sch.SchedulingException("Scopes are not compatible: $(all_scope), $(task_scope)"))
    end

    sched.proc_completions[our_proc] = best_finish
    sched.task_completions[task.uid] = best_finish
    sched.assignments[task.uid] = our_space

    return our_proc
end
