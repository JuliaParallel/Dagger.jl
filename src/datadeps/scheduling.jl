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

struct NaiveScheduler <: DataDepsScheduler end
Base.similar(::NaiveScheduler) = NaiveScheduler()
function datadeps_schedule_task(sched::NaiveScheduler, state::DataDepsState, all_procs, all_scope, task_scope, spec::DTaskSpec, task::DTask)
    # Prefer the chunk that reflects where an argument's data actually is over
    # `tochunk(value(arg))`, which is always the *origin* chunk. Under a
    # region barrier the origin was always correct (every argument starts each
    # region there), but once `state` persists across regions (see PLAN.md,
    # phases 3+) origin can be stale by the time this task is scheduled, and
    # costing the origin instead of the real location would manufacture
    # exactly the cross-space movement this project exists to remove.
    #
    # `bias == 0` skips this and reproduces the original `tochunk`-only
    # behavior exactly, same rationale as `RoundRobinScheduler`. `check_uniform`
    # guards against `bias` disagreeing across ranks (see the N.B. on
    # `RoundRobinScheduler`); unlike there, `NaiveScheduler` has no per-region
    # state to cache the check against, so it's paid every task -- cheap in
    # practice, since it degrades to a hash comparison unless
    # `CHECK_UNIFORMITY[]` is on.
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
    check_uniform(bias)
    raw_args = Base.mapany(spec.fargs) do arg
        v = value(arg)
        chunk = bias > 0 ? something(datadeps_current_chunk(state, v), tochunk(v)) : tochunk(v)
        return Argument(ArgPosition(arg.pos), chunk)
    end
    our_proc = remotecall_fetch(1, all_procs, raw_args) do all_procs, raw_args
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

struct UltraScheduler <: DataDepsScheduler
    task_to_spec::Dict{DTask,DTaskSpec}
    assignments::Dict{DTask,MemorySpace}
    dependencies::Dict{DTask,Set{DTask}}
    task_completions::Dict{DTask,UInt64}
    space_completions::Dict{MemorySpace,UInt64}
    capacities::Dict{MemorySpace,Int}

    function UltraScheduler()
        return new(Dict{DTask,DTaskSpec}(),
                    Dict{DTask,MemorySpace}(),
                    Dict{DTask,Set{DTask}}(),
                    Dict{DTask,UInt64}(),
                    Dict{MemorySpace,UInt64}(),
                    Dict{MemorySpace,Int}())
    end
end
Base.similar(::UltraScheduler) = UltraScheduler()
function datadeps_schedule_task(sched::UltraScheduler, state::DataDepsState, all_procs, all_scope, task_scope, spec::DTaskSpec, task::DTask)
    args = Base.mapany(spec.fargs) do arg
        pos, data = arg
        data, _ = unwrap_inout(data)
        if data isa DTask
            data = fetch(data; move_value=false, unwrap=false)
        end
        return pos => tochunk(data)
    end
    f_chunk = tochunk(value(spec.fargs[1]))
    task_time = remotecall_fetch(1, f_chunk, args) do f, args
        Sch.init_eager()
        sch_state = Sch.EAGER_STATE[]
        return @lock sch_state.lock begin
            sig = Sch.signature(sch_state, f, args)
            return lock(sch_state.signature_time_cost) do stc; get(stc, sig, 1000^3); end
        end
    end

    # FIXME: Copy deps are computed eagerly
    deps = @something(spec.options.syncdeps, Set{ThunkSyncdep}())

    # Find latest time-to-completion of all syncdeps
    deps_completed = UInt64(0)
    for dep in deps
        haskey(sched.task_completions, dep) || continue # copy deps aren't recorded
        deps_completed = max(deps_completed, sched.task_completions[dep])
    end

    # Find latest time-to-completion of each memory space
    # FIXME: Figure out space completions based on optimal packing
    # N.B. `exec_spaces` mirrors the computation `distribute_tasks!` does for
    # itself (`queue.jl`) -- it isn't threaded through to the scheduler, so we
    # rebuild it from `all_procs` here.
    exec_spaces = unique(vcat(map(proc->collect(memory_spaces(proc)), all_procs)...))
    spaces_completed = Dict{MemorySpace,UInt64}()
    for space in exec_spaces
        completed = UInt64(0)
        for (task, other_space) in sched.assignments
            space == other_space || continue
            completed = max(completed, sched.task_completions[task])
        end
        spaces_completed[space] = completed
    end

    # Choose the earliest-available memory space and processor
    # N.B. `our_space`/`our_proc` must be pre-declared `local` here: a `while`
    # loop opens its own scope, so a name first assigned *inside* the loop
    # (as both are, via the `findmin` destructure and `rand` below) is
    # otherwise invisible once the loop breaks -- this bit pre-existing code
    # that used `our_space`/`our_proc` below without ever having escaped the
    # loop.
    local our_space_completed, our_space, our_proc
    while true
        our_space_completed, our_space = findmin(spaces_completed)
        our_space_procs = filter(proc->proc in all_procs, processors(our_space))
        if isempty(our_space_procs)
            delete!(spaces_completed, our_space)
            continue
        end
        our_proc = rand(our_space_procs)
        break
    end

    # Bytes not already resident at `our_space` have to move there before the
    # task can run. `Sch.DEFAULT_TRANSFER_RATE` is the same bytes/time-unit
    # assumption `estimate_task_costs!` uses for cross-worker chunk transfers
    # elsewhere in the scheduler, reused here instead of inventing a second
    # bandwidth constant. `bias == 0` keeps `move_time` at zero, matching the
    # pre-locality behavior (see DATADEPS_LOCALITY_BIAS). `check_uniform`
    # guards `bias` itself against cross-rank disagreement (see the N.B. on
    # `RoundRobinScheduler`): `move_time` feeds `task_completions`, which later
    # tasks' `findmin(spaces_completed)` reads, so a rank-local `bias` would
    # desync *future* placement decisions, not just this one.
    bias = DATADEPS_LOCALITY_BIAS[]
    check_uniform(bias)
    move_time = if bias > 0
        tracked = datadeps_tracked_args(state, spec)
        missing_bytes = isempty(tracked) ? 0.0 :
            sum(our_space in state.arg_current[arg_w] ? 0.0 : w for (arg_w, w) in tracked)
        UInt64(ceil(bias * missing_bytes / Sch.DEFAULT_TRANSFER_RATE))
    else
        UInt64(0)
    end

    sched.task_to_spec[task] = spec
    sched.assignments[task] = our_space
    sched.task_completions[task] = our_space_completed + move_time + task_time

    return our_proc
end
