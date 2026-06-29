module EnzymeExt

# =============================================================================
# Reverse-mode AD for Dagger via an intercepting task queue ("Option B")
# =============================================================================
#
# Design summary
# --------------
# Instead of writing per-function Enzyme rules (`sum`, `map`, `lu`, ...), we
# treat Dagger's *task graph itself* as the AD tape and run reverse-mode at the
# queue layer. Enzyme is demoted to a per-node local VJP engine: it only ever
# differentiates a single task's payload function on already-materialized,
# concrete arguments, on one worker -- never the scheduler, never any
# `Any`-typed plumbing.
#
# The integration point is an `AbstractTaskQueue` (cf. `src/queue.jl`,
# `src/datadeps/queue.jl`). Our `EnzymeTaskQueue` is a *transparent
# record-and-forward layer*: every `Dagger.@spawn` inside the differentiated
# region is recorded onto the tape and immediately forwarded to the queue below
# us for normal (eager) execution. Because we forward immediately rather than
# buffering, ordinary code that *fetches* mid-region -- e.g. `sum(::DArray)`
# which ends in `only(collect(...))` -- works unchanged: the forward pass really
# runs while we tape it. This is the key difference from `DataDepsTaskQueue`,
# which must buffer a whole region for aliasing analysis.
#
# Reading access metadata from task options (the core enabler)
# ------------------------------------------------------------
# `Dagger.spawn` now captures every `In`/`Out`/`InOut`/`Deps` annotation into
# `spec.options.arg_accesses` (see `Dagger.ArgumentAccess`) and strips the
# wrappers from the arguments. This means the data-access metadata is available
# to *any* queue in the stack, regardless of position -- crucially, even when an
# algorithm wraps its own `spawn_datadeps` region whose `DataDepsTaskQueue` sits
# *above* us and has already rewritten/unwrapped the task graph by the time the
# tasks reach us. We therefore read access metadata exclusively from options
# (`Dagger.arg_deps_from_options`), never from argument wrappers. This is what
# lets `spawn_enzyme` differentiate through both functional code and
# Datadeps-based algorithms (tiled LU/Cholesky/QR) without those algorithms
# needing to know anything about AD.
#
# Two kinds of graph edges, two reverse paths (both over the same tape):
#
#   * FUNCTIONAL tasks  -- args are plain values or upstream `DTask`s (the
#     `sum`/`map` case). Edges are explicit data dependencies. Classic dataflow
#     tape: each backward task accumulates `argᵢ̄ += VJP_f(args, ȳ)` into the
#     adjoint of whatever produced `argᵢ`. The output adjoint is seeded at the
#     tape's *sinks* (tasks whose result is not consumed by another taped task).
#
#   * MUTATING / Datadeps tasks  -- args carry `Out`/`InOut` in their recorded
#     access metadata. Edges come from memory aliasing. Adjoints live on the
#     *buffers* (keyed by buffer identity), and the reverse rule is Enzyme's
#     reverse VJP of the kernel over `Duplicated(primal, shadow)` arguments,
#     emitted inside a nested `spawn_datadeps` so Datadeps derives the backward
#     syncdeps for us. Primal inputs are snapshotted in the forward pass.
#
# Primal availability at backward time:
#   Functional tasks don't mutate their inputs, so their primals are still live
#   (we re-fetch them). Mutating tasks overwrite buffers, so we snapshot the
#   pre-task input state into the forward pass (ordered via syncdeps).

using Dagger
using Enzyme
import Enzyme: EnzymeRules
import Dagger: DTaskPair, DTaskSpec, AbstractTaskQueue, enqueue!, Options
import Dagger: value, ispositional, iskw, pos_kw, raw_position, istask
import Dagger: arg_deps_from_options, arg_alias_from_options
import DifferentiationInterface as DI
using LinearAlgebra: LinearAlgebra, UnitLowerTriangular, UpperTriangular, tril, triu, LU

const DArrayLike = Dagger.DArray

# -----------------------------------------------------------------------------
# Custom Enzyme rule: reverse-mode VJP of the unpivoted LU panel kernel
# -----------------------------------------------------------------------------
# Dagger's tiled `lu(A, NoPivot())` factors each diagonal block with
# `LinearAlgebra.generic_lufact!` and updates the rest with BLAS `trsm!`/`gemm!`.
# Enzyme differentiates the BLAS kernels natively, but cannot differentiate
# `generic_lufact!` (it hits an internal `invoke` during type analysis). We
# supply the standard LU pullback (Seth Axen / ChainRules) for the square,
# in-place, no-pivot case so the whole tiled factorization composes:
#
#   F = factored block (L unit-lower + U upper, stored in place)
#   F̄ = tril₋(Lᴴ L̄) + triu(Ū Uᴴ)        (L̄ = tril₋(F̄ₒᵤₜ), Ū = triu(F̄ₒᵤₜ))
#   Ā = L⁻ᴴ F̄ U⁻ᴴ
#
# The factor adjoint arrives in the shadow `A.dval` (the factors alias `A`); we
# overwrite it in place with the input-block adjoint `Ā`.

function EnzymeRules.augmented_primal(config::EnzymeRules.RevConfig,
        func::Const{typeof(LinearAlgebra.generic_lufact!)},
        ::Type{RT}, A::Duplicated, pivot::Const{LinearAlgebra.NoPivot}; kwargs...) where {RT}
    res = func.val(A.val, pivot.val; kwargs...)
    tape = copy(A.val)   # the factored block (L/U), needed by the reverse pass
    primal = EnzymeRules.needs_primal(config) ? res : nothing
    shadow = EnzymeRules.needs_shadow(config) ? LU(A.dval, res.ipiv, res.info) : nothing
    return EnzymeRules.AugmentedReturn(primal, shadow, tape)
end

function EnzymeRules.reverse(config::EnzymeRules.RevConfig,
        func::Const{typeof(LinearAlgebra.generic_lufact!)},
        ::Type{RT}, tape, A::Duplicated, pivot::Const{LinearAlgebra.NoPivot}; kwargs...) where {RT}
    F = tape
    L = UnitLowerTriangular(F)
    U = UpperTriangular(F)
    Abar = A.dval
    Lbar = tril(Abar, -1)
    Ubar = triu(Abar)
    Fbar = tril(L' * Lbar, -1) + triu(Ubar * U')
    Abar .= (L') \ (Fbar / (U'))   # Ā = L⁻ᴴ F̄ U⁻ᴴ
    return (nothing, nothing)
end

# -----------------------------------------------------------------------------
# Shadow construction: `make_zero` for `DArray`
# -----------------------------------------------------------------------------
# Enzyme's reflection-based `make_zero` would recurse into a `DArray`'s
# scheduling internals (chunk refs, subdomains). Override it to produce a
# same-blocking `DArray` of zeros, with the zeroing itself distributed.

_zeros_chunk(::Type{T}, sz) where {T} = zeros(T, sz)

function Enzyme.EnzymeCore.make_zero(::Type{DT}, seen::IdDict, prev::DT,
                                      ::Val{copy_if_inactive}=Val(false)) where {DT<:DArrayLike, copy_if_inactive}
    haskey(seen, prev) && return seen[prev]::DT
    T = eltype(prev)
    cs = Dagger.chunks(prev)
    dcs = Dagger.domainchunks(prev)
    newchunks = similar(cs, Any)
    for i in eachindex(cs)
        newchunks[i] = Dagger.@spawn _zeros_chunk(T, size(dcs[i]))
    end
    z = Dagger.DArray(T, Dagger.domain(prev), dcs, newchunks, prev.partitioning, prev.concat)
    seen[prev] = z
    return z
end

# -----------------------------------------------------------------------------
# The intercepting queue
# -----------------------------------------------------------------------------

"""
    EnzymeTaskQueue <: AbstractTaskQueue

A transparent record-and-forward task queue: it records every spawned task onto
the AD tape (reading access metadata from task options) and immediately forwards
the task to the queue below for normal execution. Constructed and used via
[`spawn_enzyme`](@ref).

Fields:
- `upper_queue`: the queue we forward tasks to for actual execution.
- `seen_tasks`:  the recorded tape, in submission order.
- `adjoints`:    value-identity → adjoint accumulator. The key is the producing
  `DTask`/leaf object (functional path) or the buffer/`Chunk` (mutating path);
  the value is a `DTask` (or concrete array) computing the adjoint. Keyed by
  `objectid` via `IdDict`.
- `primal_snapshots`: per mutating task, the snapshot `DTask`s holding the
  pre-task input state needed by that task's VJP.
- `seed`:        output-adjoint seed (defaults to one-like on the output).
"""
# A per-argument view, with access info read from `spec.options.arg_accesses`.
struct ArgView
    pos::Any          # ArgPosition
    val::Any          # the physical primal value (buffer/DTask/scalar) used for
                      # snapshots and the local VJP computation
    key::Any          # the *logical* buffer identity used as the adjoint-map
                      # key (from `spec.options.arg_aliases` when present, so
                      # adjoints chain across datadeps regions; else `== val`)
    readdep::Bool
    writedep::Bool
    positional::Bool
    istask::Bool      # true if `val` is an upstream DTask (a dataflow edge)
end

# A recorded tape entry. We capture the decoded function and argument views at
# record time, *before* forwarding the task downstream, because submission
# (`eager_submit_internal!`) mutates `spec.fargs` in place (rewriting `DTask`
# args to `ThunkID`s and `Chunk`s to `WeakChunk`s). Capturing up front preserves
# the original value identities we use as adjoint keys.
struct TapeEntry
    f::Any
    views::Vector{ArgView}
    task::Dagger.DTask
    mutating::Bool
end

mutable struct EnzymeTaskQueue <: AbstractTaskQueue
    upper_queue::AbstractTaskQueue
    seen_tasks::Vector{TapeEntry}
    adjoints::IdDict{Any,Any}
    primal_snapshots::IdDict{Dagger.DTask,Vector{Any}}
    seed::Any
end
EnzymeTaskQueue(upper, seed) =
    EnzymeTaskQueue(upper, TapeEntry[], IdDict{Any,Any}(), IdDict{Dagger.DTask,Vector{Any}}(), seed)

# Record onto the tape, inserting primal snapshots for mutating tasks, then
# forward to the executor so the primal actually runs (eagerly).
function enqueue!(q::EnzymeTaskQueue, pair::DTaskPair)
    _record!(q, pair)
    enqueue!(q.upper_queue, pair)
end
function enqueue!(q::EnzymeTaskQueue, pairs::Vector{DTaskPair})
    for pair in pairs
        _record!(q, pair)
    end
    enqueue!(q.upper_queue, pairs)
end

function _record!(q::EnzymeTaskQueue, pair::DTaskPair)
    spec = pair.spec
    f, views = decode_task(spec)
    mutating = is_mutating(spec)
    push!(q.seen_tasks, TapeEntry(f, views, pair.task, mutating))
    if mutating
        # Snapshot pre-task input state so the VJP has correct inputs even after
        # the forward overwrites the live buffers. Snapshots are ordered via
        # syncdeps (run after this task's predecessors, before this task).
        q.primal_snapshots[pair.task] = snapshot_inputs!(q, pair, views)
    end
    return
end

# -----------------------------------------------------------------------------
# Region entry point
# -----------------------------------------------------------------------------

"""
    spawn_enzyme(f; seed=nothing) -> (value, adjoints)

Run `f` as a differentiated Dagger region. Unlike `spawn_datadeps`, the region
is *not* buffered: tasks execute eagerly as they are spawned, so code that
fetches intermediate results mid-region (e.g. reductions ending in `collect`)
works normally. Returns the primal `value` of `f` and an `IdDict` mapping each
leaf input (and every taped value) to its adjoint (as a `DTask`/shadow).
"""
function spawn_enzyme(f; seed=nothing)
    upper = Dagger.get_options(:task_queue, Dagger.DefaultTaskQueue())
    q = EnzymeTaskQueue(upper, seed)

    # Forward pass: runs eagerly through `q`, which tapes every task.
    primal = Dagger.with_options(f; task_queue=q)

    # Build the adjoint graph from the tape.
    build_reverse!(q, primal)

    return primal, q.adjoints
end

# -----------------------------------------------------------------------------
# Tape decoding helpers (all access metadata comes from task options)
# -----------------------------------------------------------------------------

# Decode a task spec into (f, Vector{ArgView}). Reads In/Out/InOut/Deps access
# info from the captured task options rather than from argument wrappers, so it
# works no matter where in the queue stack the task was rewritten.
function decode_task(spec::DTaskSpec)
    fargs = spec.fargs
    f = value(first(fargs))
    views = ArgView[]
    for idx in 2:length(fargs)
        arg = fargs[idx]
        val = value(arg)
        deps = arg_deps_from_options(spec.options, arg.pos)
        rd = any(d -> d[2], deps)
        wr = any(d -> d[3], deps)
        key = arg_alias_from_options(spec.options, arg.pos, val)
        push!(views, ArgView(arg.pos, val, key, rd, wr, ispositional(arg), istask(val)))
    end
    return f, views
end

# A task is "mutating" (aliasing path) iff any recorded access carries a write.
function is_mutating(spec::DTaskSpec)
    accesses = spec.options.arg_accesses
    accesses === nothing && return false
    for acc in accesses
        for d in acc.deps
            d[3] && return true
        end
    end
    return false
end

# -----------------------------------------------------------------------------
# Activity inference (type-based to avoid fetching large data eagerly)
# -----------------------------------------------------------------------------

is_active_type(::Type{Union{}}) = false
is_active_type(::Type{<:AbstractFloat}) = true
is_active_type(::Type{<:AbstractArray{<:AbstractFloat}}) = true
is_active_type(::Type{<:DArrayLike}) = true
is_active_type(::Type) = false
is_active_type(::Nothing) = false

_value_type(x::Dagger.DTask) = Dagger.chunktype(x)
_value_type(x::Dagger.Chunk) = Dagger.chunktype(x)
_value_type(x) = typeof(x)

is_active_concrete(::AbstractFloat) = true
is_active_concrete(::AbstractArray{<:AbstractFloat}) = true
is_active_concrete(::DArrayLike) = true
is_active_concrete(_) = false

# Activity inference. Prefer the static (return) type, but Dagger task return
# types are frequently uninferred (`Union{}`/`Any`); in that case fall back to
# the materialized value's concrete type. The forward pass has already run by
# reverse-build time, so fetching is cheap (results are cached).
function is_active_value(x)
    T = _value_type(x)
    if T === Union{} || T === Any
        return is_active_concrete(_materialize(x))
    end
    return is_active_type(T)
end

# -----------------------------------------------------------------------------
# Adjoint map
# -----------------------------------------------------------------------------

# Canonical adjoint-map key. Functional edges are keyed by their producing
# `DTask`, while mutating edges (and input leaves) are keyed by the *logical*
# buffer datadeps tracks -- the raw `Chunk` behind a `DTask`. Canonicalizing a
# `DTask` to that same raw `Chunk` makes the two paths share one identity, so a
# functional reduction layered on a mutating output (e.g. `sum(A*B)`,
# `sum(lu(x).factors)`) connects its adjoint into the producing buffers.
_akey(x) = x
_akey(t::Dagger.DTask) = Base.istaskstarted(t) ? fetch(t; raw=true) : t

# Accumulate `contrib` into the adjoint slot of `key` (`+=`), allocating on
# first contribution (handles fan-out: a value used by multiple tasks sums).
function _accumulate!(q::EnzymeTaskQueue, key, contrib)
    key = _akey(key)
    if haskey(q.adjoints, key)
        prev = q.adjoints[key]
        q.adjoints[key] = Dagger.@spawn _add(prev, contrib)
    else
        q.adjoints[key] = contrib
    end
    return
end
_add(a, b) = a .+ b

# Materialize a value (fetch DTask/Chunk) -- used only for small things like
# seeds and activity-by-value fallbacks.
_materialize(x) = x
_materialize(t::Dagger.DTask) = fetch(t)
_materialize(c::Dagger.Chunk) = fetch(c)

# Output-adjoint seeding.
_one_like(x::AbstractFloat) = one(x)
_one_like(x::AbstractArray) = fill!(similar(x, float(eltype(x))), one(float(eltype(x))))
_one_like(x) = one(x)
_seed_like(::Nothing, out) = _one_like(out)
_seed_like(seed::Number, out::Number) = oftype(float(out), seed)
_seed_like(seed::Number, out::AbstractArray) = fill!(similar(out, float(eltype(out))), seed)
_seed_like(seed, out) = seed

# -----------------------------------------------------------------------------
# Reverse-pass construction
# -----------------------------------------------------------------------------

function build_reverse!(q::EnzymeTaskQueue, primal)
    seed_outputs!(q, primal)

    # Functional VJPs use plain spawns; mutating VJPs are emitted inside a
    # Datadeps region so Datadeps computes their backward syncdeps for us.
    has_mutating = any(entry -> entry.mutating, q.seen_tasks)
    if has_mutating
        Dagger.spawn_datadeps() do
            _reverse_walk!(q)
        end
    else
        _reverse_walk!(q)
    end
    return q.adjoints
end

function _reverse_walk!(q::EnzymeTaskQueue)
    for entry in Iterators.reverse(q.seen_tasks)
        if entry.mutating
            emit_mutating_vjp!(q, entry)
        else
            ybar = get(q.adjoints, _akey(entry.task), nothing)
            ybar === nothing && continue   # zero adjoint flowing back: skip
            emit_functional_vjp!(q, entry, ybar)
        end
    end
end

# The region's output buffers, derived from `f`'s return value: a `DArray`'s
# chunks, or the factor `DArray`'s chunks of a factorization (LU/Cholesky/QR/
# ...). Scalars/other returns contribute none (their adjoint is seeded on the
# functional sink instead).
_output_buffers(p::DArrayLike) = Dagger.chunks(p)
function _output_buffers(p)
    if hasproperty(p, :factors) && getfield(p, :factors) isa DArrayLike
        return Dagger.chunks(getfield(p, :factors))
    end
    return ()
end

# Seed output adjoints. Two kinds of outputs:
#  * functional sinks -- taped functional tasks whose result `DTask` is not
#    consumed by any other taped task (covers scalar-returning `f`, e.g. `sum`);
#  * primal output buffers -- when `f` returns a `DArray`/factorization, seed
#    the underlying buffers directly (covers e.g. `lu(x, NoPivot())`).
# Buffer seeds are keyed by the same canonical identity (`_akey`) as the
# mutating buffers, so the reverse walk picks them up.
function seed_outputs!(q::EnzymeTaskQueue, primal)
    # Primal output buffers (DArray / factorization results).
    for c in _output_buffers(primal)
        k = _akey(c)
        haskey(q.adjoints, k) && continue
        q.adjoints[k] = Dagger.@spawn seed_buffer(c, q.seed)
    end

    # Functional sinks.
    used = Base.IdSet{Any}()
    for entry in q.seen_tasks
        for v in entry.views
            if v.val isa Dagger.DTask
                push!(used, _akey(v.val))
            end
        end
    end
    for entry in q.seen_tasks
        entry.mutating && continue
        k = _akey(entry.task)
        if !(k in used) && !haskey(q.adjoints, k)
            out = _materialize(entry.task)
            # Only genuine numeric outputs are seedable. Tasks that return
            # `nothing` or other non-numeric values (e.g. internal copy/transport
            # ops surfaced as dead-end sinks, especially across workers) carry no
            # differentiable output and must not be seeded.
            _is_seedable(out) || continue
            q.adjoints[k] = _seed_like(q.seed, out)
        end
    end
    return
end
seed_buffer(val, seed) = _seed_like(seed, val)
_is_seedable(::Any) = false
_is_seedable(::Number) = true
_is_seedable(::AbstractArray{<:Number}) = true

# --- Functional / dataflow path ----------------------------------------------
#
# For `y = f(a, b, ...)` with no mutation: accumulate `argᵢ̄ += VJP_f(args, ȳ)`
# into each active input's adjoint slot. `ȳ` is the adjoint of this task's
# output. A single backward task runs the local Enzyme reverse VJP over all
# active inputs at once and returns their contributions.
function emit_functional_vjp!(q::EnzymeTaskQueue, entry::TapeEntry, ybar)
    f = entry.f

    posvals = Any[]
    poskeys = Any[]
    kwpairs = Pair{Symbol,Any}[]
    for v in entry.views
        if v.positional
            push!(posvals, v.val)
            push!(poskeys, v.key)
        else
            push!(kwpairs, pos_kw(v.pos) => v.val)
        end
    end
    kw = NamedTuple(kwpairs)

    active = Tuple(is_active_value(v) for v in posvals)
    any(active) || return

    # One backward task computes all input contributions; Dagger fetches DTask
    # inputs to concrete values for us.
    contrib = Dagger.@spawn functional_vjp_kernel(f, kw, active, ybar, posvals...)
    for i in 1:length(posvals)
        active[i] || continue
        ci = Dagger.@spawn _get_contrib(contrib, i)
        _accumulate!(q, poskeys[i], ci)
    end
    return
end
_get_contrib(t, i) = t[i]

# Local node: reverse-mode VJP of a functional kernel `f` over its active inputs.
# Handles scalar- and array-returning `f`, and scalar (`Active`) or array
# (`Duplicated`) active inputs. Returns an n-tuple of contributions (or
# `nothing` for inactive args), already scaled by the output adjoint `ybar`.
const REVERSE_RA = Enzyme.set_runtime_activity(Enzyme.Reverse)

function functional_vjp_kernel(f, kw::NamedTuple, active::Tuple, ybar, prim_args::Vararg{Any})
    n = length(prim_args)
    g = isempty(kw) ? f : ((a...) -> f(a...; kw...))
    annots = Vector{Any}(undef, n)
    shadows = Vector{Any}(undef, n)
    for i in 1:n
        a = prim_args[i]
        if active[i]
            if a isa Number
                annots[i] = Enzyme.Active(a)
                shadows[i] = nothing
            else
                s = Enzyme.make_zero(a)
                shadows[i] = s
                annots[i] = Enzyme.Duplicated(a, s)
            end
        else
            annots[i] = Enzyme.Const(a)
            shadows[i] = nothing
        end
    end

    try
        if ybar isa Number
            ret = Enzyme.autodiff(REVERSE_RA, Enzyme.Const(g), Enzyme.Active, annots...)[1]
            return ntuple(n) do i
                active[i] || return nothing
                prim_args[i] isa Number ? ret[i] * ybar : shadows[i] .* ybar
            end
        else
            y = g(prim_args...)
            ybuf = y isa AbstractArray ? copy(y) : [y]
            ybar_buf = ybar isa AbstractArray ? copy(ybar) : fill(convert(eltype(ybuf), ybar), size(ybuf))
            ginplace! = (out, a...) -> (out .= g(a...); nothing)
            ret = Enzyme.autodiff(REVERSE_RA, Enzyme.Const(ginplace!), Enzyme.Const,
                                  Enzyme.Duplicated(ybuf, ybar_buf), annots...)[1]
            return ntuple(n) do i
                active[i] || return nothing
                # `ret` is offset by 1 because `ybuf` is the first arg of `ginplace!`.
                prim_args[i] isa Number ? ret[i+1] : shadows[i]
            end
        end
    catch err
        # Some tasks are pure data *transport* -- `collect`/gather/`fetch`/`copy`
        # -- which Enzyme cannot (and need not) differentiate: they relocate or
        # reshape values without arithmetic, so their VJP is the identity
        # (scatter the output adjoint back to the inputs, matching shapes). This
        # is what makes reductions ending in `collect` (the mid-region-fetch
        # case) differentiable through `spawn_enzyme`.
        if _is_enzyme_compile_error(err)
            return _transport_passthrough(active, ybar, prim_args)
        end
        rethrow(err)
    end
end

# Heuristic: did Enzyme fail to compile/differentiate this node? (As opposed to
# a genuine runtime error in the user's kernel, which we must surface.)
function _is_enzyme_compile_error(err)
    T = typeof(err)
    m = parentmodule(T)
    while m !== parentmodule(m)
        m === Enzyme && return true
        m = parentmodule(m)
    end
    return m === Enzyme
end

# Structural identity VJP for a value-preserving transport node.
function _transport_passthrough(active::Tuple, ybar, prim_args::Tuple)
    n = length(prim_args)
    return ntuple(n) do i
        active[i] || return nothing
        a = prim_args[i]
        if a isa Number
            return ybar isa Number ? ybar : sum(ybar)
        else
            yb = ybar isa AbstractArray ? ybar : fill(ybar, length(a))
            return length(yb) == length(a) ? reshape(collect(float.(yb)), size(a)) :
                   fill(convert(float(eltype(a)), sum(yb) / length(a)), size(a))
        end
    end
end

# --- Mutating / aliasing path ------------------------------------------------
#
# Adjoints live on the buffers (keyed by buffer/Chunk identity). We seed the
# final state of written buffers and, walking the tape in reverse, run Enzyme's
# reverse VJP of each kernel over `Duplicated(primal_snapshot, shadow)` args,
# inside the `spawn_datadeps` region opened by `build_reverse!`.

# Insert copy tasks capturing the pre-task input state of each active read/inout
# arg, ordered (via syncdeps) after this task's predecessors and before the task
# itself. Routed straight to the executor (not back through `q`).
function snapshot_inputs!(q::EnzymeTaskQueue, pair::DTaskPair, views::Vector{ArgView})
    spec = pair.spec
    pred_syncdeps = spec.options.syncdeps === nothing ? Set{Dagger.ThunkSyncdep}() :
                    copy(spec.options.syncdeps)
    scope = @something(spec.options.compute_scope, spec.options.scope, Dagger.DefaultScope())
    snaps = Any[]
    new_syncdeps = spec.options.syncdeps === nothing ? Set{Dagger.ThunkSyncdep}() : spec.options.syncdeps
    Dagger.with_options(; task_queue=q.upper_queue) do
        for v in views
            if v.readdep && is_active_value(v.val)
                # Snapshot runs after this task's predecessors (so it captures
                # the correct pre-task input state) and before this task.
                snap = Dagger.@spawn scope=scope syncdeps=copy(pred_syncdeps) copy(v.val)
                push!(snaps, snap)
                push!(new_syncdeps, Dagger.ThunkSyncdep(snap))
            else
                push!(snaps, v.val)
            end
        end
    end
    spec.options.syncdeps = new_syncdeps
    return snaps
end

# Get-or-allocate the adjoint shadow buffer for a primal buffer, keyed by its
# logical identity `key` but shaped from the physical value `val`.
function get_buffer_adjoint!(q::EnzymeTaskQueue, key, val)
    return get!(q.adjoints, _akey(key)) do
        Dagger.@spawn make_zero_buffer(val)
    end
end
make_zero_buffer(x) = Enzyme.make_zero(x)

function emit_mutating_vjp!(q::EnzymeTaskQueue, entry::TapeEntry)
    f = entry.f
    views = entry.views
    snaps = get(q.primal_snapshots, entry.task, nothing)

    # Keyword arguments (e.g. `check` for `generic_lufact!`) are passed through
    # as constants; only positional args participate in the VJP. We keep them as
    # kwargs so custom Enzyme rules that dispatch on the kwarg form still fire.
    kwpairs = Pair{Symbol,Any}[]

    # Adjoints flow into each buffer's shadow. Region-output buffers were
    # pre-seeded in `seed_outputs!`; buffers consumed by a downstream (functional
    # or mutating) task already carry that task's accumulated contribution; the
    # rest start at zero. The kernel's reverse VJP reads/updates these shadows.
    access = Symbol[]
    primals = Any[]
    shadows = Any[]
    for (i, v) in enumerate(views)
        if !v.positional
            push!(kwpairs, pos_kw(v.pos) => v.val)
            continue
        end
        primal_i = snaps === nothing ? v.val : snaps[i]
        push!(primals, primal_i)
        if is_active_value(v.val)
            push!(shadows, get_buffer_adjoint!(q, v.key, v.val))
            push!(access, v.writedep ? (v.readdep ? :inout : :out) : :in)
        else
            push!(shadows, nothing)
            push!(access, :const)
        end
    end
    kw = NamedTuple(kwpairs)

    bw_args = Any[]
    for i in 1:length(primals)
        push!(bw_args, Dagger.In(primals[i]))
        if access[i] !== :const
            push!(bw_args, Dagger.InOut(shadows[i]))
        end
    end
    Dagger.@spawn enzyme_vjp_mutating!(f, kw, Tuple(access), bw_args...)
    return
end

# Local node: reverse-mode VJP of a mutating kernel `f` over concrete buffers.
# `access` tags each original arg as :const / :in / :out / :inout. Layout of
# `args`: for each original arg, either (primal) for :const, or (primal, shadow)
# for active args.
function enzyme_vjp_mutating!(f, kw::NamedTuple, access::Tuple, args...)
    annotated = Any[]
    k = 1
    for a in access
        if a === :const
            push!(annotated, Enzyme.Const(args[k])); k += 1
        else
            primal = args[k]; shadow = args[k+1]; k += 2
            push!(annotated, Enzyme.Duplicated(primal, shadow))
        end
    end
    g = isempty(kw) ? f : ((a...) -> f(a...; kw...))
    Enzyme.autodiff(REVERSE_RA, Enzyme.Const(g), Enzyme.Const, annotated...)
    return nothing
end

# -----------------------------------------------------------------------------
# Gradient assembly + DifferentiationInterface glue
# -----------------------------------------------------------------------------

# Build a gradient `DArray` matching `A`'s domain/blocking, reading each chunk's
# adjoint out of the `adjoints` map (zero where no adjoint flowed back).
# Look up the adjoint of an input chunk. A `DArray`'s chunks may be stored as
# `DTask`s (lazy) while the taped kernels saw the *materialized* `Chunk` (a
# nested `spawn_datadeps`, e.g. the copy inside `_lucopy`, resolves `DTask`
# args to `Chunk`s before our queue records them). Normalize by also trying the
# raw `Chunk` behind a `DTask`.
function _lookup_adjoint(adjoints::IdDict, c)
    haskey(adjoints, c) && return adjoints[c]
    if c isa Dagger.DTask && Base.istaskstarted(c)
        raw = fetch(c; raw=true)
        haskey(adjoints, raw) && return adjoints[raw]
    end
    return nothing
end

function gradient_darray(A::DArrayLike, adjoints::IdDict)
    cs = Dagger.chunks(A)
    dcs = Dagger.domainchunks(A)
    T = float(eltype(A))
    newchunks = similar(cs, Any)
    for i in eachindex(cs)
        adj = _lookup_adjoint(adjoints, cs[i])
        if adj !== nothing
            newchunks[i] = adj
        else
            newchunks[i] = Dagger.@spawn _zeros_chunk(T, size(dcs[i]))
        end
    end
    return Dagger.DArray(T, Dagger.domain(A), dcs, newchunks, A.partitioning, A.concat)
end

"""
    value_and_gradient(f, AutoEnzyme(), x::DArray) -> (value, gradient::DArray)

Differentiate `f` at the `DArray` `x` using Dagger's task-graph-level
reverse-mode AD. The returned gradient is a `DArray` with the same blocking as
`x`.

When `f` returns a scalar (e.g. a reduction such as `sum`), the gradient is of
that scalar. When `f` returns a `DArray` or factorization (e.g. `x * B` or
`lu(x, NoPivot())`), its outputs are seeded with ones, so the gradient is of
`sum` over those outputs.
"""
function DI.value_and_gradient(f, ::DI.AutoEnzyme, x::DArrayLike)
    primal, adjoints = spawn_enzyme(() -> f(x))
    grad = gradient_darray(x, adjoints)
    return primal, grad
end

end # module EnzymeExt
