# ===========================================================================
# Cost models.
#
# The tape tells us *which* operations are coming. To turn that into a layout
# decision we need `cost(op, layout, sizes) -> seconds` for combinations we may
# never have executed. A pure measurement table cannot answer counterfactuals
# ("what would Cholesky cost with 512-square tiles, given we have only ever run
# it with row blocks?") and the (op x layout x size) space is far too large to
# explore online. So the primary model is analytic and parametric; measurements
# are intended to *correct* it, not replace it.
# ===========================================================================

import LinearAlgebra

"The module these macros expand references into. Captured so `@cost_model` can be used from anywhere."
const TAPES_MODULE = @__MODULE__

"""
    MachineModel

Coarse performance parameters of the current machine. Deliberately crude: the
planner only ever compares layouts against each other, so systematic error in
these constants largely cancels. What matters is that the *ratios* between
compute, communication and per-task overhead are roughly right.
"""
Base.@kwdef mutable struct MachineModel
    "Number of compute processors available."
    nprocs::Int = 1
    "Aggregate achievable FLOP/s across all processors, for Float64."
    flops_per_sec::Float64 = 5.0e10
    "Effective inter-processor bandwidth in bytes/s."
    bandwidth::Float64 = 5.0e9
    "Per-transfer latency in seconds."
    latency::Float64 = 1.0e-5
    """
    Per-task scheduler cost in seconds, **as serialized wall-clock time**, not as
    work to be divided among processors.

    Most of what a Dagger task costs before it runs — Datadeps aliasing
    analysis, dependency-edge construction, queue insertion — happens on the
    submitting thread, so adding processors barely reduces it. Measured on a
    6-thread node in a `spawn_datadeps` region: ~230us/task for the
    three-argument, tile-sharing pattern that blocked linear algebra submits,
    and ~150us/task for trivial tasks holding one private chunk each. Dropping
    to a single thread only raises the latter to ~360us, so six times the
    processors buys a 2.4x reduction: this is far closer to a serial cost than a
    parallel one, and cost models must not divide it by `nprocs`. Doing so was
    why the planner preferred tile counts an order of magnitude too high.

    The default suits a handful of threads on one node; use
    [`calibrate_task_overhead!`](@ref) to measure it.
    """
    task_overhead::Float64 = 2.5e-4
    "Usable memory per processor in bytes; used to reject layouts that will not fit."
    mem_per_proc::Float64 = 8.0e9
    """
    Whether [`calibrate_machine!`](@ref) (or the user, via [`set_machine!`](@ref))
    has supplied non-default `flops_per_sec` / `bandwidth`. Untuned defaults
    skew candidate generation and layout ranking badly on modern hardware.
    """
    calibrated::Bool = false
end

const MACHINE = Ref{Union{Nothing,MachineModel}}(nothing)

"""
    _ensure_machine() -> MachineModel

Return the cached machine model, constructing an uncalibrated default from
`Dagger.num_processors()` if needed. Does not calibrate (see
[`current_machine`](@ref)).
"""
function _ensure_machine()
    m = MACHINE[]
    m === nothing || return m
    np = try
        max(1, num_processors())
    catch
        1
    end
    m = MachineModel(; nprocs = np)
    MACHINE[] = m
    return m
end

"""
    current_machine() -> MachineModel

The machine model in effect. Constructed lazily from `Dagger.num_processors()`
on first use and cached. When the tape subsystem is enabled and the model has
not yet been calibrated, the first call runs [`calibrate_machine!`](@ref).

TODO(online-refinement): refine these rates from the scheduler's existing task
timings — Dagger already records per-task durations through `TimespanLogging`,
which is exactly the signal needed. This is also the natural place to hook the
autotuner: it can supply measured `(op, layout, size) -> seconds` points that
override the analytic model where they exist and leave it in place where they
do not.

TODO(heterogeneity): a single scalar FLOP rate is wrong on a CPU+GPU node,
which is the configuration Dagger most cares about. The model should carry a
per-processor-kind rate and the planner should weight by the fraction of tiles
each kind is expected to receive under the candidate assignment. Note that
`build_procgrid` currently filters to `ThreadProc` for symbolic assignments, so
today's assignment vocabulary cannot express a heterogeneous mapping anyway;
these two limitations should be lifted together.
"""
function current_machine()
    m = _ensure_machine()
    if is_enabled() && !m.calibrated
        calibrate_machine!(m)
    end
    return m
end

"""
    calibrate_machine!(m = _ensure_machine(); force=false) -> MachineModel

Measure aggregate GEMM throughput and streaming memory bandwidth with small
local kernels, and write the results into `m`. Takes a few tens of milliseconds
and is amortised over the session.

Called automatically on the first [`current_machine`](@ref) after
[`enable!`](@ref), and from `enable!` itself. Pass `force=true` to overwrite a
previous calibration (including one the user installed via [`set_machine!`](@ref)).

On a multi-threaded BLAS this measures *aggregate* FLOP/s (do not multiply by
`nprocs`). Bandwidth is a single-node DRAM-copy proxy for the "inter-processor"
term the cost models use; on multi-socket / distributed setups it is an
optimistic upper bound.
"""
function calibrate_machine!(m::MachineModel = _ensure_machine(); force::Bool = false)
    (!force && m.calibrated) && return m

    try
        m.nprocs = max(1, num_processors())
    catch
    end

    # --- Aggregate GEMM throughput ----------------------------------------
    # BLAS may already be multi-threaded, so the result is aggregate across
    # cores, not per-core. Keep this small: `enable!` should stay snappy.
    n = 768
    A = rand(Float64, n, n)
    B = rand(Float64, n, n)
    C = Matrix{Float64}(undef, n, n)
    LinearAlgebra.mul!(C, A, B)                          # warmup / JIT
    best = Inf
    for _ in 1:3
        best = min(best, @elapsed LinearAlgebra.mul!(C, A, B))
    end
    if isfinite(best) && best > 0
        m.flops_per_sec = max(1.0, 2.0 * Float64(n)^3 / best)
    end

    # --- Streaming memory bandwidth ---------------------------------------
    # Buffer sized to exceed typical LLC so we measure DRAM, not L3. Proxy for
    # shared-memory tile movement; optimistic for multi-node.
    #
    # The destination is freshly allocated on every repetition rather than
    # reused. That is deliberate: `bandwidth` is only ever used to price moving
    # data *between Dagger chunks* (redistribution, re-blocking, halo
    # exchange), and every such move allocates its destination chunks, so it
    # always pays first-touch page faults. Reusing a warm destination measures
    # something real but irrelevant — on a 6-thread machine it reads 16.7 GB/s
    # against 7.8 GB/s cold, and a measured DArray re-block achieves 4.2 GB/s.
    # Overstating bandwidth makes every layout change look nearly free, which
    # is exactly the error that lets a bad plan clear `CONFIG.gate_margin`.
    bytes = 64 * 1024 * 1024
    nelem = bytes ÷ sizeof(Float64)
    x = rand(Float64, nelem)
    copyto!(Vector{Float64}(undef, nelem), x)            # warmup / JIT
    bestb = Inf
    for _ in 1:3
        bestb = min(bestb, @elapsed copyto!(Vector{Float64}(undef, nelem), x))
    end
    if isfinite(bestb) && bestb > 0
        # copy reads x and writes a fresh y.
        m.bandwidth = max(1.0, 2.0 * Float64(bytes) / bestb)
    end

    # `task_overhead` is left at its default. Probing it needs live Dagger
    # spawns, which is unsafe from inside `enable!`/`current_machine`: it can
    # deadlock against an in-flight scheduler or BLAS threadpool. Call
    # `calibrate_task_overhead!` explicitly from a quiescent point instead, or
    # refine it from TimespanLogging task durations (see TODO on
    # `current_machine`).

    m.calibrated = true
    vlog("calibrated machine: nprocs=", m.nprocs,
         " flops_per_sec=", m.flops_per_sec,
         " bandwidth=", m.bandwidth)
    return m
end

"An empty task body, used only by `calibrate_task_overhead!`."
_overhead_probe!(c, a, b) = nothing

"""
    calibrate_task_overhead!(m = _ensure_machine(); nt=12) -> MachineModel

Measure `m.task_overhead` by submitting a GEMM-shaped dependency pattern of
empty tasks over tiny tiles, so the elapsed time is essentially all scheduler
cost. Uses the three-argument, tile-sharing shape that blocked linear algebra
submits, because Datadeps' cost is dominated by aliasing analysis over shared
tiles and a pattern of independent tasks measures ~1.5x too low.

**Not** called from [`enable!`](@ref) or [`calibrate_machine!`](@ref): it spawns
real Dagger tasks, which can deadlock if a scheduler or BLAS threadpool is
already in flight. Call it once from a quiescent point:

```julia
Dagger.Tapes.enable!()
Dagger.Tapes.calibrate_task_overhead!()
```
"""
function calibrate_task_overhead!(m::MachineModel = _ensure_machine(); nt::Int = 12)
    tiles = [Dagger.tochunk(zeros(4, 4)) for _ in 1:nt, _ in 1:nt]
    ntasks = 0
    submit = function ()
        n = 0
        Dagger.spawn_datadeps() do
            for k in 1:nt, j in (k+1):nt, i in (k+1):nt
                Dagger.@spawn _overhead_probe!(Dagger.InOut(tiles[i, j]),
                                               Dagger.In(tiles[i, k]),
                                               Dagger.In(tiles[k, j]))
                n += 1
            end
        end
        return n
    end
    try
        ntasks = submit()                                # warmup / JIT
        ntasks <= 0 && return m
        best = Inf
        for _ in 1:3
            best = min(best, @elapsed submit())
        end
        isfinite(best) && best > 0 &&
            (m.task_overhead = max(1.0e-6, best / ntasks))
    catch err
        vlog("task-overhead calibration failed ($err); keeping ", m.task_overhead)
        return m
    end
    vlog("calibrated task_overhead=", m.task_overhead, " over ", ntasks, " tasks")
    return m
end

"""
    set_machine!(m::MachineModel)
    set_machine!(; kwargs...)

Install or update the machine model; keyword form updates fields in place.
Supplying `flops_per_sec` or `bandwidth` marks the model as calibrated so
[`enable!`](@ref) will not overwrite it.
"""
set_machine!(m::MachineModel) = (MACHINE[] = m)
function set_machine!(; kwargs...)
    m = _ensure_machine()
    for (k, v) in kwargs
        hasfield(MachineModel, k) || throw(ArgumentError("unknown MachineModel field: $k"))
        setfield!(m, k, convert(fieldtype(MachineModel, k), v))
    end
    if any(k -> k === :flops_per_sec || k === :bandwidth || k === :calibrated, keys(kwargs))
        # User-supplied rates (or an explicit flag) count as calibrated.
        haskey(kwargs, :calibrated) || (m.calibrated = true)
    end
    return m
end

"Reset the cached machine model so it is rebuilt (and recalibrated) on next use."
reset_machine!() = (MACHINE[] = nothing)

# ---------------------------------------------------------------------------
# ArgView: what a cost model actually sees
# ---------------------------------------------------------------------------

"""
    ArgView

An operation argument as presented to a cost model: shape and element type from
the recorded [`ArgSpec`](@ref), paired with the [`LayoutChoice`](@ref) being
evaluated for it.
"""
struct ArgView
    spec::ArgSpec
    layout::LayoutChoice
end

Base.eltype(v::ArgView) = v.spec.eltype
Base.ndims(v::ArgView) = ndims(v.spec)
Base.size(v::ArgView) = size(v.spec)
Base.size(v::ArgView, d::Integer) = size(v.spec, d)
Base.length(v::ArgView) = prod(size(v); init = 1)

_elsize(::Type{Nothing}) = 0
_elsize(::Type{T}) where {T} = isbitstype(T) ? sizeof(T) : 8

"Bytes per element."
elsize(v::ArgView) = _elsize(v.spec.eltype)

"Total bytes in the (logical) array."
nbytes(v::ArgView) = Float64(length(v)) * elsize(v)

"Block size along dimension `d`, or the full tuple."
blocksize(v::ArgView) = blocksize(v.layout)
blocksize(v::ArgView, d::Integer) = blocksize(v.layout, d)

"Number of blocks along dimension `d`, or the full tuple."
nblocks(v::ArgView) = nblocks(v.layout, size(v))
nblocks(v::ArgView, d::Integer) =
    d <= ndims(v) ? max(1, cld(size(v, d), max(1, blocksize(v, d)))) : 1

"Total number of blocks."
ntiles(v::ArgView) = prod(nblocks(v); init = 1)

"Bytes in one block."
tilebytes(v::ArgView) = Float64(prod(blocksize(v); init = 1)) * elsize(v)

"Processor assignment strategy of the candidate layout."
assignment(v::ArgView) = v.layout.assignment

"""
    square_tile(v::ArgView) -> (edge, reblocked)

The block edge a blocked dense kernel will *actually* run with, and whether
getting there costs a copy.

Dagger's factorizations and triangular solves do not honour a non-square
blocking: `lu!`, `cholesky!`, `qr!` and the `ldiv!` family all funnel through
`maybe_copy_buffered(A => Blocks(b, b))` with `b = min(blocksize...)`, which
copies the whole array into square tiles, runs, and copies back. A cost model
that reads `blocksize(A, 1)` directly is therefore costing a layout the runtime
never executes — for `Blocks(4096, 683)` it charges a 4096-tall panel
factorization when what runs is a 683-square one.

Getting this wrong does not merely misprice one candidate: `AutoBlocks` is
exactly the non-square case, so it inflates the *baseline* every plan is gated
against, which turns `CONFIG.gate_margin` from a safety check into a rubber
stamp.
"""
function square_tile(v::ArgView)
    n = ndims(v)
    n == 0 && return (1, false)
    bs = blocksize(v)
    edges = ntuple(d -> max(1, min(bs[d], size(v, d))), n)
    edge = minimum(edges)
    return (edge, any(!=(edge), edges))
end

"""
    reblock_cost(v::ArgView, m::MachineModel) -> Float64

Cost of the copy-in/copy-out `maybe_copy_buffered` performs when a layout has to
be squared off before a dense kernel will accept it: the array's bytes move
twice, over one task per tile in each of the two blockings.
"""
function reblock_cost(v::ArgView, m::MachineModel)
    edge, reblocked = square_tile(v)
    reblocked || return 0.0
    nt_sq = prod(ntuple(d -> max(1, cld(size(v, d), edge)), ndims(v)); init = 1)
    return 2 * (m.latency + nbytes(v) / m.bandwidth) +
           (ntiles(v) + nt_sq) * m.task_overhead
end

"Longest/shortest block edge ratio; `1.0` for a perfect cube."
function aspect(v::ArgView)
    n = ndims(v)
    n <= 1 && return 1.0
    bs = blocksize(v)
    lo, hi = typemax(Int), 0
    for d in 1:n
        b = min(bs[d], size(v, d))
        b <= 0 && continue
        lo = min(lo, b); hi = max(hi, b)
    end
    (lo == typemax(Int) || lo <= 0) && return 1.0
    return hi / lo
end

# ---------------------------------------------------------------------------
# The cost model registry
# ---------------------------------------------------------------------------

"Operations with a hand-written [`@cost_model`](@ref), as opposed to the generic fallback."
const MODELED_OPS = Set{Symbol}()

"""
    op_cost(::Val{op}, args::Vector{ArgView}, m::MachineModel) -> Float64

Estimated wall-clock seconds for one execution of `op` with the given argument
layouts. Define methods with [`@cost_model`](@ref).

The fallback delegates to [`generic_op_cost`](@ref), so unmodelled operations
still exert sensible (if blunt) pull on the planner rather than being ignored.
"""
op_cost(::Val{Op}, args::Vector{ArgView}, m::MachineModel) where {Op} =
    generic_op_cost(args, m, Op)

"Whether `op` has a hand-written cost model."
has_cost_model(op::Symbol) = op in MODELED_OPS

"Guard against NaN / negative / infinite model output poisoning the planner."
@inline function clamp_cost(x::Float64)
    (isnan(x) || x < 0) && return Inf
    return x
end

"""
    @cost_model f(a, b, ...) = expr
    @cost_model function f(a, b, ...) ... end

Declare a cost model for the operation named `f`, returning estimated seconds.
Usable inside Dagger or from user code as `Dagger.Tapes.@cost_model`.

The declared argument names are bound to [`ArgView`](@ref)s, **in the order the
operation was recorded with** — that is, the order of arguments passed to
[`@record_op`](@ref), not the order in the underlying function signature. By
convention only `DArray`s are recorded, so scalar arguments such as `alpha` or
`uplo` do not appear.

An `ArgView` supports `size`, `ndims`, `eltype`, `length`, plus these, all of
which are bound as locals inside the body so no imports are needed:
`blocksize`, `nblocks`, `ntiles`, `tilebytes`, `nbytes`, `elsize`, `aspect`,
`assignment`.

These helpers are also in scope:

| helper           | meaning                                                     |
|:-----------------|:------------------------------------------------------------|
| `flops_time(f)`   | seconds for `f` FLOPs spread perfectly over all processors  |
| `serial_time(f)`  | seconds for `f` FLOPs on one processor (i.e. critical path) |
| `bytes_time(b)`   | seconds to move `b` bytes, including one latency            |
| `task_time(n)`    | scheduler cost for `n` tasks; serial, see `task_overhead`   |
| `reblock_time(v)` | cost of squaring off `v`'s blocking, `0.0` if already square |
| `imbalance(n)`    | load-imbalance multiplier for `n` tiles over `nprocs`       |
| `machine`         | the [`MachineModel`](@ref)                                  |
| `nprocs_avail()`  | processor count                                             |

`square_tile(v)` is also in scope, returning the `(edge, reblocked)` pair a
blocked dense kernel will really run with; prefer it over `blocksize(v, 1)` in
any model for an operation that squares its input.

If the operation is recorded with fewer arguments than the model declares, the
generic fallback is used rather than erroring.

# Example

```julia
Dagger.Tapes.@cost_model my_solve(A, B) = begin
    nt = nblocks(A, 1)
    serial_time(nt * blocksize(A, 1)^2) +
    flops_time(size(A, 1)^2 * size(B, 2)) * imbalance(nt) +
    task_time(nt^2 / 2)
end
```
"""
macro cost_model(ex)
    fname = nothing; fargs = nothing; body = nothing
    if @capture(ex, function fnm_(fa__) fb__ end)
        fname = fnm; fargs = fa; body = Expr(:block, fb...)
    elseif @capture(ex, fnm_(fa__) = fb_)
        fname = fnm; fargs = fa; body = fb
    else
        error("@cost_model expects `f(args...) = expr` or `function f(args...) ... end`")
    end
    fname isa Symbol || error("@cost_model: operation name must be a plain symbol, got `$fname`")
    all(a -> a isa Symbol, fargs) ||
        error("@cost_model: arguments must be plain names (no types, defaults or slurps)")

    nargs = length(fargs)
    A = esc(gensym(:args))
    M = esc(gensym(:machine))
    opq = QuoteNode(fname)

    # Bind the argument names, and re-export the ArgView accessor vocabulary as
    # locals so the body works regardless of what the caller has imported.
    accessors = (:blocksize, :nblocks, :ntiles, :tilebytes, :nbytes, :elsize,
                 :aspect, :assignment, :square_tile)
    accbinds = [:(local $(esc(s)) = $(getfield(TAPES_MODULE, s))) for s in accessors]
    argbinds = [:(local $(esc(a)) = $A[$i]) for (i, a) in enumerate(fargs)]

    # Name the method as `<Module>.op_cost` with the module interpolated as a
    # value. This is the form that works regardless of where the macro is
    # expanded from: an unescaped bare `op_cost` relies on hygiene resolving to
    # the macro's defining module (fragile for definitions), and an escaped
    # `Dagger.Tapes.op_cost` needs `Dagger` to be bound in the caller's scope,
    # which it is not while `Tapes` is itself still being defined.
    fnexpr = Expr(:., TAPES_MODULE, QuoteNode(:op_cost))

    quote
        function $fnexpr(::$(Val{fname}),
                         $A::Vector{$ArgView},
                         $M::$MachineModel)
            if length($A) < $nargs
                return $generic_op_cost($A, $M, $opq)
            end
            local $(esc(:machine)) = $M
            local $(esc(:nprocs_avail)) = () -> $M.nprocs
            local $(esc(:flops_time)) = f -> f / $M.flops_per_sec
            local $(esc(:serial_time)) = f -> f / ($M.flops_per_sec / max(1, $M.nprocs))
            local $(esc(:bytes_time)) = b -> ($M.latency + b / $M.bandwidth)
            local $(esc(:task_time)) = n -> n * $M.task_overhead
            local $(esc(:reblock_time)) = v -> $reblock_cost(v, $M)
            local $(esc(:imbalance)) = n -> (n <= 0 ? 1.0 :
                let np = max(1, $M.nprocs); ceil(n / np) / (n / np) end)
            $(accbinds...)
            $(argbinds...)
            return $clamp_cost(Float64($(esc(body))))
        end
        push!($MODELED_OPS, $opq)
        $opq
    end
end

# ---------------------------------------------------------------------------
# Generic fallback
# ---------------------------------------------------------------------------

"""
    OP_AFFINITY

Coarse block-shape preference per operation, consulted by
[`generic_op_cost`](@ref) when no analytic model exists. This is how an
unmodelled operation still exerts *some* pull on the planner.

- `:square`  — wants near-cubic tiles (dense factorizations, GEMM)
- `:rowwise` — wants whole rows resident per block
- `:colwise` — wants whole columns resident per block
- `:large`   — indifferent to shape, wants few large blocks (elementwise, reductions)
- `:any`     — genuinely indifferent

TODO(learning): this table is hand-maintained and will rot as operations are
added. It should be *learned*: track measured cost against the aspect ratio and
tile count actually used per op, and fit the affinity rather than asserting it.
The tape already produces the `(op, layout, outcome)` triples needed, and this
is a much smaller fitting problem than learning a full cost model.
"""
const OP_AFFINITY = Dict{Symbol,Symbol}(
    :cholesky => :square, :cholesky! => :square, :potrf! => :square,
    :lu => :square, :lu! => :square, :getrf! => :square,
    :qr => :square, :qr! => :square, :geqrf! => :square,
    :svd => :square, :svd! => :square,
    :mul! => :square, :gemm! => :square, :syrk! => :square,
    :trsm! => :square, :ldiv! => :square, :rdiv! => :square,
    :trsv! => :rowwise,
    :transpose => :square, :adjoint => :square, :permutedims => :square,
    :map => :large, :map! => :large, :broadcast => :large,
    :materialize! => :large, :copyto! => :large, :fill! => :large,
    :reduce => :large, :mapreduce => :large, :sum => :large, :norm => :large,
    :stencil => :square,
    :sort => :rowwise, :sort! => :rowwise,
)

"""
    shape_penalty(v::ArgView, op::Symbol, m::MachineModel) -> Float64

Multiplicative penalty in `[1, Inf)` for a layout that is a poor fit for `op`'s
known affinity, or that is unreasonable on its own terms: tiles too small to
amortise task overhead, tiles too large to fit a processor's memory, or too few
tiles to keep every processor busy.

These three self-evident penalties matter more than the affinity term. They are
what make layout errors *asymmetric* in the model the way they are in reality —
too-small blocks collapse the scheduler, too-large blocks run out of memory —
so the planner will not trade a small predicted win for a catastrophic risk.
"""
function shape_penalty(v::ArgView, op::Symbol, m::MachineModel)
    p = 1.0
    tb = tilebytes(v)
    nt = ntiles(v)

    # A task must do enough work to be worth launching.
    if tb > 0
        min_useful = m.task_overhead * m.bandwidth
        tb < min_useful && (p *= min_useful / tb)
    end

    # A tile (and the handful a blocked algorithm holds live) must fit.
    budget = m.mem_per_proc / 4
    tb > budget && (p *= 1.0 + 8.0 * (tb / budget - 1.0))

    # There must be enough tiles to occupy every processor.
    nt < m.nprocs && (p *= m.nprocs / max(1, nt))

    aff = get(OP_AFFINITY, op, :any)
    if aff === :square
        p *= 1.0 + 0.25 * log2(max(1.0, aspect(v)))
    elseif aff === :rowwise
        nd = ndims(v)
        nd >= 2 && nblocks(v, nd) > 1 && (p *= 1.0 + 0.5 * log2(nblocks(v, nd)))
    elseif aff === :colwise
        ndims(v) >= 1 && nblocks(v, 1) > 1 && (p *= 1.0 + 0.5 * log2(nblocks(v, 1)))
    elseif aff === :large
        p *= 1.0 + 0.05 * log2(max(1, nt))
    end

    return p
end

"""
    generic_op_cost(args, m, op=:_unknown) -> Float64

Shape-agnostic cost estimate used when no [`@cost_model`](@ref) matches.

Charges for data volume touched once, per-tile scheduler overhead, load
imbalance, per-tile latency, and the [`shape_penalty`](@ref).

This also doubles as the planner's "unknown future operation" stand-in: the
residual probability mass of a prediction is charged at this rate, so layouts
that are pathological in general are penalised even when they are perfect for
the modal chain. That is what keeps a misprediction merely suboptimal instead
of catastrophic.
"""
function generic_op_cost(args::Vector{ArgView}, m::MachineModel, op::Symbol = :_unknown)
    isempty(args) && return 0.0
    np = max(1, m.nprocs)
    total = 0.0
    for v in args
        eltype(v) === Nothing && continue
        nt = ntiles(v)
        c = nbytes(v) / m.bandwidth          # touch the data once
        c += nt * m.task_overhead            # per-tile scheduling (serial)
        c += nt * m.latency                  # per-tile boundary crossing
        c *= nt <= 0 ? 1.0 : ceil(nt / np) / (nt / np)   # load imbalance
        c *= shape_penalty(v, op, m)
        total += c
    end
    return clamp_cost(total)
end

"""
    cost_of(op, args, m=current_machine()) -> Float64

Dispatch to the registered model for `op`, falling back to the affinity-aware
generic model.
"""
cost_of(op::Symbol, args::Vector{ArgView}, m::MachineModel = current_machine()) =
    clamp_cost(op_cost(Val(op), args, m))

"""
    redistribution_cost(from, to, spec, m) -> Float64

Cost of changing an array's layout mid-chain. Charged as a full all-to-all of
the array's bytes plus per-tile latency and task overhead on both sides,
because in general every element lands on a different processor.

TODO(precision): this is a deliberate overestimate in the cases where it is
wrong — `Blocks(1024,1024) -> Blocks(512,512)` under the same assignment is a
purely local subdivision that moves nothing. Computing the true volume needs
both proc grids and an intersection of the two `DomainBlocks`; `build_procgrid`
plus `src/datadeps/aliasing.jl` has the pieces. Overestimating is the safe
direction (it suppresses speculative repartitioning), so this is not urgent,
but it does leave real wins on the table for refinement-style changes.
"""
function redistribution_cost(from::LayoutChoice, to::LayoutChoice, spec::ArgSpec,
                             m::MachineModel = current_machine())
    from == to && return 0.0
    sz = size(spec)
    bytes = Float64(prod(sz; init = 1)) * _elsize(spec.eltype)
    nt_to = prod(nblocks(to, sz); init = 1)
    nt_from = prod(nblocks(from, sz); init = 1)
    return bytes / m.bandwidth +
           (nt_to + nt_from) * (m.latency + m.task_overhead)
end

# ---------------------------------------------------------------------------
# Default models for Dagger's built-in operations.
#
# These are first-order: they capture the terms that actually *differ* between
# layouts (tile count, critical-path length, communication volume) and ignore
# constants that do not, since the planner only ever compares layouts against
# each other. They are meant to be replaced by measurement-corrected versions
# once the autotuner is wired in.
#
# The argument order below matches the order these operations are instrumented
# with in `integration.jl` — keep the two in sync.
# ---------------------------------------------------------------------------

@cost_model cholesky!(A) = begin
    n  = size(A, 1)
    # `cholesky!` squares off its input before factorizing; cost what runs.
    b, _ = square_tile(A)
    nt = max(1, cld(n, b))
    # nt POTRF panels sit on the critical path; the rest is parallel SYRK/GEMM/TRSM.
    serial_time(nt * b^3 / 3) +
    flops_time(Float64(n)^3 / 3) * imbalance(nt * (nt + 1) / 2) +
    task_time(nt^3 / 3 + nt^2) +
    bytes_time(nt^2 * b^2 * elsize(A)) +
    reblock_time(A)
end

@cost_model lu!(A) = begin
    n  = size(A, 1)
    b, _ = square_tile(A)
    nt = max(1, cld(n, b))
    serial_time(nt * b^3) +
    flops_time(2 * Float64(n)^3 / 3) * imbalance(nt * nt) +
    # Row swaps against the trailing submatrix dominate the task count.
    task_time(nt^3 / 2 + nt^3 / 3 + nt^2) +
    bytes_time(nt * nt * b^2 * elsize(A)) +
    reblock_time(A)
end

@cost_model qr!(A) = begin
    mm = size(A, 1); nn = size(A, 2)
    b, _ = square_tile(A)
    ntr = max(1, cld(mm, b)); ntc = max(1, cld(nn, b))
    serial_time(ntc * b^3) +
    flops_time(2 * Float64(mm) * nn^2 - 2 * Float64(nn)^3 / 3) * imbalance(ntr * ntc) +
    task_time(ntr * ntc^2) +
    bytes_time(ntr * ntc * b^2 * elsize(A)) +
    reblock_time(A)
end

@cost_model mul!(C, A, B) = begin
    mm = size(A, 1); kk = size(A, 2); nn = size(B, 2)
    ntm = nblocks(C, 1); ntn = nblocks(C, 2); ntk = nblocks(A, 2)
    flops_time(2.0 * mm * kk * nn) * imbalance(ntm * ntn) +
    task_time(ntm * ntn * ntk) +
    # Each C tile pulls a block row of A and a block column of B.
    bytes_time(ntm * ntn * ntk *
               (prod(blocksize(A); init = 1) + prod(blocksize(B); init = 1)) * elsize(A)) +
    # Mismatched inner blocking forces a repack of one operand.
    (blocksize(A, 2) == blocksize(B, 1) ? 0.0 : bytes_time(nbytes(B)))
end

@cost_model syrk!(C, A) = begin
    nn = size(C, 1); kk = size(A, 2)
    ntn = nblocks(C, 1); ntk = nblocks(A, 2)
    flops_time(Float64(nn)^2 * kk) * imbalance(ntn * (ntn + 1) / 2) +
    task_time(ntn * (ntn + 1) * ntk / 2) +
    bytes_time(ntn * ntn * ntk * prod(blocksize(A); init = 1) * elsize(A))
end

@cost_model trsm!(A, B) = begin
    nn = size(A, 1)
    nd = ndims(B)
    nrhs = nd >= 2 ? size(B, 2) : 1
    # `ldiv!` squares `A` off (and re-blocks `B` to match) before calling trsm!.
    b, _ = square_tile(A)
    nta = max(1, cld(nn, b)); ntb = max(1, cld(nrhs, b))
    # Triangular solve is inherently sequential along the block diagonal.
    serial_time(nta * Float64(b)^2 * b) +
    flops_time(Float64(nn)^2 * nrhs) * imbalance(nta * ntb) +
    task_time(nta * nta * ntb / 2) +
    bytes_time(nta * ntb * Float64(b)^2 * elsize(B)) +
    reblock_time(A)
end

@cost_model trsv!(A, B) = begin
    nn = size(A, 1)
    b, _ = square_tile(A)
    nta = max(1, cld(nn, b))
    serial_time(nta * Float64(b)^2) +
    flops_time(Float64(nn)^2) * imbalance(nta) +
    task_time(nta * (nta + 1) / 2) +
    bytes_time(nta * nta * Float64(b) * elsize(A)) +
    reblock_time(A)
end

@cost_model map(A) = flops_time(Float64(length(A))) * imbalance(ntiles(A)) + task_time(ntiles(A))
@cost_model map!(A) = flops_time(Float64(length(A))) * imbalance(ntiles(A)) + task_time(ntiles(A))
@cost_model copyto!(A) = bytes_time(nbytes(A)) / max(1, nprocs_avail()) + task_time(ntiles(A))

@cost_model reduce(A) = begin
    nt = ntiles(A)
    flops_time(Float64(length(A))) * imbalance(nt) +
    task_time(nt) +
    (nt > 1 ? bytes_time(nt * elsize(A)) * log2(nt) : 0.0)   # tree reduction over tiles
end

@cost_model mapreduce(A) = begin
    nt = ntiles(A)
    flops_time(Float64(length(A))) * imbalance(nt) +
    task_time(nt) +
    (nt > 1 ? bytes_time(nt * elsize(A)) * log2(nt) : 0.0)
end

@cost_model transpose(A) = begin
    # A full transpose is an all-to-all unless the blocking is symmetric.
    sym = ndims(A) >= 2 && blocksize(A, 1) == blocksize(A, 2)
    bytes_time(nbytes(A)) * (sym ? 0.5 : 1.0) + task_time(ntiles(A))
end

@cost_model permutedims(A) = bytes_time(nbytes(A)) + task_time(ntiles(A))

@cost_model stencil(A) = begin
    nt = ntiles(A)
    tile = Float64(prod(blocksize(A); init = 1))
    halo = 0.0
    for d in 1:ndims(A)
        halo += tile / max(1, blocksize(A, d))   # one face per dimension
    end
    flops_time(Float64(length(A))) * imbalance(nt) +
    task_time(nt) +
    bytes_time(2 * nt * halo * elsize(A))
end
