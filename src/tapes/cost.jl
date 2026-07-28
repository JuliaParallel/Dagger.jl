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
    "Scheduler overhead per task in seconds. Dagger tasks bottom out around 100us."
    task_overhead::Float64 = 1.0e-4
    "Usable memory per processor in bytes; used to reject layouts that will not fit."
    mem_per_proc::Float64 = 8.0e9
end

const MACHINE = Ref{Union{Nothing,MachineModel}}(nothing)

"""
    current_machine() -> MachineModel

The machine model in effect, constructed lazily from `Dagger.num_processors()`
on first use and cached.

TODO(calibration): `flops_per_sec` and `bandwidth` are guesses. They should be
measured once at `enable!` time with a small GEMM and a small point-to-point
transfer (a few hundred milliseconds, amortised over the session), and refined
online from the scheduler's existing task timings — Dagger already records
per-task durations through `TimespanLogging`, which is exactly the signal
needed. This is also the natural place to hook the autotuner: it can supply
measured `(op, layout, size) -> seconds` points that override the analytic
model where they exist and leave it in place where they do not.

TODO(heterogeneity): a single scalar FLOP rate is wrong on a CPU+GPU node,
which is the configuration Dagger most cares about. The model should carry a
per-processor-kind rate and the planner should weight by the fraction of tiles
each kind is expected to receive under the candidate assignment. Note that
`build_procgrid` currently filters to `ThreadProc` for symbolic assignments, so
today's assignment vocabulary cannot express a heterogeneous mapping anyway;
these two limitations should be lifted together.
"""
function current_machine()
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
    set_machine!(m::MachineModel)
    set_machine!(; kwargs...)

Install or update the machine model; keyword form updates fields in place.
"""
set_machine!(m::MachineModel) = (MACHINE[] = m)
function set_machine!(; kwargs...)
    m = current_machine()
    for (k, v) in kwargs
        hasfield(MachineModel, k) || throw(ArgumentError("unknown MachineModel field: $k"))
        setfield!(m, k, convert(fieldtype(MachineModel, k), v))
    end
    return m
end

"Reset the cached machine model so it is rebuilt on next use."
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
| `flops_time(f)`  | seconds for `f` FLOPs spread perfectly over all processors   |
| `serial_time(f)` | seconds for `f` FLOPs on one processor (i.e. critical path)  |
| `bytes_time(b)`  | seconds to move `b` bytes, including one latency             |
| `task_time(n)`   | scheduler overhead for `n` tasks, spread over all processors |
| `imbalance(n)`   | load-imbalance multiplier for `n` tiles over `nprocs`        |
| `machine`        | the [`MachineModel`](@ref)                                   |
| `nprocs_avail()` | processor count                                              |

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
                 :aspect, :assignment)
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
            local $(esc(:task_time)) = n -> n * $M.task_overhead / max(1, $M.nprocs)
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
        min_useful = m.task_overhead * m.bandwidth / max(1, m.nprocs)
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
        c += nt * m.task_overhead / np       # per-tile scheduling
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
           (nt_to + nt_from) * (m.latency + m.task_overhead / max(1, m.nprocs))
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
    b  = max(1, blocksize(A, 1))
    nt = nblocks(A, 1)
    # nt POTRF panels sit on the critical path; the rest is parallel SYRK/GEMM/TRSM.
    serial_time(nt * b^3 / 3) +
    flops_time(Float64(n)^3 / 3) * imbalance(nt * (nt + 1) / 2) +
    task_time(nt^3 / 3 + nt^2) +
    bytes_time(nt^2 * b^2 * elsize(A)) +
    # Rectangular tiles mismatch TRSM against SYRK and inflate the panel path.
    serial_time(nt * b^3 / 3) * (aspect(A) - 1.0)
end

@cost_model lu!(A) = begin
    n  = size(A, 1)
    b  = max(1, blocksize(A, 1))
    nt = nblocks(A, 1)
    serial_time(nt * b^3) +
    flops_time(2 * Float64(n)^3 / 3) * imbalance(nt * nt) +
    task_time(nt^3 / 3 + nt^2) +
    # Panel pivoting serialises down a block column.
    bytes_time(nt * nt * b^2 * elsize(A)) * (1.0 + 0.5 * (aspect(A) - 1.0))
end

@cost_model qr!(A) = begin
    mm = size(A, 1); nn = size(A, 2)
    b = max(1, blocksize(A, 1))
    ntr = nblocks(A, 1); ntc = nblocks(A, 2)
    serial_time(ntc * b^3) +
    flops_time(2 * Float64(mm) * nn^2 - 2 * Float64(nn)^3 / 3) * imbalance(ntr * ntc) +
    task_time(ntr * ntc^2) +
    bytes_time(ntr * ntc * b^2 * elsize(A)) * (1.0 + 0.3 * (aspect(A) - 1.0))
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
    nta = nblocks(A, 1); ntb = max(1, nblocks(B, nd))
    # Triangular solve is inherently sequential along the block diagonal.
    serial_time(nta * blocksize(A, 1)^2 * max(1, blocksize(B, nd))) +
    flops_time(Float64(nn)^2 * nrhs) * imbalance(nta * ntb) +
    task_time(nta * nta * ntb / 2) +
    bytes_time(nta * ntb * prod(blocksize(B); init = 1) * elsize(B))
end

@cost_model trsv!(A, B) = begin
    nn = size(A, 1)
    nta = nblocks(A, 1)
    serial_time(nta * blocksize(A, 1)^2) +
    flops_time(Float64(nn)^2) * imbalance(nta) +
    task_time(nta * (nta + 1) / 2) +
    bytes_time(nta * nta * blocksize(A, 1) * elsize(A))
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
