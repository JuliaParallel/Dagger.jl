# ===========================================================================
# Integration helpers.
#
# This file holds the glue that touches Dagger's existing code paths. It is
# kept separate from the rest of the subsystem so that the blast radius of the
# feature is obvious: everything else in `tapes/` is self-contained and has no
# effect until something here calls into it.
#
# It is included from `Tapes.jl`, so everything here lives in `Dagger.Tapes`
# and is referenced from Dagger as `Tapes.resolve_partitioning(...)` etc.
# ===========================================================================

"""
    resolve_partitioning(::Type{T}, dims, requested, assignment) -> (part, assignment, plan)

The single entry point for allocators. Returns the partitioning and assignment
to allocate with, plus the [`AllocationPlan`](@ref) to hand to
[`track!`](@ref).

When the subsystem is disabled this resolves `AutoBlocks()` exactly as before
(`auto_blocks(dims)`), so behaviour is bit-identical to the status quo.
"""
function resolve_partitioning(::Type{T}, dims::Tuple, requested, assignment) where {T}
    if !is_enabled()
        part = requested isa AutoBlocks ? auto_blocks(map(Int, dims)::Dims) : requested
        return (part, assignment, passthrough(part, assignment))
    end
    p = plan_allocation(T, dims; requested = requested, assignment = assignment)
    part = p.partitioning isa AutoBlocks ? auto_blocks(map(Int, dims)::Dims) : p.partitioning
    return (part, p.assignment, p)
end

"""
    @tracked_alloc T dims requested assignment expr

Convenience wrapper for an allocation site. Binds `part` and `assign` for use
inside `expr` (which must evaluate to the new `DArray`), and tracks the result.

```julia
function Base.zeros(p::BlocksOrAuto, T::Type, dims::Dims; assignment = :arbitrary)
    Dagger.Tapes.@tracked_alloc T dims p assignment begin
        _zeros_impl(part, T, dims, assign)
    end
end
```
"""
macro tracked_alloc(T, dims, requested, assignment, expr)
    quote
        local __T__ = $(esc(T))
        local __d__ = $(esc(dims))
        local __part__, __assign__, __plan__ =
            $resolve_partitioning(__T__, __d__, $(esc(requested)), $(esc(assignment)))
        local $(esc(:part)) = __part__
        local $(esc(:assign)) = __assign__
        local __A__ = $(esc(expr))
        __A__ isa $DArray ? $track!(__A__, __plan__) : __A__
    end
end

# ===========================================================================
# PATCH POINTS
#
# The changes below are written out rather than applied, because they touch
# files this subsystem does not own. Each is small and independently
# revertible. Nothing here changes behaviour while `CONFIG.enabled == false`.
# ===========================================================================

#=

## 1. `src/Dagger.jl`

Insert after the `array/darray.jl` include (currently line 126), before
`array/alloc.jl`:

```julia
include("tapes/Tapes.jl"); using .Tapes
include("tapes/integration.jl")
```

`using .Tapes` brings `@record_op`, `@cost_model` and `@expect_ops` into
`Dagger`, so users write `Dagger.@record_op` and `Dagger.@expect_ops`.

This must come *after* `array/darray.jl` (which defines `DArray`, `Blocks`,
`AutoBlocks` and `auto_blocks`) and *before* `array/alloc.jl` and the
linear-algebra files, which call into it.

## 2. `src/array/alloc.jl` — the allocation sites

Every constructor that accepts `BlocksOrAuto` is a decision point. The pattern
is the same for `rand`, `randn`, `ones`, `zeros`, `sprand` and the
`DArray{T,N}(undef, ...)` family. Taking `zeros` as the example:

```julia
function Base.zeros(p::Blocks, T::Type, dims::Dims; assignment::AssignmentType = :arbitrary)
    part, assign, plan = Tapes.resolve_partitioning(T, dims, p, assignment)
    d = ArrayDomain(map(x -> 1:x, dims))
    s = reduce(vcat, partition(part, d))
    procgrid = build_procgrid(assign, dims, part.blocksize, current_acceleration())
    a = AllocateArray(T, AllocateZeros{T}(), false, d, s, part, procgrid)
    return Tapes.track!(_to_darray(a), plan)
end

# The AutoBlocks method must stop short-circuiting to `auto_blocks` so the
# planner gets a say:
Base.zeros(p::AutoBlocks, T::Type, dims::Dims; assignment::AssignmentType = :arbitrary) =
    _zeros_planned(T, dims, p, assignment)
```

The important structural point: **`AutoBlocks` must reach
`resolve_partitioning` un-lowered**. At present `AutoBlocks` methods call
`auto_blocks(A)` immediately and forward to the `Blocks` method, which erases
the distinction between "the user asked for automatic" and "the user asked for
this specific blocking". The tape system needs that distinction — it declines
to override explicit `Blocks` by default. Threading `requested` through as
shown preserves it.

## 3. `src/array/darray.jl` — `distribute`

`distribute(A, AutoBlocks(), assignment)` is the other main entry point:

```julia
function distribute(A::AbstractArray{T,N}, ::AutoBlocks,
                    assignment::AssignmentType = :arbitrary) where {T,N}
    part, assign, plan = Tapes.resolve_partitioning(T, size(A), AutoBlocks(), assignment)
    return Tapes.track!(distribute(A, part, assign), plan)
end
```

## 4. Operation instrumentation

One `@record_op` per operation, immediately before task submission. Argument
order must match the corresponding `@cost_model` in `tapes/cost.jl` — pass only
`DArray`s.

- `src/array/cholesky.jl`, in `LinearAlgebra._chol!(A::DArray{T,2}, ...)`:
  ```julia
  Dagger.@record_op :cholesky! A
  ```

- `src/array/lu.jl`, in the `lu!` entry point:
  ```julia
  Dagger.@record_op :lu! A
  ```

- `src/array/qr.jl`, in the `qr!` entry point:
  ```julia
  Dagger.@record_op :qr! A
  ```

- `src/array/mul.jl`, in `LinearAlgebra.generic_matmatmul!(C, tA, tB, A, B, alpha, beta)`:
  ```julia
  Dagger.@record_op :mul! C A B
  ```
  and in `syrk_dagger!(C, transA, A, alpha, beta)`:
  ```julia
  Dagger.@record_op :syrk! C A
  ```
  Note `generic_matvecmul!` should record as `:mul!` too, with `B` a `DVector`;
  the cost model handles `ndims(B) == 1` via its `nd` branch.

- `src/array/trsm.jl`:
  ```julia
  Dagger.@record_op :trsm! A B     # in trsm!
  Dagger.@record_op :trsv! A B     # in trsv!
  ```

- `src/array/map-reduce.jl`: `:map`, `:map!`, `:reduce`, `:mapreduce`.
- `src/array/copy.jl`: `:copyto!`.
- `src/array/permute.jl`: `:transpose`, `:permutedims`.
- `src/array/stencil.jl`: `:stencil`.
- `src/array/sort.jl`: `:sort!`.

TODO(coverage): `src/array/operators.jl` broadcasting is the highest-value
missing instrumentation — broadcast fusion is where most real user code spends
its operations, and a fused broadcast's layout preference (`:large`, shape
indifferent) is a genuinely useful signal that the current chain misses. It
needs care because a single `materialize!` covers an arbitrary expression tree;
recording it as one `:broadcast` op loses which arrays participated.

TODO(datadeps): operations submitted inside `spawn_datadeps` are the case where
this system should shine, because Datadeps already knows the full dependency
structure of the region. Rather than instrumenting each operation, the Datadeps
queue itself could record the op sequence per aliased region at
`distribute_tasks!` time — exact lookahead for the region, no prediction
required, which subsumes the tape for that scope. That is strictly better where
it applies and should be built before broadening `@record_op` coverage much
further.

## 5. Precompilation

`src/precompile.jl` should run at least one workload with the subsystem enabled
so the planner and macro-generated cost models are precompiled; otherwise the
first *enabled* allocation pays a large latency spike that will be mistaken for
tape overhead.

=#

# ===========================================================================
# DESIGN TODOs deferred by explicit decision
# ===========================================================================

#=

## Deferred materialization

The cleanest solution to the whole problem is not to predict at all: keep an
array as a *logical description* until its first consumer is known, then
materialise it with a layout chosen from actual knowledge rather than a
forecast. That gets the first operation's layout exactly right with zero
prediction risk, and reduces the tape's job to optimising operations 2..n.

Deferred: this requires restructuring how Datadeps tracks and materialises
arrays. Specifically, `DArray` would need an unmaterialised state that
`chunks` accessors can trigger materialisation from, every internal
`A.chunks` access would need to route through that, and the Datadeps aliasing
analysis would need to reason about a domain decomposition that does not exist
yet. `AllocateArray`/`stage` is the natural seam but the change reaches much
further than that.

Note the interaction: once deferred materialization exists, the tape's role
shifts from "choose the layout" to "choose the layout for the operations after
the first", which makes the confidence gating *more* important, not less —
the easy win is gone and only the speculative part remains.

## Multiple materialised layouts for read-only data

For data that is read but not written, maintaining two physical layouts costs
memory but requires no coherence protocol at all. This is exactly what column
stores do with multiple sort-order projections (C-Store / Vertica). For a chain
where operation A wants square tiles and operations B..F want row blocks,
replicating can easily dominate repartitioning.

Deferred: doing this without introducing OOM failures requires a memory
accounting system that Dagger does not have — something that tracks per-space
residency, knows the high-water mark of the current task graph, and can evict
or refuse a replica under pressure. MemPool has pieces of this
(`MemPool.approx_size`, the storage device abstraction) but there is no
global admission control. Building replication on top of the current
best-effort memory handling would turn a performance feature into a
reliability regression.

The precondition is a data-residency tracker: (array, layout) -> space, bytes,
last-touch, read-only flag. That is the same structure needed for a proper
`redistribution_cost`, for heterogeneous cost modelling, and for out-of-core
support, so it is probably the highest-leverage missing piece overall.

## Verified lookahead vs prediction

A submission-window scheme — buffer N submitted tasks or T microseconds before
scheduling — gives *exact* lookahead with zero prediction risk, and beats this
system on any workload whose operation chain fits in one scope. The tape earns
its keep only where a window cannot reach: operations scattered across user
code, deep call stacks, library boundaries, and loop iterations that a bounded
buffer cannot span.

These are complements, not alternatives, and the strongest configuration is
both: exact layouts for the verified near-term operations, predicted layouts
for the tail. `plan_chain` already accepts an arbitrary `Vector{PredictedOp}`,
so a window would simply contribute entries with `prob = 1.0` ahead of the
predicted ones. Wiring that up is cheap once a window exists; `@expect_ops`
with an explicit list is the manual version of the same thing.

=#
