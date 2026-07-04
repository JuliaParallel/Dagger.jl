"""
    Dagger.Autotune

Hardware- and input-aware algorithm selection for Dagger operations, plus the
benchmarking system (`Autotune.benchmark`) that trains it.

Design goals (see README.md in this directory for the full story):

  * Operation entrypoints (`lu`, `cholesky`, `qr`, `\\`, `mul!`, stencils, ...)
    call `Autotune.invoke_best(op, inputs...; kwargs...)`. Autotune profiles
    the inputs, consults an on-disk benchmark database, picks the fastest
    *available* algorithm (which may live in Dagger, LinearAlgebra, CUDA,
    Krylov, KLU, ... - Dagger is not privileged), performs any data movement
    and environment configuration needed, invokes it, and optionally converts
    the result back.

  * `Autotune.benchmark(config)` sweeps a configurable space of operations,
    input shapes/sizes/eltypes/sparsities, array container types, Julia
    thread counts, target accuracies, and per-algorithm free parameters
    (e.g. DArray block size). Every process-level configuration (thread
    count, worker count) runs in a *separate* Julia subprocess inside a
    temporary project, so that crashes, OOMs, and hangs only lose one trial:
    the parent marks the trial failed, respawns a worker, and continues.

  * Everything external is accessed lazily through `Base.loaded_modules` and
    registered hooks, so this module depends only on stdlibs and can be
    loaded (and unit-tested) standalone, outside of Dagger.

Runtime knobs come in three mutability classes (`configurables.jl`):

  * `FIXED`   - cannot change in-process (Julia thread count), but can still
                be swept at benchmark time via separate worker processes.
  * `MUTABLE` - has a current value that can be changed for a modeled cost
                (BLAS thread count).
  * `FREE`    - unset, chosen freely per invocation (DArray block size).

Integration checklist for Dagger:

  1. `include("autotune/Autotune.jl")` from `Dagger.jl` (after Dagger's own
     definitions), then `using .Autotune`.
  2. Call `Autotune.set_raw_impl!(:dagger_lu, A -> <Dagger's internal LU>)`
     (and likewise for `:dagger_cholesky`, `:dagger_qr`, `:dagger_gemm`, ...)
     so that Autotune can invoke Dagger's algorithms *without* re-entering
     the autotuned entrypoints (avoiding infinite recursion).
  3. In each entrypoint:
         function LinearAlgebra.lu(A::DArray; kw...)
             Autotune.enabled() && return Autotune.invoke_best(:lu, A; kw...)
             return _dagger_lu(A; kw...)   # existing path
         end
  4. `Dagger.benchmark(args...; kw...) = Autotune.benchmark(args...; kw...)`
"""
module Autotune

using LinearAlgebra
using SparseArrays
using Distributed
using Random
using Printf
using Dates
using TOML

export benchmark, BenchmarkConfig, invoke_best, report

include("configurables.jl")
include("spec.jl")
include("registry.jl")
include("lu_backends.jl")
include("database.jl")
include("model.jl")
include("benchmark.jl")

# Global enable/disable switch for runtime algorithm selection.
const ENABLED = Ref{Bool}(true)

"""
    enabled() -> Bool

Whether runtime algorithm selection is active. When `false`, entrypoints
should fall through to their existing implementations. Selection is also
implicitly bypassed when no benchmark database has been generated yet.
"""
enabled() = ENABLED[]
enable!() = (ENABLED[] = true; nothing)
disable!() = (ENABLED[] = false; nothing)

"""
    with_autotune(f; enabled::Bool=true)

Run `f()` with autotuning temporarily enabled or disabled.
"""
function with_autotune(f; enabled::Bool=true)
    old = ENABLED[]
    ENABLED[] = enabled
    try
        return f()
    finally
        ENABLED[] = old
    end
end

function __init__()
    register_default_configurables!()
    register_default_operations!()
    register_default_algorithms!()
    register_default_converters!()
end

end # module Autotune
