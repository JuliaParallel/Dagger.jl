# configurables.jl
#
# A `Configurable` is a named environment/runtime knob that affects algorithm
# performance. Each has one of three mutability classes:
#
#   FIXED   - Cannot be changed within the current process (e.g. the Julia
#             thread count, the number of Distributed workers). At *benchmark*
#             time these are still swept: each value gets its own worker
#             process, spawned with the appropriate `--threads`/`-p` flags.
#             At *runtime*, the current value is a hard constraint - the
#             selector matches benchmark data against it (nearest value, with
#             a one-time warning if no exact match exists).
#
#   MUTABLE - Has a current value that can be changed, at a modeled cost in
#             runtime overhead (e.g. `BLAS.set_num_threads`). The selector is
#             free to pick benchmark configurations with different values of
#             MUTABLE knobs; the transition cost (set + restore) is added to
#             the predicted runtime of such candidates. `execute_plan!`
#             applies the change before invoking the algorithm and restores
#             the old value afterwards (try/finally).
#
#   FREE    - Not an ambient setting at all: chosen freely per invocation and
#             passed to the algorithm (e.g. DArray block size). These appear
#             as per-algorithm config axes in the benchmark sweep
#             (`AlgorithmSpec.free_config`), and the selector simply picks
#             whichever benchmarked value predicts fastest.

@enum Mutability FIXED MUTABLE FREE

"""
    Configurable(name, mutability; getter, setter, transition_cost, benchmark_domain)

A runtime/benchmark-time knob.

  * `getter()` returns the current value (or `nothing` if unset).
  * `setter(value)` applies a new value (MUTABLE only; `nothing` otherwise).
  * `transition_cost(from, to)` estimates the one-way cost, in seconds, of
    changing the value. Charged twice (set + restore) when the selector picks
    a non-current value.
  * `benchmark_domain()` returns the default `Vector` of values to sweep at
    benchmark time. Users can override per-run via `BenchmarkConfig`.
"""
struct Configurable
    name::Symbol
    mutability::Mutability
    getter::Function
    setter::Union{Function,Nothing}
    transition_cost::Function
    benchmark_domain::Function
end

function Configurable(name::Symbol, mutability::Mutability;
                      getter::Function = () -> nothing,
                      setter::Union{Function,Nothing} = nothing,
                      transition_cost::Function = (from, to) -> 0.0,
                      benchmark_domain::Function = () -> Any[])
    if mutability === MUTABLE && setter === nothing
        throw(ArgumentError("MUTABLE configurable $name requires a setter"))
    end
    return Configurable(name, mutability, getter, setter, transition_cost, benchmark_domain)
end

const CONFIGURABLES = Dict{Symbol,Configurable}()

"""
    register_configurable!(c::Configurable)

Register (or replace) a configurable. Users can register their own, e.g. GPU
math-mode toggles or scheduler options, and reference them by name in
benchmark axes; the worker will apply them via their setter for each trial.
"""
register_configurable!(c::Configurable) = (CONFIGURABLES[c.name] = c; c)

configurable(name::Symbol) = get(CONFIGURABLES, name, nothing)

"""
    with_configurables(f, changes::Vector{Tuple{Symbol,Any}})

Apply each `(name, value)` via the configurable's setter, run `f()`, and
restore prior values in reverse order. Unknown names and FIXED configurables
are skipped with a warning.
"""
function with_configurables(f::Function, changes::Vector{<:Tuple})
    applied = Tuple{Configurable,Any}[]
    try
        for (name, value) in changes
            c = configurable(name)
            if c === nothing
                @warn "Autotune: unknown configurable $name; skipping" maxlog=1
                continue
            end
            if c.setter === nothing
                @warn "Autotune: configurable $name has no setter; skipping" maxlog=1
                continue
            end
            old = c.getter()
            (old == value) && continue
            c.setter(value)
            push!(applied, (c, old))
        end
        return f()
    finally
        for (c, old) in Iterators.reverse(applied)
            try
                c.setter(old)
            catch err
                @warn "Autotune: failed to restore configurable $(c.name)" exception=err
            end
        end
    end
end

"""
    transition_cost(name::Symbol, from, to) -> Float64

Estimated seconds to change configurable `name` from `from` to `to`.
Returns 0.0 for unknown names or unchanged values, `Inf` for FIXED knobs.
"""
function transition_cost(name::Symbol, from, to)
    from == to && return 0.0
    c = configurable(name)
    c === nothing && return 0.0
    c.mutability === FIXED && return Inf
    return Float64(c.transition_cost(from, to))
end

# Powers of two up to the machine's hardware thread count (always including
# 1 and CPU_THREADS itself).
function default_thread_domain()
    maxt = Sys.CPU_THREADS
    dom = Int[]
    t = 1
    while t < maxt
        push!(dom, t)
        t *= 2
    end
    push!(dom, maxt)
    return unique!(dom)
end

function register_default_configurables!()
    # Julia's own thread count: fixed once the process starts, but very much
    # worth sweeping at benchmark time (each value = one worker process).
    register_configurable!(Configurable(:nthreads, FIXED;
        getter = () -> Threads.nthreads(),
        benchmark_domain = default_thread_domain))

    # Number of extra Distributed workers. Fixed per benchmark worker process
    # (spawned with `-p N`); default domain is just 0 to bound the sweep -
    # users benchmarking multi-process DArray configurations should extend it.
    register_configurable!(Configurable(:nprocs, FIXED;
        getter = () -> Distributed.nprocs() - 1,
        benchmark_domain = () -> Int[0]))

    # BLAS thread count: cheap to flip at runtime.
    register_configurable!(Configurable(:blas_threads, MUTABLE;
        getter = () -> LinearAlgebra.BLAS.get_num_threads(),
        setter = n -> LinearAlgebra.BLAS.set_num_threads(Int(n)),
        transition_cost = (from, to) -> 5.0e-6,
        benchmark_domain = default_thread_domain))

    return nothing
end
