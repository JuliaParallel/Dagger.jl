# Reactant.jl integration
#
# Dagger can hand the code within a task, or the algorithm of a whole Datadeps
# region, to Reactant.jl, which compiles it through MLIR/XLA and applies
# optimizations (fusion, layout selection, heterogeneous code generation) that
# Julia's own compiler does not perform.
#
# The user-facing entrypoint is `Dagger.@reactant`, which selects a mode and
# makes it visible to the rest of Dagger through the scoped `reactant` option.
# All of the Reactant-specific logic lives in the ReactantExt extension; the
# functions below are the hooks that it specializes, and their fallbacks run the
# code as usual (after warning once) when Reactant is not loaded.

"""
    Dagger.ReactantMode

Selects how Dagger hands work to Reactant.jl. See [`Dagger.ReactantInner`](@ref)
and [`Dagger.ReactantFull`](@ref).

Every mode carries the two requirements that [`Dagger.@reactant`](@ref) can
impose: `must_opt`, which turns a failure to compile into an error rather than
running the code without Reactant, and `must_load`, which does the same for
Reactant not being loaded. They travel with the mode, so they apply on every
worker that the mode reaches.
"""
abstract type ReactantMode end

"""
    Dagger.ReactantInner(; must_opt=false, must_load=false)

Compile each task's function with Reactant, separately, on the processor that
runs it. The task's arguments are converted to Reactant arrays before the call
and converted back (including writes to mutated arguments) afterwards, so
Dagger's scheduling, data movement, and dependency handling are unchanged.

This is the default mode of [`Dagger.@reactant`](@ref), and the most reliable,
as each task is optimized in isolation. A task whose function Reactant cannot
compile is run without Reactant, unless `must_opt` is set.
"""
Base.@kwdef struct ReactantInner <: ReactantMode
    must_opt::Bool = false
    must_load::Bool = false
end

"""
    Dagger.ReactantFull(; must_opt=false, must_load=false)

Hand the entire algorithm of a [`spawn_datadeps`](@ref) region to Reactant as a
single traced program. Dagger performs no planning or scheduling for the region:
all arguments are pulled to the calling worker, the region's tasks are traced
in submission order, and Reactant is responsible for optimizing and executing
the resulting DAG (which allows it to fuse and reorder across task boundaries).

Results are written back into the region's arguments once execution finishes, and
each of the region's tasks is completed with its result, so the region remains
observationally equivalent to running it under Datadeps. A region which Reactant
cannot compile or run is handed back to Datadeps, which runs it as usual, unless
`must_opt` is set.

Tasks launched outside of a Datadeps region are unaffected by this mode.
"""
Base.@kwdef struct ReactantFull <: ReactantMode
    must_opt::Bool = false
    must_load::Bool = false
end

"""
    Dagger.reactant_mode(mode; must_opt=false, must_load=false) -> ReactantMode

The [`Dagger.ReactantMode`](@ref) that `mode` names: `:inner`, `:full`, or a mode
itself. Requirements given here are added to those the mode already carries.
"""
reactant_mode(mode::ReactantMode; must_opt::Bool=false, must_load::Bool=false) =
    typeof(mode)(must_opt || mode.must_opt, must_load || mode.must_load)
function reactant_mode(mode::Symbol; must_opt::Bool=false, must_load::Bool=false)
    if mode === :inner
        return ReactantInner(; must_opt, must_load)
    elseif mode === :full
        return ReactantFull(; must_opt, must_load)
    end
    throw(ArgumentError("Invalid Reactant mode: $(repr(mode))\nValid modes are :inner and :full"))
end
reactant_mode(mode; must_opt::Bool=false, must_load::Bool=false) =
    throw(ArgumentError("Invalid Reactant mode: $(repr(mode))\nExpected a Symbol (:inner or :full) or a Dagger.ReactantMode"))

"""
    Dagger.ReactantUnavailableError(worker)

Thrown when Reactant.jl is not loaded in the process that needs it, and
`Dagger.@reactant`'s `must_load=true` was used to ask that this be an error rather
than a warning.
"""
struct ReactantUnavailableError <: Exception
    worker::Int
end
ReactantUnavailableError() = ReactantUnavailableError(myid())
Base.showerror(io::IO, err::ReactantUnavailableError) =
    print(io, """ReactantUnavailableError: Reactant.jl is not loaded on worker $(err.worker), and `must_load=true` was requested.
                 Add `using Reactant` (on every worker which will run tasks) to enable Reactant-accelerated execution.""")

"""
    Dagger.ReactantOptimizationError(what, cause)

Thrown when Reactant could not compile or run `what`, and `Dagger.@reactant`'s
`must_opt=true` was used to ask that this be an error rather than a fall back to
running the code without Reactant. `cause` is the failure that Reactant reported.
"""
struct ReactantOptimizationError <: Exception
    what::String
    cause::Any
end
function Base.showerror(io::IO, err::ReactantOptimizationError)
    print(io, "ReactantOptimizationError: Reactant could not optimize $(err.what), and `must_opt=true` was requested.\nCaused by: ")
    showerror(io, err.cause)
    return
end

"Set by ReactantExt when Reactant.jl is loaded into this process."
const REACTANT_LOADED = Ref(false)

"""
    Dagger.reactant_available() -> Bool

Returns `true` if Reactant.jl is loaded in this process, and thus if
[`Dagger.@reactant`](@ref) will actually use Reactant.
"""
reactant_available() = REACTANT_LOADED[]

function warn_reactant_unavailable()
    @warn """Dagger.@reactant was used, but Reactant.jl is not loaded in this process; running without Reactant.
             Add `using Reactant` (on every worker which will run tasks) to enable Reactant-accelerated execution, or `must_load=true` to make this an error.""" maxlog=1
    return
end

"""
    Dagger.@reactant expr
    Dagger.@reactant mode=:inner expr
    Dagger.@reactant mode=:full expr
    Dagger.@reactant must_opt=true expr
    Dagger.@reactant must_load=true expr

Executes `expr` with Reactant.jl integration enabled, so that Dagger tasks
launched by `expr` (including those launched by library code, such as the tasks
of `cholesky(::DArray)`) are compiled and executed by Reactant.

Two modes are available:

- `mode=:inner` (the default): each task's function is compiled by Reactant
  individually, on the processor that runs it. Dagger's scheduling and data
  movement are unchanged. See [`Dagger.ReactantInner`](@ref).
- `mode=:full`: each [`spawn_datadeps`](@ref) region within `expr` is traced as
  a whole and handed to Reactant as a single program, bypassing Dagger's
  planning and scheduling for that region. See [`Dagger.ReactantFull`](@ref).

By default, whatever Reactant cannot do is done without it: if Reactant.jl is not
loaded, a warning is emitted once per session and `expr` runs as it normally
would, and the same goes for individual tasks and regions which Reactant turns
out to be unable to compile. This is what allows the same application code to be
used with and without Reactant, but it also means that a workload can quietly
stop being accelerated. Two options make those cases loud instead:

- `must_opt=true`: a task or region which Reactant cannot compile or run throws a
  [`Dagger.ReactantOptimizationError`](@ref) instead of running without Reactant.
- `must_load=true`: Reactant.jl not being loaded (here, or on a worker that runs
  one of the tasks) throws a [`Dagger.ReactantUnavailableError`](@ref) instead of
  warning.

# Examples

```julia
using Dagger, Reactant

A = rand(Blocks(32, 32), 128, 128)
A = A * A' + 128I
chol = Dagger.@reactant cholesky(A)

# Fail rather than silently run the factorization without Reactant
chol = Dagger.@reactant must_opt=true must_load=true cholesky(A)
```
"""
macro reactant(exs...)
    if isempty(exs)
        throw(ArgumentError("@reactant requires an expression to execute"))
    end
    inner_ex = last(exs)
    mode_ex = QuoteNode(:inner)
    must_opt_ex = false
    must_load_ex = false
    for opt in exs[1:end-1]
        if !(Meta.isexpr(opt, :(=)) && length(opt.args) == 2)
            throw(ArgumentError("@reactant: invalid option `$opt` (expected `name=value`)"))
        end
        name, value_ex = opt.args
        if name === :mode
            mode_ex = value_ex
        elseif name === :must_opt
            must_opt_ex = value_ex
        elseif name === :must_load
            must_load_ex = value_ex
        else
            throw(ArgumentError("@reactant: unknown option `$name` (valid options are `mode`, `must_opt`, and `must_load`)"))
        end
    end
    return quote
        $with_reactant($reactant_mode($(esc(mode_ex));
                                     must_opt=$(esc(must_opt_ex)),
                                     must_load=$(esc(must_load_ex)))) do
            $(esc(inner_ex))
        end
    end
end

"""
    Dagger.with_reactant(f, mode::ReactantMode) -> Any
    Dagger.with_reactant(f, mode; must_opt=false, must_load=false) -> Any

Calls `f()` with Reactant integration enabled in `mode`. This is the function
form of [`Dagger.@reactant`](@ref).
"""
function with_reactant(f, mode::ReactantMode)
    if !reactant_available()
        mode.must_load && throw(ReactantUnavailableError())
        warn_reactant_unavailable()
        return f()
    end
    return with_options(f; reactant=mode)
end
with_reactant(f, mode; kwargs...) = with_reactant(f, reactant_mode(mode; kwargs...))

"""
    Dagger.reactant_execute!(mode::ReactantMode, to_proc, f, args...; kwargs...)

Executes `f(args...; kwargs...)` on `to_proc` through Reactant, as requested by
`mode`. Specialized by the ReactantExt extension; the fallback here runs the
call without Reactant, as this process does not have Reactant loaded.
"""
function reactant_execute!(mode::ReactantMode, to_proc::Processor, f, args...; kwargs...)
    @nospecialize f args kwargs
    mode.must_load && throw(ReactantUnavailableError())
    warn_reactant_unavailable()
    return execute!(to_proc, f, args...; kwargs...)
end

"""
    Dagger.reactant_spawn_datadeps(mode::ReactantMode, f) -> Any

Executes the Datadeps region `f` through Reactant, as requested by `mode`.
Specialized by the ReactantExt extension; the fallback here runs the region
through Datadeps as usual, as this process does not have Reactant loaded.
"""
function reactant_spawn_datadeps(mode::ReactantMode, f)
    mode.must_load && throw(ReactantUnavailableError())
    warn_reactant_unavailable()
    return _spawn_datadeps(f)
end

# Cache of Reactant-compiled programs, filled in by ReactantExt.
#
# N.B. Compilation is serialized under this lock: it is expensive, not
# guaranteed to be thread-safe, and Dagger will happily run many tasks
# concurrently. Executing an already-compiled program happens outside the lock.
const REACTANT_COMPILE_LOCK = Threads.ReentrantLock()
const REACTANT_COMPILE_CACHE = Dict{Any,Any}()

"""
    Dagger.reactant_cache_clear!()

Discards every Reactant program that Dagger has compiled and cached in this
process. Compiled programs are reused across tasks and calls whenever it is safe
to do so, so this is mostly useful for benchmarking compilation, or to release
the memory that they hold.
"""
function reactant_cache_clear!()
    Base.@lock REACTANT_COMPILE_LOCK empty!(REACTANT_COMPILE_CACHE)
    return
end

"""
    Dagger.reactant_cache_size() -> Int

The number of Reactant programs that Dagger currently has cached in this process.
"""
reactant_cache_size() =
    Base.@lock REACTANT_COMPILE_LOCK length(REACTANT_COMPILE_CACHE)
