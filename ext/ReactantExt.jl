module ReactantExt

import Dagger
import Dagger: ReactantMode, ReactantInner, ReactantFull
import Dagger: ReactantOptimizationError
import Dagger: Chunk, DTask, DTaskPair, Processor, In, Out, InOut, Deps
import Dagger: ScopedValue, with
import Dagger: REACTANT_COMPILE_LOCK, REACTANT_COMPILE_CACHE

import Adapt

import MemPool

import Reactant

import LinearAlgebra

function __init__()
    Dagger.REACTANT_LOADED[] = true
    return
end

#############################################################################
# Conversion between plain Julia values and Reactant values
#############################################################################

"Element types which Reactant can represent as device buffers."
const RElType = Union{Bool,
                      Int8, Int16, Int32, Int64,
                      UInt8, UInt16, UInt32, UInt64,
                      Float16, Float32, Float64,
                      ComplexF32, ComplexF64}

"""
    is_traceable(x) -> Bool

Whether `x` is an array which should be handed to Reactant as a device buffer,
and thus traced. Everything else is left alone, which means Reactant treats it as
a constant of the program it compiles.
"""
function is_traceable(@nospecialize(x))
    x isa Reactant.RArray && return false
    x isa AbstractArray || return false
    eltype(x) <: RElType || return false
    return x isa StridedArray
end

to_reactant_array(x::StridedArray) = Reactant.to_rarray(x isa Array ? x : Array(x))

"""
    ToReactant()
    ToReactant(writebacks)

An Adapt.jl adaptor which replaces the arrays within a value by Reactant arrays,
leaving everything Reactant cannot hold in a buffer alone. Adapt is what walks the
value, so arrays nested in tuples, named tuples, and other containers are
converted too; arrays themselves are converted whole, including `view`s and other
wrappers, rather than by adapting what they wrap.

Given a `writebacks` vector, each converted array is recorded there alongside the
array it came from, which is what [`write_back!`](@ref) later uses to make writes
to those buffers visible again.
"""
struct ToReactant
    writebacks::Union{Vector{Pair{Any,Any}},Nothing}
    # An array passed twice must stay one buffer, or a task which writes to it
    # through one argument would not see the write through the other
    converted::IdDict{Any,Any}

    ToReactant(writebacks=nothing) = new(writebacks, IdDict{Any,Any}())
end

function convert_array(to::ToReactant, @nospecialize(x::AbstractArray))
    is_traceable(x) || return x
    return get!(to.converted, x) do
        converted = to_reactant_array(x)
        if to.writebacks !== nothing
            push!(to.writebacks, x => converted)
        end
        return converted
    end
end

Adapt.adapt_storage(to::ToReactant, @nospecialize(x::AbstractArray)) = convert_array(to, x)

# Adapt would convert an array wrapper by rebuilding it around its converted
# parent, which is not what a task's argument should become: Datadeps runs tasks
# which write to disjoint `view`s of one array concurrently, and writing the whole
# parent back on behalf of each of them would lose all but one of their updates.
# Each array argument therefore becomes a buffer of its own, whatever it wraps.
for Wrapper in (SubArray, PermutedDimsArray, Base.ReshapedArray, Base.LogicalIndex,
                Base.NonReshapedReinterpretArray, Base.ReshapedReinterpretArray,
                LinearAlgebra.Adjoint, LinearAlgebra.Transpose,
                LinearAlgebra.LowerTriangular, LinearAlgebra.UnitLowerTriangular,
                LinearAlgebra.UpperTriangular, LinearAlgebra.UnitUpperTriangular,
                LinearAlgebra.Diagonal, LinearAlgebra.Tridiagonal, LinearAlgebra.Symmetric)
    @eval Adapt.adapt_structure(to::ToReactant, @nospecialize(x::$Wrapper)) =
        convert_array(to, x)
end

"An Adapt.jl adaptor which replaces Reactant values by plain Julia ones."
struct FromReactant end

Adapt.adapt_storage(::FromReactant, x::Reactant.AbstractConcreteArray) = Array(x)
Adapt.adapt_storage(::FromReactant, x::Reactant.AbstractConcreteNumber{T}) where T =
    convert(T, x)

# Adapt treats an `Array` as a leaf, but a compiled program can return one whose
# elements are Reactant values: Dagger's reductions, for instance, wrap each
# partial result in a 1x1 array. Those elements have to be converted too, or a
# Reactant value ends up in what `sum(::DArray)` hands back to the caller.
function Adapt.adapt_structure(to::FromReactant, x::Array)
    isbitstype(eltype(x)) && return x
    return map(element -> Adapt.adapt(to, element), x)
end

to_reactant(@nospecialize(x), adaptor::ToReactant=ToReactant()) = Adapt.adapt(adaptor, x)
from_reactant(@nospecialize(x)) = Adapt.adapt(FromReactant(), x)

"""
    write_back!(writebacks)

Copies the contents of each Reactant array recorded in `writebacks` back into the
array it was converted from. Reactant writes the results of a mutating computation
into the buffers it was given, so this is what makes in-place task functions
(`BLAS.gemm!` and friends) observable to Dagger.
"""
function write_back!(writebacks::Vector{Pair{Any,Any}})
    for (orig, converted) in writebacks
        copyto!(orig, Array(converted))
    end
    return
end

#############################################################################
# Compilation cache
#############################################################################

"""
    Uncacheable

Marks a value which Reactant bakes into a compiled program in a way we cannot
summarize cheaply, making the enclosing call ineligible for caching.
"""
struct Uncacheable end

"""
    cache_key(x) -> key

A summary of `x` which distinguishes any two values that Reactant would compile
differently.

Whatever Reactant bakes into a compiled executable - constants, and the closure
that the resulting program carries - must be *identical* for a cached executable
to be valid, so those values participate in the key by value. Reactant arrays are
passed in at call time, so only their type and size matter; the size does matter,
because an executable is compiled for fixed shapes.

A mutable value is summarized as [`Uncacheable`](@ref), since whatever was baked
into the program could since have been written to. That is what makes a task
function which captures a `DArray` - as the stages of a `DArray` broadcast do -
compile afresh on every call.
"""
cache_key(x::Reactant.AbstractConcreteArray) = (typeof(x), size(x))
cache_key(x::Reactant.AbstractConcreteNumber) = typeof(x)
cache_key(x::Union{Number,Char,Symbol,AbstractString,Type,Nothing,Missing}) = x
cache_key(x::Tuple) = map(cache_key, x)
cache_key(x::NamedTuple) = (typeof(x), map(cache_key, values(x)))
function cache_key(x::T) where T
    # `Array` and friends land here too, and are `Uncacheable` for being mutable:
    # what Reactant baked into the program is their contents
    (isstructtype(T) && !ismutabletype(T)) || return Uncacheable()
    nfields = fieldcount(T)
    nfields == 0 && return T
    return (T, ntuple(idx -> isdefined(x, idx) ? cache_key(getfield(x, idx)) : Uncacheable(),
                      nfields))
end

is_cacheable(::Uncacheable) = false
is_cacheable(x::Tuple) = all(is_cacheable, x)
is_cacheable(@nospecialize(x)) = true

function compile_program(f, args::Tuple, kwargs::NamedTuple)
    Dagger.@dagdebug nothing :reactant "Compiling $(typeof(f)) for Reactant"
    return Reactant.compile(f, args; fn_kwargs=kwargs, sync=true)
end

"""
    compiled_program(key, f, args, kwargs) -> program

Returns a Reactant-compiled version of `f(args...; kwargs...)`, reusing a
previously compiled one when `key` says that it is safe to do so. A `key` which
is not [`is_cacheable`](@ref) forces a fresh compilation.
"""
function compiled_program(key, f, args::Tuple, kwargs::NamedTuple)
    # Compilation happens under the lock even when its result cannot be cached,
    # since what the lock is for is keeping Reactant from compiling on several
    # threads at once
    return Base.@lock REACTANT_COMPILE_LOCK begin
        is_cacheable(key) || return compile_program(f, args, kwargs)
        get!(REACTANT_COMPILE_CACHE, key) do
            compile_program(f, args, kwargs)
        end
    end
end

# Functions which Reactant was unable to compile, and which are therefore run
# directly from now on. Keyed by function type, as traceability is a property of
# the code a function runs rather than of the arguments it is called with.
const UNTRACEABLE = Set{Type}()

is_untraceable(@nospecialize(F::Type)) =
    Base.@lock REACTANT_COMPILE_LOCK (F in UNTRACEABLE)

"How a task's function is named in the warnings and errors that mention it."
function describe_callable(@nospecialize(F::Type))
    if F <: Function && isdefined(F, :instance)
        return "the task function `$(nameof(F.instance))`"
    end
    # A closure, or a callable object: there is no name worth printing, and the
    # type at least says which captures it was compiled for
    return "the task function of type `$F`"
end

function mark_untraceable!(@nospecialize(F::Type), err, bt)
    fresh = Base.@lock REACTANT_COMPILE_LOCK begin
        F in UNTRACEABLE ? false : (push!(UNTRACEABLE, F); true)
    end
    fresh || return
    # Only the error itself, as tracing failures tend to come with backtraces
    # thousands of frames deep; the full report is available with the `:reactant`
    # debug category enabled
    @warn """Reactant could not compile $(describe_callable(F)), so it will be run without Reactant.
             This is expected for task functions which access their arrays elementwise, such as the kernels of `Dagger.@stencil`.
             Pass `must_opt=true` to make this an error instead.""" exception=err
    Dagger.@dagdebug nothing :reactant "Tracing $F failed:\n$(sprint(showerror, err, bt))"
    return
end

#############################################################################
# Inner mode: compile each task's function individually
#############################################################################

"""
    ReactantCall(f, must_opt)

Wraps a task's function so that it is compiled and executed by Reactant. Kept as
a callable, rather than calling Reactant directly from
`Dagger.reactant_execute!`, so that the call still goes through
`Dagger.execute!` and thus retains all of the processor-specific setup Dagger
normally performs around a task's function.
"""
struct ReactantCall{F}
    f::F
    must_opt::Bool
end

function (rc::ReactantCall)(args...; kwargs...)
    # A function known to be untraceable is not attempted again, unless the caller
    # asked to be told about it
    if !rc.must_opt && is_untraceable(typeof(rc.f))
        return rc.f(args...; kwargs...)
    end
    writebacks = Pair{Any,Any}[]
    adaptor = ToReactant(writebacks)
    rargs = to_reactant(args, adaptor)
    rkwargs = to_reactant((; kwargs...), adaptor)
    key = (cache_key(rc.f), cache_key(rargs), cache_key(rkwargs))
    # Tracing failures are recoverable: the arguments Reactant was given are
    # copies, so nothing the task was passed has been touched yet
    program, failure = try
        compiled_program(key, rc.f, rargs, rkwargs), nothing
    catch err
        nothing, (err, catch_backtrace())
    end
    if failure !== nothing
        err, bt = failure
        # Thrown from out here, rather than from the handler above, so that the
        # exception Dagger reports is this one rather than its cause
        rc.must_opt && throw(ReactantOptimizationError(describe_callable(typeof(rc.f)), err))
        mark_untraceable!(typeof(rc.f), err, bt)
        return rc.f(args...; kwargs...)
    end
    result = program(rargs...)
    # Reflect any writes the compiled program made to its arguments
    write_back!(writebacks)
    converted = from_reactant(result)
    Dagger.@dagdebug nothing :reactant "Ran $(typeof(rc.f)), returning $(typeof(result)) as $(typeof(converted))"
    return converted
end

function Dagger.reactant_execute!(mode::ReactantInner, to_proc::Processor, f, args...; kwargs...)
    @nospecialize f args kwargs
    return Dagger.execute!(to_proc, ReactantCall(f, mode.must_opt), args...; kwargs...)
end

#############################################################################
# Full mode: hand a whole Datadeps region to Reactant
#############################################################################

# Set while a Datadeps region is being captured or traced, so that regions nested
# within it become part of the same program instead of starting their own
const FULL_ACTIVE = ScopedValue{Bool}(false)

"""
    CaptureQueue()

A task queue which records tasks without launching them, used to observe the raw
algorithm of a Datadeps region: no planning, no scheduling, no data movement.
"""
struct CaptureQueue <: Dagger.AbstractTaskQueue
    pairs::Vector{DTaskPair}

    CaptureQueue() = new(DTaskPair[])
end
Dagger.enqueue!(queue::CaptureQueue, pair::DTaskPair) = push!(queue.pairs, pair)
Dagger.enqueue!(queue::CaptureQueue, pairs::Vector{DTaskPair}) = append!(queue.pairs, pairs)

"""
    RegionUnsupported(reason)

Thrown when a Datadeps region cannot be expressed as a Reactant program, which
makes Dagger run the region itself instead (see [`fall_back!`](@ref)).
"""
struct RegionUnsupported <: Exception
    reason::String
end
Base.showerror(io::IO, err::RegionUnsupported) = print(io, err.reason)

# Where each argument of a captured task comes from when the region is traced
struct FromInput
    idx::Int
end
struct FromTask
    idx::Int
end
struct FromConstant
    value::Any
end

struct RegionCall
    f::Any
    args::Vector{Any}
    kwargs::Vector{Pair{Symbol,Any}}
end

"""
    RegionProgram

The raw algorithm of a Datadeps region: its tasks in submission order, with every
argument resolved to one of the region's inputs, the result of an earlier task, or
a constant.

`sources` records where each input came from (a `Chunk`, or an array passed
directly to a task) so that results can be published back to it, and `data` holds
the input's data as pulled to the calling worker.
"""
struct RegionProgram
    calls::Vector{RegionCall}
    sources::Vector{Any}
    data::Vector{Any}
end
RegionProgram() = RegionProgram(RegionCall[], Any[], Any[])

"Identity of a region input which isn't backed by a `Chunk`."
struct ObjectKey
    id::UInt
end

# Two `Chunk`s referring to the same `DRef` are the same input
input_key(chunk::Chunk) = chunk.handle
input_key(@nospecialize(x)) = ObjectKey(objectid(x))

pull_local(chunk::Chunk) = Dagger.move(Dagger.OSProc(), chunk)
pull_local(@nospecialize(x)) = x

unwrap_dep(dep::In) = dep.x
unwrap_dep(dep::Out) = dep.x
unwrap_dep(dep::InOut) = dep.x
unwrap_dep(dep::Deps) = dep.x
unwrap_dep(@nospecialize(x)) = x

is_function_argument(arg) = Dagger.ispositional(arg) && Dagger.raw_position(arg) == 0

"""
    build_program(pairs) -> RegionProgram

Turns the tasks captured from a Datadeps region into a program that can be traced:
their functions, in submission order, with each argument resolved to one of the
region's inputs, the result of an earlier task, or a constant.

The `In`/`Out`/`InOut` annotations play no part in this. Datadeps guarantees that
a region behaves as if its tasks ran sequentially in submission order, which is
exactly what tracing them in that order produces; Reactant is then free to
recover the parallelism from the data flow it can see.
"""
function build_program(pairs::Vector{DTaskPair})
    program = RegionProgram()
    input_indices = Dict{Any,Int}()
    task_indices = IdDict{DTask,Int}()

    for pair in pairs
        f = nothing
        args = Any[]
        kwargs = Pair{Symbol,Any}[]
        for arg in pair.spec.fargs
            value = Dagger.value(arg)
            if is_function_argument(arg)
                if value isa DTask
                    throw(RegionUnsupported("a task's function is itself the result of a task"))
                end
                f = value isa Chunk ? pull_local(value) : value
                continue
            end
            source = describe_argument!(program, input_indices, task_indices, value)
            if Dagger.ispositional(arg)
                push!(args, source)
            else
                push!(kwargs, Dagger.pos_kw(arg) => source)
            end
        end
        push!(program.calls, RegionCall(f, args, kwargs))
        task_indices[pair.task] = length(program.calls)
    end

    return program
end

function describe_argument!(program::RegionProgram, input_indices, task_indices, value)
    value = unwrap_dep(value)

    if value isa DTask
        idx = get(task_indices, value, nothing)
        idx === nothing || return FromTask(idx)
        # A task from outside this region, such as a `DArray` chunk which is
        # still the task that produced it; its result is an input to the region
        if !Base.istaskstarted(value)
            throw(RegionUnsupported("a task argument was neither created within this region nor launched"))
        end
        value = fetch(value; raw=true)
    end

    if value isa Chunk || is_traceable(value)
        key = input_key(value)
        idx = get(input_indices, key, nothing)
        if idx === nothing
            data = pull_local(value)
            is_traceable(data) || return as_constant(data)
            push!(program.sources, value)
            push!(program.data, data)
            idx = length(program.data)
            input_indices[key] = idx
        end
        return FromInput(idx)
    end

    return as_constant(value)
end

# Anything Reactant cannot hold in a buffer is baked into the program instead,
# which is only sound if it cannot change: a mutable argument (a `Ref` used as a
# scalar output, say) would be written to while tracing and never again
function as_constant(@nospecialize(value))
    if ismutable(value)
        throw(RegionUnsupported("a task takes a mutable $(typeof(value)), which Reactant cannot write to"))
    end
    return FromConstant(value)
end

program_cache_key(program::RegionProgram) =
    (:reactant_full,
     Tuple(map(call_cache_key, program.calls)),
     Tuple(map(data -> (typeof(data), size(data)), program.data)))
call_cache_key(call::RegionCall) =
    (cache_key(call.f),
     Tuple(map(argument_cache_key, call.args)),
     Tuple(map(kwarg -> (first(kwarg), argument_cache_key(last(kwarg))), call.kwargs)))
argument_cache_key(arg::FromInput) = arg
argument_cache_key(arg::FromTask) = arg
argument_cache_key(arg::FromConstant) = cache_key(arg.value)

"""
    TracedTask(value)

Stands in for a `DTask` while code is being traced by Reactant. Since tasks are
executed inline into the trace, the task's result is already available.
"""
struct TracedTask{T}
    value::T
end
Base.fetch(task::TracedTask; kwargs...) = task.value
Base.wait(::TracedTask) = nothing
Base.isready(::TracedTask) = true

# Where a task's result is to be found once the compiled program has run
struct ResultOutput
    idx::Int    # position in the program's return value
end
struct ResultInput
    idx::Int    # one of the region's inputs, which the program wrote in place
end
struct ResultConstant
    value::Any  # what the result evaluated to while tracing
end

"""
    RegionTrace(program)

The bookkeeping that [`trace_region`](@ref) fills in while a region is traced:
where each task's result is to be found afterwards, and the traced values that
the compiled program must return for that to be possible.
"""
struct RegionTrace
    program::RegionProgram
    results::Vector{Any}
    outputs::Vector{Any}
end
RegionTrace(program::RegionProgram) =
    RegionTrace(program, Vector{Any}(undef, length(program.calls)), Any[])

"A Reactant-compiled Datadeps region, together with how to read its results."
struct CompiledRegion
    program::Any
    results::Vector{Any}
end
CompiledRegion(compiled, trace::RegionTrace) = CompiledRegion(compiled, trace.results)

is_traced(@nospecialize(x)) = x isa Reactant.TracedRArray || x isa Reactant.TracedRNumber
contains_traced(@nospecialize(x)) = is_traced(x)
contains_traced(x::Union{Tuple,NamedTuple}) = any(contains_traced, x)

# Values which a compiled program produces identically on every execution, and
# which can therefore be recorded once, while tracing
is_trace_constant(@nospecialize(x)) =
    x isa Union{Nothing,Missing,Number,Char,Symbol,AbstractString,Type}
is_trace_constant(x::Union{Tuple,NamedTuple}) = all(is_trace_constant, x)

"""
    classify_result!(trace, result, input_ids) -> ResultOutput | ResultInput | ResultConstant

Decides how `result`, produced by a task while tracing, will be recovered once the
compiled program has run.

A result which is one of the program's own buffers - which is what the in-place
tasks of a Datadeps region typically return - is read back from that buffer. This
matters for more than tidiness: returning every task's result separately would
cost a buffer per task, which for an algorithm like a blocked Cholesky is far more
memory than the arrays being factored.

A result which is not traced at all was baked into the program by Reactant, so it
is the same on every execution and can be recorded here. Anything else - a plain
array, say, which may well have been freshly computed - would not be, so it makes
the region unsupported.
"""
function classify_result!(trace::RegionTrace, @nospecialize(result), input_ids::IdDict{Any,Int})
    if contains_traced(result)
        idx = is_traced(result) ? get(input_ids, result, 0) : 0
        idx == 0 || return ResultInput(idx)
        push!(trace.outputs, result)
        return ResultOutput(length(trace.outputs))
    elseif is_trace_constant(result)
        return ResultConstant(result)
    end
    throw(RegionUnsupported("a task returned a $(typeof(result)), which Reactant cannot return from a compiled program"))
end

# The trace currently being built. Read at trace time only, which is what lets
# `trace_region` below be a plain function: were the region carried in a closure
# instead, Reactant would trace through it (and through every `Chunk` and `DArray`
# that the region references) on every compilation.
const CURRENT_TRACE = ScopedValue{Union{RegionTrace,Nothing}}(nothing)

resolve_argument(arg::FromInput, inputs, results) = inputs[arg.idx]
resolve_argument(arg::FromTask, inputs, results) = results[arg.idx]
resolve_argument(arg::FromConstant, inputs, results) = arg.value

function trace_region(inputs::Vararg{Any,N}) where N
    trace = CURRENT_TRACE[]::RegionTrace
    program = trace.program

    # Reactant may trace a program more than once, so start from a clean slate
    empty!(trace.outputs)
    input_ids = IdDict{Any,Int}()
    for (idx, input) in enumerate(inputs)
        input_ids[input] = idx
    end

    results = Vector{Any}(undef, length(program.calls))
    for (idx, call) in enumerate(program.calls)
        args = Any[resolve_argument(arg, inputs, results) for arg in call.args]
        kwargs = NamedTuple(key => resolve_argument(arg, inputs, results)
                            for (key, arg) in call.kwargs)
        result = call.f(args...; kwargs...)
        results[idx] = result
        # Note where each task's result will be found, so that the task it came
        # from can be completed with it and thus be `fetch`ed as usual
        trace.results[idx] = classify_result!(trace, result, input_ids)
    end
    return Tuple(trace.outputs)
end

"""
    compiled_region(program, inputs) -> CompiledRegion

Compiles `program` for `inputs`, reusing a previously compiled region when it is
safe to do so (see [`cache_key`](@ref)).
"""
function compiled_region(program::RegionProgram, inputs::Tuple)
    trace = RegionTrace(program)
    compile() = with(CURRENT_TRACE => trace) do
        CompiledRegion(compile_program(trace_region, inputs, NamedTuple()), trace)
    end
    key = program_cache_key(program)
    return Base.@lock REACTANT_COMPILE_LOCK begin
        is_cacheable(key) || return compile()
        get!(compile, REACTANT_COMPILE_CACHE, key)::CompiledRegion
    end
end

function Dagger.reactant_spawn_datadeps(mode::ReactantFull, f)
    # A nested region is already part of the enclosing region's program
    FULL_ACTIVE[] && return f()
    return with(FULL_ACTIVE => true) do
        run_full_region(f, mode.must_opt)
    end
end

function run_full_region(f, must_opt::Bool)
    # Capture the region's raw algorithm, without planning or scheduling it
    queue = CaptureQueue()
    result = Dagger.with_options(f; task_queue=queue)
    pairs = queue.pairs
    isempty(pairs) && return result

    # Nothing below touches the region's own data until the program has run to
    # completion (Reactant works on copies of it), so any failure along the way
    # can still be answered by handing the region back to Dagger
    local region, outputs, updated
    failure = nothing
    try
        program = build_program(pairs)
        Dagger.@dagdebug nothing :reactant "Tracing $(length(program.calls)) task(s) over $(length(program.data)) input(s)"
        inputs = ntuple(idx -> to_reactant_array(program.data[idx]), length(program.data))
        region = compiled_region(program, inputs)
        outputs = region.program(inputs...)
        # Publish the results back to where the region's arguments live
        updated = map(Array, inputs)
        for idx in 1:length(program.sources)
            writeback_input!(program.sources[idx], updated[idx])
        end
    catch err
        failure = (err, catch_backtrace())
    end
    if failure !== nothing
        err, bt = failure
        # Thrown from out here, rather than from the handler above, so that the
        # exception Dagger reports is this one rather than its cause
        must_opt && throw(ReactantOptimizationError("a Datadeps region of $(length(pairs)) task(s)", err))
        fall_back!(pairs, err, bt)
        return result
    end

    complete_tasks!(pairs, region, outputs, updated)

    return result
end

# Make each captured task's result available, as the scheduler would have
function complete_tasks!(pairs::Vector{DTaskPair}, region::CompiledRegion,
                         outputs::Tuple, updated::Tuple)
    for (idx, pair) in enumerate(pairs)
        value = result_value(region.results[idx], outputs, updated)
        Dagger.complete_unlaunched!(pair.task, Dagger.tochunk(value))
    end
    return
end

result_value(result::ResultOutput, outputs, updated) = from_reactant(outputs[result.idx])
result_value(result::ResultInput, outputs, updated) = updated[result.idx]
result_value(result::ResultConstant, outputs, updated) = result.value

"""
    fall_back!(pairs, err, bt)

Runs the region made up of `pairs` through Datadeps, because Reactant could not
run it (`err`). Regions differ widely in what they ask of Reactant, and a region
it cannot handle is not a reason to fail: the same code should keep working, just
without Reactant.
"""
function fall_back!(pairs::Vector{DTaskPair}, err, bt)
    @warn """Reactant could not run a Datadeps region, so it will be run by Dagger instead.
             Enable the `:reactant` debug category for the regions and errors involved, or pass `must_opt=true` to make this an error.""" exception=err maxlog=1
    Dagger.@dagdebug nothing :reactant "Region of $(length(pairs)) task(s) failed:\n$(sprint(showerror, err, bt))"
    Dagger.launch_datadeps_tasks!(pairs)
    return
end

function writeback_input!(source::Chunk, updated)
    MemPool.access_ref(source.handle, updated) do stored, updated
        stored === updated || copyto!(stored, updated)
        return
    end
    return
end
writeback_input!(source, updated) = (copyto!(source, updated); nothing)

#############################################################################
# Overlays: how Dagger's API behaves within Reactant-traced code
#############################################################################

# Traced code has no scheduler to submit to and no worker to run on, so a spawned
# task is simply executed inline, becoming part of the program being traced.
Reactant.@reactant_overlay function Dagger.spawn(f, args...; kwargs...)
    return traced_spawn(f, args, kwargs)
end
Reactant.@reactant_overlay function Dagger.typed_spawn(f, args...; kwargs...)
    return traced_spawn(f, args, kwargs)
end

function traced_spawn(f, args, kwargs)
    if length(args) >= 1 && first(args) isa Dagger.Options
        args = args[2:end]
    end
    new_args = map(traced_argument, args)
    new_kwargs = NamedTuple(key => traced_argument(value) for (key, value) in kwargs)
    return TracedTask(f(new_args...; new_kwargs...))
end

function traced_argument(arg)
    arg = unwrap_dep(arg)
    arg isa TracedTask && return arg.value
    arg isa Chunk && return pull_local(arg)
    return arg
end

# `task_processor` is defined in terms of a running `DTask`, which traced code
# need not be; report the processor that the trace is being built on instead.
Reactant.@reactant_overlay Dagger.task_processor() =
    Dagger.in_task() ? Dagger.get_tls().processor :
                       Dagger.ThreadProc(Dagger.myid(), Threads.threadid())

#############################################################################
# Kernels which Reactant cannot trace as written
#############################################################################

# `LAPACK.potrf!` is a `ccall` into LAPACK, which Reactant cannot trace, so
# Dagger's Cholesky panel factorization gets a traced implementation here.
#
# `info` is returned as a plain `0`, rather than a traced value, so that the
# positive-definiteness check in `potrf_checked!` remains a trace-time branch;
# the consequence is that a non-positive-definite matrix is not detected under
# Reactant. XLA's Cholesky also zeroes the triangle that `potrf!` would have left
# untouched, which is unobservable through the `Cholesky` factorization object
# that Dagger returns.
function Dagger.potrf_checked!(uplo, A::Reactant.AnyTracedRArray{T,2}, info_arr) where T
    lower = is_lower(uplo)
    factors = Reactant.Ops.cholesky(Reactant.TracedUtils.materialize_traced_array(A); lower)
    copyto!(A, factors)
    return A, 0
end

is_lower(uplo::AbstractChar) = uplo == 'L' || uplo == 'l'

# Reactant lowers `BLAS.syrk!` to an `enzymexla.blas_syrk` op which, as of
# Reactant v0.2.279, interprets `uplo` in row-major terms and so updates the
# opposite triangle of `C` from what BLAS does. Dagger's Cholesky relies on
# `syrk!` for its trailing updates, so compute it here from operations whose
# behavior is unambiguous - the same way Reactant itself implements `syr2k!` and
# `herk!`.
Reactant.@reactant_overlay function LinearAlgebra.BLAS.syrk!(uplo::AbstractChar,
                                                             trans::AbstractChar,
                                                             alpha::Number,
                                                             A::Reactant.AnyTracedRMatrix,
                                                             beta::Number,
                                                             C::Reactant.AnyTracedRMatrix)
    A = Reactant.TracedUtils.materialize_traced_array(A)
    product = trans == 'N' || trans == 'n' ? A * transpose(A) : transpose(A) * A
    updated = alpha .* product .+ beta .* C
    if is_lower(uplo)
        LinearAlgebra.LowerTriangular(C) .= LinearAlgebra.LowerTriangular(updated)
    else
        LinearAlgebra.UpperTriangular(C) .= LinearAlgebra.UpperTriangular(updated)
    end
    return C
end

end # module
