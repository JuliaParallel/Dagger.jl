# Task options

"""
    Options

Stores per-task options to be passed to the scheduler.

# Arguments
- `propagates::Vector{Symbol}`: The set of option names that will be propagated by this task to tasks that it spawns.
- `acceleration::Acceleration`: The acceleration (cluster/network) type to use for this task.
- `processor::Processor`: The processor associated with this task's function. Generally ignored by the scheduler.
- `compute_scope::AbstractScope`: The execution scope of the task, which determines where the task can be scheduled and executed. `scope` is another name for this option.
- `result_scope::AbstractScope`: The data scope of the task's result, which determines where the task's result can be accessed from.
- `exec_scope::AbstractScope`: The execution scope of the task, which determines where the task can be scheduled and executed. Can be set to avoid computing the scope in the scheduler, when known.
- `single::Int=0`: (Deprecated) Force task onto worker with specified id. `0` disables this option.
- `proclist=nothing`: (Deprecated) Force task to use one or more processors that are instances/subtypes of a contained type. Alternatively, a function can be supplied, and the function will be called with a processor as the sole argument and should return a `Bool` result to indicate whether or not to use the given processor. `nothing` enables all default processors.
- `get_result::Bool=false`: Whether the worker should store the result directly (`true`) or as a `Chunk` (`false`)
- `meta::Bool=false`: When `true`, values are not `move`d, and are passed directly as `Chunk`, if they are not immediate values
- `syncdeps::Set{Any}`: Contains any additional tasks to synchronize with
- `time_util::Dict{Type,Any}`: Indicates the maximum expected time utilization for this task. Each keypair maps a processor type to the utilization, where the value can be a real (approximately the number of nanoseconds taken), or `MaxUtilization()` (utilizes all processors of this type). By default, the scheduler assumes that this task only uses one processor.
- `alloc_util::Dict{Type,UInt64}`: Indicates the maximum expected memory utilization for this task. Each keypair maps a processor type to the utilization, where the value is an integer representing approximately the maximum number of bytes allocated at any one time.
- `occupancy::Dict{Type,Real}`: Indicates the maximum expected processor occupancy for this task. Each keypair maps a processor type to the utilization, where the value can be a real between 0 and 1 (the occupancy ratio, where 1 is full occupancy). By default, the scheduler assumes that this task has full occupancy.
- `checkpoint=nothing`: If not `nothing`, uses the provided function to save the result of the task to persistent storage, for later retrieval by `restore`.
- `restore=nothing`: If not `nothing`, uses the provided function to return the (cached) result of this task, were it to execute.  If this returns a `Chunk`, this task will be skipped, and its result will be set to the `Chunk`.  If `nothing` is returned, restoring is skipped, and the task will execute as usual. If this function throws an error, restoring will be skipped, and the error will be displayed.
- `storage::Union{Chunk,Nothing}=nothing`: If not `nothing`, references a `MemPool.StorageDevice` which will be passed to `MemPool.poolset` internally when constructing `Chunk`s (such as when constructing the return value). The device must support `MemPool.CPURAMResource`. When `nothing`, uses `MemPool.GLOBAL_DEVICE[]`.
- `storage_root_tag::Any=nothing`: If not `nothing`, specifies the MemPool storage leaf tag to associate with the task's result. This tag can be used by MemPool's storage devices to manipulate their behavior, such as the file name used to store data on disk."
- `storage_leaf_tag::Union{MemPool.Tag,Nothing}=nothing`: If not `nothing`, specifies the MemPool storage leaf tag to associate with the task's result. This tag can be used by MemPool's storage devices to manipulate their behavior, such as the file name used to store data on disk."
- `storage_retain::Union{Bool,Nothing}=nothing`: The value of `retain` to pass to `MemPool.poolset` when constructing the result `Chunk`. `nothing` defaults to `false`.
- `name::Union{String,Nothing}=nothing`: If not `nothing`, annotates the task with a name for logging purposes.
- `stream_input_buffer_amount::Union{Int,Nothing}=nothing`: (Streaming only) Specifies the amount of slots to allocate for the input buffer of the task. Defaults to 1.
- `stream_output_buffer_amount::Union{Int,Nothing}=nothing`: (Streaming only) Specifies the amount of slots to allocate for the output buffer of the task. Defaults to 1.
- `stream_buffer_type::Union{Type,Nothing}=nothing`: (Streaming only) Specifies the type of buffer to use for the input and output buffers of the task. Defaults to `Dagger.ProcessRingBuffer`.
- `stream_max_evals::Union{Int,Nothing}=nothing`: (Streaming only) Specifies the maximum number of times the task will be evaluated before returning a result. Defaults to infinite evaluations.
"""
Base.@kwdef mutable struct Options
    propagates::Union{Vector{Symbol},Nothing} = nothing

    acceleration::Union{Acceleration,Nothing} = nothing
    processor::Union{Processor,Nothing} = nothing
    scope::Union{AbstractScope,Nothing} = nothing
    compute_scope::Union{AbstractScope,Nothing} = scope
    result_scope::Union{AbstractScope,Nothing} = nothing
    exec_scope::Union{AbstractScope,Nothing} = nothing
    single::Union{Int,Nothing} = nothing
    proclist = nothing

    get_result::Union{Bool,Nothing} = nothing
    meta::Union{Bool,Nothing} = nothing

    syncdeps::Union{Set{ThunkSyncdep},Nothing} = nothing

    time_util::Union{Dict{Type,Any},Nothing} = nothing
    alloc_util::Union{Dict{Type,UInt64},Nothing} = nothing
    occupancy::Union{Dict{Type,Real},Nothing} = nothing

    checkpoint = nothing
    restore = nothing

    storage::Union{Chunk,Nothing} = nothing
    storage_root_tag = nothing
    storage_leaf_tag::Union{MemPool.Tag,Nothing} = nothing
    storage_retain::Union{Bool,Nothing} = nothing

    name::Union{String,Nothing} = nothing

    stream_input_buffer_amount::Union{Int,Nothing} = nothing
    stream_output_buffer_amount::Union{Int,Nothing} = nothing
    stream_buffer_type::Union{Type, Nothing} = nothing
    stream_max_evals::Union{Int,Nothing} = nothing
end
Options(::Nothing) = Options()
function Options(old_options::NamedTuple)
    new_options = Options()
    options_merge!(new_options, old_options)
    return new_options
end
function Base.copy(old_options::Options)
    new_options = Options()
    options_merge!(new_options, old_options)
    return new_options
end
# Merge b -> a, where b takes precedence
function options_merge!(options::Options, source; override=true)
    _options_merge!(options, source, override)
    return options
end
_has_option(options::Union{Options,NamedTuple}, field) = hasproperty(options, field)
_get_option(options::Union{Options,NamedTuple}, field) = getproperty(options, field)
_set_option!(options::Union{Options,NamedTuple}, field, value) = setproperty!(options, field, value)
_has_option(options::Base.Pairs, field) = haskey(options, field)
_get_option(options::Base.Pairs, field) = options[field]
_set_option!(options::Base.Pairs, field, value) = error("Cannot set option in Base.Pairs")
@generated function _options_merge!(options, source, override)
    ex = Expr(:block)
    for field in fieldnames(Options)
        push!(ex.args, quote
            if _has_option(source, $(QuoteNode(field))) && _get_option(source, $(QuoteNode(field))) !== nothing
                if override || _get_option(options, $(QuoteNode(field))) === nothing
                    _set_option!(options,
                                 $(QuoteNode(field)),
                                 _get_option(source, $(QuoteNode(field))))
                end
            end
        end)
    end
    return ex
end
function Base.setproperty!(options::Options, field::Symbol, value)
    if field == :scope || field == :compute_scope || field == :result_scope
        # If the scope is changed, we need to clear the exec_scope as it is no longer valid
        setfield!(options, :exec_scope, nothing)
    end
    fidx = findfirst(==(field), fieldnames(Options))
    ftype = fieldtypes(Options)[fidx]
    return setfield!(options, field, convert(ftype, value))
end

"""
    populate_defaults!(opts::Options, sig::Vector{DataType}) -> Options

Returns a `Options` with default values filled in for a function call with
signature `sig`, if the option was previously unspecified in `opts`.
"""
function populate_defaults!(opts::Options, sig)
    maybe_default!(opts, Val{:propagates}(), sig)
    maybe_default!(opts, Val{:acceleration}(), sig)
    maybe_default!(opts, Val{:processor}(), sig)
    maybe_default!(opts, Val{:compute_scope}(), sig)
    maybe_default!(opts, Val{:result_scope}(), sig)
    maybe_default!(opts, Val{:exec_scope}(), sig)
    maybe_default!(opts, Val{:single}(), sig)
    maybe_default!(opts, Val{:proclist}(), sig)
    maybe_default!(opts, Val{:get_result}(), sig)
    maybe_default!(opts, Val{:meta}(), sig)
    maybe_default!(opts, Val{:syncdeps}(), sig)
    maybe_default!(opts, Val{:time_util}(), sig)
    maybe_default!(opts, Val{:alloc_util}(), sig)
    maybe_default!(opts, Val{:occupancy}(), sig)
    maybe_default!(opts, Val{:checkpoint}(), sig)
    maybe_default!(opts, Val{:restore}(), sig)
    maybe_default!(opts, Val{:storage}(), sig)
    maybe_default!(opts, Val{:storage_root_tag}(), sig)
    maybe_default!(opts, Val{:storage_leaf_tag}(), sig)
    maybe_default!(opts, Val{:storage_retain}(), sig)
    maybe_default!(opts, Val{:name}(), sig)
    maybe_default!(opts, Val{:stream_input_buffer_amount}(), sig)
    maybe_default!(opts, Val{:stream_output_buffer_amount}(), sig)
    maybe_default!(opts, Val{:stream_buffer_type}(), sig)
    maybe_default!(opts, Val{:stream_max_evals}(), sig)
    return opts
end
function maybe_default!(opts::Options, ::Val{opt}, sig::Signature) where opt
    if getfield(opts, opt) === nothing
        default_opt = get!(SIGNATURE_DEFAULT_CACHE[], (sig.hash_nokw, opt)) do
            Dagger.default_option(Val{opt}(), sig.sig_nokw...)
        end
        setfield!(opts, opt, default_opt)
    end
end

const SIGNATURE_DEFAULT_CACHE = TaskLocalValue{BasicLFUCache{Tuple{UInt,Symbol},Any}}(()->BasicLFUCache{Tuple{UInt,Symbol},Any}(256))

# SchedulerOptions integration

function Dagger.options_merge!(topts::Options, sopts::SchedulerOptions)
    function field_merge!(field)
        if getfield(topts, field) === nothing && getfield(sopts, field) !== nothing
            setfield!(topts, field, getfield(sopts, field))
        end
    end
    field_merge!(:single)
    field_merge!(:proclist)
    return topts
end
function Options(sopts::SchedulerOptions)
    new_options = Options()
    Dagger.options_merge!(new_options, sopts)
    return new_options
end

# Scoped Options

const options_context = ScopedValue{NamedTuple}(NamedTuple())

"""
    with_options(f, options::NamedTuple) -> Any
    with_options(f; options...) -> Any

Sets one or more scoped options to the given values, executes `f()`, resets the
options to their previous values, and returns the result of `f()`. This is the
recommended way to set scoped options, as it only affects tasks spawned within
its scope. Note that setting an option here will propagate its value across
Julia or Dagger tasks spawned by `f()` or its callees (i.e. the options
propagate).
"""
function with_options(f, options::NamedTuple)
    prev_options = options_context[]
    with(options_context => merge(prev_options, options)) do
        f()
    end
end
with_options(f; options...) = with_options(f, NamedTuple(options))

function _without_options(f)
    with(options_context => NamedTuple()) do
        f()
    end
end

"""
    get_options(key::Symbol, default) -> Any
    get_options(key::Symbol) -> Any

Returns the value of the scoped option named `key`. If `option` does not have a
value set, then an error will be thrown, unless `default` is set, in which case
it will be returned instead of erroring.

    get_options() -> NamedTuple

Returns a `NamedTuple` of all scoped option key-value pairs.
"""
get_options() = options_context[]
get_options(key::Symbol) = getproperty(get_options(), key)
function get_options(key::Symbol, default)
    opts = get_options()
    return haskey(opts, key) ? getproperty(opts, key) : default
end

# Dispatch-based setters

"""
    default_option(::Val{name}, Tf, Targs...) where name = value

Defines the default value for option `name` to `value` when Dagger is preparing
to execute a function with type `Tf` with the argument types `Targs`. Users and
libraries may override this to set default values for tasks.

An easier way to define these defaults is with [`@option`](@ref).

Note that the actual task's argument values are not passed, as it may not
always be possible or efficient to gather all Dagger task arguments on one
worker.

This function may be executed within the scheduler, so it should generally be
made very cheap to execute. If the function throws an error, the scheduler will
use whatever the global default value is for that option instead.
"""
default_option(::Val{name}, Tf, Targs...) where name = nothing
default_option(::Val) = throw(ArgumentError("default_option requires a function type and any argument types"))

"""
    @option name myfunc(A, B, C) = value

A convenience macro for defining [`default_option`](@ref). For example:

```julia
Dagger.@option single mylocalfunc(Int) = 1
```

The above call will set the `single` option to `1` for any Dagger task calling
`mylocalfunc(Int)` with an `Int` argument.
"""
macro option(name, ex)
    @capture(ex, f_(args__) = value_)
    args = esc.(args)
    argsyms = map(_->gensym(), args)
    _args = map(arg->:(::$Type{$(argsyms[arg[1]])}), enumerate(args))
    argsubs = map(arg->:($(argsyms[arg[1]])<:$(arg[2])), enumerate(args))
    quote
        Dagger.default_option(::$Val{$name}, ::Type{$typeof($(esc(f)))}, $(_args...)) where {$(argsubs...)} = $(esc(value))
    end
end
