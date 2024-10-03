# Scoped Options

const options_context = ScopedValue{NamedTuple}(NamedTuple())

"""
    with_options(f, options::NamedTuple) -> Any
    with_options(f; options...) -> Any

Sets one or more options to the given values, executes `f()`, resets the
options to their previous values, and returns the result of `f()`. This is the
recommended way to set options, as it only affects tasks spawned within its
scope. Note that setting an option here will propagate its value across Julia
or Dagger tasks spawned by `f()` or its callees (i.e. the options propagate).
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

Returns the value of the option named `key`. If `option` does not have a value set, then an error will be thrown, unless `default` is set, in which case it will be returned instead of erroring.

    get_options() -> NamedTuple

Returns a `NamedTuple` of all option key-value pairs.
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
