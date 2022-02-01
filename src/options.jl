# Scoped Options

ContextVariablesX.@contextvar options_context::NamedTuple = NamedTuple()

function with_options(f, options::NamedTuple)
    ContextVariablesX.with_context(options_context => options) do
        f()
    end
end
with_options(f; options...) = with_options(f, NamedTuple(options))

get_options() = options_context[]
get_options(key::Symbol) = getproperty(get_options(), key)
function get_options(key::Symbol, default)
    opts = get_options()
    return haskey(opts, key) ? getproperty(opts, key) : default
end
