function istask end
function task_id end

# Use a Set for O(1) membership checks (vs O(n) for Vector).
#
# N.B. This is empty by default (debug tracing is opt-in only, via the
# `JULIA_DAGGER_DEBUG` environment variable or by `push!`-ing a category
# directly), because `@dagdebug` eagerly formats its message string (see
# `_dagdebug_emit` below) whenever its category is in this set, *before*
# Julia's own `@debug`/logger level check gets a chance to skip printing it.
# For hot-path categories like `:execute`, `:move`, `:schedule`, and
# `:processor`, that formatting (which often stringifies types and values)
# is prohibitively expensive to pay on every task, so it must not run unless
# a user has explicitly asked for tracing.
const DAGDEBUG_VALID_CATEGORIES = (:all, :global, :submit, :schedule, :scope,
                                   :take, :execute, :move, :processor, :finish,
                                   :cancel, :stream, :validate)
const DAGDEBUG_CATEGORIES = Set{Symbol}()

# Out-of-line emission keeps call-site IR minimal: just one `in` check + one
# function call per @dagdebug site, regardless of how complex the message is.
@noinline function _dagdebug_emit(thunk, cat_sym::Symbol, msg::String)
    id = -1
    if thunk isa Integer
        id = Int(thunk)
    elseif istask(thunk)
        id = task_id(thunk)
    elseif thunk === nothing
        id = 0
    else
        @warn "Unsupported thunk argument to @dagdebug: $(typeof(thunk))"
        id = -1
    end
    if id > 0
        @debug "[$id] ($cat_sym) $msg" _module=Dagger
    elseif id == 0
        @debug "($cat_sym) $msg" _module=Dagger
    end
end

macro dagdebug(thunk, category, msg, args...)
    cat_sym = category.value
    esc(quote
        if $(QuoteNode(cat_sym)) in $DAGDEBUG_CATEGORIES || :all in $DAGDEBUG_CATEGORIES
            $_dagdebug_emit($thunk, $(QuoteNode(cat_sym)), string($msg))
        end
    end)
end

# FIXME: Calculate fast-growth based on clock time, not iteration
const OPCOUNTER_CATEGORIES = Symbol[]
const OPCOUNTER_FAST_GROWTH_THRESHOLD = Ref(10_000_000)
struct OpCounter
    value::Threads.Atomic{Int}
end
OpCounter() = OpCounter(Threads.Atomic{Int}(0))
macro opcounter(category, count=1)
    cat_sym = category.value
    @gensym old
    opcounter_sym = Symbol(:OPCOUNTER_, cat_sym)
    if !isdefined(__module__, opcounter_sym)
        __module__.eval(:(#=const=# $opcounter_sym = OpCounter()))
    end
    esc(quote
        if $(QuoteNode(cat_sym)) in $OPCOUNTER_CATEGORIES
            $old = Threads.atomic_add!($opcounter_sym.value, Int($count))
            if $old > 1 && (mod1($old, $OPCOUNTER_FAST_GROWTH_THRESHOLD[]) == 1 || $count > $OPCOUNTER_FAST_GROWTH_THRESHOLD[])
                println("Fast-growing counter: $($(QuoteNode(cat_sym))) = $($old)")
            end
        end
    end)
end
opcounter(mod::Module, category::Symbol) = getfield(mod, Symbol(:OPCOUNTER_, category)).value[]