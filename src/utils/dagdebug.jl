function istask end
function task_id end

const DAGDEBUG_CATEGORIES = Symbol[:global, :submit, :schedule, :scope,
                                   :take, :execute, :move, :processor, :finish,
                                   :cancel, :stream]
macro dagdebug(thunk, category, msg, args...)
    cat_sym = category.value
    @gensym id
    debug_ex_id = :(@debug "[$($id)] ($($(repr(cat_sym)))) $($msg)" _module=Dagger _file=$(string(__source__.file)) _line=$(__source__.line))
    append!(debug_ex_id.args, args)
    debug_ex_noid = :(@debug "($($(repr(cat_sym)))) $($msg)" _module=Dagger _file=$(string(__source__.file)) _line=$(__source__.line))
    append!(debug_ex_noid.args, args)
    esc(quote
        let $id = -1
            if $thunk isa Integer
                $id = Int($thunk)
            elseif $istask($thunk)
                $id = $task_id($thunk)
            elseif $thunk === nothing
                $id = 0
            else
                @warn "Unsupported thunk argument to @dagdebug: $(typeof($thunk))"
                $id = -1
            end
            if $id > 0
                if $(QuoteNode(cat_sym)) in $DAGDEBUG_CATEGORIES || :all in $DAGDEBUG_CATEGORIES
                    $debug_ex_id
                end
            elseif $id == 0
                if $(QuoteNode(cat_sym)) in $DAGDEBUG_CATEGORIES || :all in $DAGDEBUG_CATEGORIES
                    $debug_ex_noid
                end
            end
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