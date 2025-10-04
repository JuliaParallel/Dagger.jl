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

@warn "Make this threadsafe by putting counter into Module" maxlog=1
@warn "Calculate fast-growth based on clock time, not iteration" maxlog=1
const OPCOUNTER_CATEGORIES = Symbol[]
const OPCOUNTER_FAST_GROWTH_THRESHOLD = Ref(10_000_000)
const OPCOUNTERS = Dict{Symbol,Threads.Atomic{Int}}()
macro opcounter(category, count=1)
    cat_sym = category.value
    @gensym old
    esc(quote
        if $(QuoteNode(cat_sym)) in $OPCOUNTER_CATEGORIES
            if !haskey($OPCOUNTERS, $(QuoteNode(cat_sym)))
                $OPCOUNTERS[$(QuoteNode(cat_sym))] = Threads.Atomic{Int}(0)
            end
            $old = Threads.atomic_add!($OPCOUNTERS[$(QuoteNode(cat_sym))], Int($count))
            if $old > 1 && (mod1($old, $OPCOUNTER_FAST_GROWTH_THRESHOLD[]) == 1 || $count > $OPCOUNTER_FAST_GROWTH_THRESHOLD[])
                println("Fast-growing counter: $($(QuoteNode(cat_sym))) = $($old)")
            end
        end
    end)
end
opcounters() = Dict(cat=>OPCOUNTERS[cat][] for cat in keys(OPCOUNTERS))

const LARGEST_VALUE_COUNTER = Ref(0)
function largest_value_update!(value)
    prev = LARGEST_VALUE_COUNTER[]
    if value > prev
        LARGEST_VALUE_COUNTER[] = value
        if value - prev > 10_000 || value > 1_000_000
            println("Largest value growing: $value")
        end
    end
end
largest_value_counter() = LARGEST_VALUE_COUNTER[]