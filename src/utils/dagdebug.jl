const DAGDEBUG_CATEGORIES = Symbol[:global, :submit, :schedule, :scope,
                                   :take, :execute, :move, :processor]
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
            elseif $thunk isa Thunk
                $id = $thunk.id
            elseif $thunk === nothing
                $id = 0
            else
                @warn "Unsupported thunk argument to @dagdebug: $(typeof($thunk))"
                $id = -1
            end
            if $id > 0
                if $(QuoteNode(cat_sym)) in $DAGDEBUG_CATEGORIES
                    $debug_ex_id
                end
            elseif $id == 0
                if $(QuoteNode(cat_sym)) in $DAGDEBUG_CATEGORIES
                    $debug_ex_noid
                end
            end
        end
    end)
end
