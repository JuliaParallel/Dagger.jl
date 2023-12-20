function istask end
function task_id end

const DAGDEBUG_CATEGORIES = Symbol[:global, :submit, :schedule, :scope,
                                   :take, :execute, :move, :processor]
macro dagdebug(thunk, category, msg, args...)
    cat_sym = category.value
    @gensym id dolog
    debug_ex_id = :(@debug "[$($id)] ($($(repr(cat_sym)))) $($msg)" _module=Dagger _file=$(string(__source__.file)) _line=$(__source__.line))
    append!(debug_ex_id.args, args)
    debug_ex_noid = :(@debug "($($(repr(cat_sym)))) $($msg)" _module=Dagger _file=$(string(__source__.file)) _line=$(__source__.line))
    append!(debug_ex_noid.args, args)
    esc(quote
        let $id = nothing, $dolog = true
            if $thunk isa ThunkID
                $id = $thunk
            elseif $thunk isa Integer
                $id = $ThunkID(myid(), $thunk)
            elseif $istask($thunk)
                $id = $task_id($thunk)
            elseif $thunk === nothing
            else
                @warn "Unsupported thunk argument to @dagdebug: $(typeof($thunk))"
                $dolog = false
            end
            if $dolog
                if $id !== nothing
                    $id = ($id.wid, $id.id)
                    if $(QuoteNode(cat_sym)) in $DAGDEBUG_CATEGORIES
                        $debug_ex_id
                    end
                else
                    if $(QuoteNode(cat_sym)) in $DAGDEBUG_CATEGORIES
                        $debug_ex_noid
                    end
                end
            end
        end
    end)
end
