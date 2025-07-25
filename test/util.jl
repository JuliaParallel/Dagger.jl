macro test_throws_unwrap(terr, args...)
    _test_throws_unwrap(terr, args...)
end
function replace_obj!(ex::Expr, obj)
    if ex.head == :(.)
        if ex.args[1] isa Symbol
            ex.args[1] = Expr(:(.), obj, QuoteNode(ex.args[1]))
        else
            replace_obj!(ex.args[1], obj)
        end
    end
    return ex
end
replace_obj!(ex::Symbol, obj) = Expr(:(.), obj, QuoteNode(ex))
replace_obj!(ex, obj) = ex
function _test_throws_unwrap(terr, ex; to_match=[])
    @gensym oerr rerr bt
    match_expr = Expr(:block)
    for m in to_match
        if m.head == :(=)
            lhs, rhs = replace_obj!(m.args[1], rerr), m.args[2]
            push!(match_expr.args, :(@test $lhs == $rhs))
        elseif m.head == :call
            fn = m.args[1]
            lhs, rhs = replace_obj!(m.args[2], rerr), m.args[3]
            if fn == :(<)
                push!(match_expr.args, :(@test startswith($lhs, $rhs)))
            elseif fn == :(>)
                push!(match_expr.args, :(@test endswith($lhs, $rhs)))
            else
                push!(match_expr.args, :(@test $fn($lhs, $rhs)))
            end
        else
            error("@test_throws_unwrap cannot handle expr: $m")
        end
    end
    quote
        $bt = nothing
        $oerr, $rerr = try
            nothing, $(esc(ex))
        catch err
            $bt = catch_backtrace()
            (err, Dagger.Sch.unwrap_nested_exception(err))
        end
        if $terr isa Tuple
            @test $oerr isa $terr[1]
            @test $rerr isa $terr[2]
            if $rerr isa $terr[2]
                $match_expr
            else
                println("Full error:")
                Base.showerror(stdout, $oerr)
                Base.show_backtrace(stdout, $bt)
            end
        else
            @test $rerr isa $terr
            if $rerr isa $terr
                $match_expr
            else
                println("Full error:")
                Base.showerror(stdout, $oerr)
                Base.show_backtrace(stdout, $bt)
            end
        end
    end
end
function _test_throws_unwrap(terr, args...)
    ex = last(args)
    to_match = args[1:end-1]
    _test_throws_unwrap(terr, ex; to_match=to_match)
end

# NOTE: based on test/pkg.jl::capture_stdout, but doesn't discard exceptions
macro grab_output(ex)
    quote
        mktemp() do fname, fout
            ret = nothing
            open(fname, "w") do fout
                redirect_stderr(fout) do
                    ret = $(esc(ex))
                end
                flush(fout)
            end
            ret, read(fname, String)
        end
    end
end
