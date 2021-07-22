macro test_throws_unwrap(terr, args...)
    _test_throws_unwrap(terr, args...)
end
function _test_throws_unwrap(terr, ex; to_match=[])
    @gensym rerr
    match_expr = Expr(:block)
    for m in to_match
        @assert m.head == :(=)
        push!(match_expr.args, :(@test $rerr.$(m.args[1]) == $(m.args[2])))
    end
    quote
        $rerr = try
            $(esc(ex))
        catch err
            Dagger.Sch.unwrap_nested_exception(err)
        end
        @test $rerr isa $terr
        $match_expr
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
