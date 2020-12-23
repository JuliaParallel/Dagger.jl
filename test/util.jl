macro test_throws_unwrap(terr, ex)
    quote
        rerr = try
            $(esc(ex))
        catch err
            err
        end
        @test Dagger.Sch.unwrap_nested_exception(rerr) isa $terr
    end
end
