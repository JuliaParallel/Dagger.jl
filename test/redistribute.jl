@testset "redistribution ($nw workers)" for nw=1:nworkers()
    ctx = Context(workers()[1:nw])
    x = rand(10, 10, 10)

    @testset "Array distributions" begin
        @testset "SliceDimension{$dim}" for dim=1:3
            for to_dim=1:3
                to_dim == dim && break

                dist_x = distribute(x, cutdim(dim))
                redist = redistribute(compute(ctx, dist_x), cutdim(to_dim))
                computed = compute(ctx, redist)

                @test metadata(computed) == meta_test[nw][to_dim]

                expected = map(idx->x[idx...], meta_test[nw][to_dim])
                test_each_ref(computed, expected) do chunk, correct
                    @test chunk == correct
                end

                @test gather(ctx, redist) == x
            end
        end
    end

end
