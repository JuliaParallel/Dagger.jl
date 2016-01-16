
@testset "Distribution ($nw workers)" for nw=1:nworkers()
    ctx = Context(workers()[1:nw])
    x = rand(10, 10, 10)

    @testset "SliceDimension{$dim}" for dim=1:3

        dist_x = distribute(x, SliceDimension{dim}())
        computed = compute(ctx, dist_x)

        @test metadata(computed) == meta_test[nw][dim]

        expected = map(idx->x[idx...], meta_test[nw][dim])
        test_each_ref(computed, expected) do chunk, correct
            @test chunk == correct
        end

        @test gather(ctx, dist_x) == x
    end

    @testset "Bcast" for dim=1:3

        dist_1 = distribute(1, Bcast())
        computed = compute(ctx, dist_1)

        @test metadata(computed) == meta_test[nw][dim]

        expected = map(idx->x[idx...], meta_test[nw][dim])
        test_each_ref(computed, expected) do chunk, correct
            @test chunk == correct
        end

        @test gather(ctx, dist_x) == x
    end
end
