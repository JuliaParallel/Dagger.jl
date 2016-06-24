import Dagger: SliceDimension, Bcast

meta_test = Any[
    Any[
        Any[UnitRange[1:10,1:10,1:10]],
        Any[UnitRange[1:10,1:10,1:10]],
        Any[UnitRange[1:10,1:10,1:10]],
    ], Any[
        Any[UnitRange[1:5,1:10,1:10],UnitRange[6:10,1:10,1:10]],
        Any[UnitRange[1:10,1:5,1:10],UnitRange[1:10,6:10,1:10]],
        Any[UnitRange[1:10,1:10,1:5],UnitRange[1:10,1:10,6:10]],
    ], Any[
        Any[UnitRange[1:3,1:10,1:10],UnitRange[4:7,1:10,1:10],UnitRange[8:10,1:10,1:10]],
        Any[UnitRange[1:10,1:3,1:10],UnitRange[1:10,4:7,1:10],UnitRange[1:10,8:10,1:10]],
        Any[UnitRange[1:10,1:10,1:3],UnitRange[1:10,1:10,4:7],UnitRange[1:10,1:10,8:10]],
    ], Any[
        Any[UnitRange[1:3,1:10,1:10],UnitRange[4:5,1:10,1:10],UnitRange[6:7,1:10,1:10],UnitRange[8:10,1:10,1:10]],
        Any[UnitRange[1:10,1:3,1:10],UnitRange[1:10,4:5,1:10],UnitRange[1:10,6:7,1:10],UnitRange[1:10,8:10,1:10]],
        Any[UnitRange[1:10,1:10,1:3],UnitRange[1:10,1:10,4:5],UnitRange[1:10,1:10,6:7],UnitRange[1:10,1:10,8:10]],
    ]
]

@testset "Distribution ($nw workers)" for nw=1:nworkers()
    ctx = Context(workers()[1:nw])
    x = rand(10, 10, 10)

    @testset "Array distributions" begin
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
    end

    @testset "Bcast" for dim=1:3

        dist_1 = distribute(1, Bcast())
        computed = compute(ctx, dist_1)

        expected = ones(Int, nw)
        test_each_ref(computed, expected) do chunk, correct
            @test chunk == 1
        end
    end

end
