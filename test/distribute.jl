import ComputeFramework: SliceDimension

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

@testset "Distribution ($n workers)" for n=1:nworkers()
    ctx = Context(workers()[1:n])

    @testset "Array distributions" begin
        @testset "SliceDimension" begin
            x = rand(10, 10, 10)

            dist_1 = distribute(x, SliceDimension{1}())
            c1 = compute(ctx, dist_1)
            @test metadata(c1) == meta_test[n][1]
            test_each_ref(c1, map(idx->x[idx...], meta_test[n][1])) do chunk, correct
                @test chunk == correct
            end
            @test gather(ctx, dist_1) == x


            dist_2 = distribute(x, SliceDimension{2}())
            c2 = compute(ctx, dist_2)
            test_each_ref(c2, map(idx->x[idx...], meta_test[n][2])) do chunk, correct
                @test chunk == correct
            end
            @test metadata(c2) == meta_test[n][2]
            @test gather(ctx, dist_2) == x

            dist_3 = distribute(x, SliceDimension{3}())
            c3 = compute(ctx, dist_3)
            test_each_ref(c3, map(idx->x[idx...], meta_test[n][3])) do chunk, correct
                @test chunk == correct
            end
            @test metadata(c3) == meta_test[n][3]
            @test gather(ctx, dist_3) == x
        end
    end
end
