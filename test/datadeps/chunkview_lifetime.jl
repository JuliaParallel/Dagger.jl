function test_chunkview_lifetime()
    @testset "ChunkView parent survives regions, hierarchical=$hierarchical" for hierarchical in (false,true)
        source=reshape(Float32.(1:16),4,4)
        a=fetch(Dagger.DArray(source,Dagger.Blocks(4,4)))
        b=fetch(Dagger.DArray(zeros(Float32,4,2),Dagger.Blocks(4,2)))
        av=view(only(a.chunks),:,2:3)
        for _ in 1:3
            Dagger.spawn_datadeps(;hierarchical) do
                Dagger.@spawn copyto!(Dagger.Out(only(b.chunks)),Dagger.In(av))
            end
            @test collect(a)==source
            @test collect(b)==source[:,2:3]
            Dagger.spawn_datadeps(;hierarchical) do
                Dagger.@spawn copyto!(Dagger.Out(av),Dagger.In(only(b.chunks)))
            end
            @test collect(a)==source
        end
    end
end
