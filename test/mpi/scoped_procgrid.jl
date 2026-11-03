function test_scoped_mpi_procgrid(scope)
    Dagger.with_options(;scope) do
        ps=MPIExt.uniform_mpi_processors(Dagger.current_acceleration())
        ntiles=2length(ps)
        a=fetch(Dagger.DArray(zeros(Float32,2,2ntiles),Dagger.Blocks(2,2)))
        @test [c.handle.rank for c in a.chunks[:]] == [ps[mod1(i,length(ps))].rank for i in 1:ntiles]
        @test all(c->Dagger.proc_in_scope(c.processor,scope),a.chunks)
        @test collect(a)==zeros(Float32,2,2ntiles)
    end
end
