@testset "Dense direct collection" begin
    for T in (Int, Float32, ComplexF64, String), (dims,blocks) in (((19,17),(6,5)),((13,),(5,)),((7,5,3),(3,2,2)),((2,2),(8,8)))
        x=reshape(T===String ? string.(1:prod(dims)) : T.(1:prod(dims)),dims)
        a=fetch(Dagger.DArray(x,Dagger.Blocks(blocks...)))
        y=collect(a)
        @test y==x
        @test y isa Array{T,length(dims)}
        @test Dagger._collect_dense_tiles(a)==x
        y[1]=T===String ? "changed" : zero(T)
        @test collect(a)==x
    end
    for dims in ((0,3),(3,0),(0,))
        a=Dagger.DArray(zeros(dims),Dagger.Blocks(ntuple(_->2,length(dims))...))
        @test size(collect(a))==dims
    end
    x=reshape(1:323,19,17)
    a=fetch(Dagger.DArray(collect(x),Dagger.Blocks(6,5)))
    calls=Ref(0)
    custom=(xs...;dims)->(calls[]+=1; cat(xs...;dims))
    b=Dagger.DArray(eltype(a),a.domain,a.subdomains,a.chunks,a.partitioning,custom)
    @test collect(b)==x
    @test calls[]>0
end

@testset "Structured and raw tiles" begin
    a=fetch(Dagger.DArray(reshape(collect(1:16),4,4),Dagger.Blocks(4,4)))
    raw=reshape([reshape(collect(1:16),4,4)],1,1)
    b=Dagger.DArray(Int,a.domain,a.subdomains,raw,a.partitioning)
    @test collect(b)==reshape(1:16,4,4)
    diagtiles=reshape([Dagger.tochunk(Diagonal(collect(1:4)))],1,1)
    c=Dagger.DArray(Int,a.domain,a.subdomains,diagtiles,a.partitioning)
    @test collect(c)==Matrix(Diagonal(collect(1:4)))
end
