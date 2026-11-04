# Loaded after AMDGPU by the ROCm suite; don't read uninitialized contents.
@testset "ROCm replica allocation" begin
    ROC=Base.get_extension(Dagger,:ROCExt).AMDGPU
    for T in (Float32,Float64,ComplexF64), dims in ((7,5),(0,3),(11,))
        source=ROC.ROCArray(ones(T,dims))
        space=Dagger.memory_space(source)
        @test Dagger.can_alloc_uninit(space,typeof(source))
        @test !Dagger.slot_may_be_uninit(space,space,typeof(source))
        @test Dagger.alloc_header(source)==dims
        dest=Dagger.alloc_uninit(space,typeof(source),dims)
        @test typeof(dest)==typeof(source)
        @test size(dest)==dims
        @test dest!==source
        copyto!(dest,source)
        @test Array(dest)==ones(T,dims)
    end
end
