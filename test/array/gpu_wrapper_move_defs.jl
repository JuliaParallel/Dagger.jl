@everywhere using KernelAbstractions
@everywhere begin
    @kernel function wrapper_view_copy_kernel!(out,src,offset)
        i,j=@index(Global,NTuple)
        out[i,j]=src[i,j]+offset
    end
end

function test_gpu_wrapper_move(source_scope,dest_scope)
    Dagger.with_options(;scope=Dagger.UnionScope(source_scope,dest_scope)) do
    reference=reshape(Float32.(1:48),6,8)
    source_proc=only(Dagger.compatible_processors(source_scope))
    dest_proc=only(Dagger.compatible_processors(dest_scope))
    source=fetch(Dagger.DArray(reference,Dagger.Blocks(6,8),reshape([source_proc],1,1)))
    output=fetch(Dagger.DArray(zeros(Float32,4,3),Dagger.Blocks(4,3),reshape([dest_proc],1,1)))
    for hierarchical in (false,true),iteration in 1:3
        halo=view(source.chunks[1,1],2:5,2:4)
        Dagger.spawn_datadeps(;hierarchical) do
            Dagger.@spawn scope=dest_scope Dagger.Kernel(wrapper_view_copy_kernel!)(Dagger.Out(output.chunks[1,1]),Dagger.In(halo),Float32(iteration);ndrange=(4,3))
        end
        @test collect(output)==reference[2:5,2:4].+Float32(iteration)
    end
    @test collect(source)==reference
    end
end
