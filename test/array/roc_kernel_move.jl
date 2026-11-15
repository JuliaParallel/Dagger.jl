# Kernel is a zero-field callable, not a Function. This intentionally names an
# absent remote owner: moving code must not touch that process's GPU context.
using Test, Distributed, Dagger, AMDGPU, KernelAbstractions
@kernel function roc_move_identity_kernel!(out,src)
    i=@index(Global,Linear)
    out[i]=src[i]
end
@testset "ROCm kernel code moves without activating a device" begin
    wrapper=Dagger.Kernel(roc_move_identity_kernel!)
    source=Dagger.OSProc()
    local_gpu=Dagger.ROCArrayDeviceProc(myid(),1)
    remote_gpu=Dagger.ROCArrayDeviceProc(myid()+10000,1)
    for target in (local_gpu,remote_gpu)
        @test Dagger.move(source,target,wrapper) === wrapper
        @test Dagger.move(source,target,identity) === identity
        chunk=Dagger.tochunk(wrapper)
        @test Dagger.move(source,target,chunk) === wrapper
    end
end
