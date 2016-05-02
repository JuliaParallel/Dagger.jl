@testset "GPU" begin

@require ArrayFire begin

x = rand(AFArray{Float64}, 5000, 5000)
y = x * x

xd = Distribute(BlockPartition(4000, 4000), x)
ctx = [GPUProc(2), GPUProc(3), GPUProc(4)] |> Context
yd = gather(compute(ctx, xd * xd))
@test sumabs2(yd - Array(y)) < 1e-10

end

end
