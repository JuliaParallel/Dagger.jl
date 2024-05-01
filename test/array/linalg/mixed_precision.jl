using Dagger
using LinearAlgebra
using KernelFunctions
using Distances

k = GammaExponentialKernel(; γ=0.5, metric=Euclidean());
x = randn(4000, 2000);
A =  kernelmatrix(k, x);
DA = view(A, Blocks(400, 400));
MP  = fill("FP64", 5, 5);
DMP = view(MP, Blocks(1, 1));

Dagger.adaptive_mp!(DA, DMP, 10^-4);
collect(DMP)
