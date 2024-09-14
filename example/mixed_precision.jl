"""
This example shows how to compute kernel matrix and infer the precision per tile.
    It import KernelFunctions and Distances Julia packages 
    to compute distance matrix based by using Euclidean distance 
    and then it calls GammaExponentialKernel for each resulted distance
"""
using Revise
using Dagger
using LinearAlgebra
using KernelFunctions
using Distances
T = Float64
#Define Gamma value and distance matric to be used when computing kernel matrix
k = GammaExponentialKernel(; γ=0.5, metric=Euclidean());
m, n = 1000, 1000
#It generates matrix of normally-distributed random numbers
x = randn(m, n);

#This function will compute the distance between all points of x then it will apply Exponential Kernel
A =  kernelmatrix(k, x);
A[diagind(A)] .+= 0.1
CopyA = copy(A)
#A = copy(CopyA)
#LAPACK.potrf!('L', A)
#Create DA of the kernel matrix
DA = view(A, Blocks(200, 200));

MP = Dagger.adapt_precision(DA, 10^-4)

Dagger.MixedPrecisionChol!(DA, LowerTriangular, MP)
#LinearAlgebra._chol!(DA, LowerTriangular)
#Cholesky!(DA)
A = collect(DA)

B = rand(m, 1)
Bcopy = copy(B)

BLAS.trsm!('L', 'L', 'N', 'N', T(1.0), A, B) 
BLAS.trsm!('L', 'L', 'T', 'N', T(1.0), A, B)

norm(Bcopy - CopyA * B)
norm(Bcopy - CopyA * B)/ n/ (norm(Bcopy)-norm(CopyA)*  norm(B))