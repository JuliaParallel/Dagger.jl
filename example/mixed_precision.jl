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

#Define Gamma value and distance matric to be used when computing kernel matrix
k = GammaExponentialKernel(; γ=0.5, metric=Euclidean());

#It generates matrix of normally-distributed random numbers
x = randn(1000, 1000);

#This function will compute the distance between all points of x then it will apply Exponential Kernel
A =  kernelmatrix(k, x);

#Create DA of the kernel matrix
DA = view(A, Blocks(200, 200));

MP = Dagger.adapt_precision(DA, 10^-4)

