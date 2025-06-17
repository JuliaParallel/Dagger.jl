using Dagger
using MPI
using SparseArrays

Dagger.accelerate!(:mpi)


if MPI.Comm_rank(MPI.COMM_WORLD) == 0
    B = rand(4, 4)
    Dagger.send_yield!(B, MPI.COMM_WORLD, 1, 0)
    println("rank $(MPI.Comm_rank(MPI.COMM_WORLD)) send_yield! Array: B: $B")
else
    B = zeros(4, 4)
    Dagger.recv_yield!(B, MPI.COMM_WORLD, 0, 0) 
    println("rank $(MPI.Comm_rank(MPI.COMM_WORLD)) recv_yield! Array: B: $B")
end

MPI.Barrier(MPI.COMM_WORLD)

if MPI.Comm_rank(MPI.COMM_WORLD) == 0
    B = "hello"
    Dagger.send_yield!(B, MPI.COMM_WORLD, 1, 2)
    println("rank $(MPI.Comm_rank(MPI.COMM_WORLD)) send_yield String: B: $B")
else
    B = "Goodbye"
    B1, _ = Dagger.recv_yield!(B, MPI.COMM_WORLD, 0, 2) 
    println("rank $(MPI.Comm_rank(MPI.COMM_WORLD)) recv_yield! String: B1: $B1")
end

MPI.Barrier(MPI.COMM_WORLD)

if MPI.Comm_rank(MPI.COMM_WORLD) == 0
    B = sprand(4,4,0.2)
    Dagger.send_yield(B, MPI.COMM_WORLD, 1, 1)
    println("rank $(MPI.Comm_rank(MPI.COMM_WORLD)) send_yield (half in-place) Sparse: B: $B")
else
    B1 = Dagger.recv_yield(MPI.COMM_WORLD, 0, 1)
    println("rank $(MPI.Comm_rank(MPI.COMM_WORLD)) recv_yield (half in-place) Sparse: B1: $B1")
end

MPI.Barrier(MPI.COMM_WORLD)

if MPI.Comm_rank(MPI.COMM_WORLD) == 0
    B = rand(4, 4)
    Dagger.send_yield(B, MPI.COMM_WORLD, 1, 0)
    println("rank $(MPI.Comm_rank(MPI.COMM_WORLD)) send_yield (half in-place) Dense: B: $B")
else
    
    B = Dagger.recv_yield( MPI.COMM_WORLD, 0, 0) 
    println("rank $(MPI.Comm_rank(MPI.COMM_WORLD)) recv_yield (half in-place) Dense: B: $B")
end

MPI.Barrier(MPI.COMM_WORLD)



#=
A = rand(Blocks(2,2), 4, 4)
Ac = collect(A)
println(Ac)
=#

#move!(identity, Ac[1].space , Ac[2].space, Ac[1], Ac[2])