using Dagger
using MPI

Dagger.accelerate!(:mpi)
#=
if MPI.Comm_rank(MPI.COMM_WORLD) == 0
    B = rand(4, 4)
    Dagger.send_yield(B, MPI.COMM_WORLD, 1, 0)
    println("rank $(MPI.Comm_rank(MPI.COMM_WORLD)) B: $B")
else
    B = zeros(4, 4)
    Dagger.recv_yield!(B, MPI.COMM_WORLD, 0, 0) 
    println("rank $(MPI.Comm_rank(MPI.COMM_WORLD)) B: $B")
end

if MPI.Comm_rank(MPI.COMM_WORLD) == 0
    B = "hello"
    Dagger.send_yield(B, MPI.COMM_WORLD, 1, 1)
    println("rank $(MPI.Comm_rank(MPI.COMM_WORLD)) B: $B")
else
    B = "Goodbye"
    B1, _ = Dagger.recv_yield!(B, MPI.COMM_WORLD, 0, 1) 
    println("rank $(MPI.Comm_rank(MPI.COMM_WORLD)) B1: $B1")
end
=#
A = rand(Blocks(2,2), 4, 4)
Ac = collect(A)
println(Ac)


#move!(identity, Ac[1].space , Ac[2].space, Ac[1], Ac[2])


