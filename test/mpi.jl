using Dagger
using MPI
using LinearAlgebra
using SparseArrays

Dagger.accelerate!(:mpi)

comm = MPI.COMM_WORLD
rank = MPI.Comm_rank(comm)
size = MPI.Comm_size(comm)

# Use a large array (adjust size as needed for your RAM)
N = 100
tag = 123

if rank == 0
    arr = sprand(N, N, 0.6)
else
    arr = spzeros(N, N)
end

# --- Out-of-place broadcast ---
function bcast_outofplace()
    MPI.Barrier(comm)
    if rank == 0
        Dagger.bcast_send_yield(arr, comm, 0, tag+1)
    else
        Dagger.bcast_recv_yield(comm, 0, tag+1)
    end
    MPI.Barrier(comm)
end
# --- In-place broadcast ---

function bcast_inplace()
    MPI.Barrier(comm)
    if rank == 0
        Dagger.bcast_send_yield!(arr, comm, 0, tag)
    else
        Dagger.bcast_recv_yield!(arr, comm, 0, tag)
    end
    MPI.Barrier(comm)
end

function bcast_inplace_metadata()
    MPI.Barrier(comm)
    if rank == 0
        Dagger.bcast_send_yield_metadata(arr, comm, 0)
    end
    MPI.Barrier(comm)
end


inplace = @time bcast_inplace()


MPI.Barrier(comm)
MPI.Finalize()




#=
A = rand(Blocks(2,2), 4, 4)
Ac = collect(A)
println(Ac)


move!(identity, Ac[1].space , Ac[2].space, Ac[1], Ac[2])
=#

