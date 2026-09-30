using Dagger, MPI, LinearAlgebra, Random

Dagger.accelerate!(:mpi)
Dagger.check_uniformity!(true)
comm = MPI.COMM_WORLD
rank = MPI.Comm_rank(comm)

Random.seed!(42)
A = rand(Float64, 64, 64)
A = A * A'
A[diagind(A)] .+= size(A, 1)
B = copy(A)

DA = zeros(Blocks(16, 16), Float64, 64, 64)
copyto!(DA, A)
LinearAlgebra._chol!(DA, UpperTriangular)
U_dist = UpperTriangular(collect(DA))

C = cholesky(B)
err = norm(U_dist - C.U) / norm(C.U)
recon = norm(U_dist' * U_dist - B) / norm(B)
Core.println("[$rank] chol err=$err recon=$recon")
@assert err < 1e-12 && recon < 1e-12
MPI.Barrier(comm)
Core.println("[$rank] chol OK")
