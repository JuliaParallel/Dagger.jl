using Dagger, MPI, Plots, TimespanLogging, DataFrames, LinearAlgebra

Dagger.accelerate!(:mpi)
A = randn(Blocks(256, 256), 1024, 1024)
B = randn(Blocks(256, 256), 1024, 1024)
C = zeros(Blocks(256, 256), 1024, 1024)

using Pkg
Pkg.precompile()

Dagger.enable_logging!()
LinearAlgebra.BLAS.set_num_threads(1)
@time LinearAlgebra.generic_matmatmul!(C, 'N', 'N', A, B, LinearAlgebra.MulAddMul(1.0, 0.0))
logs = Dagger.fetch_logs!()
Dagger.disable_logging!()
rank = MPI.Comm_rank(MPI.COMM_WORLD)
if rank == 0
    p = Dagger.render_logs(logs, :plots_gantt)
    savefig(p, "test/profiling/results/gantt_chart.png")
end
MPI.Finalize()