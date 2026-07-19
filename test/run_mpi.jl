# Launch an MPI test script through the MPI.jl-provided `mpiexec`, so no
# system MPI installation is required (MPI.jl uses its configured binary,
# MPICH_jll by default). The child processes inherit the active project.
#
# Usage:
#   julia --project=<env> test/run_mpi.jl <nranks> <threads> <script> [args...]
#
# Example:
#   julia --project=test/mpienv test/run_mpi.jl 4 2 test/mpi.jl

using MPI

nranks = parse(Int, ARGS[1])
threads = ARGS[2]
script = ARGS[3]
extra = ARGS[4:end]
project = Base.active_project()
juliabin = Base.julia_cmd().exec[1]

MPI.mpiexec() do mpiexec
    cmd = `$mpiexec -n $nranks $juliabin --project=$project --threads=$threads $script $extra`
    @info "Launching MPI job" nranks threads script
    proc = run(ignorestatus(cmd))
    exit(proc.exitcode)
end
