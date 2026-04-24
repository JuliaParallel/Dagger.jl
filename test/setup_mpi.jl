if USE_MPI
    @everywhere using MPI
    # Install the mpiexecjl wrapper to the depot's bin directory.
    MPI.install_mpiexecjl(; destdir=joinpath(DEPOT_PATH[1], "bin"), force=true)
end
