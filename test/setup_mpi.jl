if USE_MPI
    using MPI
    MPI.install_mpiexecjl(; destdir=joinpath(DEPOT_PATH[1], "bin"), force=true)
end
