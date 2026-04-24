if USE_MPI
    using Pkg
    Pkg.add("MPI")
    Pkg.add("MPIPreferences")
end

@everywhere begin
    if $USE_MPI
        using MPI
        using MPIPreferences

        # Configure each rank to use JLL binary
        MPIPreferences.use_jll_binary("MPICH_jll"; export_prefs=true)
    end
end

if USE_MPI
    # Install mpiexecjl wrapper to standard bin location
    MPI.install_mpiexecjl(; destdir=joinpath(DEPOT_PATH[1], "bin"), force=true)
end
