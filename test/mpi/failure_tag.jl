# A failure broadcast needs a valid MPI tag: an id past `MPI.tag_ub()` (a long run on an MPI with a small limit)
# used to die in `Isend` with MPI_ERR_TAG, and the actual exception was lost.
@testset "failure broadcast tag" begin
    ext = Base.get_extension(Dagger, :MPIExt)
    ub = Int(MPI.tag_ub())
    @test ext.mpi_failure_tag(5) == UInt32(5)
    @test ext.mpi_failure_tag(ub - 1) == UInt32(ub - 1)
    @test ext.mpi_failure_tag(ub) === nothing
    @test ext.mpi_failure_tag(ub + 12345) === nothing
    @test ext.mpi_failure_tag(-1) === nothing

    # A failing failure-only task whose id is no tag: logged with its exception, nothing is sent, nothing throws.
    me = MPI.Comm_rank(MPI.COMM_WORLD)
    old = Dagger.ELIDE_FOREIGN_TASKS[]
    Dagger.check_uniformity!(false)
    Dagger.elide_foreign_tasks!(true)
    try
        opts = Dagger.Options(; scope=Dagger.ExactScope(first(filter(p -> p.rank == me, mpi_procs()))))
        thunk = Dagger.Thunk(identity, nothing => 1; id=ub + 5, options=opts)
        accel = Dagger.current_acceleration()
        @test_logs (:error, r"not broadcast") match_mode=:any Dagger.Sch.task_failed!(accel, thunk, ErrorException("boom"))
    finally
        Dagger.elide_foreign_tasks!(old)
        Dagger.check_uniformity!(true)
    end
end
