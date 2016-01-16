import ComputeFramework: DistData, refs
@everywhere begin
    type RemoteTestSet <: Test.AbstractTestSet
        description::AbstractString
        hostref::RemoteRef
    end
    RemoteTestSet(desc; hostref=RemoteRef()) =
        RemoteTestSet(desc, hostref)

    function Test.record(ts::RemoteTestSet, t)
        put!(ts.hostref, (myid(), t))
    end

    function Test.finish(ts::RemoteTestSet)
    end

    function test_runner(f, localpart, aux, ref)
        Test.@testset RemoteTestSet hostref=ref "Remote test" begin
            f(localpart, aux)
        end
    end
end

function test_each_ref(f::Function, node::DistData, args::Vector)
    # Ship a test to each ref
    test_results = Array(Any, length(refs(node)))
    host_testset = Test.get_testset()

    result_ref = RemoteRef()
    @async while true
        pid, result = take!(result_ref)
        if isa(result, Test.Fail)
            println("Test failure on worker $pid")
        end
        Test.record(host_testset, result)
    end

    @sync begin
        for (idx, r) in enumerate(refs(node))
            pid, ref = r
            @async begin
                result = remotecall_fetch(pid,
                    (f, x, y, r) -> test_runner(f, fetch(x), y, r), f, ref, args[idx], result_ref)
            end
        end
    end
end
