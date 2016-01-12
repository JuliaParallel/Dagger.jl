import ComputeFramework: DistData, refs

@everywhere function test_runner(f, localpart, aux)
    Test.@testset "" begin
        f(localpart, aux)
    end
end

function merge_testset(host, remote)
    if isa(remote, Test.DefaultTestSet)
        append!(host.results, remote.results)
    else
        warn("Could not consolidate results from a $(typeof(remote)) test results set")
    end
    host
end

function test_each_ref(f::Function, node::DistData, args::Vector)
    # Ship a test to each ref
    test_results = Array(Any, length(refs(node)))
    host_testset = Test.get_testset()
    desc = host_testset.description

    @sync begin
        for (idx, r) in enumerate(refs(node))
            pid, ref = r
            @async begin
                result = remotecall_fetch(pid,
                    (x, y) -> test_runner(f, fetch(x), y), ref, args[idx])
                test_results[idx] = merge_testset(host_testset, result)
            end
        end
    end
    reduce(merge_testset, Test.get_testset(), test_results)
end
