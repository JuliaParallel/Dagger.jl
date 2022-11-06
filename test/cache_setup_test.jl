using Distributed
@everywhere using Pkg
@everywhere Pkg.activate("..")
using Dagger
@assert Dagger.enable_disk_caching!()
total_mem = @static VERSION >= v"1.8-" ? Sys.total_physical_memory : Sys.total_memory
expected_mem_limit = Dagger.mem_limit(total_mem(), 30, length(procs()))
applied_mem_limits = fetch.(remotecall.(() -> Dagger.MemPool.GLOBAL_DEVICE[].mem_limit, procs()))
@assert all(applied_mem_limits .== expected_mem_limit)
