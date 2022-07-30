@everywhere using DTables, CSV, Arrow, Random, OnlineStats, Dates, MemPool

# n = tryparse(Int, ARGS[1])
# max_chunksize = tryparse(Int, ARGS[2])
# unique_values = tryparse(Int32, ARGS[3])
# ncolumns = tryparse(Int, ARGS[4])
#

@everywhere function fetch_wait(dt::DTable)
    println(MemPool.GLOBAL_DEVICE[])
    foreach(fetch, dt.chunks)
end

function dtable_suite(ctx; method, accels)
    @assert method == "dagger" "DTable suite does not support non-Dagger execution"
    @assert isempty(accels) "DTable suite does not support acceleration"

    n = Int(2e8)
    max_chunksize = Int(1e8)
    unique_values = Int(1e3)
    ncolumns = 4
    nchunks = (n+max_chunksize-1) ÷ max_chunksize

    function genchunk(rng, nchunks)
        (;[Symbol("a$i") => rand(rng, Int32(1):Int32(unique_values), n÷nchunks) for i in 1:ncolumns]...)
    end

    suite = BenchmarkGroup()

    #= FIXME: Way too slow
    suite["DTable single CSV chunked reading"] = @benchmarkable begin
        @info "Loading CSV.Chunks -> DTable"
        c = CSV.Chunks(
            joinpath(path, "datapart_1.csv"),
            ntasks=(($n+$max_chunksize-1) ÷ $max_chunksize),
            types = Int32
        )
        dt = Dagger.with_options(;storage=Dagger.tochunk(MemPool.CPURAMDevice())) do
            DTable(c)
        end
        wait(dt)
    end setup=begin
        genchunk = $genchunk
        @info "Writing single CSV"
        path = mktempdir()
        nchunks = 1 #overwrite nchunks to create one big file
        [CSV.write(joinpath(path, "datapart_$i.csv"), genchunk(MersenneTwister(1111+i), nchunks)) for i in 1:nchunks]
    end teardown=begin
        rm(path; recursive=true)
        @everywhere GC.gc()
    end
    =#

    function dtable_multi_memory(name, op)
        suite["DTable in-memory ($name)"] = @benchmarkable begin
            @info "$(time_ns()) DTable -> $($name)"
            dt = $op(dt)
            fetch_wait(dt)
        end setup=begin
            @info "$(time_ns()) Generating multiple chunks"
            nchunks = $nchunks
            dt = DTable([Dagger.spawn($genchunk, MersenneTwister(1111+i), nchunks) for i in 1:nchunks], NamedTuple)
            fetch_wait(dt)
        end teardown=begin
            @info "$(time_ns()) Done"
            @everywhere GC.gc()
        end
    end
    function dtable_multi_csv(name, op)
        suite["DTable multiple CSV reading ($name)"] = @benchmarkable begin
            @info "$(time_ns()) Loading CSV -> DTable"
            dt = DTable(x -> CSV.read(x, NamedTuple, types=Int32), readdir(path, join=true); device=MemPool.CPURAMDevice())
            @info "$(time_ns()) DTable -> $($name)"
            dt = $op(dt)
            fetch_wait(dt)
        end setup=begin
            genchunk = $genchunk
            @info "$(time_ns()) Writing mutiple CSVs"
            path = mktempdir()
            nchunks = $nchunks
            wait.([Threads.@spawn CSV.write(joinpath(path, "datapart_$i.csv"), genchunk(MersenneTwister(1111+i), nchunks)) for i in 1:nchunks])
        end teardown=begin
            @info "$(time_ns()) Done"
            rm(path; recursive=true)
            @everywhere GC.gc()
        end
    end
    function dtable_multi_arrow(name, op)
        suite["DTable multiple Arrow reading ($name)"] = @benchmarkable begin
            @info "$(time_ns()) Loading Arrow -> DTable"
            dt = DTable(Arrow.Table, readdir(path, join=true); device=MemPool.CPURAMDevice())
            @info "$(time_ns()) DTable -> $($name)"
            dt = $op(dt)
            fetch_wait(dt)
        end setup=begin
            genchunk = $genchunk
            @info "$(time_ns()) Writing multiple Arrow files"
            path = mktempdir()
            nchunks = $nchunks
            wait.([Threads.@spawn Arrow.write(joinpath(path, "datapart_$i.arrow"), genchunk(MersenneTwister(1111+i), nchunks)) for i in 1:nchunks])
        end teardown=begin
            @info "$(time_ns()) Done"
            rm(path; recursive=true)
            @everywhere GC.gc()
        end
    end
    for (name, op) in (("none", identity),
                       ("map", dt->map(r->(;s=sum(r)), dt)),
                       # FIXME: ("reduce", dt->reduce(+, dt)),
                       ("filter", dt->filter(r->all(>(1), r), dt)))
        dtable_multi_memory(name, op)
        dtable_multi_arrow(name, op)
        #dtable_multi_csv(name, op)
    end

    #= TODO: Analyze serial tail
    suite["DTable innerjoin"] = @benchmarkable begin
        @info "Joining DTables"
        dt = Dagger.innerjoin(d_left, d_right, on=:a1, r_unique=true)
        wait(dt)
    end setup=begin
        @info "Generating DTable"
        nchunks = $nchunks
        d_left = DTable([Dagger.spawn($genchunk, MersenneTwister(1111+i), nchunks) for i in 1:nchunks], NamedTuple)
        d_right = DTable((a1=Int32.(1:$unique_values), a5=.-Int32.(1:$unique_values)), Int($unique_values))
    end teardown=begin
        @everywhere GC.gc()
    end
    =#

    suite
end
