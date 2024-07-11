tests = [
    ("Thunk", "thunk.jl"),
    ("Scheduler", "scheduler.jl"),
    ("Processors", "processors.jl"),
    ("Memory Spaces", "memory-spaces.jl"),
    ("Logging", "logging.jl"),
    ("Checkpointing", "checkpoint.jl"),
    ("Scopes", "scopes.jl"),
    ("Options", "options.jl"),
    ("Mutation", "mutation.jl"),
    ("Task Queues", "task-queues.jl"),
    ("Datadeps", "datadeps.jl"),
    ("Domain Utilities", "domain.jl"),
    ("Array - Allocation", "array/allocation.jl"),
    ("Array - Indexing", "array/indexing.jl"),
    ("Array - Core", "array/core.jl"),
    ("Array - Copyto", "array/copyto.jl"),
    ("Array - MapReduce", "array/mapreduce.jl"),
    ("Array - LinearAlgebra - Matmul", "array/linalg/matmul.jl"),
    ("Array - LinearAlgebra - Cholesky", "array/linalg/cholesky.jl"),
    ("Array - Random", "array/random.jl"),
    ("Caching", "cache.jl"),
    ("Disk Caching", "diskcaching.jl"),
    ("File IO", "file-io.jl"),
    #("Fault Tolerance", "fault-tolerance.jl"),
]
all_test_names = map(test -> replace(last(test), ".jl"=>""), tests)
additional_workers::Int = 3

if PROGRAM_FILE != "" && realpath(PROGRAM_FILE) == @__FILE__
    pushfirst!(LOAD_PATH, @__DIR__)
    pushfirst!(LOAD_PATH, joinpath(@__DIR__, ".."))
    using Pkg
    Pkg.activate(@__DIR__)
    Pkg.instantiate()

    using ArgParse
    s = ArgParseSettings(description = "Dagger Testsuite")
    @eval begin
        @add_arg_table! s begin
            "--test"
                nargs = '*'
                default = all_test_names
                help = "Enables the specified test to run in the testsuite"
            "-s", "--simulate"
                action = :store_true
                help = "Don't actually run the tests"
            "-p", "--procs"
                arg_type = Int
                default = additional_workers
                help = "How many additional workers to launch"
        end
    end

    parsed_args = parse_args(s)
    to_test = String[]
    for test in parsed_args["test"]
        if isdir(joinpath(@__DIR__, test))
            for (_, other_test) in tests
                if startswith(other_test, test)
                    push!(to_test, other_test)
                    continue
                end
            end
        elseif test in all_test_names
            push!(to_test, test)
        else
            println(stderr, "Unknown test: $test")
            println(stderr, "Available tests:")
            for ((test_title, _), test_name) in zip(tests, all_test_names)
                println(stderr, "  $test_name: $test_title")
            end
            exit(1)
        end
    end

    @info "Running tests: $(join(to_test, ", "))"
    parsed_args["simulate"] && exit(0)

    additional_workers = parsed_args["procs"]
else
    to_test = all_test_names
    @info "Running all tests"
end


using Distributed
if additional_workers > 0
    # We put this inside a branch because addprocs() takes a minimum of 1s to
    # complete even if doing nothing, which is annoying.
    addprocs(additional_workers; exeflags="--project=$(joinpath(@__DIR__, ".."))")
end

include("imports.jl")
include("util.jl")
include("fakeproc.jl")

using Test
using Dagger
using UUIDs
import MemPool

try
    for test in to_test
        test_title = tests[findfirst(x->x[2]==test * ".jl", tests)][1]
        test_name = all_test_names[findfirst(x->x==test, all_test_names)]
        println()
        @info "Testing $test_title ($test_name)"
        @testset "$test_title" include(test * ".jl")
    end
catch
    printstyled(stderr, "Tests Failed!\n"; color=:red)
    rethrow()
finally
    state = Dagger.Sch.EAGER_STATE[]
    if state !== nothing
        notify(state.halt)
    end
    sleep(1)
    if nprocs() > 1
        rmprocs(workers())
    end
end

printstyled(stderr, "Tests Completed!\n"; color=:green)
