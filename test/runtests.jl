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
    ("Array", "array.jl"),
    ("Linear Algebra", "linalg.jl"),
    ("Caching", "cache.jl"),
    ("Disk Caching", "diskcaching.jl"),
    ("File IO", "file-io.jl"),
    ("Fault Tolerance", "fault-tolerance.jl"),
]
all_test_names = map(test -> replace(last(test), ".jl"=>""), tests)
if realpath(PROGRAM_FILE) == @__FILE__
    push!(LOAD_PATH, joinpath(@__DIR__, ".."))
    push!(LOAD_PATH, @__DIR__)
    using Pkg
    Pkg.activate(@__DIR__)

    using ArgParse
    s = ArgParseSettings(description = "Dagger Testsuite")
    @eval begin
        @add_arg_table! s begin
            "--test"
                nargs = '*'
                default = all_test_names
                help = "Enables the specified test to run in the testsuite"
        end
    end
    parsed_args = parse_args(s)
    for test in parsed_args["test"]
        if !(test in all_test_names)
            println(stderr, "Unknown test: $test")
            println(stderr, "Available tests:")
            for test in all_test_names
                println(stderr, "  $test")
            end
            exit(1)
        end
    end
    to_test = parsed_args["test"]
    @info "Running tests: $(join(to_test, ", "))"
else
    to_test = all_test_names
    @info "Running all tests"
end

using Distributed
addprocs(3)

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
    rmprocs(workers())
end

printstyled(stderr, "Tests Completed!\n"; color=:green)
