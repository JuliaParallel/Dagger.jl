
@testset "Rules" begin

# test basic patterns
mktempdir() do dir

    write("$(dir)/a.txt", "a")
    write("$(dir)/b.txt", "b")

    rule_write(x, y) = Dagger.Rule(x => y; forcerun=true) do input, output
        write(output[1], read(input[1], String))
        output
    end

    rule_merge(x, y) = Dagger.Rule(x => y; forcerun=true) do input, output
        write(output[1], join(read.(input, String)))
        output
    end

    # Linear: a -> b -> c
    r1 = rule_write("$(dir)/a.txt", "$(dir)/b1.txt")
    r2 = rule_write("$(dir)/b1.txt", "$(dir)/c.txt")
    t1 = Dagger.@spawn r1()
    t2 = Dagger.@spawn r2(t1)
    @test read(fetch(t2)[1], String) == "a"

    # Fan-in: a.txt, b.txt -> ab.txt
    r3 = rule_merge(["$(dir)/a.txt", "$(dir)/b.txt"], "$(dir)/ab.txt")
    t3 = Dagger.@spawn r3()
    @test read(fetch(t3)[1], String) == "ab"

    # Fan-out: a.txt -> a1.txt, a2.txt
    r4 = Dagger.Rule("$(dir)/a.txt" => ["$(dir)/a1.txt", "$(dir)/a2.txt"]; forcerun=true) do i, o
        for f in o; 
            write(f, read(i[1], String))
        end
        o
    end
    t4 = Dagger.@spawn r4()
    @test read.(fetch(t4), String) == ["a", "a"]

    # Diamond: a -> b,c -> d
    r5 = rule_write("$(dir)/a.txt", "$(dir)/b.txt")
    r6 = rule_write("$(dir)/a.txt", "$(dir)/c.txt")
    r7 = rule_merge(["$(dir)/b.txt", "$(dir)/c.txt"], "$(dir)/d.txt")
    tb = Dagger.@spawn r5()
    tc = Dagger.@spawn r6()
    td = Dagger.@spawn r7(tb, tc)
    @test read(fetch(td)[1], String) == "aa"
end

# more realistic use case
mktempdir() do dir

    # avoid CSV & DataFrame dependency in tests
    writefile(file, x) = begin
        open(file, "w") do io
            for i in 1:length(x)-1
                write(io, string(x[i]) * "\n")
            end
            write(io, string(x[end]))
        end
    end

    readfile(file) = read(file, String) |> x->split(x, "\n") .|> x->parse(Float64, x)
    
    x = rand(10)
    writefile("$(dir)/test.txt", x)
    @test x == readfile("$(dir)/test.txt")

    # prepare inputs

    mean_squared_input = Float64[]
    for sample_idx in 1:5
        x = rand(10)
        writefile("$(dir)/sample_$(sample_idx).csv", x)
        push!(mean_squared_input, mean(x.^2))
    end

    samples = ["$(dir)/sample_$(sample_idx).csv" for sample_idx in 1:5]

    # define and run

    get_rule_square(sample) = Dagger.Rule(sample => replace(sample, "sample_" => "sample_squared_"); forcerun=false) do input, output
        x = readfile(input[1])
        xsquared = x .^ 2
        writefile(output[1], xsquared)
        output
    end

    squared_rules = get_rule_square.(samples)
    squared_rule_outputs = [only(r.outputs) for r in squared_rules]

    make_summary = Dagger.Rule(squared_rule_outputs => "$(dir)/samples_summary.csv"; forcerun=false) do inputs, output
        xs = readfile.(inputs)
        mean_squared = [mean(x) for x in xs]
        writefile(output[1], mean_squared)
        output
    end

    squared = [Dagger.@spawn r() for r in squared_rules]
    @warn "running first summary_file"
    summary_file = Dagger.@spawn make_summary(squared...)
    
    out = readfile(fetch(summary_file)[1])
    
    @test out == mean_squared_input

    @test Dagger.needs_update(make_summary) == false
    sleep(1)
    run(`touch $(squared_rule_outputs[1])`)
    sleep(1)
    @test Dagger.needs_update(make_summary) == true

    run(`rm $(squared_rule_outputs[1])`)
    @warn "running second summary_file"
    summary_file = Dagger.@spawn make_summary(squared...)
    @test_throws Dagger.DTaskFailedException fetch(summary_file)

end

end