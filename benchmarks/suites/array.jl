AmulB_compatible(x,y) =
    try; size(x), size(y), x * y; true; catch err false end

AmulB_compatible(x::Computation,y::Computation) = AmulB_compatible(domain(x), domain(y))

function AcmulB_compatible(x,y)
    AmulB_compatible(domain(x)', domain(y))
end

function array_suite_inner(ctx, X, Y, f=x->x)
    suite = BenchmarkGroup(["array"])

    suite["alloc"] = @benchmarkable (compute($f($X)); GC.gc())

    Y = compute(ctx, f(X))
    suite["X.+1.0"]  = @benchmarkable (compute($ctx, $f($Y.+1)); GC.gc())
    suite["X+X"]     = @benchmarkable (compute($ctx, $f($Y+$Y)); GC.gc())
    suite["X'"]      = @benchmarkable (compute($ctx, $f($Y')); GC.gc())
    suite["sin.(X)"] = @benchmarkable (compute($ctx, $f(sin.($Y))); GC.gc())

    if ndims(Y) == 2
        suite["X+X'"] = @benchmarkable (compute($ctx, $f($Y+$Y')); GC.gc())
        suite["X*X"]  = @benchmarkable (compute($ctx, $f($Y*$Y)); GC.gc())
        suite["X'*X"] = @benchmarkable (compute($ctx, $f($Y'*$Y)); GC.gc())
    end

    #= TODO: Re-enable
    pairs = Set{Tuple}()
    for i in (1, 2, 5, 10, 64, 100, 1028)
        for bscale in (1, 0.5, 0.3, 0.25, 0.1)
            b = ceil(Int, i*bscale)
            if !((b,i) in pairs)
                push!(pairs, (b,i))
                X = zeros(Blocks(b), i)
                Y = compute(ctx, f(X))
                suite["vector $i/$b"] = array_suite_inner(ctx, X, Y, f)
            end
            if !(((b,b),(i,i)) in pairs)
                push!(pairs, ((b,b),(i,i)))
                X = zeros(Blocks(b,b), i, i)
                Y = compute(ctx, f(X))
                suite["matrix ($i,$i)/$b"] = array_suite_inner(ctx, X, Y, f)
            end
        end
    end
    =#

    suite
end
