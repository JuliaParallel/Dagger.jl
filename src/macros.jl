export @par

macro par(expr...)
    if length(expr) == 1
        loop = expr[1]
        ctx = :(Context())
        @assert loop.head == :for
    elseif length(expr) == 2
        ctx = expr[1]
        loop = expr[2]
    else
        error("Wrong syntax for @par macro")
    end

    if loop.args[1].head == :block
        error("Foreach only works on one collection for now")
    end

    assignment = loop.args[1]
    x = assignment.args[1]
    X = assignment.args[2]

    body = loop.args[2]

    :(gather($(esc(ctx)), foreach($(esc(X))) do $x
        $(esc(body))
    end))
end
