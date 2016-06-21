import ComputeFramework: Thunk
export show_plan

function node_label(io, t::Thunk, c)
    if isgeneric(t.f)
        println(io, "$(t.id) [label=\"$(t.f)\"]")
    else
        println(io, "$(t.id) [label=\"fn\"]")
    end
    c
end

function node_label(io, t,c)
    l = replace(string(t), "\"", "")
    println(io, dec(hash(t)), " [label=\"$l\"]")
end

global _part_labels = Dict()

function node_label(io, t::Part, c)
    _part_labels[t]="part_$c"
    c+1
end

function node_id(t::Thunk)
    t.id
end

function node_id(t)
    dec(hash(t))
end

function node_id(t::Part)
    _part_labels[t]
end

function write_dag(io, t)
    !istask(t) && return io
    deps = dependents(t)
    c=1
    for k in keys(deps)
        c = node_label(io, k, c)
    end
    for (k, v) in deps
        for dep in v
            println(io, "$(node_id(k)) -> $(node_id(dep))")
        end
    end
end

function show_plan(t::Thunk)
    io = IOBuffer()
    write_dag(io, t)
    """digraph {
        graph [layout=dot, rankdir=TB];
        $(takebuf_string(io))
    }"""
end

function show_plan(c::Computation)
    t = thunkize(Context(), stage(Context(), c))
    show_plan(t)
end

# function printing

argname(x::Symbol) = x

function argname(x)
    @assert x.head == :(::)
    x.args[1]
end

function show_statement(io, x)
    if x.head == :return
        x = x.args[1]
    end
    print(io, x)
end

function fnbody(io, x)
    body = x.args[3]
    statements = filter(x -> !isa(x, LineNumberNode), body.args)
    for s in statements
        show_statement(io, s)
    end
end

function show_ast(io, f)
    ast = Base.uncompressed_ast(f.code)
    args = map(x -> string(argname(x)), ast.args[1])
    write(io, "(", join(args, ','), ") -> ")
    fnbody(io, ast)
end

function showfn(io, f::Function)
    if isgeneric(f)
        show(io, f)
    else
        show_ast(io, f)
    end
end
showfn(io, f) = show(io, f)
