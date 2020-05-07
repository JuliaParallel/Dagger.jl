export show_plan

### DAG-based graphing

function write_node(io, t::Thunk, c)
    f = isa(t.f, Function) ? "$(t.f)" : "fn"
    println(io, "$(t.id) [label=\"$f - $(t.id)\"]")
    c
end

function write_node(io, t, c)
    l = replace(string(t), "\"", "")
    println(io, dec(hash(t)), " [label=\"$l\"]")
end

global _part_labels = Dict()

function write_node(io, t::Chunk, c)
    _part_labels[t]="part_$c"
    c+1
end

function node_id(t::Thunk)
    t.id
end

function node_id(t)
    dec(hash(t))
end

function node_id(t::Chunk)
    _part_labels[t]
end

function write_dag(io, t)
    !istask(t) && return
    deps = dependents(t)
    c=1
    for k in keys(deps)
        c = write_node(io, k, c)
    end
    for (k, v) in deps
        for dep in v
            if isa(k, Union{Chunk, Thunk})
                println(io, "$(node_id(k)) -> $(node_id(dep))")
            end
        end
    end
end

### Timespan-based graphing

pretty_time(ts::Timespan) = pretty_time(ts.finish-ts.start)
function pretty_time(t)
    if t > 1000^3
        "$(t/(1000^3)) s"
    elseif t > 1000^2
        "$(t/(1000^2)) ms"
    elseif t > 1000
        "$(t/1000) us"
    else
        "$t ns"
    end
end

function write_node(io, ts::Timespan, c)
    f = ts.timeline
    f = isa(f, Function) ? "$f" : "fn"
    t_comp = pretty_time(ts)
    println(io, "$(ts.id) [label=\"$f - $(ts.id)\nCompute: $t_comp\"]")
    c
end

function write_edge(io, ts_comm::Timespan, logs)
    f, id = ts_comm.timeline
    # FIXME: We should print these edges too
    id === nothing && return
    t_comm = pretty_time(ts_comm)
    print(io, "$id -> $(ts_comm.id) [label=\"Comm: $t_comm")
    ts_idx = findfirst(x->x.category==:move &&
                                ts_comm.id==x.id &&
                                id==x.timeline[2], logs)
    if ts_idx !== nothing
        ts_move = logs[ts_idx]
        t_move = pretty_time(ts_move)
        print(io, "\nMove: $t_move")
    end
    println(io, "\"]")
end

function write_dag(io, logs::Vector)
    c = 1
    # FIXME: For edges with nothing ids, print non-task argument node
    for ts in filter(x->x.category==:compute, logs)
        c = write_node(io, ts, c)
    end
    for ts in filter(x->x.category==:comm, logs)
        write_edge(io, ts, logs)
    end
end

function show_plan(io::IO, t)
    print(io, """digraph {
    graph [layout=dot, rankdir=TB];""")
    write_dag(io, t)
    println(io, "}")
end

function show_plan(t::Union{Thunk,Vector})
    io = IOBuffer()
    show_plan(io, t)
    return String(take!(io))
end

function show_plan(c)
    t = thunkize(Context(), stage(Context(), c))
    show_plan(t)
end

function show_plan(t::Tuple)
    show_plan(TupleCompute(t))
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
    if isa(f, Function)
        show(io, f)
    else
        show_ast(io, f)
    end
end
showfn(io, f) = show(io, f)
