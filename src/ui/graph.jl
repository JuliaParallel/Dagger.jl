export show_plan

### DAG-based graphing

function write_node(io, t::Thunk, c)
    f = isa(t.f, Function) ? "$(t.f)" : "fn"
    println(io, "n_$(t.id) [label=\"$f - $(t.id)\"];")
    c
end

dec(x) = Base.dec(x, 0, false)
function write_node(io, t, c, id=dec(hash(t)))
    l = replace(node_label(t), "\""=>"")
    println(io, "n_$id", " [label=\"$l\"];")
    c
end
function node_label(t)
    iob = IOBuffer()
    Base.show(iob, t)
    String(take!(iob))
end
function node_label(x::T) where T<:AbstractArray
    "$T\nShape: $(size(x))\nSize: $(pretty_size(sizeof(x)))"
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

function write_dag(io, t::Thunk)
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
    r(t) = round(t; digits=3)
    if t > 1000^3
        "$(r(t/(1000^3))) s"
    elseif t > 1000^2
        "$(r(t/(1000^2))) ms"
    elseif t > 1000
        "$(r(t/1000)) us"
    else
        "$(r(t)) ns"
    end
end
function pretty_size(sz)
    if sz > 1024^4
        "$(sz/(1024^4)) TB (terabytes)"
    elseif sz > 1024^3
        "$(sz/(1024^3)) GB (gigabytes)"
    elseif sz > 1024^2
        "$(sz/(1024^2)) MB (megabytes)"
    elseif sz > 1024
        "$(sz/1024) KB (kilobytes)"
    else
        "$sz B (bytes)"
    end
end

function write_node(io, ts::Timespan, c)
    f, res_type, res_sz = ts.timeline
    f = isa(f, Function) ? "$f" : "fn"
    t_comp = pretty_time(ts)
    sz_comp = pretty_size(res_sz)
    println(io, "n_$(ts.id) [label=\"$f - $(ts.id)\nCompute: $t_comp\nResult Type: $res_type\nResult Size: $sz_comp\"]")
    c
end

function write_edge(io, ts_comm::Timespan, logs)
    f, id = ts_comm.timeline
    # FIXME: We should print these edges too
    id === nothing && return
    t_comm = pretty_time(ts_comm)
    print(io, "n_$id -> n_$(ts_comm.id[1]) [label=\"Comm: $t_comm")
    ts_idx = findfirst(x->x.category==:move &&
                                ts_comm.id==x.id &&
                                id==x.timeline[2], logs)
    if ts_idx !== nothing
        ts_move = logs[ts_idx]
        t_move = pretty_time(ts_move)
        print(io, "\nMove: $t_move")
    end
    println(io, "\"];")
end

write_edge(io, from::String, to::String) = println(io, "n_$from -> n_$to")

getargs!(d, t) = nothing
function getargs!(d, t::Thunk)
    d[t.id] = [filter(!istask, [t.inputs...,])...,]
    foreach(i->getargs!(d, i), t.inputs)
end
function write_dag(io, logs::Vector, t=nothing)
    argmap = Dict{Int,Vector}()
    getargs!(argmap, t)
    c = 1
    for ts in filter(x->x.category==:compute, logs)
        c = write_node(io, ts, c)
    end
    argnodemap = Dict{Int,Vector{String}}()
    argids = IdDict{Any,String}()
    for id in keys(argmap)
        nodes = String[]
        arg_c = 1
        for arg in argmap[id]
            name = "$(id)_arg_$(arg_c)"
            if !isimmutable(arg)
                if arg in keys(argids)
                    name = argids[arg]
                else
                    argids[arg] = name
                    c = write_node(io, arg, c, name)
                end
                push!(nodes, name)
            else
                c = write_node(io, arg, c, name)
                push!(nodes, name)
            end
            arg_c += 1
        end
        argnodemap[id] = nodes
    end
    for ts in filter(x->x.category==:comm, logs)
        write_edge(io, ts, logs)
    end
    for id in keys(argnodemap)
        for arg in argnodemap[id]
            write_edge(io, arg, string(id))
        end
    end
end

function show_plan(io::IO, t)
    print(io, """digraph {
    graph [layout=dot, rankdir=TB];""")
    write_dag(io, t)
    println(io, "}")
end
function show_plan(io::IO, logs::Vector{Timespan}, t::Thunk)
    print(io, """digraph {
    graph [layout=dot, rankdir=TB];""")
    write_dag(io, logs, t)
    println(io, "}")
end

function show_plan(t::Union{Thunk,Vector{Timespan}})
    io = IOBuffer()
    show_plan(io, t)
    return String(take!(io))
end
function show_plan(logs::Vector{Timespan}, t::Thunk)
    io = IOBuffer()
    show_plan(io, logs, t)
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
