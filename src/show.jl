import Base.show

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

function showsplat(io::IO, xs::Tuple)
    first = true
    for x in xs
        !first && write(io, ", ")
        first = false
        show(io, x)
    end
end

function show(io::IO, x::Comp)
    showfn(io, x.f)
    write(io, " o ")
    showfn(io, x.g)
end

function show(io::IO, x::MapNode)
    write(io, "map(")
    showfn(io, x.f)
    write(io, ", ")
    showsplat(io, x.input)
    write(io, ")")
end

function show_mrnode(io::IO, name, f, op, v0, input)
    write(io, name, "(")
    showfn(io, f)
    write(io, ", ")
    showfn(io, op)
    write(io, ", ")
    show(io, v0)
    write(io, ", ")
    showsplat(io, input)
    write(io, ")")
end

function show_mrnode(io::IO, name, op, v0, input)
    write(io, name, "(")
    showfn(io, op)
    write(io, ", ")
    show(io, v0)
    write(io, ", ")
    showsplat(io, input)
    write(io, ")")
end

function show{T<:Tuple}(io::IO, x::MapReduceNode{T, IdFun})
    show_mrnode(io, "reduce", x.op, x.v0, x.input)
end

function show(io::IO, x::MapReduceNode)
    show_mrnode(io, "mapreduce", x.f, x.op, x.v0, x.input)
end

function show{T<:Tuple}(io::IO, x::MapReduceByKey{T, IdFun})
    show_mrnode(io, "reducebykey", x.op, x.v0, x.input)
end

function show(io::IO, x::MapReduceByKey)
    show_mrnode(io, "mapreducebykey", x.f, x.op, x.v0, x.input)
end

function show(io::IO, x::FilterNode)
    write(io, "map(")
    showfn(io, x.f)
    write(io, ", ")
    show(io, x.input)
    write(io, ")")
end

function show(io::IO, x::Partitioned)
    write(io, "Partitioned(")
    write(io, summary(x.obj))
    write(io, ")")
end
