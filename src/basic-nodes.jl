export broadcast, distribute, mappart

######## Distribute ########

# Distribute and MapPart are the two most basic primitives
# used to construct all other primitives

immutable Distribute{T, L<:AbstractLayout} <: ComputeNode
    obj::T
    layout::L
end

"""
    distribute(object, [layout=default_layout(object)])

Distribute `object` according to `layout`
"""
distribute(obj, layout=default_layout(obj)) = Distribute(obj, layout)

function compute(ctx, x::Distribute)

    targets =  chunk_targets(ctx)
    chunks, metadata = partition(ctx, x.obj, x.layout)

    refs = Pair[(targets[i] => remotecall(() -> chunks[i], targets[i]))
                for i in 1:length(targets)]

    DistData(eltype(chunks), refs, x.layout, metadata)
end

"""
    default_layout(object)

Returns a default distribution for `object`.
"""
default_layout(x::AbstractArray) = cutdim(ndims(x))


"""
    broadcast(x)

Broadcast `x` to all workers
"""
broadcast(x) = Distribute(x, Bcast())

######## MapPart ########

immutable MapPart{T} <: ComputeNode
    f
    input::Tuple
end

"""
    mappart(f, nodes::AbstractNode...)

Apply `f` on corresponding chunks of `nodes`. Other compute nodes
fall back to mappart to `compute`.
"""
mappart(f, ns::AbstractNode...) =
    MapPart{typejoin(map(typeof, ns)...)}(f, ns)
mappart(f, ns::Tuple) = mappart(f, ns...)

function compute(ctx, node::MapPart; output_layout=nothing, output_metadata=nothing)
    inputs = [compute(ctx, node) for node in node.input]
    stage1 = mappart(node.f, inputs...)

    if is(output_layout, nothing)
        output_layout = layout(inputs[1])
    end

    if is(output_metadata, nothing)
        output_metadata = metadata(inputs[1])
    end

    if isa(stage1, MapPart{DistData})
        compute(ctx, stage1; output_layout=output_layout, output_metadata=output_metadata) # defined below
    else
        error("Could not compute parents")
    end
end

function compute(ctx, node::MapPart{DistData}; output_layout=UnknownLayout(), output_metadata=Dict())

    input = node.input
    refsets = zip(map(x -> map(y->y[2], refs(x)), input)...) |> collect
    pids = map(x->x[1], refs(input[1]))
    pid_chunks = zip(pids, map(tuplize, refsets)) |> collect

    f = node.f
    futures = Pair[pid => @spawnat pid f(map(fetch, rs)...)
        for (pid, rs) in pid_chunks]
    DistData(futures, output_layout, output_metadata)
end
