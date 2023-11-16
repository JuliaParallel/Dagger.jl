using DelimitedFiles
using Serialization

function load_dir(dir::String; freeze::Bool=false)
    files = map(file->joinpath(dir, file), readdir(dir))
    dg = DGraph()
    function _get_desc_kv(io::IO, key::String)
        l = readline(io)
        k, v = split(l, '=')
        if k != key
            error("Expected $key, got $k")
        end
        return v
    end
    local T, directed, np
    part_descs = NamedTuple[]
    open(joinpath(dir, "dgraph.txt"), "r") do io
        @assert parse(Int, _get_desc_kv(io, "version")) == 1
        T = Base.eval(Meta.parse(_get_desc_kv(io, "T")))
        directed = Base.eval(Meta.parse(_get_desc_kv(io, "directed")))
        np = parse(Int, _get_desc_kv(io, "np"))
        @assert isempty(readline(io))
        for _ in 1:np
            part_file = readline(io)
            back_file = readline(io)
            part_vert_meta_file = readline(io)
            part_edge_meta_file = readline(io)
            back_edge_meta_file = readline(io)
            n_verts = parse(Int, readline(io))
            n_part_edges = parse(Int, readline(io))
            n_back_edges = parse(Int, readline(io))
            n_back_own_edges = parse(Int, readline(io))
            push!(part_descs, (;part_file, back_file,
                                part_vert_meta_file, part_edge_meta_file, back_edge_meta_file,
                                n_verts, n_part_edges, n_back_edges, n_back_own_edges))
            @assert isempty(readline(io))
        end
        @assert eof(io)
    end
    function _load_back_data(T, D, file)
        edges = readdlm(file)
        srcs = edges[:,1]
        dsts = edges[:,2]
        adj = AdjList{T,D}()
        @assert add_edges!(adj, zip(srcs, dsts)) == size(edges, 1)
        return adj
    end
    for desc in part_descs
        (;part_file, back_file,
          part_vert_meta_file, part_edge_meta_file, back_edge_meta_file,
          n_verts, n_part_edges, n_back_edges, n_back_own_edges) = desc
        part_data = Dagger.@spawn loadgraph(part_file)
        back_data = if stat(back_file).size > 0
            Dagger.@spawn _load_back_data(T, directed, back_file)
        else
            Dagger.@spawn AdjList{T,directed}()
        end
        part_vert_meta_data = part_vert_meta_file != "-" ? (Dagger.@spawn deserialize(part_vert_meta_file)) : nothing
        part_edge_meta_data = part_edge_meta_file != "-" ? (Dagger.@spawn deserialize(part_edge_meta_file)) : nothing
        back_edge_meta_data = back_edge_meta_file != "-" ? (Dagger.@spawn deserialize(back_edge_meta_file)) : nothing
        add_partition!(dg, part_data, back_data,
                       part_vert_meta_data, part_edge_meta_data, back_edge_meta_data,
                       n_verts, n_part_edges, n_back_edges, n_back_own_edges)
    end
    freeze && freeze!(dg)
    return dg
end
function save_dir(dir::String, g::DGraph{T,D}) where {T,D}
    mkpath(dir)
    nd = ndigits(nparts(g))
    open(joinpath(dir, "dgraph.txt"), "w+") do io
        println(io, "version=1")
        println(io, "T=$T")
        println(io, "directed=$D")
        println(io, "np=$(nparts(g))")
        println(io)
        for part in 1:nparts(g)
            part_name = lpad(part, nd, '0')

            # FIXME: Write data directly on owner

            # Write partition graph as LightGraphs file (.lgz)
            sg = get_partition(g, part)
            part_file = joinpath(dir, part_name*".part.lgz")
            savegraph(part_file, sg)
            println(io, part_file)

            # Write background edges as delimited file (.txt)
            bg = map(Tuple, edges(get_background(g, part)))
            back_file = joinpath(dir, part_name*".back.txt")
            writedlm(back_file, bg)
            println(io, back_file)

            # Write metadata as serialized data (.jls)
            if has_vertex_metadata(g)
                meta = get_partition_vertex_metadata(g, part)
                meta_file = joinpath(dir, part_name*".part.vertmeta.jls")
                serialize(meta_file, meta)
                println(io, meta_file)
            else
                println(io, "-")
            end
            if has_edge_metadata(g)
                meta = get_partition_edge_metadata(g, part)
                meta_file = joinpath(dir, part_name*".part.edgemeta.jls")
                serialize(meta_file, meta)
                println(io, meta_file)

                meta = get_background_edge_metadata(g, part)
                meta_file = joinpath(dir, part_name*".back.edgemeta.jls")
                serialize(meta_file, meta)
                println(io, meta_file)
            else
                println(io, "-")
                println(io, "-")
            end

            println(io, partition_nv(g, part))
            n_part_edges, n_back_edges, n_back_own_edges = partition_ne(g, part)
            println(io, n_part_edges)
            println(io, n_back_edges)
            println(io, n_back_own_edges)
            println(io)
        end
    end
end
