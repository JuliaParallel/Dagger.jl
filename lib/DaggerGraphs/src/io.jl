using DelimitedFiles

function load_dir(dir::String; freeze::Bool=false)
    files = map(file->joinpath(dir, file), readdir(dir))
    part_files = filter(file->endswith(file, ".part.lgz"), files)
    back_files = filter(file->endswith(file, ".back.txt"), files)
    dg = DGraph()
    for file in part_files
        g = loadgraph(file)
        add_partition!(dg, g)
    end
    for file in back_files
        stat(file).size > 0 || continue
        edges = readdlm(file)
        srcs = edges[:,1]
        dsts = edges[:,2]
        add_edges!(dg, zip(srcs, dsts))
    end
    freeze && freeze!(dg)
    return dg
end
function save_dir(dir::String, g)
    mkpath(dir)
    nd = ndigits(nparts(g))
    for part in 1:nparts(g)
        part_name = lpad(part, nd, '0')
        sg = get_partition(g, part)
        savegraph(joinpath(dir, part_name*".part.lgz"), sg)
        bg = map(Tuple, edges(get_background(g, part)))
        writedlm(joinpath(dir, part_name*".back.txt"), bg)
    end
end
