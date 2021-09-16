import Base: keys, iterate, length

mutable struct GDTable
    dtable::DTable
    cols::Union{Vector{Symbol}, Nothing}
    index::Dict
end

grouped_cols(gd::GDTable) = gd.cols === nothing ? [:keys] : gd.cols

keys(gd::GDTable) = keys(gd.index)

fetch(gd::GDTable) = fetch(gd.dtable)

gd_partition(gd::GDTable, key) = gd_partition(gd, gd.index[key])
gd_partition(gd::GDTable, indices::Vector{Int}) = DTable(getindex.(Ref(gd.dtable.chunks), indices), gd.dtable.tabletype) 

length(gd::GDTable) = length(keys(gd.index))

function iterate(gd::GDTable)
    it = iterate(gd.index)
    if it !== nothing
        (i, state) = it
        return i[1] => gd_partition(gd, i[2]), state
    end
    nothing
end

function iterate(gd::GDTable, state)
    it = iterate(gd.index, state)
    if it !== nothing
        (i, state) = it
        return i[1] => gd_partition(gd, i[2]), state
    end
    nothing
end
