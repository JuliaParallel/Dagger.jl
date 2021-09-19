import Base: keys, iterate, length

mutable struct GDTable
    dtable::DTable
    cols::Union{Vector{Symbol}, Nothing}
    index::Dict

    GDTable(dtable, cols, index) = new(dtable, cols, deepcopy(index))
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

"""
    trim!(gd::GDTable) -> GDTable

Removes empty chunks from `gd`.
"""
function trim!(gd::GDTable)
    d = gd.dtable
    check_result = [Dagger.@spawn isnonempty(c) for c in d.chunks]
    results = fetch.(check_result)
    ok_indices = Set(getindex.(filter(x->x[2], collect(enumerate(results))),1))
    d.chunks = getindex.(Ref(d.chunks), ok_indices)

    for key in keys(gd.index)
        gd.index[key] = filter(x -> x ∈ ok_indices, gd.index[key])
    end
    gd
end

"""
    trim(gd::GDTable) -> GDTable

Returns `gd` with empty chunks removed.
"""
trim(gd::GDTable) = trim!(GDTable(DTable(gd.dtable.chunks, gd.dtable.tabletype), gd.cols, gd.index))
