import Base: keys, iterate, length

"""
    GDTable

Structure representing a grouped `DTable`.
It wraps over a DTable object and provides additional information on how the `GDTable` is grouped.
To represent the grouping a `cols` field is used, which contains the column symbols used for
grouping and an `index`, which allows to effectively lookup the partitions grouped under a single key.
"""
mutable struct GDTable
    dtable::DTable
    cols::Union{Vector{Symbol}, Nothing}
    index::Dict

    GDTable(dtable, cols, index) = new(dtable, cols, deepcopy(index))
end

fetch(gd::GDTable) = fetch(gd.dtable)

"""
    grouped_cols(gd::GDTable) -> Vector{Symbol}

Returns the symbols of columns used in the grouping.
In case grouping on a function was performed a 
"""
grouped_cols(gd::GDTable) = gd.cols === nothing ? [:KEYS] : gd.cols

keys(gd::GDTable) = keys(gd.index)

partition(gd::GDTable, key) = partition(gd, gd.index[key])
partition(gd::GDTable, indices::Vector{Int}) = DTable(getindex.(Ref(gd.dtable.chunks), indices), gd.dtable.tabletype) 

length(gd::GDTable) = length(keys(gd.index))


function iterate(gd::GDTable)
    it = iterate(gd.index)
    if it !== nothing
        (i, state) = it
        return i[1] => partition(gd, i[2]), state
    end
    nothing
end


function iterate(gd::GDTable, state)
    it = iterate(gd.index, state)
    if it !== nothing
        (i, state) = it
        return i[1] => partition(gd, i[2]), state
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
    ok_indices = Set(getindex.(filter(x -> x[2], collect(enumerate(results))),1))
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


"""
    tabletype!(gd::GDTable)

Provides the type of the underlying table partition and caches it in `gd`.

In case the tabletype cannot be obtained the default return value is `NamedTuple`.
"""
tabletype!(gd::GDTable) = gd.dtable.tabletype = resolve_tabletype(gd.dtable)


"""
    tabletype(gd::GDTable)

Provides the type of the underlying table partition.
Uses the cached tabletype if available.

In case the tabletype cannot be obtained the default return value is `NamedTuple`.
"""
tabletype(gd::GDTable) = gd.dtable.tabletype === nothing ? resolve_tabletype(gd.dtable) : gd.dtable.tabletype


show(io::IO, gd::GDTable) = show(io, MIME"text/plain"(), gd)


function show(io::IO, ::MIME"text/plain", gd::GDTable)
    tabletype = isnothing(gd.dtable.tabletype) ? "unknown (use `tabletype!(::GDTable)`)" : gd.dtable.tabletype
    grouped_by_cols = isnothing(gd.cols) ? "custom function" : grouped_cols(gd)
    println(io, "GDTable with $(length(gd.dtable.chunks)) partitions and $(length(keys(gd.index))) keys")
    println(io, "Tabletype: $tabletype")
    print(io, "Grouped by: $grouped_by_cols")
    nothing
end
