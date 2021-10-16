import Base: keys, iterate, length, getindex

"""
    GDTable

Structure representing a grouped `DTable`.
It wraps over a `DTable` object and provides additional information on how the table is grouped.
To represent the grouping a `cols` field is used, which contains the column symbols used for
grouping and an `index`, which allows to effectively lookup the partitions grouped under a single key.
"""
mutable struct GDTable
    dtable::DTable
    cols::Union{Vector{Symbol}, Nothing}
    index::Dict
    grouping_function::Union{Function, Nothing}

    GDTable(dtable, cols, index) = GDTable(dtable, cols, index, nothing)
    GDTable(dtable, cols, index, grouping_function) =
        new(dtable, cols, deepcopy(index), grouping_function)
end

DTable(gd::GDTable) = DTable(gd.dtable.chunks, gd.dtable.tabletype)

fetch(gd::GDTable) = fetch(gd.dtable)
fetch(gd::GDTable, sink) = fetch(gd.dtable, sink)

"""
    grouped_cols(gd::GDTable) -> Vector{Symbol}

Returns the symbols of columns used in the grouping.
In case grouping on a function was performed a `:KEYS` symbol will be returned.
"""
grouped_cols(gd::GDTable) = gd.cols === nothing ? [:KEYS] : gd.cols

"""
    keys(gd::GDTable) -> KeySet

Returns the keys that `gd` is grouped by.
"""
keys(gd::GDTable) = keys(gd.index)

partition(gd::GDTable, key) = partition(gd, gd.index[key])
partition(gd::GDTable, indices::Vector{UInt}) = DTable(VTYPE(getindex.(Ref(gd.dtable.chunks), indices)), gd.dtable.tabletype)

length(gd::GDTable) = length(keys(gd.index))


iterate(gd::GDTable) = _iterate(gd, iterate(gd.index))
iterate(gd::GDTable, state) = _iterate(gd, iterate(gd.index, state))

function _iterate(gd::GDTable, it)
    if it !== nothing
        ((key, partition_indices), state) = it
        return key => partition(gd, partition_indices), state
    end
    return nothing
end


"""
    trim!(gd::GDTable) -> GDTable

Removes empty chunks from `gd` and unused keys from its index.
"""
function trim!(gd::GDTable)
    d = gd.dtable
    check_result = [Dagger.@spawn isnonempty(c) for c in d.chunks]
    results = fetch.(check_result)

    ok_indices = filter(x -> results[x], 1:length(results))
    d.chunks = getindex.(Ref(d.chunks), sort(ok_indices))

    offsets = zeros(UInt, length(results))

    counter = zero(UInt)
    for (i, r) in enumerate(results)
        counter = r ? counter : counter + 1
        offsets[i] = counter
    end

    for key in keys(gd.index)
        ind = gd.index[key]
        filter!(x -> results[x], ind)

        if isempty(ind)
            delete!(gd.index, key)
        else
            gd.index[key] = ind .- getindex.(Ref(offsets), ind)
        end
    end
    gd
end


"""
    trim(gd::GDTable) -> GDTable

Returns `gd` with empty chunks and keys removed.
"""
trim(gd::GDTable) = trim!(GDTable(DTable(gd), gd.cols, gd.index))


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
    grouped_by_cols = isnothing(gd.cols) ? string(gd.grouping_function) : grouped_cols(gd)
    println(io, "GDTable with $(length(gd.dtable.chunks)) partitions and $(length(keys(gd.index))) keys")
    println(io, "Tabletype: $tabletype")
    print(io, "Grouped by: $grouped_by_cols")
    nothing
end

"""
    getindex(gdt::GDTable, key) -> DTable

Retrieves a `DTable` from `gdt` with rows belonging to the provided grouping key.
"""
function getindex(gdt::GDTable, key)
    ck = convert(keytype(gdt.index), key)
    ck ∉ keys(gdt) && throw(KeyError(ck))
    partition(gdt, ck)
end
