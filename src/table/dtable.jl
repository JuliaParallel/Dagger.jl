import Tables
import TableOperations
import SentinelArrays

import Base: fetch, show, length, iterate

export DTable, tabletype, tabletype!, trim, trim!, leftjoin, innerjoin, DTableColumn

const VTYPE = Vector{Union{Dagger.Chunk,Dagger.EagerThunk}}

"""
    DTable

Structure representing the distributed table based on Dagger.

The table is stored as a vector of `Chunk` structures which hold partitions of the table.
That vector can also store `EagerThunk` structures when an operation that modifies
the underlying partitions was applied to it (currently only `filter`).
"""
mutable struct DTable
    chunks::VTYPE
    tabletype
    schema::Union{Nothing,Tables.Schema}
    DTable(chunks::VTYPE, tabletype) = new(chunks, tabletype, nothing)
end

DTable(chunks::Vector{Dagger.EagerThunk}, args...) = DTable(VTYPE(chunks), args...)
DTable(chunks::Vector{Dagger.Chunk}, args...) = DTable(VTYPE(chunks), args...)

"""
    DTable(table; tabletype=nothing) -> DTable

Constructs a `DTable` using a `Tables.jl`-compatible input `table`.
Calls `Tables.partitions` on `table` and assumes the provided partitioning.
"""
function DTable(table; tabletype=nothing)
    chunks = Vector{Dagger.Chunk}()
    type = nothing
    sink = nothing
    for partition in Tables.partitions(table)
        if sink === nothing
            sink = Tables.materializer(tabletype !== nothing ? tabletype() : partition)
        end

        tpart = sink(partition)
        push!(chunks, Dagger.tochunk(tpart))

        if type === nothing
            type = typeof(tpart).name.wrapper
        end
    end
    return DTable(chunks, type)
end


"""
    DTable(table, chunksize; tabletype=nothing) -> DTable

Constructs a `DTable` using a `Tables.jl` compatible `table` input.
It assumes no initial partitioning of the table and uses the `chunksize`
argument to partition the table (based on row count).

Providing `tabletype` kwarg overrides the internal table partition type.
"""
function DTable(table, chunksize::Integer; tabletype=nothing)
    chunks = Vector{Dagger.Chunk}()
    type = nothing
    sink = Tables.materializer(tabletype !== nothing ? tabletype() : table)
    for outer_partition in Tables.partitions(table)
        for inner_partition in Tables.partitions(TableOperations.makepartitions(outer_partition, chunksize))
            tpart = sink(inner_partition)
            push!(chunks, Dagger.tochunk(tpart))
            if type === nothing
                type = typeof(tpart).name.wrapper
            end
        end
    end
    return DTable(chunks, type)
end

"""
    DTable(loader_function, files::Vector{String}; tabletype=nothing)

Constructs a `DTable` using a list of filenames and a `loader_function`.
Partitioning is based on the contents of the files provided, which means that
one file is used to create one partition.

Providing `tabletype` kwarg overrides the internal table partition type.
"""
function DTable(loader_function, files::Vector{String}; tabletype=nothing)
    chunks = Vector{Dagger.EagerThunk}()
    sizehint!(chunks, length(files))

    append!(chunks, map(file -> Dagger.spawn(_file_load, file, loader_function, tabletype), files))

    return DTable(chunks, tabletype)
end

function _file_load(filename, loader_function, tabletype)
    part = loader_function(filename)
    sink = Tables.materializer(tabletype === nothing ? part : tabletype())
    tpart = sink(part)
    return tpart
end

"""
    fetch(d::DTable)

Collects all the chunks in the `DTable` into a single, non-distributed
instance of the underlying table type.

Fetching an empty DTable results in returning an empty `NamedTuple` regardless of the underlying `tabletype`.
"""
function fetch(d::DTable)
    sink = Tables.materializer(tabletype(d)())
    sink(_retrieve_partitions(d))
end

"""
    fetch(d::DTable, sink)

Collects all the chunks in the `DTable` into a single, non-distributed
instance of table type created using the provided `sink` function.
"""
fetch(d::DTable, sink) = sink(_retrieve_partitions(d))

function _retrieve_partitions(d::DTable)
    d2 = trim(d)
    return nchunks(d2) > 0 ?
        TableOperations.joinpartitions(Tables.partitioner(_retrieve, d2.chunks)) : NamedTuple()
end

_retrieve(x::Dagger.EagerThunk) = fetch(x)
_retrieve(x::Dagger.Chunk) = collect(x)

"""
    tabletype!(d::DTable)

Provides the type of the underlying table partition and caches it in `d`.

In case the tabletype cannot be obtained the default return value is `NamedTuple`.
"""
tabletype!(d::DTable) = d.tabletype = resolve_tabletype(d)

"""
    tabletype(d::DTable)

Provides the type of the underlying table partition.
Uses the cached tabletype if available.

In case the tabletype cannot be obtained the default return value is `NamedTuple`.
"""
tabletype(d::DTable) = d.tabletype === nothing ? resolve_tabletype(d) : d.tabletype

function resolve_tabletype(d::DTable)
    _type = c -> isnonempty(c) ? typeof(c).name.wrapper : nothing
    t = nothing

    if nchunks(d) > 0
        for chunk in d.chunks
            t = fetch(Dagger.@spawn _type(chunk))
            t !== nothing && break
        end
    end
    t !== nothing ? t : NamedTuple
end

function isnonempty(chunk)
    length(Tables.rows(chunk)) > 0 && length(Tables.columnnames(chunk)) > 0
end

"""
    trim!(d::DTable) -> DTable

Removes empty chunks from `d`.
"""
function trim!(d::DTable)
    check_result = [Dagger.@spawn isnonempty(c) for c in d.chunks]
    d.chunks = getindex.(filter(x -> fetch(check_result[x[1]]), collect(enumerate(d.chunks))), 2)
    d
end

"""
    trim(d::DTable) -> DTable

Returns `d` with empty chunks removed.
"""
trim(d::DTable) = trim!(DTable(d.chunks, d.tabletype))

show(io::IO, d::DTable) = show(io, MIME"text/plain"(), d)

function show(io::IO, ::MIME"text/plain", d::DTable)
    tabletype = d.tabletype === nothing ? "unknown (use `tabletype!(::DTable)`)" : d.tabletype
    println(io, "DTable with $(nchunks(d)) partitions")
    print(io, "Tabletype: $tabletype")
    nothing
end

function chunk_lengths(table::DTable)
    f = x -> length(Tables.rows(x))
    fetch.([Dagger.@spawn f(c) for c in table.chunks])
end

function length(table::DTable)
    sum(chunk_lengths(table))
end

function _columnnames_svector(d::DTable)
    colnames_tuple = determine_columnnames(d)
    colnames_tuple !== nothing ? [sym for sym in colnames_tuple] : nothing
end

@inline nchunks(d::DTable) = length(d.chunks)

merge_chunks(sink, chunks) = sink(TableOperations.joinpartitions(Tables.partitioner(_retrieve, chunks)))



mutable struct DTableColumn
    dtable::DTable
    current_chunk::Int
    col::Int
    colname::Symbol
    chunk_lengths::Vector
end

function DTableColumn(dtable::DTable, col::Int)
    DTableColumn(
        dtable,
        0,
        col,
        _columnnames_svector(dtable)[col],
        chunk_lengths(dtable)
    )
end



function getindex(dtablecolumn::DTableColumn, idx::Int)
    chunk_idx = 0
    s = 1
    for (i, e) in enumerate(dtablecolumn.chunk_lengths)
        if s <= idx < s + e
            chunk_idx = i
            break
        end
        s=s+e
    end
    chunk_idx == 0 && throw(BoundsError())
    offset = idx - s + 1
    chunk = fetch(dtablecolumn.dtable.chunks[chunk_idx])

    row, iter = iterate(Tables.rows(chunk))
    for _ in 1:(offset - 1)
        row, iter = iterate(Tables.rows(chunk), iter)
    end
    Tables.getcolumn(row, dtablecolumn.col)
end

function length(dtablecolumn::DTableColumn)
    sum(dtablecolumn.chunk_lengths)
end

function iterate(dtablecolumn::DTableColumn)
    length(dtablecolumn) == 0 && return nothing
    iter = nothing
    chunkidx = 1
    chunk = nothing
    while iter === nothing
        if chunkidx <= length(dtablecolumn.dtable.chunks)
            chunk = fetch(dtablecolumn.dtable.chunks[chunkidx])
        else
            return nothing
        end

        iter = iterate(Tables.rows(chunk))
    end
    iter === nothing && return nothing
    row, i = iter

    return (Tables.getcolumn(row, dtablecolumn.col), (chunkidx, chunk, i))
end

function iterate(dtablecolumn::DTableColumn, iter)
    (chunkidx, chunk, i) = iter
    rrr = iterate(Tables.rows(chunk), i)
    while rrr === nothing
        chunkidx += 1
        if chunkidx <= length(dtablecolumn.dtable.chunks)
            chunk = fetch(dtablecolumn.dtable.chunks[chunkidx])
        else
            return nothing
        end
        rrr = iterate(Tables.rows(chunk))
    end
    rrr === nothing && return nothing
    row, i = rrr
    return (Tables.getcolumn(row, dtablecolumn.col), (chunkidx, chunk, i))
end
