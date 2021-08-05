import Tables
import TableOperations

import Base: fetch

export DTable, tabletype

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
    DTable(chunks::VTYPE, tabletype) = new(chunks, tabletype)
end

DTable(chunks::Vector{Dagger.EagerThunk}, args...) = DTable(VTYPE(chunks), args...)
DTable(chunks::Vector{Dagger.Chunk}, args...) = DTable(VTYPE(chunks), args...)

include("iterators.jl")
include("operations.jl")

"""
    DTable(table, chunksize; tabletype=nothing) -> DTable

Constructs a `DTable` using a `Tables.jl` compatible `table` input.
It assumes no initial partitioning of the table and uses the `chunksize`
argument to partition the table (based on row count).

Providing `tabletype` kwarg overrides the internal table partition type.
"""
function DTable(table, chunksize::Integer; tabletype=nothing)
    if !Tables.istable(table)
        throw(ArgumentError("Provided input is not Tables.jl compatible."))
    end

    parts = Tables.partitions(TableOperations.makepartitions(table, chunksize))
    sink = Tables.materializer(tabletype !== nothing ? tabletype() : table)

    chunks = Vector{Dagger.Chunk}()
    sizehint!(chunks, length(parts))

    type = nothing

    for p in parts
        tpart = sink(p)
        push!(chunks, Dagger.tochunk(tpart))
        if type === nothing
            type = typeof(tpart).name.wrapper
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

_retrieve_partitions(d::DTable) = TableOperations.joinpartitions(Tables.partitioner(_retrieve, d.chunks))

_retrieve(x::Dagger.EagerThunk) = fetch(x)
_retrieve(x::Dagger.Chunk) = collect(x)

"""
    tabletype(d::DTable)

Provides the type of the underlying table partition.
"""
function tabletype(d::DTable)
    if d.tabletype === nothing
        f = c -> typeof(c).name.wrapper
        d.tabletype = fetch(Dagger.@spawn f(d.chunks[1]))
    end
    return d.tabletype
end
