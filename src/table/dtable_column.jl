
mutable struct DTableColumn{T,TT}
    dtable::DTable
    current_chunk::Int
    col::Int
    colname::Symbol
    chunk_lengths::Vector{Int}
    current_iterator::Union{Nothing,TT}
    chunkstore::Union{Nothing,Vector{T}}
end

__ff = (ch, col) -> Tables.getcolumn(Tables.columns(ch), col)

function DTableColumn(dtable::DTable, col::Int)
    column_eltype = Tables.schema(Tables.columns(dtable)).types[col]
    iterator_type = fetch(Dagger.spawn((ch, _col) -> typeof(iterate(__ff(ch, _col))), dtable.chunks[1], col))

    DTableColumn{column_eltype,iterator_type}(
        dtable,
        0,
        col,
        _columnnames_svector(dtable)[col],
        chunk_lengths(dtable),
        nothing,
        nothing,
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
        s = s + e
    end
    chunk_idx == 0 && throw(BoundsError())
    offset = idx - s + 1
    chunk = fetch(Dagger.spawn(__ff, dtablecolumn.dtable.chunks[chunk_idx], dtablecolumn.col))

    row, iter = iterate(Tables.rows(chunk))
    for _ in 1:(offset-1)
        row, iter = iterate(Tables.rows(chunk), iter)
    end
    Tables.getcolumn(row, dtablecolumn.col)
end

length(dtablecolumn::DTableColumn) = sum(dtablecolumn.chunk_lengths)


function pull_next_chunk(dtablecolumn::DTableColumn, chunkidx::Int)
    while dtablecolumn.current_iterator === nothing
        chunkidx += 1
        if chunkidx <= length(dtablecolumn.dtable.chunks)
            dtablecolumn.chunkstore =
                fetch(Dagger.spawn(__ff, dtablecolumn.dtable.chunks[chunkidx], dtablecolumn.col))
        else
            return chunkidx
        end
        dtablecolumn.current_iterator = iterate(dtablecolumn.chunkstore)
    end
    return chunkidx
end


function iterate(dtablecolumn::DTableColumn)
    if length(dtablecolumn) == 0
        return nothing
    end
    dtablecolumn.chunkstore = nothing
    dtablecolumn.current_iterator = nothing
    chunkidx = pull_next_chunk(dtablecolumn, 0)
    ci = dtablecolumn.current_iterator
    if ci === nothing
        return nothing
    else
        return (ci[1], (chunkidx, ci[2]))
    end
end

function iterate(dtablecolumn::DTableColumn, iter)
    (chunkidx, i) = iter
    cs = dtablecolumn.chunkstore
    ci = nothing
    if cs !== nothing
        ci = iterate(cs, i)
    else
        return nothing
    end
    dtablecolumn.current_iterator = ci
    chunkidx = pull_next_chunk(dtablecolumn, chunkidx)
    ci = dtablecolumn.current_iterator
    if ci === nothing
        return nothing
    else
        return (ci[1], (chunkidx, ci[2]))
    end
end
