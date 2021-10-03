Tables.istable(table::DTable) = true
Tables.rowaccess(table::DTable) = true
Tables.rows(table::DTable) = DTableRowIterator(table)

struct DTableRowIterator
    d::DTable
end

function Base.iterate(iter::DTableRowIterator; c_idx=0)
    r = nothing
    i = nothing
    while isnothing(r) && c_idx < length(iter.d.chunks)
        c_idx += 1 
        p = _retrieve(iter.d.chunks[c_idx])
        i = Tables.rows(p)
        r = iterate(i)
    end
    isnothing(r) ? nothing : (r[1], (i, r[2], c_idx))
end

function Base.iterate(iter::DTableRowIterator, state)
    (i, i_state, c_idx) = state
    r = iterate(i, i_state)
    !isnothing(r) && return (r[1], (i, r[2], c_idx))
    iterate(iter, c_idx=c_idx)
end

Tables.columnaccess(table::DTable) = true
Tables.columns(table::DTable) = DTableColumnIterator(table)

function Tables.getcolumn(table::DTable, col::Symbol)
    chunk_col = (_chunk, _col) -> Tables.getcolumn(_chunk, _col)
    v = [Dagger.spawn(chunk_col, chunk, col) for chunk in table.chunks]

    p = (_chunk, _col) -> (;_col => _retrieve(_chunk))
    pp = x -> p(x, col)
    Tables.getcolumn(TableOperations.joinpartitions(Tables.partitioner(pp, v)), col)
end

function Tables.columnnames(table::DTable)
    chunk_f = chunk -> begin
        r = isnonempty(chunk)
        (r, r ? Tables.columnnames(chunk) : nothing)
    end
    c_idx = 1
    r = (false, nothing)
    while !r[1]
        r = fetch(Dagger.spawn(chunk_f, table.chunks[c_idx]))
    end
    !isnothing(r[2]) ? r[2] : error("Column names not found")
end


struct DTableColumnIterator
    d::DTable
end

Tables.columnnames(table::DTableColumnIterator) = Tables.columnnames(table.d)
Tables.getcolumn(table::DTableColumnIterator, col::Symbol) = Tables.getcolumn(table.d, col)

function iterate(table::DTableColumnIterator)
    # todo
end

function iterate(table::DTableColumnIterator, state)
    # todo
end