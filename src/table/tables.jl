#######################################
# Iterator structures

struct DTableRowIterator
    d::DTable
end

struct DTableColumnIterator
    d::DTable
end

struct DTablePartitionIterator
    d::DTable
end



#######################################
# DTable functions

Tables.istable(table::DTable) = true
Tables.rowaccess(table::DTable) = true
Tables.rows(table::DTable) = DTableRowIterator(table)
Tables.columnaccess(table::DTable) = true
Tables.columns(table::DTable) = DTableColumnIterator(table)


function Tables.schema(table::DTable)
    if table.schema !== nothing
        return table.schema
    end
    # Figure out schema
    chunk_f = chunk -> begin
        r = isnonempty(chunk)
        (r, r ? Tables.schema(chunk) : nothing)
    end
    c_idx = 1
    r = (false, nothing)
    while !r[1]
        r = fetch(Dagger.spawn(chunk_f, table.chunks[c_idx]))
        c_idx += 1
    end
    # cache results
    table.schema = r[2]
end


function Tables.columnnames(table::DTable)
    s = Tables.schema(table)
    s === nothing ? nothing : s.names
end


function _getcolumn(table::DTable, col::Union{Symbol, Int})
    chunk_col = (_chunk, _col) -> Tables.getcolumn(_chunk, _col)
    v = [Dagger.spawn(chunk_col, chunk, col) for chunk in table.chunks]
    p = (_chunk) -> (; :_ => fetch(_chunk))
    Tables.getcolumn(TableOperations.joinpartitions(Tables.partitioner(p, v)), :_)
end


Tables.getcolumn(table::DTable, col::Symbol) = _getcolumn(table, col)
Tables.getcolumn(table::DTable, idx::Int) = _getcolumn(table, idx)



#######################################
# DTableRowIterator functions

Tables.schema(table::DTableRowIterator) = Tables.schema(table.d)
length(table::DTableRowIterator) = length(table.d)

function _iterate(iter::DTableRowIterator, chunk_index)
    i = nothing
    row_iterator = nothing
    while i === nothing && chunk_index <= length(iter.d.chunks)
        partition = _retrieve(iter.d.chunks[chunk_index])
        row_iterator = Tables.rows(partition)
        i = iterate(row_iterator)
        chunk_index += 1
    end
    if i === nothing 
        return nothing
    else
        row, row_state = i
        next_chunk_index = chunk_index
        return (row, (row_iterator, row_state, next_chunk_index))
    end
end

Base.iterate(iter::DTableRowIterator) = _iterate(iter, 1)

function Base.iterate(iter::DTableRowIterator, state)
    (row_iterator, row_state, next_chunk_index) = state
    i = iterate(row_iterator, row_state)
    if i === nothing 
        _iterate(iter, next_chunk_index)
    else
        row, row_state = i
        return (row, (row_iterator, row_state, next_chunk_index))
    end
end



#######################################
# DTableColumnIterator functions

Tables.schema(table::DTableColumnIterator) = Tables.schema(table.d)
Tables.columnnames(table::DTableColumnIterator) = Tables.columnnames(table.d)
Tables.getcolumn(table::DTableColumnIterator, col::Symbol) = Tables.getcolumn(table.d, col)
Tables.getcolumn(table::DTableColumnIterator, idx::Int) = Tables.getcolumn(table.d, idx)
length(table::DTableColumnIterator) = length(Tables.columnnames(table))

function _iterate(table::DTableColumnIterator, column_index)
    columns = Tables.columnnames(table)
    if (columns === nothing || length(columns) < column_index)
        return nothing
    else
        return (Tables.getcolumn(table, column_index), column_index + 1)
    end
end

Base.iterate(table::DTableColumnIterator) = _iterate(table, 1)
Base.iterate(table::DTableColumnIterator, state) = _iterate(table, state)



#######################################
# DTablePartitionIterator functions

Tables.partitions(table::DTable) = DTablePartitionIterator(table)
length(table::DTablePartitionIterator) = length(table.d.chunks)

function _iterate(table::DTablePartitionIterator, chunk_index)
    length(table.d.chunks) < chunk_index && return nothing
    (_retrieve(table.d.chunks[chunk_index]), chunk_index + 1)
end

Base.iterate(table::DTablePartitionIterator) = _iterate(table, 1)
Base.iterate(table::DTablePartitionIterator, state) = _iterate(table, state)



#######################################
# GDTable
# For normal rows/columns access it should act the same as a DTable

Tables.istable(table::GDTable) = true
Tables.rowaccess(table::GDTable) = true
Tables.rows(table::GDTable) = DTableRowIterator(table.dtable)
Tables.columnaccess(table::GDTable) = true
Tables.columns(table::GDTable) = DTableColumnIterator(table.dtable)
Tables.schema(table::GDTable) = Tables.schema(table.dtable)
Tables.getcolumn(table::GDTable, col::Symbol) = Tables.getcolumn(table.dtable, col)
Tables.getcolumn(table::GDTable, idx::Int) = Tables.getcolumn(table.dtable, idx)
Tables.columnnames(table::GDTable) = Tables.columnnames(table.dtable) 

#######################################
# GDTable partitions
# Here it makes sense to provide partitions as full key groups
# Same as normal iteration over GDTable, but returns partitions only without keys

struct GDTablePartitionIterator
    d::GDTable
end

Tables.partitions(table::GDTable) = GDTablePartitionIterator(table)

function _iterate(table::GDTablePartitionIterator, it)
    if it === nothing
        return nothing
    else
        ((_, partition), index_iter_state) = it
        return (partition, index_iter_state)
    end
end

iterate(table::GDTablePartitionIterator) = _iterate(table, iterate(table.d))
iterate(table::GDTablePartitionIterator, index_iter_state) = _iterate(table, iterate(table.d, index_iter_state))
