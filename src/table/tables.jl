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
    if !isnothing(table.schema)
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
    isnothing(s) ? nothing : s.names
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



#######################################
# DTableColumnIterator functions

Tables.schema(table::DTableColumnIterator) = Tables.schema(table.d)
Tables.columnnames(table::DTableColumnIterator) = Tables.columnnames(table.d)
Tables.getcolumn(table::DTableColumnIterator, col::Symbol) = Tables.getcolumn(table.d, col)
Tables.getcolumn(table::DTableColumnIterator, idx::Int) = Tables.getcolumn(table.d, idx)
length(table::DTableColumnIterator) = length(Tables.columnnames(table))


function Base.iterate(table::DTableColumnIterator; idx=1)
    columns = Tables.columnnames(table)
    (isnothing(columns) || length(columns) < idx) && return nothing
    (Tables.getcolumn(table, idx), idx + 1)
end


Base.iterate(table::DTableColumnIterator, state) = iterate(table; idx=state)



#######################################
# DTablePartitionIterator functions

Tables.partitions(table::DTable) = DTablePartitionIterator(table)
length(table::DTablePartitionIterator) = length(table.d.chunks)

function Base.iterate(table::DTablePartitionIterator; idx=1)
    length(table.d.chunks) < idx && return nothing
    (_retrieve(table.d.chunks[idx]), idx + 1)
end


Base.iterate(table::DTablePartitionIterator, state) = iterate(table; idx=state)



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

function iterate(table::GDTablePartitionIterator)
    it = iterate(table.d.dtable)
    it === nothing ? nothing : (it[1][2], it[2])
end

function iterate(table::GDTablePartitionIterator, state)
    it = iterate(table.d.dtable, state)
    it === nothing ? nothing : (it[1][2], it[2])
end
