import Base: eachrow, eachcol

struct DTableRows
    dtable::DTable
end

struct DTableColumns
    dtable::DTable
end

Base.eachrow(d::DTable) = DTableRows(d)
Base.eachcol(d::DTable) = DTableColumns(d)
