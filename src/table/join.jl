r = (id=10,)

d1=  DTable((id=1:10,a=1.0:10.0), 5)
d2 = (id=1:10, b=11:20)

function leftjoin(d1::DTable, d2)
    col = :id
    rowmap = row -> (Tables.getcolumn(row, :id),)
    match = Vector{Bool}(undef, length(Tables.rows(d1)))
    for o in Tables.rows(d1)

        for i in Tables.rows(d2)
        
            continue
        end
    end
end