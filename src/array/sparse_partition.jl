export partition_sparse
using SparseArrays

function partition_sparse(colptr, nnz, sz, nparts)
    nnz_per_chunk = nnz/nparts
    dom = ArrayDomain(map(x->1:x, sz))

    nxt = nnz_per_chunk
    colstart = 1
    splits = UnitRange[]

    for i=1:nparts-1
       idxs = searchsorted(colptr, nxt)
       if length(idxs) == 0
           # the exact index was not found. latch on to the nearest column boundary
           idx = first(idxs)
           if abs(colptr[idx]-nxt) < abs(colptr[idx-1]-nxt)
               nextidx = idx
           else
               nextidx = idx-1
           end
       else
           nextidx = last(idxs)
       end
       push!(splits, colstart:nextidx)
       colstart=nextidx+1
       nxt=nxt + nnz_per_chunk
    end
    push!(splits, colstart:(length(colptr)-1))


    cumlength = cumsum(map(length, splits))
    subdomains = DomainBlocks((1,1), ([size(dom, 1)], cumlength))
    DomainSplit(dom, subdomains)
end

partition_sparse(S::SparseMatrixCSC, nparts) =
    partition_sparse(S.colptr, length(S.nzval), size(S), nparts)
