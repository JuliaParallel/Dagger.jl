"""
Column pointers
"""
immutable SparseCSCDomain <: Domain
    adomain::ArrayDomain
    colptr::Array
end

size(d::SparseCSCDomain, arg...) = size(d.adomain, arg...)
length(d::SparseCSCDomain) = Int(d.colptr[end]-1)
indexranges(d::SparseCSCDomain) = indexranges(d.adomain)
parts(d::SparseCSCDomain) = d

function alignfirst(d::SparseCSCDomain)
    adomain = alignfirst(d.adomain)
    colptr = d.colptr .- (d.colptr[1] - 1)
    SparseCSCDomain(adomain, colptr)
end

domain(S::SparseMatrixCSC) = SparseCSCDomain(ArrayDomain(UnitRange[1:S.m, 1:S.n]), S.colptr)

actual_sizeof(S::SparseMatrixCSC) = Bytes(sizeof(S) + sizeof(S.nzval) + sizeof(S.rowval) + sizeof(S.colptr))

function intersect(a::SparseCSCDomain, b::SparseCSCDomain)
    # Return an ArrayDomain here.
    intersect(a.adomain, b.adomain)
end

function intersect(a::SparseCSCDomain, b::ArrayDomain)
    intersect(a.adomain, b)
end

function intersect(a::ArrayDomain, b::SparseCSCDomain)
    intersect(a, b.adomain)
end

function lookup(a::SparseCSCDomain, b::ArrayDomain)
    lookup(a.adomain, b)
end

function getindex(a::SparseCSCDomain, b::SparseCSCDomain)
    a.adomain[b.adomain]
end

function getindex(a::ArrayDomain, b::SparseCSCDomain)
    a[b.adomain]
end
function getindex(a::SparseCSCDomain, b::Domain)
    a.adomain[b]
end

default_partition(::SparseCSCDomain) = SliceDimension(2)

function getsubcolptr(colptr, colrange)
    sub(colptr, first(colrange):(last(colrange)+1))
end

function partition(p::SliceDimension{2}, dom::SparseCSCDomain,
                      elsize::Bytes, chsize::Bytes)

    nnz_per_chunk = chsize/elsize
    # Note: here we ignore size added by colptr
    count = 1
    colstart = 1
    splits = UnitRange[]

    for col in 1:size(dom,2)
        nnzpos = dom.colptr[col+1]
        if (nnzpos >= (count + nnz_per_chunk)) || (col == size(dom,2))
            push!(splits, colstart:col)
            colstart = col+1
            count = nnzpos
        end
    end

    subdomains = [SparseCSCDomain(ArrayDomain(UnitRange[dom.adomain.indexranges[1], s]), getsubcolptr(dom.colptr, s))
                     for s in splits]
    DomainSplit(dom, subdomains)
end

function partition(p::SliceDimension{2}, dom::SparseCSCDomain, nparts::Int)
    nnz = length(dom)
    nnz_per_chunk = nnz/nparts

    nxt = nnz_per_chunk
    colstart = 1
    splits = UnitRange[]

    colptr = dom.colptr
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

    subdomains = [SparseCSCDomain(ArrayDomain((dom.adomain.indexranges[1], s)), getsubcolptr(dom.colptr, s))
                     for s in splits]
    DomainSplit(dom, subdomains)
end

function partition(::SliceDimension{1}, dom::SparseCSCDomain, nparts::Int)
    # say idgaf, bail to dense array partitioning
    partition(SliceDimension(1), dom.adomain, nparts)
end

function getindex(S::SparseMatrixCSC, d::SparseCSCDomain)
    S[d.adomain]
end

# Workaround because cat(n, a, b) returns a dense array
cat(::SliceDimension{1}, a::SparseMatrixCSC, b::SparseMatrixCSC) = vcat(a, b)
cat(::SliceDimension{2}, a::SparseMatrixCSC, b::SparseMatrixCSC) = hcat(a, b)
