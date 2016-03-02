# Maintain a per-worker pool of memory

using Blobs
using Base.Random: UUID
import Blobs: load,save

APPROX_CHUNKSIZE = 64MB
MAX_CHUNKS = ceil(Int, Sys.free_memory()/(nworkers()*2APPROX_CHUNKSIZE).val)

global _mem_pool = BlobCollection(Any, Mutable(CFWWriter()), CFWReader(); maxcache::Int=MAX_CHUNKS)
function gather(ctx, t::MemToken)
    Blobs.load(t.pool, t.id)
end


#### CFWReader
# Blobs.save method for CFWReader calls different things based on the data

immutable CFWReader <: BlobIO end
immutable CFWWriter <: BlobIO end

immutable CFWIOMeta <: BlobMeta{Any}
    datatype::Type
    filename::AbstractString
    sizemeta::Any
end
sizemeta(x::Array) = size(x)
sizemeta(x::SparseMatrixCSC) = nothing # this is encoded in the file

function save(data, meta::CFWIOMeta, writer::CFWWriter)
    open(meta.filename, "w") do io
        _save(io, data)
    end
end

function load(data, meta::CFWIOMeta, reader::CFWReader)
    open(meta.filename, "w") do io
        _load(io, meta.datatype, meta.sizemeta, data)
    end
end

### Dense arrays
function _save(io::IO, m::Array)
    write(io, reinterpret(UInt8, m, (sizeof(m),)))
    nothing
end

function _load{T<:Array}(io::IO, t::Type{T}, sz)
    Mmap.mmap(io, t, sz)
end

### Sparse arrays
function _save{Tv, Ti}(io::IO, m::SparseMatrixCSC{Tv,Ti})
    write(io, m.m)
    write(io, m.n)
    write(io, length(m.nzval))

    typ_io = IOBuffer()
    serialize(typ_io, (Tv, Ti))
    buf = takebuf_array(typ_io)
    write(io, sizeof(buf))
    write(io, buf)

    write(io, reinterpret(UInt8, m.colptr, (sizeof(m.colptr),)))
    write(io, reinterpret(UInt8, m.rowval, (sizeof(m.rowval),)))
    write(io, reinterpret(UInt8, m.nzval, (sizeof(m.nzval),)))
    m
end

function _load{T<:SparseMatrixCSC}(io::IO, t::Type{T}, _)
    m = read(io, Int)
    n = read(io, Int)
    nnz = read(io, Int)

    typ_len = read(io, Int)
    typ_bytes = readbytes(io, typ_len)
    (Tv, Ti) = deserialize(IOBuffer(typ_bytes))

    pos = position(io)
    colptr = Mmap.mmap(io, Vector{Ti}, (n+1,), pos)

    pos += sizeof(Ti)*(n+1)
    rowval = Mmap.mmap(io, Vector{Ti}, (nnz,), pos)

    pos += sizeof(Ti)*nnz
    nnzval = Mmap.mmap(io, Vector{Tv}, (nnz,), pos)

    SparseMatrixCSC(m, n, colptr, rowval, nnzval)
end

# The below is stuff specific to ComputeFramework
immutable MemToken <: PartIO
    fromdisk::Bool
    pool::UUID
    id::UUID
    blobmeta::CFWIOMeta
end

function make_token(x::Any, collection=_mem_pool)
    meta = CFWIOMeta(typeof(x), tempname(), sizemeta(x))
    b = append!(collection, Any, meta, StrongLocality, Nullable(x))
    MemToken(collection.id, b.id)
end
