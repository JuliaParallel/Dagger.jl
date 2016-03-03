

# `release_blob` and `make_blob` are abstractions
# over Blobs. ComputeFramework only uses these functions
"""
Release a blob.

If Blob is in memory, don't do anything

if blob has been saved to disk, delete it.
"""
function release_blob(meta)
    # cannot yabel to sar
    if meta.availableondisk
        rm(meta.filename)
    end
end

fetch_again(x::Blob) = load(_mem_pool.id, x.id)

"""
Create a blob, but return a MemToken to the blob
"""
function make_blob(data)
    # initially blob is not on disk
    meta = CFWIOMeta(false, false, typeof(x), tempname(), sizemeta(x))
    tok = append!(collection, Any, meta, StrongLocality, Nullable(x)) |>
        make_token
    tok
end

using Blobs
import Blobs: load,save

APPROX_CHUNKSIZE = 64MB
MAX_CHUNKS = ceil(Int, Sys.free_memory()/(nworkers()*2APPROX_CHUNKSIZE).val)

global _mem_pool = BlobCollection(Any, Mutable(CFWWriter()), CFWReader(); maxcache::Int=MAX_CHUNKS)
immutable CFWReader <: BlobIO end
immutable CFWWriter <: BlobIO end
type CFWIOMeta <: BlobMeta{Any}
    ondisk::Bool
    availableondisk::Bool
    datatype::Type
    filename::AbstractString
    sizemeta::Any
    data::Nullable
end
function Base.serialize(s::SerializationState, x::CFWIOMeta)
    serialize(s, x.ondisk)
    serialize(s, x.availableondisk)
    Serializer.serialize_type(s, x.datatype)
    serialize(s, x.filename)
    serialize(s, x.sizemeta)
    if !x.availableondisk
        serialize(s, x.data)
    else
        serialize(s, Nullable())
    end
end
function Base.deserialize(s::SerializationState, ::Type{CFWIOMeta})
    ondisk = deserialize(s)
    availableondisk = deserialize(s)
    datatype = deserialize(s)
    filename = deserialize(s)
    sizemeta = deserialize(s)
    data = deserialize(s)
    CFWIOMeta(ondisk, availableondisk, datatype, filename, sizemeta, data)
end
sizemeta(x::Array) = size(x)
sizemeta(x::SparseMatrixCSC) = nothing # this is encoded in the file

function save(data, meta::CFWIOMeta, writer::CFWWriter)
    open(meta.filename, "w") do io
        _save(io, data)
    end
    meta.availableondisk = true
    meta.ondisk = true
    meta.data = Nullable()
end
function load(meta::CFWIOMeta, reader::CFWReader)
    if meta.ondisk
        if meta.availableondisk
            open(meta.filename, "w") do io
                _load(io, meta.datatype, meta.sizemeta, data)
            end
        else
            error("Inconsistent state: data ondisk flag is set but not available")
        end
    else
        isnull(meta.data) && error("Data no ondisk or in memory.")
        return get(meta.data)
    end

    meta.ondisk = false
    meta.availableondisk = true # the file still exists
    meta.data = Nullable(data)
    data
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
    nothing
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
