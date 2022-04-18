export save, load
using SparseArrays

"""
    FileReader

Used as a `Chunk` handle for reading a file, starting at a given offset.
"""
mutable struct FileReader{T}
    file::AbstractString
    chunktype::Type{T}
    data_offset::Int
    mmap::Bool
end

"""
    save(io::IO, val)

Save a value into the IO buffer. In the case of arrays and sparse
matrices, this will save it in a memory-mappable way.

`load(io::IO, t::Type, domain)` will load the object given its domain
"""
function save(ctx, io::IO, val)
    error("Save method for $(typeof(val)) not defined")
end


###### Save chunks ######

const PARTSPEC = 0x00
const CAT = 0x01

# subparts are saved as Parts

"""
    save(ctx, chunk::Union{Chunk, Thunk}, file_path::AbsractString)

Save a chunk to a file at `file_path`.
"""
function save(ctx, chunk::Union{Chunk, Thunk}, file_path::AbstractString)
    open(file_path, "w") do io
        save(ctx, io, chunk, file_path)
    end
end

"""
    save(ctx, chunk, file_path)

Special case distmem writing - write to disk on the process with the chunk.
"""
function save(ctx, chunk::Chunk{X,DRef}, file_path::AbstractString) where X
    pid = chunk.handle.where

    remotecall_fetch(pid, file_path, chunk.handle) do path, rref
        open(path, "w") do io
            save(ctx, io, chunk, file_path)
        end
    end
end

function save(ctx, io::IO, chunk::Chunk, file_path)
    meta_io = IOBuffer()

    serialize(meta_io, (chunktype(chunk), domain(chunk)))
    meta = take!(meta_io)

    write(io, PARTSPEC)
    write(io, length(meta))
    write(io, meta)
    data_offset = position(io)

    save(ctx, io, collect(ctx, chunk))

    Chunk(chunktype(chunk), domain(chunk), FileReader(file_path, chunktype(chunk), data_offset, false), false)
end

function save(ctx, io::IO, chunk::DArray, file_path::AbstractString, saved_parts::AbstractArray)

    metadata = (chunktype(chunk), domain(chunk), saved_parts)

    # save yourself
    write(io, CAT)
    serialize(io, metadata)

    DArray(metadata...)
    # write each child
end


function save(ctx, io::IO, chunk::DArray, file_path)
    dir_path = file_path*"_data"
    if !isdir(dir_path)
        mkdir(dir_path)
    end

    # save the chunks
    saved_parts = [save(ctx, c, joinpath(dir_path, lpad(i, 4, "0")))
        for (i, c) in enumerate(chunks(chunk))]

    save(ctx, io, chunk, file_path, saved_parts)
    # write each child
end

function save(ctx, chunk::Chunk{X, FileReader}, file_path::AbstractString) where X
   if abspath(file_path) == abspath(chunk.reader.file)
       chunk
   else
       cp(chunk.reader.file, file_path)
       Chunk(chunktype(chunk), domain(chunk),
          FileReader(file_path, chunktype(chunk),
                     chunk.reader.data_offset, false), false)
   end
end

save(chunk::Union{Chunk, Thunk}, file_path::AbstractString) = save(Context(global_context()), chunk, file_path)



###### Load chunks ######

"""
    load(ctx::Context, file_path)

Load an Union{Chunk, Thunk} from a file.
"""
function load(ctx::Context, file_path::AbstractString; mmap=false)

    open(file_path) do f
        part_typ = read(f, UInt8)
        if part_typ == PARTSPEC
            c = load(ctx, Chunk, file_path, mmap, f)
        elseif part_typ == CAT
            c = load(ctx, DArray, file_path, mmap, f)
        else
            error("Could not determine chunk type")
        end
    end
    c
end

"""
    load(ctx::Context, ::Type{Chunk}, fpath, io)

Load a Chunk object from a file, the file path
is required for creating a FileReader object
"""
function load(ctx::Context, ::Type{Chunk}, fname, mmap, io)
    meta_len = read(io, Int)
    io = IOBuffer(read(io, meta_len))

    (T, dmn, sz) = deserialize(io)

    DArray(Chunk(T, dmn, sz,
        FileReader(fname, T, meta_len+1, mmap), false))
end

function load(ctx::Context, ::Type{DArray}, file_path, mmap, io)
    dir_path = file_path*"_data"

    metadata = deserialize(io)
    c = DArray(metadata...)
    for p in chunks(c)
        if isa(p.handle, FileReader)
            p.handle.mmap = mmap
        end
    end
    DArray(c)
end


###### Save and Load for actual data #####

function save(ctx::Context, io::IO, m::Array)
    write(io, reinterpret(UInt8, m, (sizeof(m),)))
    m
end

function save(ctx::Context, io::IO, m::BitArray)
    save(ctx, io, convert(Array{Bool}, m))
end

function collect(ctx::Context, c::Chunk{X,FileReader{T}}) where {X,T<:Array}
    h = c.handle
    io = open(h.file, "r+")
    seek(io, h.data_offset)
    arr = h.mmap ? Mmap.mmap(io, h.chunktype, size(c.domain)) :
        reshape(reinterpret(eltype(T), read(io)), size(c.domain))
    close(io)
    arr
end

function collect(ctx::Context, c::Chunk{X, FileReader{T}}) where {X,T<:BitArray}
    h = c.handle
    io = open(h.file, "r+")
    seek(io, h.data_offset)

    arr = h.mmap ? Mmap.mmap(io, Bool, size(c.domain)) :
        reshape(reinterpret(Bool, read(io)), size(c.domain))
    close(io)
    arr
end

function save(ctx::Context, io::IO, m::SparseMatrixCSC{Tv,Ti}) where {Tv, Ti}
    write(io, m.m)
    write(io, m.n)
    write(io, length(m.nzval))

    typ_io = IOBuffer()
    serialize(typ_io, (Tv, Ti))
    buf = take!(typ_io)
    write(io, sizeof(buf))
    write(io, buf)

    write(io, reinterpret(UInt8, m.colptr, (sizeof(m.colptr),)))
    write(io, reinterpret(UInt8, m.rowval, (sizeof(m.rowval),)))
    write(io, reinterpret(UInt8, m.nzval, (sizeof(m.nzval),)))
    m
end

function collect(ctx::Context, c::Chunk{X, FileReader{T}}) where {X, T<:SparseMatrixCSC}
    h = c.handle
    io = open(h.file, "r+")
    seek(io, h.data_offset)

    m = read(io, Int)
    n = read(io, Int)
    nnz = read(io, Int)

    typ_len = read(io, Int)
    typ_bytes = read(io, typ_len)
    (Tv, Ti) = deserialize(IOBuffer(typ_bytes))

    pos = position(io)
    colptr = Mmap.mmap(io, Vector{Ti}, (n+1,), pos)

    pos += sizeof(Ti)*(n+1)
    rowval = Mmap.mmap(io, Vector{Ti}, (nnz,), pos)

    pos += sizeof(Ti)*nnz
    nnzval = Mmap.mmap(io, Vector{Tv}, (nnz,), pos)
    close(io)

    SparseMatrixCSC(m, n, colptr, rowval, nnzval)
end

function getsub(ctx::Context, c::Chunk{X,FileReader{T}}, d) where {X,T<:AbstractArray}
    Chunk(collect(ctx, c)[d])
end


#### Save computation

struct Save <: Computation
    input
    name::AbstractString
end

function save(p::Computation, name::AbstractString)
    Save(p, name)
end

function stage(ctx::Context, s::Save)
    x = cached_stage(ctx, s.input)
    dir_path = s.name * "_data"
    if !isdir(dir_path)
        mkdir(dir_path)
    end
    function save_part(idx, data)
        p = tochunk(data)
        path = joinpath(dir_path, lpad(idx, 4, "0"))
        saved = save(ctx, p, path)

        # release reference created for the purpose of save
        release_token(p.handle)
        saved
    end

    saved_parts = similar(chunks(x), Thunk)
    for i=1:length(chunks(x))
        saved_parts[i] = Thunk(save_part, i, chunks(x)[i])
    end

    sz = size(chunks(x))
    function save_cat_meta(chunks...)
        f = open(s.name, "w")
        saved_parts = reshape(Union{Chunk, Thunk}[c for c in chunks], sz)
        res = save(ctx, f, x, s.name, saved_parts)
        close(f)
        res
    end

    # The DAG has to block till saving is complete.
    res = Thunk(save_cat_meta, saved_parts...; meta=true)
end
