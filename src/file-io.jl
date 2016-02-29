export save, load

immutable FileReader{T} <: PartIO
    file::AbstractString
    parttype::Type{T}
    data_offset::Int
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


###### Save parts ######

const PARTSPEC = 0x00
const CAT = 0x01

# subparts are saved as Parts

"""
    save(ctx, part::AbstractPart, file_path::AbsractString)

Save a part to a file at `file_path`.
"""
function save(ctx, part::AbstractPart, file_path::AbstractString)
    open(file_path, "w") do io
        save(ctx, io, part, file_path)
    end
end

"""
special case distmem writing - write to disk on the process with the part.
"""
function save(ctx, part::PartSpec{DistMem}, file_path::AbstractString)
    pid = part.handle.ref.where

    remotecall_fetch(pid, file_path, part.handle.ref) do path, rref
        open(path, "w") do io
            save(ctx, io, part, file_path)
        end
    end
end

function save(ctx, io::IO, part::PartSpec, file_path)
    meta_io = IOBuffer()

    serialize(meta_io, (parttype(part), domain(part)))
    meta = takebuf_array(meta_io)

    write(io, PARTSPEC)
    write(io, length(meta))
    write(io, meta)
    data_offset = position(io)

    save(ctx, io, gather(ctx, part))

    PartSpec(parttype(part), domain(part), FileReader(file_path, parttype(part), data_offset))
end

function save(ctx, io::IO, part::Cat, file_path::AbstractString, saved_children::AbstractArray)

    metadata = (partition(part), parttype(part), domain(part), saved_children)

    # save yourself
    write(io, CAT)
    serialize(io, metadata)

    Cat(metadata...)
    # write each child
end


function save(ctx, io::IO, part::Cat, file_path)
    dir_path = file_path*"_data"
    if !isdir(dir_path)
        mkdir(dir_path)
    end

    # save the children
    saved_children = [save(ctx, c, joinpath(dir_path, lpad(i, 4, "0")))
        for (i, c) in enumerate(part.children)]

    save(ctx, io, part, file_path, saved_children)
    # write each child
end

function save(ctx, io::IO, part::Sub)
    save(ctx, io, PartSpec(gather(ctx, part)))
end

function save(ctx, part::PartSpec{FileReader}, file_path::AbstractString)
   if abspath(file_path) == abspath(part.reader.file)
       part
   else
       cp(part.reader.file, file_path)
       PartSpec(parttype(part), domain(part),
          FileReader(file_path, parttype(part),
                     part.reader.data_offset))
   end
end

save(part::AbstractPart, file_path::AbstractString) = save(Context(), part, file_path)



###### Load parts ######

"""
    load(ctx, file_path)

Load an AbstractPart from a file.
"""
function load(ctx, file_path::AbstractString)

    f = open(file_path)
    part_typ = read(f, UInt8)
    if part_typ == PARTSPEC
        c = load(ctx, PartSpec, file_path, f)
    elseif part_typ == CAT
        c = load(ctx, Cat, file_path, f)
    else
        error("Could not determine part type")
    end
    close(f)
    c
end

"""
    load(ctx, ::Type{PartSpec}, fpath, io)

Load a PartSpec object from a file, the file path
is required for creating a FileReader object
"""
function load(ctx, ::Type{PartSpec}, fname, io)
    meta_len = read(io, Int)
    io = IOBuffer(readbytes(io, meta_len))

    (T, dmn, sz) = deserialize(io)

    PartSpec(T, dmn, sz,
        FileReader(fname, T, meta_len+1))
end

function load(ctx, ::Type{Cat}, file_path, io)
    dir_path = file_path*"_data"

    metadata = deserialize(io)
    Cat(metadata...)
end


###### Save and Load for actual data #####

function save(ctx, io::IO, m::Array)
    write(io, reinterpret(UInt8, m, (sizeof(m),)))
    m
end

function gather{T<:Array}(ctx, c::PartSpec{FileReader{T}})
    h = c.handle
    io = open(h.file, "r+")
    seek(io, h.data_offset)
    arr = Mmap.mmap(io, h.parttype, size(c.domain))
    close(io)
    arr
end

function save{Tv, Ti}(ctx, io::IO, m::SparseMatrixCSC{Tv,Ti})
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

function gather{T<:SparseMatrixCSC}(ctx, c::PartSpec{FileReader{T}})
    h = c.handle
    io = open(h.file, "r+")
    seek(io, h.data_offset)

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
    close(io)

    SparseMatrixCSC(m, n, colptr, rowval, nnzval)
end

function getsub{T<:AbstractArray}(ctx, c::PartSpec{FileReader{T}}, d)
    PartSpec(gather(ctx, c)[d])
end

