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
function save(ctx, part::Part{DistMem}, file_path::AbstractString)
    pid = part.handle.ref.where

    remotecall_fetch(pid, file_path, part.handle.ref) do path, rref
        open(path, "w") do io
            save(ctx, io, part, file_path)
        end
    end
end

function save(ctx, io::IO, part::Part, file_path)
    meta_io = IOBuffer()

    serialize(meta_io, (parttype(part), domain(part)))
    meta = takebuf_array(meta_io)

    write(io, PARTSPEC)
    write(io, length(meta))
    write(io, meta)
    data_offset = position(io)

    save(ctx, io, gather(ctx, part))

    Part(parttype(part), domain(part), FileReader(file_path, parttype(part), data_offset), false)
end

function save(ctx, io::IO, part::Cat, file_path::AbstractString, saved_parts::AbstractArray)

    metadata = (parttype(part), domain(part), saved_parts)

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

    # save the parts
    saved_parts = [save(ctx, c, joinpath(dir_path, lpad(i, 4, "0")))
        for (i, c) in enumerate(parts(part))]

    save(ctx, io, part, file_path, saved_parts)
    # write each child
end

function save(ctx, io::IO, part::Sub)
    save(ctx, io, Part(gather(ctx, part)))
end

function save(ctx, part::Part{FileReader}, file_path::AbstractString)
   if abspath(file_path) == abspath(part.reader.file)
       part
   else
       cp(part.reader.file, file_path)
       Part(parttype(part), domain(part),
          FileReader(file_path, parttype(part),
                     part.reader.data_offset), false)
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
        c = load(ctx, Part, file_path, f)
    elseif part_typ == CAT
        c = load(ctx, Cat, file_path, f)
    else
        error("Could not determine part type")
    end
    close(f)
    c
end

"""
    load(ctx, ::Type{Part}, fpath, io)

Load a Part object from a file, the file path
is required for creating a FileReader object
"""
function load(ctx, ::Type{Part}, fname, io)
    meta_len = read(io, Int)
    io = IOBuffer(read(io, meta_len))

    (T, dmn, sz) = deserialize(io)

    Computed(Part(T, dmn, sz,
        FileReader(fname, T, meta_len+1), false))
end

function load(ctx, ::Type{Cat}, file_path, io)
    dir_path = file_path*"_data"

    metadata = deserialize(io)
    Computed(Cat(metadata...))
end


###### Save and Load for actual data #####

function save(ctx, io::IO, m::Array)
    write(io, reinterpret(UInt8, m, (sizeof(m),)))
    m
end

function save(ctx, io::IO, m::BitArray)
    save(ctx, io, convert(Array{Bool}, m))
end

function gather{T<:Array}(ctx, c::Part{FileReader{T}})
    h = c.handle
    io = open(h.file, "r+")
    seek(io, h.data_offset)
    arr = reshape(reinterpret(eltype(T), read(io)), size(c.domain))
    #arr = Mmap.mmap(io, h.parttype, size(c.domain))
    close(io)
    arr
end

function gather{T<:BitArray}(ctx, c::Part{FileReader{T}})
    h = c.handle
    io = open(h.file, "r+")
    seek(io, h.data_offset)
    arr = reshape(reinterpret(Bool, read(io)), size(c.domain))
    #arr = Mmap.mmap(io, h.parttype, size(c.domain))
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

function gather{T<:SparseMatrixCSC}(ctx, c::Part{FileReader{T}})
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

function getsub{T<:AbstractArray}(ctx, c::Part{FileReader{T}}, d)
    Part(gather(ctx, c)[d])
end


#### Save computation

immutable Save <: Computation
    input::Computation
    name::AbstractString
end

function save(p::Computation, name::AbstractString)
    Save(p, name)
end

function stage(ctx, s::Save)
    x = cached_stage(ctx, s.input)
    dir_path = s.name * "_data"
    if !isdir(dir_path)
        mkdir(dir_path)
    end
    function save_part(idx, data)
        p = part(data)
        path = joinpath(dir_path, lpad(idx, 4, "0"))
        saved = save(ctx, p, path)

        # release reference created for the purpose of save
        release_token(p.handle.ref)
        saved
    end

    saved_parts = similar(parts(x), Thunk)
    for i=1:length(parts(x))
        saved_parts[i] = Thunk(save_part, (i, parts(x)[i]))
    end

    sz = size(parts(x))
    function save_cat_meta(parts...)
        f = open(s.name, "w")
        saved_parts = reshape(AbstractPart[c for c in parts], sz)
        res = save(ctx, f, x, s.name, saved_parts)
        close(f)
        res
    end

    # The DAG has to block till saving is complete.
    res = Thunk(save_cat_meta, (saved_parts...); meta=true)
end
