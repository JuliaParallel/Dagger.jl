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

function gather{T<:Array}(ctx, c::PartSpec{FileReader{T}})
    h = c.handle
    io = open(h.file, "r+")
    seek(io, h.data_offset)
    arr = Mmap.mmap(io, h.parttype, size(c.domain))
    close(io)
    arr
end

function getsub{T<:AbstractArray}(ctx, c::PartSpec{FileReader{T}}, d)
    PartSpec(gather(ctx, c)[d])
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
    save_part(p) = save(ctx, part(p), tempname())
    saved_parts = map(x.parts) do p
        Thunk(save_part, (p,))
    end
    function save_cat_meta(parts...)
        f = open(s.name, "w")
        saved_parts = AbstractPart[c for c in parts]
        res = save(ctx, f, x, s.name, saved_parts)
        close(f)
        res
    end

    # The DAG has to block till saving is complete.
    res = Thunk(save_cat_meta, (saved_parts...); meta=true)
end
