##
# give a stream view to a block from any seekable iostream
# By @tanmaykm
#
import Base: close, eof, read, read!, peek, seek, write, filesize, position, seekend, seekstart, skip, nb_available

struct BlockIO <: IO
    s::IO
    r::UnitRange
    l::Int

    function find_end_pos(bio::BlockIO, end_byte::Char)
        seekend(bio)
        try
            while(!eof(bio.s) && (end_byte != read(bio, UInt8))) continue end
        end
        position(bio.s)
    end
    function find_start_pos(bio::BlockIO, end_byte::Char)
        (bio.r.start == 1) && (return bio.r.start)
        seekstart(bio)
        !eof(bio.s) && while(end_byte != read(bio, UInt8)) continue end
        position(bio.s)+1
    end

    function BlockIO(s::IO, r::UnitRange, match_ends::Union{Char,Nothing}=nothing)
        # TODO: use mark when available
        seekend(s)
        ep = position(s)

        r = min(r.start,ep+1):min(r.start+length(r)-1,ep)
        bio = new(s, r, length(r))
        if(nothing != match_ends)
            p1 = find_start_pos(bio, match_ends)
            p2 = find_end_pos(bio, match_ends)
            r = p1:p2
            bio = new(s, r, length(r))
        end
        seekstart(bio)
        bio
    end
end

BlockIO(bio::BlockIO, match_ends::Union{Char,Nothing}=nothing) = BlockIO(bio.s, bio.r, match_ends)

close(bio::BlockIO) = close(bio.s)
eof(bio::BlockIO) = (position(bio) >= bio.l)
read(bio::BlockIO, x::Type{UInt8}) = read(bio.s, x)
read!(bio::BlockIO, a::Vector{UInt8}) = (length(a) <= nb_available(bio)) ? read!(bio.s, a) : throw(EOFError())
read!(bio::BlockIO, a::Array{T}) where {T} = (length(a)*sizeof(T) <= nb_available(bio)) ? read!(bio.s, a) : throw(EOFError())

read(bio::BlockIO, nb::Integer = bio.l) = String(read!(bio, Array{UInt8}(nb)))

peek(bio::BlockIO) = peek(bio.s)
write(bio::BlockIO, p::Ptr, nb::Integer) = write(bio, p, int(nb))
write(bio::BlockIO, p::Ptr, nb::Int) = write(bio.s, p, nb)
write(bio::BlockIO, x::UInt8) = write(bio, UInt8[x])
write(bio::BlockIO, a::Array{T}, len) where {T} = write_sub(bio, a, 1, len)
write(bio::BlockIO, a::Array{T}) where {T} = write(bio, a, length(a))
write_sub(bio::BlockIO, a::Array{T}, offs, len) where {T} = isbitstype(T) ? write(bio, pointer(a,offs), len*sizeof(T)) : error("$T is not bits type")

nb_available(bio::BlockIO) = (bio.l - position(bio))
position(bio::BlockIO) = position(bio.s) - bio.r.start + 1

filesize(bio::BlockIO) = bio.l

seek(bio::BlockIO, n::Integer) = seek(bio.s, n+bio.r.start-1)
seekend(bio::BlockIO) = seek(bio, filesize(bio))
seekstart(bio::BlockIO) = seek(bio, 0)
skip(bio::BlockIO, n::Integer) = seek(bio, n+position(bio))

