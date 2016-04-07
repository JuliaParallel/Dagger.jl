
showsz(sz) = join(sz, "x")
function Base.show{B<:BlockPartition}(io::IO, c::Cat{B})
    write(io, showsz(size(domain(c))))
    write(io, ' ')
    write(io, string(parttype(c)))
    write(io, " in ")
    write(io, showsz(size(parts(c))))
    write(io, " parts each of (max size) ")
    write(io, showsz(partition(c).blocksize))
end
