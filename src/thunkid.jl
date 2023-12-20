"An identifier to uniquely identify a `Thunk`."
struct ThunkID
    wid::Int
    id::Int
end
Base.hash(id::ThunkID, h::UInt) = hash((id.wid, id.id), hash(ThunkID, h))
Base.show(io::IO, id::ThunkID) = print(io, "ThunkID($(id.wid), $(id.id))")
