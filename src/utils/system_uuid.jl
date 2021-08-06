const SYSTEM_UUIDS = Dict{Int,UUID}()

function system_uuid_fallback()
    get!(SYSTEM_UUIDS, myid()) do
        uuid_file = joinpath(tempdir(), "dagger-system-uuid")
        if !isfile(uuid_file)
            uuid = uuid4()
            open(uuid_file, "w") do io
                write(io, string(uuid))
            end
            return uuid
        else
            return parse(UUID, read(uuid_file, String))
        end
    end
    SYSTEM_UUIDS[myid()]
end

if Sys.islinux()
    system_uuid() = system_uuid_fallback()
    # TODO: dmidecode
else
    system_uuid() = system_uuid_fallback()
end

"Gets the unique UUID for the server hosting worker `wid`."
function system_uuid(wid::Integer)
    get!(SYSTEM_UUIDS, wid) do
        remotecall_fetch(system_uuid, wid)
    end
end
