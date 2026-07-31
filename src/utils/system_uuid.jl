const SYSTEM_UUIDS = Dict{Int,UUID}()

function system_uuid_fallback()
    get!(SYSTEM_UUIDS, myid()) do
        username = hash(get(ENV, "USER", "__global"))
        uuid_file = joinpath(tempdir(), "dagger-system-uuid-$username")
        if !isfile(uuid_file)
            temp_uuid_file, temp_io = mktemp(; cleanup=false)
            uuid = uuid4()
            write(temp_io, string(uuid))
            flush(temp_io)
            close(temp_io)
            try
                # Try to make this the UUID file
                mv(temp_uuid_file, uuid_file; force=false)
                return uuid
            catch err
                err isa ArgumentError || rethrow(err)
                # Failed, clean up temp file, and read existing UUID file
                rm(temp_uuid_file)
            end
        end
        return parse(UUID, read(uuid_file, String))
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
