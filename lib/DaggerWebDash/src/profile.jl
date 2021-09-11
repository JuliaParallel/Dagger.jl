using ProfileSVG

"""
    ProfileMetrics

Tracks compute profile traces.
"""
struct ProfileMetrics end
function (::ProfileMetrics)(ev)
    pr = ev.profiler_samples
    samples, lidict = pr.samples, pr.lineinfo
    function demote_frames(samples)
        new_samples = UInt[]
        startidx = 1
        for i in 1:length(samples)
            if samples[i] == 0
                append!(new_samples, samples[startidx:(i-1)]) # FIXME: i-5
                push!(new_samples, 0)
                startidx = i+1
            end
        end
        new_samples
    end
    @static if VERSION >= v"1.8-"
        samples = demote_frames(samples)
    end
    if ev.category != :compute || length(samples) == 0
        return ""
    end
    path, io = mktemp(; cleanup=false)
    ProfileSVG.save(io, samples; lidict)
    close(io)
    path = replace(path, tempdir()*"/" => "")
    ExpiringFile(path)
end

"An automatically-deleting file."
mutable struct ExpiringFile
    path::String
    function ExpiringFile(path)
        ef = new(path)
        finalizer(ef) do ef
            try
                rm(tempdir()*"/"*path)
            catch
                @warn "Failed to remove /tmp/$path"
            end
        end
        ef
    end
end
StructTypes.StructType(::Type{ExpiringFile}) = StructTypes.CustomStruct()
StructTypes.lower(ef::ExpiringFile) = ef.path
