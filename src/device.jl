import Base: remotecall, fetch!

"""
A device which implements `remotecall` and `fetch` methods
"""
abstract Device

immutable Proc <: Device
    pid::Int
end

remotecall(p::Proc, args...) = remotecall(p.pid, args...)
fetch!(p::Proc, args...) = fetch!(p.pid, args...)
