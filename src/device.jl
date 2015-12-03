import Base: remotecall, take!

"""
A device which implements `remotecall` and `take!` methods
"""
abstract Device

immutable Proc <: Device
    pid::Int
end

remotecall(p::Proc, args...) = remotecall(p.pid, args...)
take!(p::Proc, ref) = take!(ref)
