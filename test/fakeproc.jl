@everywhere begin

using Dagger
import Dagger: OSProc, ThreadProc

struct FakeProc <: Dagger.Processor
    owner::Int
end
FakeProc() = FakeProc(myid())
Dagger.get_parent(proc::FakeProc) = OSProc(proc.owner)
Base.:(==)(proc1::FakeProc, proc2::FakeProc) = proc1.owner == proc2.owner

struct FakeVal
    x
end
fakesum(xs...) = FakeVal(sum(map(y->y.x, xs)))

Dagger.iscompatible_func(proc::FakeProc, opts, f) = true
Dagger.iscompatible_arg(proc::FakeProc, opts, ::Type{<:Integer}) = true
Dagger.iscompatible_arg(proc::FakeProc, opts, ::Type{<:FakeVal}) = true
Dagger.move(from_proc::OSProc, to_proc::FakeProc, x::Integer) = FakeVal(x)
Dagger.move(from_proc::ThreadProc, to_proc::FakeProc, x::Integer) = FakeVal(x)
Dagger.execute!(proc::FakeProc, func, args...) = FakeVal(42+func(args...).x)

end
