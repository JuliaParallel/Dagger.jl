@everywhere begin

using Dagger

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
Dagger.iscompatible_arg(proc::FakeProc, opts, x::Integer) = true
Dagger.iscompatible_arg(proc::FakeProc, opts, x::FakeVal) = true
Dagger.move(from_proc::OSProc, to_proc::FakeProc, x::Integer) = FakeVal(x)
Dagger.move(from_proc::FakeProc, to_proc::OSProc, x::Vector) =
    map(y->y.x, x)
Dagger.move(from_proc::FakeProc, to_proc::OSProc, x::FakeVal) =
    x.x
Dagger.execute!(proc::FakeProc, func, args...) = FakeVal(42+func(args...).x)

end
