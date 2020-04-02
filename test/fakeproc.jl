@everywhere begin

using Dagger

struct FakeProc <: Dagger.Processor end
struct FakeVal
    x
end
fakesum(xs...) = FakeVal(sum(map(y->y.x, xs)))

Dagger.iscompatible(proc::FakeProc, opts, x::Integer) = true
Dagger.iscompatible(proc::FakeProc, opts, x::FakeVal) = true
Dagger.move(ctx, from_proc::OSProc, to_proc::FakeProc, x::Integer) = FakeVal(x)
Dagger.move(ctx, from_proc::FakeProc, to_proc::OSProc, x::Vector) = map(y->y.x, x)
Dagger.move(ctx, from_proc::FakeProc, to_proc::OSProc, x::FakeVal) = x.x
Dagger.execute!(proc::FakeProc, func, args...) = FakeVal(42+func(args...).x)

end
