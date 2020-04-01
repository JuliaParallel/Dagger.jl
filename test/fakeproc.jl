@everywhere begin

using Dagger

struct FakeProc <: Dagger.Processor end

Dagger.iscompatible(proc::FakeProc, opts, x::Integer) = true
Dagger.iscompatible(proc::FakeProc, opts, x::String) = true
Dagger.move(ctx, from_proc::OSProc, to_proc::FakeProc, x::Integer) = x
Dagger.move(ctx, from_proc::FakeProc, to_proc::OSProc, x::Integer) = x
Dagger.move(ctx, from_proc::FakeProc, to_proc::OSProc, x::String) = parse(Int,x)
Dagger.execute!(proc::FakeProc, func, arg::Integer) = func(arg)
Dagger.execute!(proc::FakeProc, func, args...) = "42" * func(string.(args)...)

push!(Dagger.PROCESSOR_CALLBACKS, proc -> begin
    push!(proc.children, FakeProc())
end)

end
