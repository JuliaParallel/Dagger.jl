using JLD, Serialization
using BenchmarkTools
using TypedTables

res = if endswith(ARGS[1], ".jld")
    JLD.load(ARGS[1])
elseif endswith(ARGS[1], ".jls")
    deserialize(ARGS[1])
else
    error("Unknown file type")
end

serial_results = filter(x->!occursin("dagger", x[1]), res["results"])
@assert length(keys(serial_results)) > 0 "No serial results found"
dagger_results = filter(x->occursin("dagger", x[1]), res["results"])
@assert length(keys(dagger_results)) > 0 "No Dagger results found"

scale_set = sort([key=>parse(Int, lstrip(last(split(key, ':')), ' ')) for key in keys(first(serial_results)[2])]; by=x->x[2])
nw_set = sort([key=>parse(Int, lstrip(last(split(key, ':')), ' ')) for key in keys(first(dagger_results)[2][first(first(scale_set))])]; by=x->x[2])
raw_table = NamedTuple[]
for bset_key in keys(res["results"])
    bset = res["results"][bset_key]
    if typeof(bset[first(first(scale_set))]) <: BenchmarkGroup
        procs = parse(Int, lstrip(last(split(first(first(bset[first(first(scale_set))])), ':')), ' '))
        for nw in nw_set
            for i in 1:length(scale_set)
                set_times = [minimum(bset[scale][nw[1]]).time/(10^9) for scale in first.(scale_set)]
                push!(raw_table, (name=bset_key, time=set_times[i], scale=last.(scale_set)[i], procs=nw[2]))
            end
        end
    else
        set_times = [minimum(bset[scale]).time/(10^9) for scale in first.(scale_set)]
        procs = 8  # default for OpenBLAS
        for i in 1:length(set_times)
            push!(raw_table, (name=bset_key, time=set_times[i], scale=last.(scale_set)[i], procs=procs))
        end
    end
end
table = Table(raw_table)

btable = copy(table[map(x->!x, occursin.(Ref("dagger"), table.name))])
dtable = copy(table[occursin.(Ref("dagger"), table.name)])

#table = Table(name=[:Base for _ in 1:3], time=serial_times, scale=last.(scale_set), procs=[8 for _ in 1:3])

#btable = copy(table)

#=
for (nw,nw_val) in nw_set
    dagger_times = [minimum(dagger_results[scale][nw]).time/(10^9) for scale in first.(scale_set)]
    t = Table(name=[:Dagger for _ in 1:3], time=dagger_times, scale=last.(scale_set), procs=[parse(Int,split(nw, ":")[2]) for _ in 1:3])
    append!(table, t)
end
=#

#dtable = table[table.name .== :Dagger]

# Plotting

using CairoMakie
using Colors, ColorSchemes 


# 1. Strong scaling analysis
#   - Keep the problem size constant
#   - Increase amount of compute

fig = Figure(resolution = (1200, 800))
strong_scaling(t1, tn, N) = t1/(N*tn)

ssp = fig[1, 1] = Axis(fig, title = "Strong Scaling")
ssp.xlabel = "Number of processes"
ssp.ylabel = "Scaling efficiency"

line_plots = Any[]
legend_names = String[]

scales = unique(dtable.scale)

colors = distinguishable_colors(length(scales), ColorSchemes.seaborn_deep.colors)

for (i, scale) in enumerate(scales)
    stable = dtable[dtable.scale .== scale]
    t1 = first(stable[stable.procs .== minimum(dtable.procs)].time)
    ss_efficiency = strong_scaling.(t1, stable.time, stable.procs)
    push!(line_plots, lines!(ssp, stable.procs, ss_efficiency, linewidth=3.0, color = colors[i]))
    push!(legend_names, "scale = $scale")
end

legend = fig[1, 2] = Legend(fig, line_plots, legend_names)

save("strong_scaling.png", fig)

# 2. Weak scaling
#   - Increase work and available compute
# Q: Are we actually scaling the work correctly?
#    too little data

fig = Figure(resolution = (1200, 800))
weak_scaling(t1, tn, p_prime, p) = t1/((p_prime/p)*tn)

t1 = first(dtable[map(row->(row.scale == 10) && (row.procs == 1), dtable)]).time

fig = Figure(resolution = (1200, 800))
perf = fig[1, 1] = Axis(fig, title = "Weak Scaling")
perf.xlabel = "Number of processes"
perf.ylabel = "Scaling efficiency"

line_plots = Any[]
legend_names = String[]

wstable = similar(dtable, 0)
for pair in [(10,1),(35,4),(85,8)]
    append!(wstable, dtable[map(row->(row.scale == pair[1]) && (row.procs == pair[2]), rows(dtable))])
end
push!(line_plots, lines!(perf, wstable.procs, weak_scaling.(t1, wstable.time, wstable.procs .* 10, wstable.scale), linewidth=3.0))
push!(legend_names, "cpu+dagger")

legend = fig[1, 2] = Legend(fig, line_plots, legend_names)
save("weak_scaling.png", fig)

# 3. Comparision against Base

fig = Figure(resolution = (1200, 800))
perf = fig[1, 1] = Axis(fig, title = "Dagger vs Base")
perf.xlabel = "Scaling factor"
perf.ylabel = "time (s)"

line_plots = Any[]
legend_names = String[]

procs = unique(dtable.procs)

colors = distinguishable_colors(length(procs) + 1, ColorSchemes.seaborn_deep.colors)

for (i, nproc) in enumerate(procs)
    stable = dtable[dtable.procs .== nproc]
    push!(line_plots, lines!(perf, stable.scale, stable.time, linewidth=3.0, color = colors[i]))
    push!(legend_names, "Dagger (nprocs = $nproc)")
end

push!(line_plots, lines!(perf, btable.scale, btable.time, linewidth=3.0, color = colors[end]))
push!(legend_names, "Base (threads = 8)")

legend = fig[1, 2] = Legend(fig, line_plots, legend_names)
save("raw_timings.png", fig)


# 4. Speedup
fig = Figure(resolution = (1200, 800))
speedup = fig[1, 1] = Axis(fig, title = "Speedup vs. 1 processor")
speedup.xlabel = "Number of processors"
speedup.ylabel = "Speedup"

line_plots = Any[]
legend_names = String[]

colors = distinguishable_colors(length(procs), ColorSchemes.seaborn_deep.colors)

t1 = sort(dtable[dtable.scale .== 10]; by=r->r.procs)

for (i, scale) in enumerate(unique(dtable.scale))
    stable = dtable[dtable.scale .== scale]
    sort!(stable, by=r->r.procs)
    push!(line_plots, lines!(speedup, stable.procs, stable.time ./ t1.time, linewidth=3.0, color = colors[i]))
    push!(legend_names, "Dagger (scale = $scale)")
end

legend = fig[1, 2] = Legend(fig, line_plots, legend_names)
save("speedup.png", fig)
