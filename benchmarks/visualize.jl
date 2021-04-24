using JLD
using BenchmarkTools
using TypedTables

res = JLD.load(ARGS[1])

serial_results = res["results"]["Serial"]
dagger_results = res["results"]["Dagger"]

scale_set = sort([key=>parse(Int, lstrip(last(split(key, ':')), ' ')) for key in keys(serial_results)]; by=x->x[2])
serial_times = [minimum(serial_results[scale]).time/(10^9) for scale in first.(scale_set)]
nw_set = sort([key=>parse(Int, lstrip(last(split(key, ':')), ' ')) for key in keys(dagger_results[first(first(scale_set))])]; by=x->x[2])

table = Table(name=[:Base for _ in 1:3], time=serial_times, scale=last.(scale_set), procs=[8 for _ in 1:3])

btable = copy(table)

for (nw,nw_val) in nw_set
    dagger_times = [minimum(dagger_results[scale][nw]).time/(10^9) for scale in first.(scale_set)]
    t = Table(name=[:Dagger for _ in 1:3], time=dagger_times, scale=last.(scale_set), procs=[parse(Int,split(nw, ":")[2]) for _ in 1:3])
    append!(table, t)
end

dtable = table[table.name .== :Dagger]

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

colors = distinguishable_colors(lenght(scales), ColorSchemes.seaborn_deep.colors)

for (i, scale) in enumerate(scales)
    stable = dtable[dtable.scale .== scale]
    t1 = first(stable[stable.procs .== 1].time)
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
weak_scaling(t1, tn) = t1/tn

dtable = table[table.name .== :Dagger]
wstable = filter(row->row.scale == row.procs, dtable)
wstable = sort(wstable, by=r->r.scale)
t1 = first(wstable).time

fig = Figure(resolution = (1200, 800))
perf = fig[1, 1] = Axis(fig, title = "Weak scaling")
perf.xlabel = "nprocs"
perf.ylabel = "Efficiency"

lines!(perf, wstable.procs, weak_scaling.(t1, wstable.time), linewidth=3.0)
save("weak_scaling.png", fig)

# 3. Comparision against Base

fig = Figure(resolution = (1200, 800))
perf = fig[1, 1] = Axis(fig, title = "DaggerArrays vs Base")
perf.xlabel = "Scaling factor"
perf.ylabel = "time (s)"

line_plots = Any[]
legend_names = String[]

procs = unique(dtable.procs)

colors = distinguishable_colors(lenght(procs) + 1, ColorSchemes.seaborn_deep.colors)

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
speedup = fig[1, 1] = Axis(fig, title = "DaggerArrays vs Base (8 threads)")
speedup.xlabel = "Scaling factor"
speedup.ylabel = "Speedup Base/Dagger"

line_plots = Any[]
legend_names = String[]

colors = distinguishable_colors(length(procs), ColorSchemes.seaborn_deep.colors)

sort!(btable, by=r->r.scale)

for (i, nproc) in enumerate(unique(dtable.procs))
    nproc < 8 && continue
    stable = dtable[dtable.procs .== nproc]
    sort!(stable, by=r->r.scale)
    push!(line_plots, lines!(speedup, stable.scale, btable.time ./ stable.time, linewidth=3.0, color = colors[i]))
    push!(legend_names, "Dagger (nprocs = $nproc)")
end

legend = fig[1, 2] = Legend(fig, line_plots, legend_names)
save("speedup.png", fig)
