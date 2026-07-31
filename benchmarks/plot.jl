using JLD
using BenchmarkTools
using UnicodePlots

res = JLD.load(ARGS[1])
serial_results = res["results"]["Serial"]
dagger_results = res["results"]["Dagger"]
pf = res["peakflops"]
renders = res["renders"]

theory_flops(nrow, ncol, nfeatures) = 11 * ncol * nrow * nfeatures + 2 * (ncol + nrow) * nfeatures

scale_set = sort([key=>parse(Int, lstrip(last(split(key, ':')), ' ')) for key in keys(serial_results)]; by=x->x[2])

expected_seconds = [theory_flops(2001*scale, 10002, 12)/pf for scale in last.(scale_set)]
plt = lineplot(last.(scale_set), expected_seconds; name="Speed of Light", title="NNMF Execution Times", ylabel="Best Time (s)", xlabel="Problem size", width=60, height=25, xlim=[minimum(last.(scale_set)), maximum(last.(scale_set))], ylim=[0, 20])

serial_times = [minimum(serial_results[scale]).time/(10^9) for scale in first.(scale_set)]
lineplot!(plt, last.(scale_set), serial_times; name="Serial")

nw_set = sort([key=>parse(Int, lstrip(last(split(key, ':')), ' ')) for key in keys(dagger_results[first(first(scale_set))])]; by=x->x[2])
for (nw,nw_val) in nw_set
    dagger_times = [minimum(dagger_results[scale][nw]).time/(10^9) for scale in first.(scale_set)]
    lineplot!(plt, last.(scale_set), dagger_times; name="Dagger $nw")
end
display(plt)
println()
