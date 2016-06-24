addprocs(3)

using Dagger

words = split(TextFile("text8"), ' ')

count_one = map(x -> x => 1, words)
wcount = reducebykey(+, 0, count_one)

@show compute(Context(), wcount)
