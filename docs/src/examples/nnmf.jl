# #

# ## Setting up the cluster

using Distributed
addprocs(4, exeflags=["--project=$(Base.active_project())"])

# ## Loading Dagger

@everywhere using Dagger
import Dagger: Computation, reduceblock

# ## Hierachical parallelism
# 
# Each Julia worker is modeled as a `OSProc` which can have multiple `ThreadProc`
# assigned to it. Let's walk all workers and discover how many `ThreadProc`s there are.

p = sum(length(Dagger.get_processors(OSProc(id))) for id in workers())

# ## NNMF

function nnmf(X, W, H)
    # H update
    H = (H .* (W' * (X ./ (W * H))) ./ (sum(W; dims=1))')
    # W update
    W = (W .* ((X ./ (W * H)) * (H')) ./ (sum(H; dims=2)'))
    # error estimate
    (X - W * H, W, H)
end

# ## Sample data

ncol = 2000
nrow = 10000
nfeatures = 12

# Eagerly allocating 

X = compute(rand(Blocks(nrow, ncol÷p), nrow, ncol))
W = compute(rand(Blocks(nrow, ncol÷p), nrow, nfeatures))
H = compute(rand(Blocks(nrow, ncol÷p), nfeatures, ncol))

# ## Run a NMF step

err, W, H = nnmf(X, W, H)
compute(err)
