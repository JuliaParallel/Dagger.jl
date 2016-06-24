using Dagger
@everywhere function update(i,k, U, R, lambdaI)
    # get non-zero rows and the values for
    # column i
    @show i
    nzidx = find(Dagger.gather(R[:, i]))
    U_subset = Dagger.gather(U[nzidx])
    Ui = reinterpret(Float64, U_subset, (k, length(nzidx)))
    vec = Ui * Dagger.gather(R[nzidx, i])

    mat = (Ui * Ui') #+ (length(watched) * lambdaI)
    try
        res = mat \ vec
        return reinterpret(NTuple{k, Float64}, res, (1,))[1]
    catch err
        return reinterpret(NTuple{k, Float64}, rand(k), (1,))[1]
    end
end

function als(R)
    k = 10
    per_block = floor(Int, 1600000 / k)
    @show per_block
    @show nᵤ, nₘ = size(domain(R.result))

    user_blocks = @show compute(Distribute(BlockPartition(min(floor(Int, nᵤ/10), per_block)), 1:nᵤ))
    item_blocks = @show compute(Distribute(BlockPartition(min(floor(Int, nᵤ/10), per_block)), 1:nₘ))

   @time begin
       println("Creating M")
      M_ = map(x-> ntuple(_->rand(), Val{k}), item_blocks)
      M_means = gather(reducedim(+, R, 1))./nₘ
      M_ = map(i-> tuple(M_means[i], ntuple(_->rand(), Val{k-1})...), item_blocks)
      M = compute(save(M_, "M"))
  
      println("Creating R_t")
      Rᵀ = compute(save(R', "R_t"))
      U_ = map(x-> ntuple(_->rand(), Val{k}), user_blocks)
      U = compute(save(U_, "U"))
  end


    M = compute(map(x->x, load(Context(), "M")))
    Rᵀ = compute(load(Context(), "R_t"))
    #U = load(Context(), "U")
    local U

    @show ctx=Context()
    for itr=1:10
        U = compute(ctx,save(map(u -> update(u,k, M, Rᵀ, 1), user_blocks), "U_$itr"))
        M = compute(ctx,save(map(m -> update(m,k, U, R , 1), item_blocks), "M_$itr"))
    end
    U, M
end

R = load(Context(), "X_col")
