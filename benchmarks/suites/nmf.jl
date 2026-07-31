function nnmf(X, W, H)
    # H update
    H = (H .* (W' * (X ./ (W * H)))
         ./ (sum(W; dims=1))')
    # W update
    W = (W .* ((X ./ (W * H)) * (H'))
         ./ (sum(H; dims=2)'))
    # error estimate
    X - W * H
end

theory_flops(nrow, ncol, nfeatures) = 11 * ncol * nrow * nfeatures + 2 * (ncol + nrow) * nfeatures

function nmf_suite(ctx; method, accels)
    suite = BenchmarkGroup()

    accel = !isempty(accels) ? only(accels) : "cpu"

    X = Ref{Any}()
    W = Ref{Any}()
    H = Ref{Any}()

    for scale in scales
        ncol = 2001 * scale
        nrow = 1002
        nfeatures = 12

        if method == "raw"
            suite["NNMF scaled by: $scale"] = @benchmarkable begin
                nnmf($X[], $W[], $H[])
            end setup=begin
                _scale = $scale
                @info "Starting non-Dagger NNMF (scale by $_scale)"
                if $accel == "cuda"
                    $X[] = CUDA.rand(Float32, $nrow, $ncol)
                    $W[] = CUDA.rand(Float32, $nrow, $nfeatures)
                    $H[] = CUDA.rand(Float32, $nfeatures, $ncol)
                elseif $accel == "amdgpu"
                    $X[] = ROCArray(rand(Float32, $nrow, $ncol))
                    $W[] = ROCArray(rand(Float32, $nrow, $nfeatures))
                    $H[] = ROCArray(rand(Float32, $nfeatures, $ncol))
                elseif $accel == "cpu"
                    $X[] = rand(Float32, $nrow, $ncol)
                    $W[] = rand(Float32, $nrow, $nfeatures)
                    $H[] = rand(Float32, $nfeatures, $ncol)
                end
            end teardown=begin
                $X[] = nothing
                $W[] = nothing
                $H[] = nothing
                @everywhere GC.gc()
            end
        elseif method == "dagger"
            RENDERS[scale] = Dict{Int,Vector}()
            nw = length(workers())
            nsuite = BenchmarkGroup()
            while nw > 0
                scope = if accel == "cuda"
                    error("Not implemented")
                    Dagger.Sch.SchedulerOptions(;proctypes=[
                        DaggerGPU.CuArrayDeviceProc
                    ])
                elseif accel == "amdgpu"
                    error("Not implemented")
                    Dagger.Sch.SchedulerOptions(;proctypes=[
                        DaggerGPU.ROCArrayProc
                    ])
                elseif accel == "cpu"
                    scope = Dagger.scope(;workers=workers()[1:nw])
                else
                    error("Unknown accelerator $accel")
                end
                #bsz = ncol ÷ length(workers())
                bsz = ncol ÷ 64
                nsuite["Workers: $nw"] = @benchmarkable begin
                    Dagger.with_options(;scope=$scope) do
                        fetch(nnmf($X[], $W[], $H[]))
                    end
                end setup=begin
                    _nw, _scale = $nw, $scale
                    @info "Starting $_nw worker Dagger NNMF (scale by $_scale)"
                    if $accel == "cuda"
                        # FIXME: Allocate with CUDA.rand if possible
                        $X[] = Dagger.mapchunks(CuArray, rand(Blocks($bsz, $bsz), Float32, $nrow, $ncol))
                        $W[] = Dagger.mapchunks(CuArray, rand(Blocks($bsz, $bsz), Float32, $nrow, $nfeatures))
                        $H[] = Dagger.mapchunks(CuArray, rand(Blocks($bsz, $bsz), Float32, $nfeatures, $ncol))
                    elseif $accel == "amdgpu"
                        $X[] = Dagger.mapchunks(ROCArray, rand(Blocks($bsz, $bsz), Float32, $nrow, $ncol))
                        $W[] = Dagger.mapchunks(ROCArray, rand(Blocks($bsz, $bsz), Float32, $nrow, $nfeatures))
                        $H[] = Dagger.mapchunks(ROCArray, rand(Blocks($bsz, $bsz), Float32, $nfeatures, $ncol))
                    elseif $accel == "cpu"
                        $X[] = rand(Blocks($bsz, $bsz), Float32, $nrow, $ncol)
                        $W[] = rand(Blocks($bsz, $bsz), Float32, $nrow, $nfeatures)
                        $H[] = rand(Blocks($bsz, $bsz), Float32, $nfeatures, $ncol)
                    end
                end teardown=begin
                    if render != "" && !live
                        Dagger.continue_rendering[] = false
                        for i in 1:5
                            isready(Dagger.render_results) && break
                            sleep(1)
                        end
                        if isready(Dagger.render_results)
                            video_paths = take!(Dagger.render_results)
                            try
                                video_data = Dict(key=>read(video_paths[key]) for key in keys(video_paths))
                                push!(get!(()->[], RENDERS[$scale], $nw), video_data)
                            catch err
                                @error "Failed to process render results" exception=(err,catch_backtrace())
                            end
                        else
                            @warn "Failed to fetch render results"
                        end
                    end
                    $X[] = nothing
                    $W[] = nothing
                    $H[] = nothing
                    @everywhere GC.gc()
                end
                nw ÷= 2
            end
            suite["NNMF scaled by: $scale"] = nsuite
        end
    end

    suite
end
