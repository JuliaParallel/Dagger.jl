# Stencil Operations



```julia
N = 27
nt = 3
tiles = zeros(Blocks(N, N), Bool, N*nt, N*nt)
outputs = zeros(Blocks(N, N), Bool, N*nt, N*nt)

# Create fun initial state
tiles[13, 14] = 1
tiles[14, 14] = 1
tiles[15, 14] = 1
tiles[15, 15] = 1
tiles[14, 16] = 1
@view(tiles[(2N+1):3N, (2N+1):3N]) .= rand(Bool, N, N)

import Dagger: @stencil, Wrap

anim = @animate for _ in 1:niters
    Dagger.spawn_datadeps() do
        @stencil begin
            outputs[idx] = begin
                nhood = @neighbors(tiles[idx], 1, Wrap())
                neighs = sum(nhood) - tiles[idx]
                if tiles[idx] && neighs < 2
                    0
                elseif tiles[idx] && neighs > 3
                    0
                elseif !tiles[idx] && neighs == 3
                    1
                else
                    tiles[idx]
                end
            end
            tiles[idx] = outputs[idx]
        end
    end
    heatmap(Int.(collect(outputs)))
end
path = mp4(anim; fps=5, show_msg=true).filename
```
