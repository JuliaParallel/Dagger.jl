using Distributed
if nprocs() == 1
    addprocs(4; exeflags="--project=$(@__DIR__)")
end

using Revise
using Dagger
using LinearAlgebra

@everywhere begin
using Metalhead
using Images
using Flux
import Flux: onecold, onehotbatch
import Flux.Losses: logitcrossentropy
using DataAugmentation
using Optimisers
end

@everywhere println("Hello, world from $(myid())!")

using Plots, DataFrames
using GraphViz
function with_plots(f)
    Dagger.enable_logging!(; metrics=false, all_task_deps=true)
    GC.enable(false)
    try
        f()
    finally
        GC.enable(true)
        logs = Dagger.fetch_logs!()
        Dagger.disable_logging!()
        display(Dagger.render_logs(logs, :plots_gantt; target=:execution, color_init_hash=UInt(1)))
        display(Dagger.render_logs(logs, :graphviz))
    end
end
macro with_plots(ex)
    quote
        with_plots(()->$(esc(ex)))
    end
end

function demo(;niters=1, npar=4)
    # Load the 3D printer images
    good_imgs = []
    bad_imgs = []
    for dir in readdir("AIDemo/images")
        good = dir == "OK"
        for file in readdir(joinpath("AIDemo/images", dir))
            if !endswith(file, ".jpg")
                continue
            end
            #=if !endswith(file, ".jpg")
                run(`magick AIDemo/images/$dir/$file AIDemo/images/$dir/$file.jpg`)
                img = Images.load(joinpath("AIDemo/images", dir, file*".jpg"))
                rm(joinpath("AIDemo/images", dir, file*".jpg"))
            else=#
                img = Images.load(joinpath("AIDemo/images", dir, file))
            #end
            if !all(size(img) .>= (224, 224))
                continue
            end
            if !(img isa Matrix{RGB{N0f8}})
                continue
            end
            if good
                push!(good_imgs, img)
            else
                push!(bad_imgs, img)
            end
        end
    end
    @show length(good_imgs)
    @show length(bad_imgs)

    # FIXME: Remove me
    good_imgs = good_imgs[1:10]
    bad_imgs = bad_imgs[1:10]

    # Normalize the images
    good_imgs_normalized = [normalize_image(img) for img in good_imgs]
    bad_imgs_normalized = [normalize_image(img) for img in bad_imgs]

    # Load ImageNet labels
    if !isfile("AIDemo/imagenet_classes.txt")
        download("https://raw.githubusercontent.com/pytorch/hub/master/imagenet_classes.txt", "AIDemo/imagenet_classes.txt")
    end
    labels = readlines("AIDemo/imagenet_classes.txt")

    # We'll use label 997 as positive, and 998 as negative
    good_label_idx = 997
    bad_label_idx = 998
    good_label = labels[good_label_idx]
    bad_label = labels[bad_label_idx]
    @show good_label
    @show bad_label

    # Load the ResNet model
    model_resnet = Metalhead.ResNet(34; pretrain=true)

    predict_label(model, img, labels) = only(onecold(model(Flux.unsqueeze(img, 4)), labels))

    # Print the predicted label for the original data
    println("Original label (good):")
    println(predict_label(model_resnet, good_imgs_normalized[1], labels))
    pct_good = sum([predict_label(model_resnet, img, labels) == good_label for img in good_imgs_normalized]) / length(good_imgs_normalized)
    println("% good correct: $pct_good")
    println("Original label (bad):")
    println(predict_label(model_resnet, bad_imgs_normalized[1], labels))
    pct_bad = sum([predict_label(model_resnet, img, labels) == bad_label for img in bad_imgs_normalized]) / length(bad_imgs_normalized)
    println("% bad correct: $pct_bad")

    # Create the training data
    all_data = vcat(
        collect(zip(good_imgs_normalized, [good_label_idx for _ in 1:length(good_imgs_normalized)])),
        collect(zip(bad_imgs_normalized, [bad_label_idx for _ in 1:length(bad_imgs_normalized)]))
    )
    raw_data = [(reshape(img, 224, 224, 3, 1), onehotbatch([label], 1:1000)) for (img, label) in all_data]

    # Create a DataLoader that will shuffle the data
    data = Flux.DataLoader(raw_data; batchsize=0, shuffle=true)

    # Train the model with Dagger
    models = [model_resnet for _ in 1:npar]
    @time for iter in 1:niters
        models_in_progress = Vector{DTask}(undef, npar)
        for par_idx in 1:npar
            models_in_progress[par_idx] = Dagger.@spawn train_model(models[par_idx], data)
        end
        models = [fetch(model) for model in models_in_progress]

        # Average the trained model parameters
        for (idx, param) in enumerate(Flux.params(model_resnet))
            param .= reduce((a, b) -> a .+ b, [Flux.params(model)[idx] for model in models]) ./ npar
        end

        # Reset the models to the newly-trained model
        for idx in eachindex(models)
            models[idx] = model_resnet
        end
    end

    # Print the newly-predicted label for the original data
    println("Newly-predicted label (good):")
    println(predict_label(model_resnet, good_imgs_normalized[1], labels))
    pct_good = sum([predict_label(model_resnet, img, labels) == good_label for img in good_imgs_normalized]) / length(good_imgs_normalized)
    println("% good correct: $pct_good")
    println("Newly-predicted label (bad):")
    println(predict_label(model_resnet, bad_imgs_normalized[1], labels))
    pct_bad = sum([predict_label(model_resnet, img, labels) == bad_label for img in bad_imgs_normalized]) / length(bad_imgs_normalized)
    println("% bad correct: $pct_bad")
end
@everywhere function normalize_image(img)
    # Select the center 224x224 patch of the image and normalize it
    @assert all(size(img) .>= (224, 224))
    DATA_MEAN = (0.485, 0.456, 0.406)
    DATA_STD = (0.229, 0.224, 0.225)
    augmentations = CenterCrop((224, 224)) |>
                    ImageToTensor() |>
                    Normalize(DATA_MEAN, DATA_STD)
    return apply(augmentations, Image(img)) |> itemdata
end
@everywhere function train_model(model, data)
    # Initialize the optimiser
    opt = Optimisers.Adam()
    state = Optimisers.setup(opt, model)

    # Train the model
    for (batch_idx, (image, target)) in enumerate(data)
        @info "Starting batch $batch_idx..."

        # Calculate the gradients
        gs, _ = gradient(model, image) do m, x
            logitcrossentropy(m(x), target)
        end

        # Update the model weights
        state, model = Optimisers.update(state, model, gs)
    end
    return model
end
#=@with_plots=# demo(; niters=1, npar=4)