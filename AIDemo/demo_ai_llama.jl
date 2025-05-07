#using Llama2
#=
function demo()
    # Ensure the model and tokenizer are downloaded
    #@assert isfile("Demo/Meta-Llama-3-8B.Q4_K_S.gguf")
    @assert isfile("Demo/Phi-3-mini-4k-instruct-q4.gguf")

    # Load the model
    #model = load_gguf_model("Demo/Meta-Llama-3-8B.Q4_K_S.gguf"; mmap=true)
    model = load_gguf_model("Demo/Phi-3-mini-4k-instruct-q4.gguf"; mmap=true)

    # Sample from the model
    return sample(model, "What's the capital of France?"; temperature = 0.0f0)
end
demo()
=#