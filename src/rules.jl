struct Rule
    action::Function
    inputs::Vector{String}
    outputs::Vector{String}
    forcerun::Bool

    function Rule(action::Function, inputs::Vector{String}, outputs::Vector{String}, forcerun::Bool)
        new(action, inputs, outputs, forcerun)
    end
end

# String inputs/outputs variants
Rule(action::Function, inputs::String, outputs::String; forcerun::Bool=false) = 
    Rule(action, [inputs], [outputs], forcerun)

Rule(action::Function, inputs::Vector{String}, outputs::String; forcerun::Bool=false) = 
    Rule(action, inputs, [outputs], forcerun)

Rule(action::Function, inputs::String, outputs::Vector{String}; forcerun::Bool=false) = 
    Rule(action, [inputs], outputs, forcerun)

Rule(action::Function, io::Pair; forcerun::Bool=false) = 
    Rule(action, io.first, io.second; forcerun)

Rule(action::Function, inputs::Vector{String}, outputs::Vector{String}; forcerun::Bool=false) = 
    Rule(action, inputs, outputs, forcerun)

function Base.show(io::IO, r::Rule)
    print(io, "Rule(")
    print(io, r.action)
    
    # Show inputs and outputs
    in_str = length(r.inputs) == 1 ? "\"$(r.inputs[1])\"" : string(r.inputs)
    out_str = length(r.outputs) == 1 ? "\"$(r.outputs[1])\"" : string(r.outputs)
    print(io, ", ", in_str, " => ", out_str)
    
    # Show forcerun if true
    r.forcerun && print(io, "; forcerun=true")
    
    print(io, ")")
end

function Base.show(io::IO, ::MIME"text/plain", r::Rule)
    println(io, "Rule(;forcerun=$(r.forcerun)):")
    println(io, "  Action:      ", r.action)
    println(io, "  Inputs:      ", r.inputs)
    println(io, "  Outputs:     ", r.outputs)
end

function needs_update(task::Rule)
    
    # all input files should be present so I can check their dates
    missingfiles = @. !isfile(task.inputs)
    any(missingfiles) && throw(AssertionError("Rule declares $(task.inputs) as input\n but $(task.inputs[missingfiles]) do not exist."))

    task.forcerun && return true
    any(!isfile, task.outputs) && return true

    # Get the latest modification time of inputs
    input_mtime = maximum(mtime.(task.inputs))
    # Get the earliest modification time of outputs
    output_mtime = minimum(mtime.(task.outputs))
    # Run if any input is newer than any output
    input_mtime > output_mtime
end

function (task::Rule)(inputs...) # Inputs in not used, only for dagger to build the DAG

    if needs_update(task)
        "[RUN] Running $(task) (thread $(Threads.threadid()))" |> println
        task.action(task.inputs, task.outputs)
    else
        "[ - ] Skipping $(task)" |> println
    end
    task.outputs
end