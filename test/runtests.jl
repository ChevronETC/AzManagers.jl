test_groups = [
    "test_scalesets.jl",
    "test_environments.jl",
    "test_detach.jl",
    "test_misc.jl",
]

println("Launching $(length(test_groups)) test groups in parallel...")

julia_cmd = Base.julia_cmd()
project = Base.active_project()

function prefix_stream(input::IO, prefix::String, output::IO)
    @async try
        for line in eachline(input)
            println(output, "[$prefix] $line")
            flush(output)
        end
    catch e
        e isa EOFError || rethrow()
    end
end

procs = Dict{String, Base.Process}()
tasks = Dict{String, Vector{Task}}()
for g in test_groups
    path = joinpath(@__DIR__, g)
    label = replace(g, "test_" => "", ".jl" => "")
    cmd = `$julia_cmd --project=$project $path`
    pipe = Pipe()
    errpipe = Pipe()
    proc = run(pipeline(cmd; stdout=pipe, stderr=errpipe); wait=false)
    close(pipe.in)
    close(errpipe.in)
    procs[g] = proc
    tasks[g] = Task[
        prefix_stream(pipe.out, label, stdout),
        prefix_stream(errpipe.out, label, stderr),
    ]
end

failed = String[]
for (g, p) in procs
    wait(p)
    for t in tasks[g]
        wait(t)
    end
    if p.exitcode != 0
        push!(failed, g)
        println("FAILED: $g (exit code $(p.exitcode))")
    else
        println("PASSED: $g")
    end
end

if !isempty(failed)
    error("Test groups failed: $(join(failed, ", "))")
end

println("All test groups passed.")
