test_groups = [
    "test_scalesets.jl",
    "test_environments.jl",
    "test_detach.jl",
    "test_misc.jl",
]

println("Launching $(length(test_groups)) test groups in parallel...")

# Inherit the current project environment so subprocesses have access to AzManagers
julia_cmd = Base.julia_cmd()
project = Base.active_project()

procs = Dict{String, Base.Process}()
for g in test_groups
    path = joinpath(@__DIR__, g)
    cmd = `$julia_cmd --project=$project $path`
    procs[g] = open(pipeline(cmd; stdout, stderr); read=false).process
end

failed = String[]
for (g, p) in procs
    wait(p)
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
