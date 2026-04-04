include(joinpath(@__DIR__, "test_utils.jl"))

@testset "environment, addproc" begin
    @info "[$(elapsed())s] environment addproc test: setting up project..."
    mkpath("myproject")
    cd("myproject")
    quiet_pkg() do
        Pkg.activate(".")
        Pkg.add("AzSessions")
        Pkg.add("Distributed")
        Pkg.add("JSON")
        Pkg.add("HTTP")
        Pkg.add(PackageSpec(name="AzManagers", rev=azmanagers_rev))
    end
    @info "[$(elapsed())s] environment addproc test: project setup complete"

    write("LocalPreferences.toml", "[FooPackage]\nfoo = \"bar\"\n")

    r = randstring('a':'z',4)
    bname = "test$r"

    @info "[$(elapsed())s] environment addproc test: provisioning VM (customenv=true)..."
    testvm = nothing
    try
        testvm = addproc(templatename; basename=bname, session=session, customenv=true)
    catch e
        if e isa AzManagers.DetachedServiceTimeoutException
            @warn "[$(elapsed())s] environment addproc test: VM setup timed out, cleaning up..."
            testvm = e.vm
            with_timeout(()->rmproc(testvm; session=session), 120; msg="rmproc")
            @test false  # fail the test explicitly
            return
        end
        rethrow()
    end
    @info "[$(elapsed())s] environment addproc test: VM ready, running detached job..."
    testjob = nothing
    for attempt in 1:5
        try
            testjob = @detachat testvm begin
                using Pkg
                pinfo = Pkg.project()
                write(stdout, "project path is $(dirname(pinfo.path))\n")
                write(stdout, "$(readdir(dirname(pinfo.path)))")
            end
            break
        catch e
            if attempt == 5
                rethrow()
            end
            @warn "[$(elapsed())s] environment addproc test: detachat attempt $attempt failed, retrying in 10s..." exception=e
            sleep(10)
        end
    end
    @info "[$(elapsed())s] environment addproc test: waiting for detached job..."
    with_timeout(()->wait(testjob), 300; msg="wait(testjob)")
    testjob_stdout = read(testjob)
    @test contains(testjob_stdout, "myproject")

    x = readdir(".")
    @test contains(testjob_stdout, "LocalPreferences.toml")
    @test contains(testjob_stdout, "Manifest.toml")
    @test contains(testjob_stdout, "Project.toml")

    @info "[$(elapsed())s] environment addproc test: cleaning up..."
    with_timeout(()->rmproc(testvm; session=session), 120; msg="rmproc")
end

@testset "environment, addprocs" begin
    @info "[$(elapsed())s] environment addprocs test: setting up project..."
    mkpath("myproject")
    cd("myproject")
    quiet_pkg() do
        Pkg.activate(".")
        Pkg.add("AzSessions")
        Pkg.add("Distributed")
        Pkg.add("JSON")
        Pkg.add("HTTP")
        Pkg.add(PackageSpec(name="AzManagers", rev=azmanagers_rev))
    end
    @info "[$(elapsed())s] environment addprocs test: project setup complete"

    write("LocalPreferences.toml", "[FooPackage]\nfoo = \"bar\"\n")

    group = "test$(randstring('a':'z',4))"

    @info "[$(elapsed())s] environment addprocs test: provisioning 1 instance (customenv=true)..."
    addprocs(templatename, 1; waitfor=true, group=group, session=session, customenv=true)
    @info "[$(elapsed())s] environment addprocs test: cluster up"
    @everywhere using Pkg
    pinfo = remotecall_fetch(Pkg.project, workers()[1])
    @test contains(pinfo.path, "myproject")

    files = remotecall_fetch(Pkg.readdir, workers()[1], dirname(pinfo.path))
    x = readdir(".")
    @test "LocalPreferences.toml" ∈ files
    @test "Project.toml" ∈ files
    @test "Manifest.toml" ∈ files

    @info "[$(elapsed())s] environment addprocs test: cleaning up..."
    with_timeout(()->rmprocs(workers()), 120; msg="rmprocs")
end

@testset "tags, addprocs" begin
    @info "[$(elapsed())s] tags test: setting up project..."
    mkpath("myproject")
    cd("myproject")
    quiet_pkg() do
        Pkg.activate(".")
        Pkg.add("AzSessions")
        Pkg.add("Distributed")
        Pkg.add("JSON")
        Pkg.add("HTTP")
        Pkg.add(PackageSpec(name="AzManagers", rev=azmanagers_rev))
    end

    group = "test$(randstring('a':'z',4))"

    templates_scaleset = JSON.parse(read(AzManagers.templates_filename_scaleset(), String))
    template = templates_scaleset[templatename]

    _template = template["value"]
    if haskey(_template, "tags")
        _template["tags"]["foo"] = "bar"
    else
        _template["tags"] = Dict("foo"=>"bar")
    end

    @info "[$(elapsed())s] tags test: provisioning 1 instance..."
    addprocs(template, 1; waitfor=true, group=group, session=session)
    @info "[$(elapsed())s] tags test: verifying tags..."

    _r = HTTP.request(
        "GET",
        "https://management.azure.com/subscriptions/$subscriptionid/resourceGroups/$resourcegroup/providers/Microsoft.Compute/virtualMachineScaleSets/$group?api-version=2019-12-01",
        ["Authorization"=>"Bearer $(token(session))"]
    )

    r = JSON.parse(String(_r.body))
    @test r["tags"]["foo"] == "bar"

    @info "[$(elapsed())s] tags test: cleaning up..."
    with_timeout(()->rmprocs(workers()), 120; msg="rmprocs")
end
