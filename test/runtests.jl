using Distributed, AzManagers, Random, TOML, Test, HTTP, AzSessions, JSON, Pkg

session = AzSession(;protocal=AzClientCredentials)

azmanagers_pinfo = Pkg.project()
pkgs=TOML.parse(read(joinpath(dirname(azmanagers_pinfo.path),"Manifest.toml"), String))
pkg = VERSION < v"1.7.0" ? pkgs["AzManagers"][1] : pkgs["deps"]["AzManagers"][1]
azmanagers_rev=get(pkg, "repo-rev", "")

templatename = "cbox02"
template = JSON.parse(read(AzManagers.templates_filename_scaleset(), String))[templatename]
subscriptionid = template["subscriptionid"]
resourcegroup = template["resourcegroup"]

#=
TODO - flexible orchestration is broken for CI
error:
No outbound connectivity configured for virtual machine .... Please attach standard load balancer or public IP address to VM, create NAT gateway
or configure user-defined routes (UDR) in the subnet. Learn more at aka.ms/defaultoutboundaccess.
=#
@testset "AzManagers, addprocs, ppi=$ppi, flexible=$flexible" for ppi in (1,), flexible in (false,#=true=#)
    ninstances = 4
    group = "test$(randstring('a':'z',4))"
    
    # Set up iteration vars
    url = "https://management.azure.com/subscriptions/$subscriptionid/resourceGroups/$resourcegroup/providers/Microsoft.Compute/virtualMachineScaleSets/$group?api-version=2019-12-01"
    tppi = ppi*ninstances                       # Total number of Julia processes in the entire scale set

    #
    # Unit Test 1 - Create scale set and start Julia processes
    #
    if flexible
        addprocs(templatename, ninstances;
            waitfor = true,
            ppi,
            group,
            session,
            spot = true,
            spot_base_regular_priority_count = 2)
    else
        addprocs(templatename, ninstances;
            waitfor = true,
            ppi,
            group,
            session)
    end
    
    # Verify that the scale set is present
    _r = HTTP.request("GET", url, Dict("Authorization"=>"Bearer $(token(session))"); verbose=0)
    @test _r.status == 200

    #
    # Unit Test 2 - Verify that (Total # of Julia processes specified) == (Total # of Julia processes actual)
    #
    @test nworkers() == tppi

    #
    # Unit Test 3 - Verify that there is a healthy connection to each node
    #
    myworkers = [remotecall_fetch(gethostname, workers()[i]) for i=1:tppi]

    #
    # Unit Test 4 - Verify that none of the created Julia processes are the master Julia process
    #
    master = gethostname()
    unique_workers = unique(myworkers)

    @test length(unique_workers) == ninstances
    for worker in myworkers 
        @test master != worker
    end

    #
    # Unit Test 5 - Verify that the cloud-init startup script ran successfully
    #
    for i = 1:tppi
        @test_broken remotecall_fetch(isfile, workers()[i], ".git-credentials")
    end

    #
    # Unit Test 6 - Verify that physical_hostname on worker
    #
    wrkers = Distributed.map_pid_wrkr
    for i in workers()
        userdata = wrkers[i].config.userdata 
        @test !isnothing(get(userdata, "physical_hostname", nothing))
    end
    #
    # Unit Test 7 - Delete the Julia processes, scale set instances and the scale set itself
    #

    # First, verify that the scale set is present
    _r = HTTP.request("GET", url, Dict("Authorization"=>"Bearer $(token(session))"); verbose=0)
    @test _r.status == 200

    @info "Deleting cluster..."
    rmprocs(workers())

    # Last, verify that the scale set has been deleted
    itry = 0
    while true
        itry += 1
        try
            HTTP.request("GET", url, Dict("Authorization"=>"Bearer $(token(session))"); verbose=0)
        catch _e
            e = JSON.parse(String(_e.response.body))
            if _e.status == 404 && e["error"]["code"] == "ResourceNotFound"
                @info "Cluster deleted!"
                break
            end
            if itry == 10
                @warn "cluster not deleted"
                break
            end
        end
        sleep(5)
    end
end

@testset "environment, addproc" begin
    mkpath("myproject")
    cd("myproject")
    Pkg.activate(".")
    Pkg.add("AzSessions")
    Pkg.add("Distributed")
    Pkg.add("JSON")
    Pkg.add("HTTP")

    Pkg.add(PackageSpec(name="AzManagers", rev=azmanagers_rev))

    write("LocalPreferences.toml", "[FooPackage]\nfoo = \"bar\"\n")

    r = randstring('a':'z',4)
    bname = "test$r"

    testvm = addproc(templatename; basename=bname, session=session, customenv=true)
    testjob = @detachat testvm begin
        using Pkg
        pinfo = Pkg.project()
        write(stdout, "project path is $(dirname(pinfo.path))\n")
        write(stdout, "$(readdir(dirname(pinfo.path)))")
    end
    wait(testjob)
    testjob_stdout = read(testjob)
    @test contains(testjob_stdout, "myproject")

    x = readdir(".")
    @test contains(testjob_stdout, "LocalPreferences.toml")
    @test contains(testjob_stdout, "Manifest.toml")
    @test contains(testjob_stdout, "Project.toml")

    rmproc(testvm; session=session)
end

@testset "environment, addprocs" begin
    mkpath("myproject")
    cd("myproject")
    Pkg.activate(".")
    Pkg.add("AzSessions")
    Pkg.add("Distributed")
    Pkg.add("JSON")
    Pkg.add("HTTP")

    write("LocalPreferences.toml", "[FooPackage]\nfoo = \"bar\"\n")

    Pkg.add(PackageSpec(name="AzManagers", rev=azmanagers_rev))

    group = "test$(randstring('a':'z',4))"

    addprocs(templatename, 1; waitfor=true, group=group, session=session, customenv=true)
    @everywhere using Pkg
    pinfo = remotecall_fetch(Pkg.project, workers()[1])
    @test contains(pinfo.path, "myproject")

    files = remotecall_fetch(Pkg.readdir, workers()[1], dirname(pinfo.path))
    x = readdir(".")
    @test "LocalPreferences.toml" ∈ files
    @test "Project.toml" ∈ files
    @test "Manifest.toml" ∈ files

    rmprocs(workers())

end

@testset "tags, addprocs" begin
    mkpath("myproject")
    cd("myproject")
    Pkg.activate(".")
    Pkg.add("AzSessions")
    Pkg.add("Distributed")
    Pkg.add("JSON")
    Pkg.add("HTTP")

    Pkg.add(PackageSpec(name="AzManagers", rev=azmanagers_rev))

    group = "test$(randstring('a':'z',4))"

    templates_scaleset = JSON.parse(read(AzManagers.templates_filename_scaleset(), String))
    template = templates_scaleset[templatename]

    _template = template["value"]
    if haskey(_template, "tags")
        _template["tags"]["foo"] = "bar"
    else
        _template["tags"] = Dict("foo"=>"bar")
    end

    addprocs(template, 1; waitfor=true, group=group, session=session)

    _r = HTTP.request(
        "GET",
        "https://management.azure.com/subscriptions/$subscriptionid/resourceGroups/$resourcegroup/providers/Microsoft.Compute/virtualMachineScaleSets/$group?api-version=2019-12-01",
        ["Authorization"=>"Bearer $(token(session))"]
    )

    r = JSON.parse(String(_r.body))
    @test r["tags"]["foo"] == "bar"

    rmprocs(workers())
end

@testset "AzManagers, addproc" begin
    r = randstring('a':'z',4)
    basename = "test$r"
    testvm = addproc(templatename; basename=basename, session=session)
    testjob = @detachat testvm begin
        write(stdout, "write to stdout\n")
        write(stderr, "write to stderr\n")
    end
    wait(testjob)
    @test read(testjob) == "write to stdout\n"
    @test read(testjob; stdio=stderr) == "write to stderr\n"
    rmproc(testvm; session=session)

    testvm = addproc(templatename, name=basename, session=session)
    testjob = @detachat testvm begin
        write(stdout, "write to stdout\n")
        write(stderr, "write to stderr\n")
    end
    wait(testjob)
    @test read(testjob) == "write to stdout\n"
    @test read(testjob; stdio=stderr) == "write to stderr\n"
    rmproc(testvm; session=session)
end

@testset "AzManagers, detach" for kwargs in ( (dummy="dummy"), )

    #
    # Unit Test 1 - Create a detached job and persist the server
    #
    job1 = @detach vm(;vm_template=templatename, session=session, persist=true) begin
        write(stdout, "job1 - stdout string")
        write(stderr, "job1 - stderr string")
    end

    #
    # Unit Test 2 - Send a new job to the server started above
    #
    job2 = @detachat job1.vm begin
        write(stdout, "job2 - stdout string")
        write(stderr, "job2 - stderr string")
    end

    # Wait for jobs to finish
    wait(job1)
    wait(job2)

    @test status(job1) == "done"
    @test read(job1;stdio=stdout) == "job1 - stdout string"
    @test read(job1;stdio=stderr) == "job1 - stderr string"

    @test status(job2) == "done"
    @test read(job2;stdio=stdout) == "job2 - stdout string"
    @test read(job2;stdio=stderr) == "job2 - stderr string"

    #
    # Unit Test 3 - shut-down the detached server
    #
    rmproc(job1.vm; session=session)

    #
    # Unit Test 4 - create a new job on a new detached server that auto-destructs upon completion of its work
    #
    job3 = @detach vm(;vm_template=templatename, session=session, persist=false) begin
    end
end

@testset "AzManagers, detach, variablebundle" begin
    r = randstring('a':'z',4)
    basename = "test$r"
    testvm = addproc(templatename; basename=basename, session=session)
    variablebundle!(a=1.0,b=3.14)
    testjob = @detachat testvm begin
        if variablebundle(:a) ≈ 1.0 && variablebundle(:b) ≈ 3.14
            write(stdout, "passed")
        else
            write(stdout, "failed")
        end
    end
    wait(testjob)
    @test contains(read(testjob), "passed")
end

@testset "AzManagers, retrywarn" begin
    r = HTTP.Response(
        429,
        ["retry-after"=>60, "x-ms-ratelimit-remaining-resource"=>"foo"],
        "")

    e = HTTP.StatusError(429, "foo", "foo", r)
    AzManagers.retrywarn(1, 2, 60, e)
end