using Distributed, AzManagers, Random, TOML, Test, HTTP, AzSessions, JSON, Pkg
using MPI

function with_timeout(f, seconds; msg="operation")
    t = @async f()
    deadline = time() + seconds
    while !istaskdone(t) && time() < deadline
        sleep(1)
    end
    if !istaskdone(t)
        error("$msg timed out after $(seconds)s")
    end
    fetch(t)
end

const test_start_time = time()
elapsed() = round(time() - test_start_time; digits=1)

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

    @info "[$(elapsed())s] addprocs test: provisioning $ninstances instances (ppi=$ppi, flexible=$flexible, group=$group)..."
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
    @info "[$(elapsed())s] addprocs test: cluster up, $(nworkers()) workers running"

    # Verify that the scale set is present
    _r = HTTP.request("GET", url, Dict("Authorization"=>"Bearer $(token(session))"); verbose=0)
    @test _r.status == 200

    @test nworkers() == tppi

    myworkers = [remotecall_fetch(gethostname, workers()[i]) for i=1:tppi]

    master = gethostname()
    unique_workers = unique(myworkers)

    @test length(unique_workers) == ninstances
    for worker in myworkers
        @test master != worker
    end

    for i = 1:tppi
        @test_broken remotecall_fetch(isfile, workers()[i], ".git-credentials")
    end

    # First, verify that the scale set is present
    _r = HTTP.request("GET", url, Dict("Authorization"=>"Bearer $(token(session))"); verbose=0)
    @test _r.status == 200

    @info "[$(elapsed())s] addprocs test: deleting cluster..."
    with_timeout(120; msg="rmprocs") do rmprocs(workers()) end

    # Last, verify that the scale set has been deleted
    itry = 0
    while true
        itry += 1
        try
            HTTP.request("GET", url, Dict("Authorization"=>"Bearer $(token(session))"); verbose=0)
        catch _e
            e = JSON.parse(String(_e.response.body))
            if _e.status == 404 && e["error"]["code"] == "ResourceNotFound"
                @info "[$(elapsed())s] addprocs test: cluster deleted"
                break
            end
            if itry == 10
                @warn "[$(elapsed())s] addprocs test: cluster not deleted after $itry attempts"
                break
            end
        end
        sleep(5)
    end
end

@testset "addprocs, spot" begin
    @info "[$(elapsed())s] spot test: provisioning 1 instance (threads=2,0, spot=false)..."
    group = "test$(randstring('a':'z',4))"
    julia_num_threads = VERSION >= v"1.9" ? "2,0" : "2"
    addprocs(templatename, 1; waitfor = true, group, session, julia_num_threads)

    @test remotecall_fetch(Threads.nthreads, workers()[1]) == 2

    if VERSION >= v"1.9"
        @test remotecall_fetch(Threads.nthreads, workers()[1], :interactive) == 0
    end
    @info "[$(elapsed())s] spot test: cleaning up non-spot workers..."
    with_timeout(120; msg="rmprocs") do rmprocs(workers()) end

    @info "[$(elapsed())s] spot test: provisioning 1 instance (threads=2,0, spot=true)..."
    group = "test$(randstring('a':'z',4))"
    julia_num_threads = VERSION >= v"1.9" ? "2,0" : "2"
    addprocs(templatename, 1; waitfor = true, group, session, julia_num_threads, spot = true)

    @test remotecall_fetch(Threads.nthreads, workers()[1]) == 2

    if VERSION >= v"1.9"
        if workers()[1] != 1
            @test remotecall_fetch(Threads.nthreads, workers()[1], :interactive) == 1
        end
    end
    @info "[$(elapsed())s] spot test: cleaning up spot workers..."
    with_timeout(120; msg="rmprocs") do rmprocs(workers()) end

    @info "[$(elapsed())s] spot test: provisioning 1 instance (threads=3,2, spot=true)..."
    group = "test$(randstring('a':'z',4))"
    julia_num_threads = VERSION >= v"1.9" ? "3,2" : "3"
    addprocs(templatename, 1; waitfor = true, group, session, julia_num_threads, spot=true)

    @test remotecall_fetch(Threads.nthreads, workers()[1]) == 3

    if VERSION >= v"1.9"
        @test remotecall_fetch(Threads.nthreads, workers()[1], :interactive) == 2
    end
    @info "[$(elapsed())s] spot test: cleaning up..."
    with_timeout(120; msg="rmprocs") do rmprocs(workers()) end
end

if VERSION >= v"1.9"
    @testset "spot eviction" begin
        @info "[$(elapsed())s] spot eviction test: provisioning 2 instances..."
        group = "test$(randstring('a':'z',4))"
        julia_num_threads = "2,1"
        addprocs(templatename, 2; waitfor = true, group, session, julia_num_threads, spot = true)

        @info "[$(elapsed())s] spot eviction test: simulating eviction on worker $(workers()[1])..."
        AzManagers.simulate_spot_eviction(workers()[1])

        tic = time()
        while time() - tic < 300
            if nprocs() < 3
                @info "[$(elapsed())s] spot eviction test: cluster responded in $(round(time() - tic; digits=1))s"
                break
            end
            sleep(10)
        end
        @test nprocs() < 3
        @info "[$(elapsed())s] spot eviction test: cleaning up..."
        with_timeout(120; msg="rmprocs") do rmprocs(workers()) end
    end
end

@testset "environment, addproc" begin
    @info "[$(elapsed())s] environment addproc test: setting up project..."
    mkpath("myproject")
    cd("myproject")
    Pkg.activate(".")
    Pkg.add("AzSessions")
    Pkg.add("Distributed")
    Pkg.add("JSON")
    Pkg.add("HTTP")

    Pkg.add(PackageSpec(name="AzManagers", rev=azmanagers_rev))
    @info "[$(elapsed())s] environment addproc test: project setup complete"

    write("LocalPreferences.toml", "[FooPackage]\nfoo = \"bar\"\n")

    r = randstring('a':'z',4)
    bname = "test$r"

    @info "[$(elapsed())s] environment addproc test: provisioning VM (customenv=true)..."
    testvm = addproc(templatename; basename=bname, session=session, customenv=true)
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
    with_timeout(300; msg="wait(testjob)") do wait(testjob) end
    testjob_stdout = read(testjob)
    @test contains(testjob_stdout, "myproject")

    x = readdir(".")
    @test contains(testjob_stdout, "LocalPreferences.toml")
    @test contains(testjob_stdout, "Manifest.toml")
    @test contains(testjob_stdout, "Project.toml")

    @info "[$(elapsed())s] environment addproc test: cleaning up..."
    with_timeout(120; msg="rmproc") do rmproc(testvm; session=session) end
end

@testset "environment, addprocs" begin
    @info "[$(elapsed())s] environment addprocs test: setting up project..."
    mkpath("myproject")
    cd("myproject")
    Pkg.activate(".")
    Pkg.add("AzSessions")
    Pkg.add("Distributed")
    Pkg.add("JSON")
    Pkg.add("HTTP")

    write("LocalPreferences.toml", "[FooPackage]\nfoo = \"bar\"\n")

    Pkg.add(PackageSpec(name="AzManagers", rev=azmanagers_rev))
    @info "[$(elapsed())s] environment addprocs test: project setup complete"

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
    with_timeout(120; msg="rmprocs") do rmprocs(workers()) end

end

@testset "tags, addprocs" begin
    @info "[$(elapsed())s] tags test: setting up project..."
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
    with_timeout(120; msg="rmprocs") do rmprocs(workers()) end
end

@testset "AzManagers, addproc, and test if nthreads propagates properly" begin
    @info "[$(elapsed())s] nthreads test: provisioning VM (threads=1,2)..."
    r = randstring('a':'z',4)
    basename = "test$r"
    testvm = addproc(templatename; basename=basename, session=session, julia_num_threads="1,2")
    @info "[$(elapsed())s] nthreads test: VM ready, running detached job..."
    testjob = @detachat testvm begin
        write(stdout, "write to stdout\n")
        write(stderr, "nthreads: $(Threads.nthreads()),$(Threads.nthreads(:interactive))\n")
    end
    with_timeout(300; msg="wait(testjob)") do wait(testjob) end
    @test read(testjob) == "write to stdout\n"
    @test read(testjob; stdio=stderr) == "nthreads: 1,2\n"
    @info "[$(elapsed())s] nthreads test: cleaning up first VM..."
    with_timeout(120; msg="rmproc") do rmproc(testvm; session=session) end

    @info "[$(elapsed())s] nthreads test: provisioning VM (reusing name=$basename)..."
    testvm = addproc(templatename, name=basename, session=session)
    @info "[$(elapsed())s] nthreads test: VM ready, running detached job..."
    testjob = @detachat testvm begin
        write(stdout, "write to stdout\n")
        write(stderr, "write to stderr\n")
    end
    with_timeout(300; msg="wait(testjob)") do wait(testjob) end
    @test read(testjob) == "write to stdout\n"
    @test read(testjob; stdio=stderr) == "write to stderr\n"
    @info "[$(elapsed())s] nthreads test: cleaning up..."
    with_timeout(120; msg="rmproc") do rmproc(testvm; session=session) end
end

@testset "AzManagers, detach" for kwargs in ( (dummy="dummy"), )

    @info "[$(elapsed())s] detach test: creating persistent detached job..."
    job1 = @detach vm(;vm_template=templatename, session=session, persist=true) begin
        write(stdout, "job1 - stdout string")
        write(stderr, "job1 - stderr string")
    end

    @info "[$(elapsed())s] detach test: sending second job to same server..."
    job2 = @detachat job1.vm begin
        write(stdout, "job2 - stdout string")
        write(stderr, "job2 - stderr string")
    end

    @info "[$(elapsed())s] detach test: waiting for jobs..."
    with_timeout(300; msg="wait(job1)") do wait(job1) end
    with_timeout(300; msg="wait(job2)") do wait(job2) end

    @test status(job1) == "done"
    @test read(job1;stdio=stdout) == "job1 - stdout string"
    @test read(job1;stdio=stderr) == "job1 - stderr string"

    @test status(job2) == "done"
    @test read(job2;stdio=stdout) == "job2 - stdout string"
    @test read(job2;stdio=stderr) == "job2 - stderr string"

    @info "[$(elapsed())s] detach test: shutting down detached server..."
    with_timeout(120; msg="rmproc") do rmproc(job1.vm; session=session) end

    @info "[$(elapsed())s] detach test: creating auto-destruct detached job..."
    job3 = @detach vm(;vm_template=templatename, session=session, persist=false) begin
    end
end

@testset "AzManagers, detach, variablebundle" begin
    @info "[$(elapsed())s] variablebundle test: provisioning VM..."
    r = randstring('a':'z',4)
    basename = "test$r"
    testvm = addproc(templatename; basename=basename, session=session)
    variablebundle!(a=1.0,b=3.14)
    @info "[$(elapsed())s] variablebundle test: running detached job..."
    testjob = @detachat testvm begin
        if variablebundle(:a) ≈ 1.0 && variablebundle(:b) ≈ 3.14
            write(stdout, "passed")
        else
            write(stdout, "failed")
        end
    end
    with_timeout(300; msg="wait(testjob)") do wait(testjob) end
    @test contains(read(testjob), "passed")
end

@testset "AzManagers, retrywarn" begin
    @info "[$(elapsed())s] retrywarn test: testing retry warning output..."
    r = HTTP.Response(
        429,
        ["retry-after"=>60, "x-ms-ratelimit-remaining-resource"=>"foo"],
        "")

    e = HTTP.StatusError(429, "foo", "foo", r)
    AzManagers.retrywarn(1, 2, 60, e)
end

@testset "AzManagers, physical_hostname" begin
    @info "[$(elapsed())s] physical_hostname test: provisioning 2 instances..."
    group = "test$(randstring('a':'z',4))"

    templates_scaleset = JSON.parse(read(AzManagers.templates_filename_scaleset(), String))
    template = templates_scaleset[templatename]

    addprocs(template, 2; waitfor=true, group=group, session=session)
    @info "[$(elapsed())s] physical_hostname test: cluster up, checking hostnames..."

    wrkers = Distributed.map_pid_wrkr
    for i in workers()
        userdata = wrkers[i].config.userdata
        @info userdata
        name = get(userdata, "physical_hostname", "unknown")

        @test name !== "unknown" && match(r"[A-Z0-9]", name) !== nothing
    end

    @info "[$(elapsed())s] physical_hostname test: cleaning up..."
    with_timeout(120; msg="rmprocs") do rmprocs(workers()) end

end

@testset "AzManagers, addproc physical_hostname" begin
    @info "[$(elapsed())s] addproc physical_hostname test: provisioning VM..."
    r = randstring('a':'z',4)
    basename = "test$r"
    testvm = addproc(templatename; basename=basename, session=session)

    ip = testvm["ip"]
    port = testvm["port"]
    url = "http://$ip:$port/cofii/detached/vm"
    _r = HTTP.get(url)
    r = JSON.parse(String(_r.body))

    name = r["physical_hostname"]
    @test name !== "unknown" && match(r"[A-Z0-9]", name) !== nothing

    @info "[$(elapsed())s] addproc physical_hostname test: cleaning up..."
    with_timeout(120; msg="rmproc") do rmproc(testvm; session=session) end
end

@testset "AzManagers, nphysical_cores $machine_name" for machine_name in ("cbox96","cbox64","ussc/t107/v4/amd/cbox176")
    @info "[$(elapsed())s] nphysical_cores test: checking $machine_name..."
    ncores = nphysical_cores(machine_name)

    if machine_name == "cbox96"
        @test ncores == 96
    elseif machine_name == "cbox64"
        @test ncores == 64
    elseif machine_name == "ussc/t107/v4/amd/cbox176"
        @test ncores == 176
    end
end

@testset "AzManagers, nphysical_cores $templatename"
    @info "[$(elapsed())s] nphysical_cores template test: checking $templatename..."
    templates_scaleset = JSON.parse(read(AzManagers.templates_filename_vm(), String))
    template = templates_vm[templatename]
    ncores = nphysical_cores(template)

    @test ncores == 2
end
