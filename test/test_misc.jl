include(joinpath(@__DIR__, "test_utils.jl"))

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

    templates_scaleset = JSON.parse(read(AzManagers.templates_filename_scaleset(), String); dicttype=Dict)
    template = templates_scaleset[templatename]

    addprocs(AzManager(session), template, 2; waitfor=true, group=group)
    @info "[$(elapsed())s] physical_hostname test: cluster up, checking hostnames..."

    wrkers = Distributed.map_pid_wrkr
    for i in workers()
        userdata = wrkers[i].config.userdata
        @info userdata
        name = get(userdata, "physical_hostname", "unknown")

        @test name !== "unknown" && match(r"[A-Z0-9]", name) !== nothing
    end

    @info "[$(elapsed())s] physical_hostname test: cleaning up..."
    with_timeout(()->rmprocs(workers()), 120; msg="rmprocs")
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
    with_timeout(()->rmproc(testvm; session=session), 120; msg="rmproc")
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
    templates_scaleset = JSON.parse(read(AzManagers.templates_filename_vm(), String); dicttype=Dict)
    template = templates_vm[templatename]
    ncores = nphysical_cores(template)

    @test ncores == 2
end
