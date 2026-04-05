include(joinpath(@__DIR__, "test_utils.jl"))

run_group("misc") do

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

@testset "AzManagers, nphysical_cores $name" for (name, expected) in [("cbox02", 2), ("cbox04", 4), ("cbox08", 8)]
    @info "[$(elapsed())s] nphysical_cores test: checking $name..."
    ncores = nphysical_cores(name; session=session)
    @test ncores == expected
end

@testset "AzManagers, nphysical_cores Dict $templatename" begin
    @info "[$(elapsed())s] nphysical_cores template test: checking $templatename..."
    templates_vm = JSON.parse(read(AzManagers.templates_filename_vm(), String); dicttype=Dict)
    template = templates_vm[templatename]
    ncores = nphysical_cores(template; session=session)

    @test ncores == 2
end

end  # run_group
