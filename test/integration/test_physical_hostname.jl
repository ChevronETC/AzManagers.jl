include(joinpath(@__DIR__, "common.jl"))

@testset "physical hostname" begin
    # Test physical_hostname on both scale set workers and detached VMs
    # using a single scale set for both checks

    group = unique_group()
    testvm = nothing
    try
        @testset "scaleset workers" begin
            @info "Sub-test 1/2: Scale set with 2 workers → expect physical_hostname set on each worker" group
            addprocs(AzManager(), TEMPLATE, 2; waitfor=true, group=group, session=SESSION, exename=EXENAME, overprovision=false)

            wrkers = Distributed.map_pid_wrkr
            for i in workers()
                userdata = wrkers[i].config.userdata
                name = get(userdata, "physical_hostname", "unknown")
                @test name !== "unknown"
                @test match(r"[A-Z0-9]", name) !== nothing
            end
        end
    finally
        cleanup_workers()
        wait_for_scaleset_deletion(group)
    end

    try
        @testset "detached VM" begin
            @info "Sub-test 2/2: Detached VM → expect physical_hostname in VM metadata"
            basename = "testph-$(randstring('a':'z',4))"
            testvm = addproc(TEMPLATENAME; basename=basename, session=SESSION, exename=EXENAME)

            ip = testvm["ip"]
            port = testvm["port"]
            url = "http://$ip:$port/cofii/detached/vm"
            _r = HTTP.get(url)
            r = JSON.parse(String(_r.body))

            name = r["physical_hostname"]
            @test name !== "unknown"
            @test match(r"[A-Z0-9]", name) !== nothing
        end
    finally
        testvm !== nothing && cleanup_vm(testvm)
    end
end
