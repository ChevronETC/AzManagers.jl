include(joinpath(@__DIR__, "common.jl"))

@testset "addprocs — basic scale set" begin
    group = unique_group()
    ninstances = 4
    ppi = 1
    tppi = ppi * ninstances
    @info "Sub-test 1/2: Basic scale set, $(ninstances) VMs, overprovision=false → expect $(tppi) workers on distinct hosts" group

    try
        addprocs(AzManager(), TEMPLATENAME, ninstances;
            waitfor=true, ppi=ppi, group=group, session=SESSION, exename=EXENAME, overprovision=false)

        # Scale set exists
        url = scaleset_url(group)
        _r = HTTP.request("GET", url, ["Authorization" => "Bearer $(token(SESSION))"]; verbose=0)
        @test _r.status == 200

        # Correct number of workers
        @test nworkers() == tppi

        # Workers are distinct from master
        myworkers = [remotecall_fetch(gethostname, workers()[i]) for i in 1:tppi]
        master = gethostname()
        unique_workers = unique(myworkers)
        @test length(unique_workers) == ninstances
        for w in myworkers
            @test master != w
        end
    finally
        @info "Tearing down scale set '$group'..."
        cleanup_workers()
        wait_for_scaleset_deletion(group)
    end
end

@testset "addprocs — overprovision=true" begin
    group = unique_group()
    ninstances = 4
    ppi = 1
    tppi = ppi * ninstances
    @info "Sub-test 2/2: Overprovision=true, $(ninstances) VMs → expect $(tppi) workers on distinct hosts" group

    try
        addprocs(AzManager(), TEMPLATENAME, ninstances;
            waitfor=true, ppi=ppi, group=group, session=SESSION, exename=EXENAME, overprovision=true)

        @test nworkers() == tppi

        myworkers = [remotecall_fetch(gethostname, workers()[i]) for i in 1:tppi]
        master = gethostname()
        unique_workers = unique(myworkers)
        @test length(unique_workers) == ninstances
        for w in myworkers
            @test master != w
        end
    finally
        @info "Tearing down scale set '$group'..."
        cleanup_workers()
        wait_for_scaleset_deletion(group)
    end
end
