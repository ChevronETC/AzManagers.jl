include(joinpath(@__DIR__, "common.jl"))

@testset "spot eviction" begin
    group = unique_group()
    @info "Spot eviction: 2 spot VMs, simulate eviction on worker 1 → expect cluster shrinks within 300s" group
    try
        addprocs(AzManager(), TEMPLATENAME, 2; waitfor=true, group=group, session=SESSION, julia_num_threads="2,1", spot=true, exename=EXENAME, overprovision=false)

        AzManagers.simulate_spot_eviction(workers()[1])

        tic = time()
        while time() - tic < 300
            if nprocs() < 3
                @info "Cluster responded to spot eviction in $(round(time()-tic, digits=1))s"
                break
            end
            sleep(10)
        end
        @test nprocs() < 3
    finally
        cleanup_workers()
        wait_for_scaleset_deletion(group)
    end
end
