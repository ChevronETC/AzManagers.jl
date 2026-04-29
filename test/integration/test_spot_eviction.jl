include(joinpath(@__DIR__, "common.jl"))

@testset "spot eviction" begin
    group = unique_group()
    @info "Spot eviction: 2 spot VMs, simulate eviction on worker 1 → expect cluster shrinks within 300s" group
    try
        addprocs(AzManager(), TEMPLATENAME, 2; waitfor=true, group=group, session=SESSION, julia_num_threads="2,1", spot=true, exename=EXENAME, overprovision=false)

        evicted_worker = workers()[1]
        AzManagers.simulate_spot_eviction(evicted_worker)

        tic = time()
        eviction_detected = false
        while time() - tic < 300
            if nprocs() < 3
                @info "Cluster responded to spot eviction in $(round(time()-tic, digits=1))s"
                eviction_detected = true
                break
            end
            sleep(5)
        end
        if !eviction_detected
            @warn "Eviction not detected after 300s. nprocs=$(nprocs()), workers=$(workers())"
        end
        @test eviction_detected
    finally
        cleanup_workers()
        wait_for_scaleset_deletion(group)
    end
end
