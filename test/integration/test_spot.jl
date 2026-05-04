include(joinpath(@__DIR__, "common.jl"))

@testset "addprocs — spot VMs and thread propagation" begin
    # Non-spot with explicit threads
    group = unique_group()
    @info "Sub-test 1/3: Non-spot, threads=\"2,0\" → expect default=2, interactive≥0" group
    try
        addprocs(AzManager(), TEMPLATENAME, 1; waitfor=true, group=group, session=SESSION, julia_num_threads="2,0", exename=EXENAME, overprovision=false)
        @test remotecall_fetch(Threads.nthreads, workers()[1]) == 2
        # Julia ≥1.12 always has at least 1 interactive thread even when not requested
        @test remotecall_fetch(Threads.nthreads, workers()[1], :interactive) >= 0
    finally
        cleanup_workers()
        wait_for_scaleset_deletion(group)
    end

    # Spot — should auto-add interactive thread
    group = unique_group()
    @info "Sub-test 2/3: Spot, threads=\"2,0\" → expect default=2, interactive=1 (auto-augmented)" group
    try
        addprocs(AzManager(), TEMPLATENAME, 1; waitfor=true, group=group, session=SESSION, julia_num_threads="2,0", spot=true, exename=EXENAME, overprovision=false)
        @test remotecall_fetch(Threads.nthreads, workers()[1]) == 2
        if workers()[1] != 1
            @test remotecall_fetch(Threads.nthreads, workers()[1], :interactive) == 1
        end
    finally
        cleanup_workers()
        wait_for_scaleset_deletion(group)
    end

    # Spot with explicit interactive threads
    group = unique_group()
    @info "Sub-test 3/3: Spot, threads=\"3,2\" → expect default=3, interactive=2" group
    try
        addprocs(AzManager(), TEMPLATENAME, 1; waitfor=true, group=group, session=SESSION, julia_num_threads="3,2", spot=true, exename=EXENAME, overprovision=false)
        @test remotecall_fetch(Threads.nthreads, workers()[1]) == 3
        @test remotecall_fetch(Threads.nthreads, workers()[1], :interactive) == 2
    finally
        cleanup_workers()
        wait_for_scaleset_deletion(group)
    end
end
