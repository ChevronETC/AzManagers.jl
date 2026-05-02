include(joinpath(@__DIR__, "common.jl"))

@testset "prune with Resource Graph" begin
    group = unique_group()
    ninstances = 2
    ppi = 1
    tppi = ppi * ninstances

    @info "Prune test: provision $ninstances VMs, verify list_all_scaleset_vms + prune functions" group

    try
        addprocs(AzManager(), TEMPLATENAME, ninstances;
            waitfor=true, ppi=ppi, group=group, session=SESSION, exename=EXENAME, overprovision=false)

        worker_pids = workers()
        @test length(worker_pids) == tppi

        manager = AzManagers.azmanager()

        # ── Wait for Resource Graph to reflect the VMs (eventual consistency) ──
        @testset "list_all_scaleset_vms returns correct data" begin
            all_vms = Dict{AzManagers.ScaleSet, Vector}()
            vms = []
            timeout = 120
            starttime = time()
            while time() - starttime < timeout
                all_vms = AzManagers.list_all_scaleset_vms(manager)
                matching_keys = filter(k -> k.scalesetname == group, collect(keys(all_vms)))
                if !isempty(matching_keys)
                    vms = all_vms[matching_keys[1]]
                    length(vms) >= ninstances && break
                end
                @info "Resource Graph returned $(length(vms)) VMs, waiting for $ninstances..." elapsed="$(round(time()-starttime, digits=0))s"
                sleep(10)
            end

            @test length(vms) >= ninstances
            @info "Resource Graph returned $(length(vms)) VMs for scale set '$group' after $(round(time()-starttime, digits=1))s"

            # Each VM should have the fields both prune functions need
            for vm in vms
                @test haskey(vm, "id")
                @test haskey(vm, "name")
                @test haskey(vm, "properties")
            end
        end

        # ── prune_cluster with active workers: should keep them all ──
        @testset "prune_cluster retains active workers" begin
            workers_before = workers()
            all_vms = AzManagers.list_all_scaleset_vms(manager)
            AzManagers.prune_cluster(all_vms)
            @test workers() == workers_before
            @info "prune_cluster: $(length(workers())) workers retained"
        end

        # ── prune_scalesets with active workers: should not queue any for deletion ──
        @testset "prune_scalesets does not queue active VMs" begin
            all_vms = AzManagers.list_all_scaleset_vms(manager)
            AzManagers.prune_scalesets(all_vms)
            matching_keys = filter(k -> k.scalesetname == group, collect(keys(AzManagers.pending_down(manager))))
            new_pending = isempty(matching_keys) ? 0 : length(AzManagers.pending_down(manager)[matching_keys[1]])
            @test new_pending == 0
            @info "prune_scalesets: no VMs queued for deletion"
        end

        # ── Simulate orphaned worker: deregister one, then prune should detect it ──
        @testset "prune_cluster removes orphaned worker" begin
            # Pick one worker to "orphan" — remove from Distributed but leave VM running
            orphan_pid = workers()[1]
            @info "Orphaning worker $orphan_pid (deregistering from cluster, VM still running)"
            Distributed.set_worker_state(Distributed.map_pid_wrkr[orphan_pid], Distributed.W_TERMINATED)
            Distributed.deregister_worker(orphan_pid)

            @test !(orphan_pid in workers())
            workers_after_orphan = workers()

            # prune_cluster should NOT remove remaining workers (they still have VMs)
            all_vms = AzManagers.list_all_scaleset_vms(manager)
            AzManagers.prune_cluster(all_vms)
            @test workers() == workers_after_orphan
            @info "prune_cluster: correctly retained $(length(workers())) remaining workers after orphaning"
        end
    finally
        cleanup_workers()
        @info "Tearing down scale set '$group'..."
        AzManagers.rmgroup(group; session=SESSION)
        wait_for_scaleset_deletion(group)
    end
end
