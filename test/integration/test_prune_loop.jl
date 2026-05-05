include(joinpath(@__DIR__, "common.jl"))

# Override prune interval and join timeout for this test
ENV["JULIA_AZMANAGERS_PRUNE_POLL_INTERVAL"] = "15"
ENV["JULIA_AZMANAGERS_VM_JOIN_TIMEOUT"] = "10"

@testset "prune loop detects orphaned VM" begin
    group = unique_group()
    ninstances = 2

    @info "Prune loop test: provision $ninstances VMs, orphan one, wait for prune timer to detect it" group

    try
        addprocs(AzManager(), TEMPLATENAME, ninstances;
            waitfor=true, ppi=1, group=group, session=SESSION, exename=EXENAME, overprovision=false)

        @test nworkers() == ninstances

        manager = AzManagers.azmanager()

        # Identify the worker to orphan and capture its instance ID
        orphan_pid = workers()[1]
        wrkr = Distributed.map_pid_wrkr[orphan_pid]
        orphan_instanceid = wrkr.config.userdata["instanceid"]
        orphan_ssname = wrkr.config.userdata["scalesetname"]
        @info "Will orphan worker $orphan_pid (instanceid=$orphan_instanceid, scaleset=$orphan_ssname)"

        # Deregister the worker from the cluster — VM is still running in the scale set
        Distributed.set_worker_state(Distributed.map_pid_wrkr[orphan_pid], Distributed.W_TERMINATED)
        Distributed.deregister_worker(orphan_pid)
        @test nworkers() == ninstances - 1

        # Now wait for the prune timer to fire and detect the orphaned VM.
        # With PRUNE_POLL_INTERVAL=15 and VM_JOIN_TIMEOUT=10, the prune loop
        # should detect the orphan within ~30s (at most 2 prune ticks).
        @info "Waiting for prune timer to detect orphaned VM..."
        orphan_detected = false
        timeout = 60
        starttime = time()
        while time() - starttime < timeout
            sleep(5)
            # Check if the orphaned instance was queued for deletion
            for (ss, ids) in AzManagers.pending_down(manager)
                if ss.scalesetname == orphan_ssname && orphan_instanceid ∈ ids
                    orphan_detected = true
                    break
                end
            end
            orphan_detected && break

            # Also check if it was added to the pruned list
            for (ss, ids) in manager.pruned
                if ss.scalesetname == orphan_ssname && orphan_instanceid ∈ ids
                    orphan_detected = true
                    break
                end
            end
            orphan_detected && break
        end

        elapsed = round(time() - starttime, digits=1)
        @test orphan_detected
        @info "Prune loop detected orphaned VM after $(elapsed)s" instanceid=orphan_instanceid

        # Remaining worker should still be alive
        @test nworkers() == ninstances - 1
    finally
        cleanup_workers()
        @info "Tearing down scale set '$group'..."
        AzManagers.rmgroup(group; session=SESSION)
        wait_for_scaleset_deletion(group)
    end
end
