include(joinpath(@__DIR__, "common.jl"))

@testset "deletion tracking lifecycle" begin
    @info "Deletion tracking: provision VM → rmproc → verify event → wait → confirm deletion"

    testvm = nothing
    try
        basename = "deltrk-$(randstring('a':'z',4))"
        testvm = addproc(TEMPLATENAME; basename=basename, session=SESSION, exename=EXENAME)
        vmname = testvm["name"]
        @info "VM '$vmname' provisioned"

        manager = AzManagers.azmanager()

        # rmproc fires DeletionStarted event; give event loop a moment to process it
        rmproc(testvm; session=SESSION)
        sleep(2)

        # 1. Verify the deletion was tracked
        @test isdefined(manager, :pending_deletions)
        matching = filter(d -> d.vmname == vmname, manager.pending_deletions)
        @test length(matching) == 1
        @test matching[1].url != ""
        @info "DeletionStarted event tracked" vmname url=matching[1].url

        # 2. Poll check_pending_deletions until Azure confirms (or timeout)
        deletion_confirmed = false
        timeout = 180
        starttime = time()
        while time() - starttime < timeout
            AzManagers.check_pending_deletions(manager)
            remaining = filter(d -> d.vmname == vmname, manager.pending_deletions)
            if isempty(remaining)
                deletion_confirmed = true
                break
            end
            sleep(20)
        end

        @test deletion_confirmed
        @info "VM '$vmname' deletion confirmed via async operation URL" elapsed="$(round(time()-starttime, digits=1))s"

        # Mark as cleaned up so finally block doesn't try again
        testvm = nothing
    finally
        testvm !== nothing && cleanup_vm(testvm)
    end
end
