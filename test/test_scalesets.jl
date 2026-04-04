include(joinpath(@__DIR__, "test_utils.jl"))

@testset "AzManagers, addprocs, ppi=$ppi, flexible=$flexible" for ppi in (1,), flexible in (false,)
    ninstances = 4
    group = "test$(randstring('a':'z',4))"

    url = "https://management.azure.com/subscriptions/$subscriptionid/resourceGroups/$resourcegroup/providers/Microsoft.Compute/virtualMachineScaleSets/$group?api-version=2019-12-01"
    tppi = ppi*ninstances

    @info "[$(elapsed())s] addprocs test: provisioning $ninstances instances (ppi=$ppi, flexible=$flexible, group=$group)..."
    if flexible
        addprocs(templatename, ninstances;
            waitfor = true, ppi, group, session,
            spot = true, spot_base_regular_priority_count = 2)
    else
        addprocs(templatename, ninstances;
            waitfor = true, ppi, group, session)
    end
    @info "[$(elapsed())s] addprocs test: cluster up, $(nworkers()) workers running"

    _r = HTTP.request("GET", url, Dict("Authorization"=>"Bearer $(token(session))"); verbose=0)
    @test _r.status == 200
    @test nworkers() == tppi

    myworkers = [remotecall_fetch(gethostname, workers()[i]) for i=1:tppi]
    master = gethostname()
    unique_workers = unique(myworkers)

    @test length(unique_workers) == ninstances
    for worker in myworkers
        @test master != worker
    end

    for i = 1:tppi
        @test_broken remotecall_fetch(isfile, workers()[i], ".git-credentials")
    end

    _r = HTTP.request("GET", url, Dict("Authorization"=>"Bearer $(token(session))"); verbose=0)
    @test _r.status == 200

    @info "[$(elapsed())s] addprocs test: deleting cluster..."
    with_timeout(()->rmprocs(workers()), 120; msg="rmprocs")

    itry = 0
    while true
        itry += 1
        try
            HTTP.request("GET", url, Dict("Authorization"=>"Bearer $(token(session))"); verbose=0)
        catch _e
            e = JSON.parse(String(_e.response.body))
            if _e.status == 404 && e["error"]["code"] == "ResourceNotFound"
                @info "[$(elapsed())s] addprocs test: cluster deleted"
                break
            end
            if itry == 10
                @warn "[$(elapsed())s] addprocs test: cluster not deleted after $itry attempts"
                break
            end
        end
        sleep(5)
    end
end

@testset "addprocs, spot" begin
    @info "[$(elapsed())s] spot test: provisioning 1 instance (threads=2,0, spot=false)..."
    group = "test$(randstring('a':'z',4))"
    julia_num_threads = VERSION >= v"1.9" ? "2,0" : "2"
    addprocs(templatename, 1; waitfor = true, group, session, julia_num_threads)

    @test remotecall_fetch(Threads.nthreads, workers()[1]) == 2
    if VERSION >= v"1.9"
        @test remotecall_fetch(Threads.nthreads, workers()[1], :interactive) == 0
    end
    @info "[$(elapsed())s] spot test: cleaning up non-spot workers..."
    with_timeout(()->rmprocs(workers()), 120; msg="rmprocs")

    @info "[$(elapsed())s] spot test: provisioning 1 instance (threads=2,0, spot=true)..."
    group = "test$(randstring('a':'z',4))"
    julia_num_threads = VERSION >= v"1.9" ? "2,0" : "2"
    addprocs(templatename, 1; waitfor = true, group, session, julia_num_threads, spot = true)

    @test remotecall_fetch(Threads.nthreads, workers()[1]) == 2
    if VERSION >= v"1.9"
        if workers()[1] != 1
            @test remotecall_fetch(Threads.nthreads, workers()[1], :interactive) == 1
        end
    end
    @info "[$(elapsed())s] spot test: cleaning up spot workers..."
    with_timeout(()->rmprocs(workers()), 120; msg="rmprocs")

    @info "[$(elapsed())s] spot test: provisioning 1 instance (threads=3,2, spot=true)..."
    group = "test$(randstring('a':'z',4))"
    julia_num_threads = VERSION >= v"1.9" ? "3,2" : "3"
    addprocs(templatename, 1; waitfor = true, group, session, julia_num_threads, spot=true)

    @test remotecall_fetch(Threads.nthreads, workers()[1]) == 3
    if VERSION >= v"1.9"
        @test remotecall_fetch(Threads.nthreads, workers()[1], :interactive) == 2
    end
    @info "[$(elapsed())s] spot test: cleaning up..."
    with_timeout(()->rmprocs(workers()), 120; msg="rmprocs")
end

if VERSION >= v"1.9"
    @testset "spot eviction" begin
        @info "[$(elapsed())s] spot eviction test: provisioning 2 instances..."
        group = "test$(randstring('a':'z',4))"
        julia_num_threads = "2,1"
        addprocs(templatename, 2; waitfor = true, group, session, julia_num_threads, spot = true)

        @info "[$(elapsed())s] spot eviction test: simulating eviction on worker $(workers()[1])..."
        AzManagers.simulate_spot_eviction(workers()[1])

        tic = time()
        while time() - tic < 300
            if nprocs() < 3
                @info "[$(elapsed())s] spot eviction test: cluster responded in $(round(time() - tic; digits=1))s"
                break
            end
            sleep(10)
        end
        @test nprocs() < 3
        @info "[$(elapsed())s] spot eviction test: cleaning up..."
        try
            with_timeout(()->rmprocs(workers()), 120; msg="rmprocs")
        catch
            @warn "[$(elapsed())s] spot eviction test: rmprocs timed out, force-deregistering workers..."
            for pid in workers()
                try
                    lock(Distributed.worker_lock)
                    if haskey(Distributed.map_pid_wrkr, pid)
                        Distributed.set_worker_state(Distributed.map_pid_wrkr[pid], Distributed.W_TERMINATED)
                        Distributed.deregister_worker(pid)
                    end
                catch
                finally
                    unlock(Distributed.worker_lock)
                end
            end
        end
    end
end
