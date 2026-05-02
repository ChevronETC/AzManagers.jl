#
# Local-only scale test: 100 nodes.
# NOT included in runtests.jl — run manually:
#   julia --project=. test/integration/test_scale.jl
#
include(joinpath(@__DIR__, "common.jl"))

const NINSTANCES = parse(Int, get(ENV, "AZMANAGERS_SCALE_TEST_NODES", "100"))

@testset "addprocs — scale test ($NINSTANCES nodes)" begin
    group = unique_group()
    ppi = 1
    tppi = ppi * NINSTANCES
    @info "Scale test: $NINSTANCES VMs, ppi=$ppi → expect $tppi workers" group

    try
        t_start = time()
        addprocs(AzManager(), TEMPLATENAME, NINSTANCES;
            waitfor=true, ppi=ppi, group=group, session=SESSION, exename=EXENAME, overprovision=false)
        t_provision = time() - t_start

        @info "All workers up" elapsed="$(round(t_provision, digits=1))s" nworkers=nworkers()

        # Correct number of workers
        @test nworkers() == tppi

        # Workers are on distinct hosts
        myworkers = [remotecall_fetch(gethostname, w) for w in workers()]
        unique_workers = unique(myworkers)
        @test length(unique_workers) == NINSTANCES

        # Basic work distribution sanity check
        results = pmap(_ -> gethostname(), workers())
        @test length(results) == tppi

        @info "Scale test passed" nodes=NINSTANCES provision_time="$(round(t_provision, digits=1))s"
    finally
        @info "Tearing down scale set '$group'..."
        cleanup_workers(; timeout=120)
        wait_for_scaleset_deletion(group; timeout=300)
    end
end
