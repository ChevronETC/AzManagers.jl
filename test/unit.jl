using AzManagers, HTTP, JSON, Pkg, Test

function synthetic_topology()
    sockets = [
        SocketTopology(0, collect(0:7)),
        SocketTopology(1, collect(8:15))]
    numa_nodes = [
        NumaTopology(0, collect(0:3), 0),
        NumaTopology(1, collect(4:7), 0),
        NumaTopology(2, collect(8:11), 1),
        NumaTopology(3, collect(12:15), 1)]

    MachineTopology(16, collect(0:15), sockets, numa_nodes, false)
end

function synthetic_lscpu()
    """
    # CPU,Core,Socket,Node
    0,0,0,0
    1,1,0,0
    2,2,0,1
    3,3,0,1
    4,0,0,0
    5,1,0,0
    6,2,0,1
    7,3,0,1
    """
end

function synthetic_lscpu_json()
    JSON.json(Dict("lscpu" => [
        Dict("field" => "CPU(s):", "data" => "16"),
        Dict("field" => "On-line CPU(s) list:", "data" => "0-15"),
        Dict("field" => "Thread(s) per core:", "data" => "2"),
        Dict("field" => "Core(s) per socket:", "data" => "4"),
        Dict("field" => "Socket(s):", "data" => "2"),
        Dict("field" => "NUMA node0 CPU(s):", "data" => "0-3,8-11"),
        Dict("field" => "NUMA node1 CPU(s):", "data" => "4-7,12-15")]))
end

function uneven_topology()
    sockets = [SocketTopology(0, collect(0:9))]
    numa_nodes = [NumaTopology(0, collect(0:9), 0)]

    MachineTopology(10, collect(0:9), sockets, numa_nodes, false)
end

@testset "unit: Azure URL helpers" begin
    expected_url = "https://management.azure.com/subscriptions/sub/" *
        "providers/Microsoft.Compute/locations/eastus/usages?api-version=2019-07-01"

    @test AzManagers.azure_compute_usages_url("sub", "eastus") == expected_url
end

@testset "unit: Azure API helpers" begin
    response = HTTP.Response(
        429,
        ["x-ms-ratelimit-remaining-resource" => "quota"],
        "")
    error = HTTP.StatusError(429, "GET", "/target", response)

    @test AzManagers.isretryable(error)
    @test AzManagers.status(error) == 429
    @test AzManagers.remaining_resource(response) == "quota"
    @test !AzManagers.isretryable(ArgumentError("no retry"))
end

@testset "unit: mocked Azure pagination" begin
    nextlink_responses = Dict(
        "page-2" => HTTP.Response(
            200,
            JSON.json(Dict("value" => [2], "nextLink" => "page-3"))),
        "page-3" => HTTP.Response(
            200,
            JSON.json(Dict("value" => [3]))))
    nextlink_requests = String[]
    nextlink_request = function (url)
        push!(nextlink_requests, url)
        nextlink_responses[url]
    end

    values, last_response = AzManagers.collect_nextlink_pages!(
        nextlink_request,
        [1],
        "page-2")

    @test values == [1, 2, 3]
    @test nextlink_requests == ["page-2", "page-3"]
    @test JSON.parse(String(last_response.body))["value"] == [3]

    resourcegraph_responses = [
        HTTP.Response(
            200,
            JSON.json(Dict("data" => ["vm-1"], "\$skipToken" => "next"))),
        HTTP.Response(
            200,
            JSON.json(Dict("data" => ["vm-2"])))]
    resourcegraph_bodies = Dict[]
    resourcegraph_request = function (body)
        push!(resourcegraph_bodies, copy(body))
        popfirst!(resourcegraph_responses)
    end

    data, _ = AzManagers.collect_resourcegraph_pages(
        resourcegraph_request,
        Dict("query" => "Resources"))

    @test data == ["vm-1", "vm-2"]
    @test !haskey(resourcegraph_bodies[1], "\$skipToken")
    @test resourcegraph_bodies[2]["\$skipToken"] == "next"
end

@testset "unit: automatic worker placement" begin
    topology = synthetic_topology()

    one_worker = plan_worker_placements(topology, 1)
    @test length(one_worker) == 1
    @test one_worker[1].cpu_set == collect(0:15)
    @test one_worker[1].julia_threads == 16

    socket_workers = plan_worker_placements(topology, 2)
    @test getfield.(socket_workers, :socket) == [0, 1]
    @test getfield.(socket_workers, :cpu_set) == [collect(0:7), collect(8:15)]

    numa_workers = plan_worker_placements(topology, 4)
    @test getfield.(numa_workers, :numa_node) == [0, 1, 2, 3]
    @test all(worker -> worker.julia_threads == 4, numa_workers)

    split_workers = plan_worker_placements(topology, 8)
    @test length(split_workers) == 8
    @test getfield.(split_workers, :cpu_set)[1] == [0, 1]
    @test getfield.(split_workers, :cpu_set)[end] == [14, 15]

    @test_logs (:warn, "CPU cores do not divide evenly across workers") begin
        uneven_workers = plan_worker_placements(uneven_topology(), 4)
        @test length.(getfield.(uneven_workers, :cpu_set)) == [3, 3, 2, 2]
    end

    @test_throws ArgumentError plan_worker_placements(topology, 17)
end

@testset "unit: lscpu topology parsing" begin
    topology = AzManagers.parse_lscpu_topology(synthetic_lscpu())

    @test topology.physical_cores == 4
    @test topology.logical_cpus == collect(0:3)
    @test topology.hyperthreading
    @test getfield.(topology.numa_nodes, :id) == [0, 1]
    @test getfield.(topology.numa_nodes, :cpu_set) ==
        [collect(0:1), collect(2:3)]

    json_topology = AzManagers.parse_lscpu_json_topology(synthetic_lscpu_json())
    @test json_topology.physical_cores == 8
    @test json_topology.hyperthreading
    @test getfield.(json_topology.sockets, :cpu_set) ==
        [collect(0:3), collect(4:7)]
    @test getfield.(json_topology.numa_nodes, :cpu_set) ==
        [collect(0:3), collect(4:7)]
end

@testset "unit: numactl --hardware parsing" begin
    sample = """
    available: 2 nodes (0-1)
    node 0 cpus: 0 1 2 3
    node 0 size: 16374 MB
    node 0 free: 256 MB
    node 1 cpus: 4 5 6 7
    node 1 size: 16384 MB
    node 1 free: 1024 MB
    node distances:
    node   0   1
      0:  10  21
      1:  21  10
    """
    topology = AzManagers.parse_numactl_topology(sample)
    @test topology.physical_cores == 8
    @test topology.logical_cpus == collect(0:7)
    @test getfield.(topology.numa_nodes, :id) == [0, 1]
    @test getfield.(topology.numa_nodes, :cpu_set) ==
        [collect(0:3), collect(4:7)]
    @test getfield.(topology.sockets, :id) == [0, 1]
    @test !topology.hyperthreading

    @test_throws ArgumentError AzManagers.parse_numactl_topology("available: 0\n")
end

@testset "unit: Hwloc topology probe" begin
    topology = AzManagers.hwloc_topology()
    @test topology isa AzManagers.MachineTopology
    @test topology.physical_cores >= 1
    @test !isempty(topology.logical_cpus)
    @test !isempty(topology.sockets)
end

@testset "unit: ppi option validation" begin
    @test isnothing(AzManagers.validate_ppi_options(4, 0))
    @test isnothing(AzManagers.validate_ppi_options(1, 2))
    @test isnothing(AzManagers.validate_ppi_options(2, 1))
    @test_throws ArgumentError AzManagers.validate_ppi_options(0, 0)
    @test_throws ArgumentError AzManagers.validate_ppi_options(1, -1)
end

@testset "unit: MPI rank placement planning" begin
    topology = synthetic_topology()
    worker_placements = plan_worker_placements(topology, 2)

    even_ranks = AzManagers.plan_mpi_rank_placements(worker_placements[1], 4)
    @test length(even_ranks) == 4
    @test getfield.(even_ranks, :rank_index) == 0:3
    @test getfield.(even_ranks, :cpu_set) ==
        [[0, 1], [2, 3], [4, 5], [6, 7]]
    @test all(rank -> rank.worker_localid == 1, even_ranks)
    @test all(rank -> rank.omp_threads == 2, even_ranks)

    second_worker_ranks = AzManagers.plan_mpi_rank_placements(
        worker_placements[2], 2)
    @test getfield.(second_worker_ranks, :cpu_set) ==
        [collect(8:11), collect(12:15)]

    @test_logs (:warn,) begin
        uneven = AzManagers.plan_mpi_rank_placements(worker_placements[1], 3)
        @test length.(getfield.(uneven, :cpu_set)) == [3, 3, 2]
    end

    @test_throws ArgumentError AzManagers.plan_mpi_rank_placements(worker_placements[1], 0)
    @test_throws ArgumentError AzManagers.plan_mpi_rank_placements(worker_placements[1], 99)

    nested = AzManagers.plan_mpi_placements(topology, 2, 2)
    @test length(nested) == 2
    @test all(((wp, ranks),) -> length(ranks) == 2, nested)
    @test nested[1][1].cpu_set == collect(0:7)
    @test nested[2][2][2].cpu_set == collect(12:15)
    @test nested[2][2][1].worker_localid == 2
end

@testset "unit: nested MPI launch block" begin
    block = AzManagers.nested_mpi_launch_block(
        "julia",
        "-t 4,1",
        2,
        4,
        "--report-bindings",
        "abc",
        "10.0.0.1",
        9009,
        "")

    @test contains(block, "AZM_WORKER_CPU_SETS=(")
    @test contains(block, "AzManagers.plan_worker_placements(topology, 2)")
    @test contains(block, "mpirun -n 4 --cpu-set ")
    @test contains(block, "--bind-to cpu-list:ordered --report-bindings")
    @test contains(block, "azure_worker_mpi(\"abc\", \"10.0.0.1\", 9009, 2,")
    @test contains(block, "AZM_PIDS+=(\$!)")
    @test contains(block, "for pid in \"\${AZM_PIDS[@]}\"")
end

@testset "unit: mpirun command rendering" begin
    topology = synthetic_topology()
    placement, ranks = AzManagers.plan_mpi_placements(topology, 2, 2)[1]

    cmd = AzManagers.mpirun_command(
        placement,
        ranks,
        "julia",
        "-e 'using AzManagers; AzManagers.azure_worker_mpi(...)'";
        extra_flags = "--report-bindings")

    @test startswith(cmd, "mpirun -n 2 --cpu-set 0-7 --bind-to cpu-list:ordered")
    @test contains(cmd, "--report-bindings")
    @test endswith(cmd, "azure_worker_mpi(...)'")

    @test_throws ArgumentError AzManagers.mpirun_command(
        placement,
        AzManagers.MpiRankPlacement[],
        "julia",
        "")
end

@testset "unit: placement launch details" begin
    topology = synthetic_topology()
    placement = plan_worker_placements(topology, 4)[2]

    @test AzManagers.cpu_set_string([0, 1, 2, 4, 5, 8]) == "0-2,4-5,8"
    @test AzManagers.julia_threads_string(placement) == "4,1"
    @test AzManagers.numactl_arguments(placement) ==
        ["--physcpubind=4-7", "--membind=1"]

    env = AzManagers.placement_environment(placement)
    @test env["JULIA_NUM_THREADS"] == "4,1"
    @test env["OMP_NUM_THREADS"] == "4"
    @test env["OMP_PROC_BIND"] == "close"
    @test env["OMP_PLACES"] == "cores"

    metadata = AzManagers.worker_placement_metadata(topology, placement, 4)
    @test metadata["ppi"] == 4
    @test metadata["cpu_set"] == "4-7"
    @test metadata["pinning_backend"] == "numactl"
    thread_metadata = AzManagers.worker_placement_metadata(
        topology,
        placement,
        4;
        pinning_backend = "ThreadPinning")
    @test thread_metadata["pinning_backend"] == "ThreadPinning"
    @test AzManagers.placement_userdata(metadata) == metadata
    @test AzManagers.numactl_prefix(placement) ==
        "numactl --physcpubind=4-7 --membind=1 "
    @test AzManagers.worker_launch_command(
        "julia",
        ``,
        placement;
        use_numactl = true) ==
            `numactl --physcpubind=4-7 --membind=1 julia -t 4,1 --worker`
end

@testset "unit: shell environment rendering" begin
    rendered = AzManagers.build_envstring(
        Dict("FOO" => "bar baz", "QUOTE" => "a'b"))

    @test contains(rendered, "export FOO='bar baz'")
    @test contains(rendered, "export QUOTE='a'\"'\"'b'")
    @test_throws ArgumentError AzManagers.build_envstring(Dict("1BAD" => "value"))

    placement = plan_worker_placements(synthetic_topology(), 4)[1]
    placement_exports = AzManagers.placement_export_string(placement)
    @test contains(placement_exports, "export JULIA_NUM_THREADS='4,1'")
    @test contains(placement_exports, "export OMP_PROC_BIND='close'")
end

@testset "unit: environment compression round trip" begin
    mktempdir() do environment_dir
        write(joinpath(environment_dir, "Project.toml"), "[deps]\n")
        write(joinpath(environment_dir, "Manifest.toml"), "# manifest\n")
        write(joinpath(environment_dir, "LocalPreferences.toml"), "flag = true\n")

        project, manifest, preferences =
            AzManagers.compress_environment(environment_dir)

        mktempdir() do depot_dir
            old_depot_path = copy(DEPOT_PATH)
            try
                empty!(DEPOT_PATH)
                push!(DEPOT_PATH, depot_dir)

                AzManagers.decompress_environment(
                    project,
                    manifest,
                    preferences,
                    "azmanagers-unit")

                remote_dir = joinpath(Pkg.envdir(), "azmanagers-unit")
                @test read(joinpath(remote_dir, "Project.toml"), String) ==
                    "[deps]\n"
                @test read(joinpath(remote_dir, "Manifest.toml"), String) ==
                    "# manifest\n"
                @test read(
                    joinpath(remote_dir, "LocalPreferences.toml"),
                    String) == "flag = true\n"
            finally
                empty!(DEPOT_PATH)
                append!(DEPOT_PATH, old_depot_path)
            end
        end
    end
end

@testset "unit: VM template resource IDs" begin
    template = AzManagers.build_vmtemplate(
        "vm-name";
        subscriptionid = "sub",
        admin_username = "user",
        location = "eastus",
        resourcegroup = "rg",
        resourcegroup_vnet = "network-rg",
        imagegallery = "gallery",
        imagename = "image",
        vmsize = "Standard_D2s_v5")

    network_interfaces =
        template["value"]["properties"]["networkProfile"]["networkInterfaces"]
    nic_id = network_interfaces[1]["id"]

    @test startswith(nic_id, "/subscriptions/sub/")
    @test contains(nic_id, "/resourceGroups/network-rg/")
end

@testset "unit: detached wait error response" begin
    try
        AzManagers.DETACHED_JOBS["unit"] = Dict{String,Any}(
            "process" => "not-a-process",
            "codefile" => "unit-code.jl",
            "code" => "error(\"boom\")")

        request = HTTP.Request("POST", "/cofii/detached/job/unit/wait")
        response = AzManagers.detachedwait(request)
        body = JSON.parse(String(response.body))

        @test response.status == 400
        @test haskey(body, "error")
        @test contains(body["error"], "Code listing")
    finally
        delete!(AzManagers.DETACHED_JOBS, "unit")
    end
end

@testset "unit: manifest state holder" begin
    manifest = AzManagers.AzManagersManifest()
    @test manifest["resourcegroup"] == ""
    manifest["resourcegroup"] = "rg-1"
    manifest["subscriptionid"] = "sub-1"
    @test manifest.resourcegroup == "rg-1"
    @test manifest["subscriptionid"] == "sub-1"
    @test "ssh_user" in keys(manifest)
    @test haskey(manifest, "subscriptionid")
    @test_throws KeyError manifest["does-not-exist"]
    @test_throws KeyError manifest["bogus"] = "x"
end

@testset "unit: variable bundle state" begin
    @test AzManagers.VARIABLE_BUNDLE === AzManagers.VARIABLE_BUNDLE_STATE.bundle
    empty!(AzManagers.VARIABLE_BUNDLE_STATE.bundle)
    AzManagers.variablebundle!(; alpha=1, beta="two")
    @test AzManagers.variablebundle(:alpha) == 1
    @test AzManagers.variablebundle(:beta) == "two"
    AzManagers.variablebundle!(Dict("gamma" => 3))
    @test AzManagers.variablebundle(:gamma) == 3
    empty!(AzManagers.VARIABLE_BUNDLE_STATE.bundle)
end

@testset "unit: pin_julia_threads" begin
    @test AzManagers.pin_julia_threads(Int[]) == false
    @test AzManagers.pin_julia_threads([0]) isa Bool
end
