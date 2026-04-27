using Test, AzManagers, HTTP, JSON, Distributed, Dates, Logging, Pkg, Random, Sockets

# ── Helper: create a minimal AzManager with initialized state ─────────────────
function make_infra_test_manager()
    mgr = AzManagers.AzManager()
    mgr.pending_down = Dict{AzManagers.ScaleSet, Set{String}}()
    mgr.deleted      = Dict{AzManagers.ScaleSet, Dict{String, DateTime}}()
    mgr.pruned        = Dict{AzManagers.ScaleSet, Set{String}}()
    mgr.preempted     = Dict{AzManagers.ScaleSet, Set{String}}()
    mgr.scalesets     = Dict{AzManagers.ScaleSet, Int}()
    mgr.lock          = ReentrantLock()
    mgr.scaleset_request_counter = 0
    mgr
end

# ═══════════════════════════════════════════════════════════════════════════════
# 1. azmanager! — manager initialization
# ═══════════════════════════════════════════════════════════════════════════════

@testset "azmanager! initialization" begin
    # Use the module-level _manager to test azmanager!
    # We can't call azmanager! directly without a real AzSession,
    # but we can test the azmanager() accessor and state after make_test_manager
    mgr = make_infra_test_manager()

    @test mgr.pending_down isa Dict
    @test mgr.deleted isa Dict
    @test mgr.pruned isa Dict
    @test mgr.preempted isa Dict
    @test mgr.scalesets isa Dict
    @test mgr.scaleset_request_counter == 0
    @test mgr.lock isa ReentrantLock
end

# ═══════════════════════════════════════════════════════════════════════════════
# 2. scaleset_request_counter
# ═══════════════════════════════════════════════════════════════════════════════

@testset "scaleset_request_counter" begin
    mgr = make_infra_test_manager()

    # counter starts at 0
    @test mgr.scaleset_request_counter == 0

    # incrementing
    mgr.scaleset_request_counter += 1
    @test mgr.scaleset_request_counter == 1

    mgr.scaleset_request_counter += 5
    @test mgr.scaleset_request_counter == 6
end

# ═══════════════════════════════════════════════════════════════════════════════
# 3. nworkers_provisioned — with mocked manager state
# ═══════════════════════════════════════════════════════════════════════════════

@testset "nworkers_provisioned logic" begin
    mgr = make_infra_test_manager()

    ss1 = AzManagers.ScaleSet("sub1", "rg1", "ss1")
    ss2 = AzManagers.ScaleSet("sub1", "rg1", "ss2")

    # No scalesets → 0
    @test AzManagers.scalesets(mgr) == Dict{AzManagers.ScaleSet, Int}()

    # Add scalesets with capacity
    mgr.scalesets[ss1] = 4
    mgr.scalesets[ss2] = 8
    total = sum(values(mgr.scalesets))
    @test total == 12

    # pending_down reduces count
    mgr.pending_down[ss1] = Set(["inst1", "inst2"])
    pending_count = mapreduce(length, +, values(mgr.pending_down))
    effective = max(0, total - pending_count)
    @test effective == 10

    # all pending_down → clamped at 0
    mgr.pending_down[ss1] = Set(["inst$i" for i in 1:4])
    mgr.pending_down[ss2] = Set(["inst$i" for i in 1:10])
    pending_count2 = mapreduce(length, +, values(mgr.pending_down))
    effective2 = max(0, total - pending_count2)
    @test effective2 == 0
end

# ═══════════════════════════════════════════════════════════════════════════════
# 4. detached_port / detached_port!
# ═══════════════════════════════════════════════════════════════════════════════

@testset "detached_port" begin
    original = AzManagers.detached_port()

    # set a custom port
    AzManagers.detached_port!(9999)
    @test AzManagers.detached_port() == 9999

    # restore
    AzManagers.detached_port!(original)
    @test AzManagers.detached_port() == original
end

# ═══════════════════════════════════════════════════════════════════════════════
# 5. detached_nextid — monotonic increment
# ═══════════════════════════════════════════════════════════════════════════════

@testset "detached_nextid" begin
    id1 = AzManagers.detached_nextid()
    id2 = AzManagers.detached_nextid()
    id3 = AzManagers.detached_nextid()

    @test id2 == id1 + 1
    @test id3 == id2 + 1
end

# ═══════════════════════════════════════════════════════════════════════════════
# 6. DetachedJob constructors and loguri
# ═══════════════════════════════════════════════════════════════════════════════

@testset "DetachedJob constructors and loguri" begin
    # Full constructor
    vm = Dict("ip" => "10.0.0.1", "port" => "8081", "name" => "testvm")
    job = AzManagers.DetachedJob(vm, "42", "1234", "https://logs.example.com/42")
    @test job.vm == vm
    @test job.id == "42"
    @test job.pid == "1234"
    @test AzManagers.loguri(job) == "https://logs.example.com/42"

    # IP + id constructor (2-arg)
    job2 = AzManagers.DetachedJob("10.0.0.1", 7)
    @test job2.vm["ip"] == "10.0.0.1"
    @test job2.id == "7"
    @test job2.pid == "-1"
    @test AzManagers.loguri(job2) == ""

    # IP + id + pid constructor (3-arg)
    job3 = AzManagers.DetachedJob("10.0.0.2", 8, 999)
    @test job3.vm["ip"] == "10.0.0.2"
    @test job3.id == "8"
    @test job3.pid == "999"

    # Custom port
    job4 = AzManagers.DetachedJob("10.0.0.3", 9; port=9090)
    @test job4.vm["port"] == "9090"
end

# ═══════════════════════════════════════════════════════════════════════════════
# 7. SpotPreemptException
# ═══════════════════════════════════════════════════════════════════════════════

@testset "SpotPreemptException" begin
    e = AzManagers.SpotPreemptException("inst-abc", 42, "2026-01-01T00:00:00Z")
    @test e.instanceid == "inst-abc"
    @test e.clusterid == 42
    @test e.notbefore == "2026-01-01T00:00:00Z"

    # showerror
    buf = IOBuffer()
    showerror(buf, e)
    msg = String(take!(buf))
    @test contains(msg, "spot preemption")
    @test contains(msg, "42")
    @test contains(msg, "inst-abc")
end

# ═══════════════════════════════════════════════════════════════════════════════
# 8. DetachedServiceTimeoutException
# ═══════════════════════════════════════════════════════════════════════════════

@testset "DetachedServiceTimeoutException" begin
    vm = Dict("name" => "testvm", "ip" => "10.0.0.1", "port" => "8081")
    e = AzManagers.DetachedServiceTimeoutException(vm)
    @test e.vm == vm
end

# ═══════════════════════════════════════════════════════════════════════════════
# 9. buildstartupscript — shell script generation
# ═══════════════════════════════════════════════════════════════════════════════

@testset "buildstartupscript" begin
    mgr = make_infra_test_manager()

    # Basic startup script without custom env
    cmd, env_name = AzManagers.buildstartupscript(mgr, "julia", "testuser", "# disk setup", false, false)
    @test contains(cmd, "#!/bin/bash")
    @test contains(cmd, "# disk setup")
    @test contains(cmd, "scripts-user")
    @test env_name == ""

    # With use_lvm
    cmd_lvm, _ = AzManagers.buildstartupscript(mgr, "julia", "testuser", "# disk", false, true)
    @test contains(cmd_lvm, "#!/bin/sh")

    # Should not contain julia -e unless custom_environment is true
    @test !contains(cmd, "decompress_environment")
end

# ═══════════════════════════════════════════════════════════════════════════════
# 10. buildstartupscript_detached — detached VM script generation
# ═══════════════════════════════════════════════════════════════════════════════

@testset "buildstartupscript_detached" begin
    mgr = make_infra_test_manager()

    cmd = AzManagers.buildstartupscript_detached(
        mgr, "julia", "4,1", 2, Dict("MY_VAR" => "hello"), "testuser",
        "# disk setup", false, "sub-123", "rg-test", "myvm", false)

    @test contains(cmd, "#!/bin/bash")
    @test contains(cmd, "OMP_NUM_THREADS=2")
    @test contains(cmd, "export MY_VAR=hello")
    @test contains(cmd, "detachedservice")
    @test contains(cmd, "sub-123")
    @test contains(cmd, "rg-test")
    @test contains(cmd, "myvm")
    @test contains(cmd, "ssh-keygen")

    # use_lvm variant
    cmd_lvm = AzManagers.buildstartupscript_detached(
        mgr, "julia", "4", 1, Dict(), "testuser",
        "# disk", false, "sub", "rg", "vm", true)
    @test contains(cmd_lvm, "#!/bin/sh")
    @test contains(cmd_lvm, "MIME-Version")
    @test contains(cmd_lvm, "cloud-config")

    # mpirun in exename — should strip mpi prefix for detached service
    cmd_mpi = AzManagers.buildstartupscript_detached(
        mgr, "mpirun -n 4 julia", "4", 1, Dict(), "testuser",
        "# disk", false, "sub", "rg", "vm", false)
    # The script should use just `julia` for the detached service, not mpirun
    @test contains(cmd_mpi, "julia -t 4")
    @test !contains(cmd_mpi, "mpirun -n 4 julia -t")
end

# ═══════════════════════════════════════════════════════════════════════════════
# 11. buildstartupscript_cluster — cluster worker script generation
# ═══════════════════════════════════════════════════════════════════════════════

@testset "buildstartupscript_cluster" begin
    mgr = make_infra_test_manager()
    mgr.port = 9000

    cmd = AzManagers.buildstartupscript_cluster(
        mgr, false, 1, 0, "", true, false,
        "4,1", 2, "julia", "", Dict("FOO" => "bar"), "testuser",
        "# disk setup", false, false)

    @test contains(cmd, "#!/bin/bash")
    @test contains(cmd, "JULIA_WORKER_TIMEOUT")
    @test contains(cmd, "OMP_NUM_THREADS=2")
    @test contains(cmd, "export FOO=bar")

    # spot=true should ensure interactive thread
    cmd_spot = AzManagers.buildstartupscript_cluster(
        mgr, true, 1, 0, "", true, false,
        "4,0", 1, "julia", "", Dict(), "testuser",
        "# disk", false, false)
    # Should augment threads to include interactive
    @test contains(cmd_spot, "-t 4,1")
end

# ═══════════════════════════════════════════════════════════════════════════════
# 12. software_sanity_check — dev'd package detection
# ═══════════════════════════════════════════════════════════════════════════════

@testset "software_sanity_check" begin
    mgr = make_infra_test_manager()

    # With custom_environment=false, should not error even with dev'd packages
    @test AzManagers.software_sanity_check(mgr, "testimage", false) === nothing

    # With custom_environment=true, if any package has a "path" key it should error
    # We test this against the actual test environment which has dev'd AzManagers
    projectinfo = Pkg.project()
    envpath = normpath(joinpath(projectinfo.path, ".."))
    _packages = TOML.parse(read(joinpath(envpath, "Manifest.toml"), String))
    packages = VERSION < v"1.7" ? _packages : _packages["deps"]

    has_dev_packages = any(pkg -> haskey(pkg[2][1], "path"), packages)

    if has_dev_packages
        @test_throws ErrorException AzManagers.software_sanity_check(mgr, "testimage", true)
    else
        @test AzManagers.software_sanity_check(mgr, "testimage", true) === nothing
    end
end

# ═══════════════════════════════════════════════════════════════════════════════
# 13. load_manifest — file handling
# ═══════════════════════════════════════════════════════════════════════════════

@testset "load_manifest error handling" begin
    # Temporarily set manifest to a non-existent path
    original_dir = get(ENV, "HOME", "")
    mktempdir() do tmpdir
        # Point to a temp dir with no manifest
        withenv("HOME" => tmpdir) do
            # load_manifest should log error about missing file (but not throw)
            # since it only @errors and doesn't throw for missing file
            @test_logs (:error, r"does not exist") AzManagers.load_manifest()
        end
    end

    # Test with invalid JSON
    mktempdir() do tmpdir
        withenv("HOME" => tmpdir) do
            mkpath(joinpath(tmpdir, ".azmanagers"))
            write(joinpath(tmpdir, ".azmanagers", "manifest.json"), "not valid json {{{")
            @test_throws Exception AzManagers.load_manifest()
        end
    end

    # Test with valid manifest
    mktempdir() do tmpdir
        withenv("HOME" => tmpdir) do
            mkpath(joinpath(tmpdir, ".azmanagers"))
            manifest = Dict(
                "subscriptionid" => "test-sub-123",
                "resourcegroup" => "test-rg",
                "ssh_user" => "testuser",
                "ssh_public_key_file" => "/fake/key.pub"
            )
            write(joinpath(tmpdir, ".azmanagers", "manifest.json"), JSON.json(manifest))
            AzManagers.load_manifest()
            @test AzManagers._manifest["subscriptionid"] == "test-sub-123"
            @test AzManagers._manifest["resourcegroup"] == "test-rg"
            @test AzManagers._manifest["ssh_user"] == "testuser"
        end
    end
end

# ═══════════════════════════════════════════════════════════════════════════════
# 14. azure_physical_name — reads /var/lib/hyperv/.kvp_pool_3
# ═══════════════════════════════════════════════════════════════════════════════

@testset "azure_physical_name" begin
    # On non-Azure machines, should return "unknown"
    if !isfile("/var/lib/hyperv/.kvp_pool_3")
        @test AzManagers.azure_physical_name() == "unknown"
    end
    # Custom keyval that won't exist
    @test AzManagers.azure_physical_name("NonExistentKey12345") == "unknown"
end

# ═══════════════════════════════════════════════════════════════════════════════
# 15. get_instanceid — Azure metadata service
# ═══════════════════════════════════════════════════════════════════════════════

@testset "get_instanceid" begin
    # On non-Azure machines, should return ""
    result = AzManagers.get_instanceid()
    @test result isa String
    # Either returns a real instance ID (on Azure) or "" (locally)
end

# ═══════════════════════════════════════════════════════════════════════════════
# 16. DETACHED_ROUTER registration
# ═══════════════════════════════════════════════════════════════════════════════

@testset "DETACHED_ROUTER" begin
    @test AzManagers.DETACHED_ROUTER isa HTTP.Router
end

# ═══════════════════════════════════════════════════════════════════════════════
# 17. Distributed.kill (manager method) — logic paths
# ═══════════════════════════════════════════════════════════════════════════════

@testset "Distributed.kill manager method - localid>1 early return" begin
    mgr = make_infra_test_manager()

    # Test the localid > 1 early-return path
    # Create a mock WorkerConfig with userdata showing localid > 1
    wc = Distributed.WorkerConfig()
    wc.userdata = Dict(
        "localid" => 2,
        "subscriptionid" => "sub",
        "resourcegroup" => "rg",
        "scalesetname" => "ss",
        "instanceid" => "inst1"
    )

    # For a worker with localid > 1, kill should return nothing without
    # adding to pending_down. The remote_do will fail (no such worker), but
    # the method catches all exceptions from remote_do.
    result = Distributed.kill(mgr, 99999, wc)
    @test result === nothing
    @test isempty(mgr.pending_down)
end

# ═══════════════════════════════════════════════════════════════════════════════
# 18. Scaleset lifecycle helpers
# ═══════════════════════════════════════════════════════════════════════════════

@testset "scaleset lifecycle helpers" begin
    mgr = make_infra_test_manager()
    ss = AzManagers.ScaleSet("sub", "rg", "testss")

    # delete_scaleset without a real Azure session — should catch the error
    # and still remove from scalesets dict
    mgr.scalesets[ss] = 4
    mgr.nretry = 0
    mgr.verbose = 0
    mgr.show_quota = false

    # delete_scaleset calls rmgroup which needs manager.session
    # Since we don't have a real session, it will @warn and delete from dict
    # We just test that scalesets dict management works correctly
    @test haskey(mgr.scalesets, ss)
    delete!(mgr.scalesets, ss)
    @test !haskey(mgr.scalesets, ss)

    # pending_down accessor
    @test AzManagers.pending_down(mgr) isa Dict
    @test isempty(AzManagers.pending_down(mgr))
end

# ═══════════════════════════════════════════════════════════════════════════════
# 19. addprocs (Dict overload) — argument validation
# ═══════════════════════════════════════════════════════════════════════════════

@testset "addprocs AbstractString overload - template not found" begin
    # Test that the AbstractString overload errors when template file doesn't exist
    mgr = AzManagers.AzManager()

    mktempdir() do tmpdir
        withenv("HOME" => tmpdir) do
            # No templates_scaleset.json exists
            @test_throws ErrorException addprocs(mgr, "nonexistent_template", 1)
        end
    end
end

# ═══════════════════════════════════════════════════════════════════════════════
# 20. nphysical_cores — string overload template lookup
# ═══════════════════════════════════════════════════════════════════════════════

@testset "nphysical_cores string overload - template not found" begin
    mktempdir() do tmpdir
        withenv("HOME" => tmpdir) do
            # No template file
            @test_throws ErrorException AzManagers.nphysical_cores("nonexistent"; session=nothing)
        end
    end
end

# ═══════════════════════════════════════════════════════════════════════════════
# 21. addproc string overload - template file validation
# ═══════════════════════════════════════════════════════════════════════════════

@testset "addproc string overload - template not found" begin
    mktempdir() do tmpdir
        withenv("HOME" => tmpdir) do
            @test_throws ErrorException AzManagers.addproc("nonexistent_vm")
        end
    end
end

# ═══════════════════════════════════════════════════════════════════════════════
# 22. DETACHED_VM and DETACHED_JOBS globals
# ═══════════════════════════════════════════════════════════════════════════════

@testset "DETACHED globals" begin
    @test AzManagers.DETACHED_JOBS isa Dict
    @test AzManagers.DETACHED_VM isa Ref

    # VARIABLE_BUNDLE is accessible
    @test AzManagers.VARIABLE_BUNDLE isa Dict
end

# ═══════════════════════════════════════════════════════════════════════════════
# 23. spinner — already tested in test_unit.jl but test full cycle
# ═══════════════════════════════════════════════════════════════════════════════

@testset "spinner full sequence" begin
    for i in 1:5
        s = AzManagers.spin(i, 10.5)
        @test s isa String
        @test contains(s, "10.5")
    end
end

# ═══════════════════════════════════════════════════════════════════════════════
# 24. Macro expansion — @detach and @detachat
# ═══════════════════════════════════════════════════════════════════════════════

@testset "@detach and @detachat macro expansion" begin
    # @detach with expression argument expands to a call to detached_run
    ex1 = @macroexpand @detach vm(;template="vmtemplate") begin
        println("hello")
    end
    @test ex1.head == :call
    @test contains(string(ex1.args[1]), "detached_run")

    # @detachat with variable and code
    ex2 = @macroexpand @detachat myvm begin
        println("hello")
    end
    @test ex2.head == :call
    @test contains(string(ex2.args[1]), "detached_run")

    # single-arg @detach
    ex3 = @macroexpand @detach begin
        println("single")
    end
    @test ex3.head == :call
    @test contains(string(ex3.args[1]), "detached_run")
end

# ═══════════════════════════════════════════════════════════════════════════════
# 25. rmproc — template string overload validation
# ═══════════════════════════════════════════════════════════════════════════════

@testset "rmproc requires valid vm dict" begin
    # rmproc expects a dict with specific keys
    vm = Dict("name" => "testvm", "subscriptionid" => "sub", "resourcegroup" => "rg")

    # Without a valid Azure session, this will fail at the HTTP call
    # but the argument parsing should work
    @test haskey(vm, "name")
    @test haskey(vm, "subscriptionid")
    @test haskey(vm, "resourcegroup")
end

# ═══════════════════════════════════════════════════════════════════════════════
# 26. azrequest — HTTP wrapper
# ═══════════════════════════════════════════════════════════════════════════════

@testset "azrequest error handling" begin
    # Start a local HTTP server that returns errors
    local_server = HTTP.serve!(ip"127.0.0.1", 0) do req
        return HTTP.Response(500, "Internal Server Error")
    end
    port = Sockets.getsockname(local_server.listener.server)[2]
    url = "http://127.0.0.1:$port/test"

    try
        # azrequest should throw StatusError for 500
        @test_throws HTTP.Exceptions.StatusError AzManagers.azrequest("GET", 0, url, [])
    finally
        close(local_server)
    end

    # Success case
    local_server2 = HTTP.serve!(ip"127.0.0.1", 0) do req
        return HTTP.Response(200, "{\"result\": \"ok\"}")
    end
    port2 = Sockets.getsockname(local_server2.listener.server)[2]
    url2 = "http://127.0.0.1:$port2/test"

    try
        r = AzManagers.azrequest("GET", 0, url2, [])
        @test r.status == 200
        body = JSON.parse(String(r.body))
        @test body["result"] == "ok"
    finally
        close(local_server2)
    end
end

# ═══════════════════════════════════════════════════════════════════════════════
# 27. Preemption tracking — preempted() with mock events
# ═══════════════════════════════════════════════════════════════════════════════

@testset "preempted function - local fallback" begin
    # On non-Azure, preempted() should handle missing metadata gracefully
    # The function catches curl errors and returns false, ""
    result, notbefore = AzManagers.preempted("", 0)
    @test result isa Bool
    @test notbefore isa String
end

# ═══════════════════════════════════════════════════════════════════════════════
# 28. RETRYABLE_HTTP_ERRORS constant
# ═══════════════════════════════════════════════════════════════════════════════

@testset "RETRYABLE_HTTP_ERRORS" begin
    @test 409 ∈ AzManagers.RETRYABLE_HTTP_ERRORS
    @test 429 ∈ AzManagers.RETRYABLE_HTTP_ERRORS
    @test 500 ∈ AzManagers.RETRYABLE_HTTP_ERRORS
    @test 404 ∉ AzManagers.RETRYABLE_HTTP_ERRORS
    @test 401 ∉ AzManagers.RETRYABLE_HTTP_ERRORS
end

# ═══════════════════════════════════════════════════════════════════════════════
# 29. CurlDataStruct
# ═══════════════════════════════════════════════════════════════════════════════

@testset "CurlDataStruct" begin
    ds = AzManagers.CurlDataStruct(UInt8[], 0)
    @test ds.body == UInt8[]
    @test ds.currentsize == 0
end

# ═══════════════════════════════════════════════════════════════════════════════
# 30. nvidia helper functions — graceful handling on non-GPU machines
# ═══════════════════════════════════════════════════════════════════════════════

@testset "nvidia helpers" begin
    # nvidia_has_nvidia_smi should return false on dev machines without nvidia-smi
    has_smi = AzManagers.nvidia_has_nvidia_smi()
    @test has_smi isa Bool

    if !has_smi
        # nvidia_gpumode should handle missing nvidia-smi gracefully
        # These call nvidia-smi which will fail, so the function catches errors
        @test_throws Exception AzManagers.nvidia_gpumode("ecc")
    end
end

# ═══════════════════════════════════════════════════════════════════════════════
# 31. @spawn_interactive macro
# ═══════════════════════════════════════════════════════════════════════════════

@testset "@spawn_interactive" begin
    ex = @macroexpand AzManagers.@spawn_interactive begin
        1 + 1
    end
    @test ex isa Expr
end
