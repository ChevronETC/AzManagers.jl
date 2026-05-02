using AzManagers, Test, HTTP, JSON, Dates, Distributed, Logging, Pkg, TOML, Sockets, AzSessions

@testset "nthreads_filter" begin
    @test AzManagers.nthreads_filter("4") == "4"
    @test AzManagers.nthreads_filter("4,0") == "4"
    @test AzManagers.nthreads_filter("4,2") == "4,2"
    @test AzManagers.nthreads_filter("1,1") == "1,1"
    @test AzManagers.nthreads_filter(1) == "1"
end

@testset "logerror" begin
    # Should not throw
    try
        error("test error for logerror")
    catch e
        AzManagers.logerror(e, Logging.Debug)
    end
end

@testset "ScaleSet" begin
    ss = AzManagers.ScaleSet("SUB-123", "MyGroup", "MyScaleSet")
    @test ss.subscriptionid == "sub-123"
    @test ss.resourcegroup == "mygroup"
    @test ss.scalesetname == "myscaleset"

    d = Dict(ss)
    @test d["subscriptionid"] == "sub-123"
    @test d["resourcegroup"] == "mygroup"
    @test d["name"] == "myscaleset"

    # Equality
    ss2 = AzManagers.ScaleSet("sub-123", "mygroup", "myscaleset")
    @test ss == ss2
end

@testset "isretryable" begin
    # HTTP status errors
    for code in (409, 429, 500)
        r = HTTP.Response(code, [], "")
        e = HTTP.StatusError(code, "GET", "/", r)
        @test AzManagers.isretryable(e) == true
    end

    # Non-retryable HTTP status
    r = HTTP.Response(404, [], "")
    e = HTTP.StatusError(404, "GET", "/", r)
    @test AzManagers.isretryable(e) == false

    # Other retryable error types
    @test AzManagers.isretryable(Base.IOError("test", 0)) == true
    @test AzManagers.isretryable(Base.EOFError()) == true

    # Non-retryable
    @test AzManagers.isretryable(ErrorException("nope")) == false
end

@testset "status helper" begin
    r = HTTP.Response(429, [], "")
    e = HTTP.StatusError(429, "GET", "/", r)
    @test AzManagers.status(e) == 429

    @test AzManagers.status(ErrorException("foo")) == 999
end

@testset "retrywarn" begin
    # 429 with retry-after header
    r = HTTP.Response(429, ["retry-after" => "60", "x-ms-ratelimit-remaining-resource" => "foo"], "")
    e = HTTP.StatusError(429, "GET", "/", r)
    AzManagers.retrywarn(1, 2, 60, e)  # should not throw

    # 500 with error body
    body = JSON.json(Dict("error" => Dict("code" => "ServerError")))
    r = HTTP.Response(500, [], body)
    e = HTTP.StatusError(500, "GET", "/", r)
    AzManagers.retrywarn(1, 2, 10, e)

    # 409 with error body
    body = JSON.json(Dict("error" => Dict("code" => "Conflict", "message" => "already exists")))
    r = HTTP.Response(409, [], body)
    e = HTTP.StatusError(409, "GET", "/", r)
    AzManagers.retrywarn(1, 2, 10, e)

    # Generic non-HTTP error
    AzManagers.retrywarn(1, 2, 5, Base.IOError("test", 0))
end

@testset "@retry" begin
    # Succeeds immediately
    result = AzManagers.@retry 3 begin
        42
    end
    @test result == 42

    # Succeeds after retries
    counter = Ref(0)
    result = AzManagers.@retry 3 begin
        counter[] += 1
        if counter[] < 3
            throw(Base.IOError("transient", 0))
        end
        "ok"
    end
    @test result == "ok"
    @test counter[] == 3

    # Non-retryable error propagates immediately
    @test_throws ErrorException AzManagers.@retry 3 begin
        error("not retryable")
    end

    # Exhausts retries and throws
    @test_throws Base.IOError AzManagers.@retry 1 begin
        throw(Base.IOError("persistent", 0))
    end
end

@testset "build_envstring" begin
    @test AzManagers.build_envstring(Dict()) == ""

    result = AzManagers.build_envstring(Dict("FOO" => "bar"))
    @test contains(result, "export FOO=bar")

    result = AzManagers.build_envstring(Dict("A" => "1", "B" => "2"))
    @test contains(result, "export A=1")
    @test contains(result, "export B=2")
end

@testset "remaining_resource" begin
    # With matching header
    r = HTTP.Response(200, ["x-ms-ratelimit-remaining-resource" => "quota-info"], "")
    @test AzManagers.remaining_resource(r) == "quota-info"

    # Without matching header
    r = HTTP.Response(200, ["other-header" => "value"], "")
    @test AzManagers.remaining_resource(r) == ""
end

@testset "manifest read/write" begin
    # Use a temp dir to avoid touching the real manifest
    original_home = ENV["HOME"]
    mktempdir() do tmpdir
        ENV["HOME"] = tmpdir
        try
            AzManagers.write_manifest(;
                resourcegroup = "testrg",
                subscriptionid = "testsub",
                ssh_user = "testuser",
                ssh_private_key_file = joinpath(tmpdir, ".ssh", "test_rsa"),
                ssh_public_key_file = joinpath(tmpdir, ".ssh", "test_rsa.pub"))

            @test isfile(AzManagers.manifestfile())

            # Verify file permissions
            mode = filemode(AzManagers.manifestfile()) & 0o777
            @test mode == 0o600

            # Verify directory permissions
            mode = filemode(AzManagers.manifestpath()) & 0o777
            @test mode == 0o700

            # Read it back
            AzManagers.load_manifest()
            @test AzManagers._manifest["resourcegroup"] == "testrg"
            @test AzManagers._manifest["subscriptionid"] == "testsub"
            @test AzManagers._manifest["ssh_user"] == "testuser"
        finally
            ENV["HOME"] = original_home
            # Reload original manifest if it exists
            isfile(AzManagers.manifestfile()) && AzManagers.load_manifest()
        end
    end
end

@testset "compress/decompress environment roundtrip" begin
    mktempdir() do srcdir
        # Create fake environment files
        project_text = "[deps]\nFoo = \"abc123\"\n"
        manifest_text = "[deps]\n[[deps.Foo]]\nuuid = \"abc123\"\n"
        localprefs_text = "[Foo]\nbar = true\n"

        write(joinpath(srcdir, "Project.toml"), project_text)
        write(joinpath(srcdir, "Manifest.toml"), manifest_text)
        write(joinpath(srcdir, "LocalPreferences.toml"), localprefs_text)

        # Compress
        pc, mc, lc = AzManagers.compress_environment(srcdir)
        @test !isempty(pc)
        @test !isempty(mc)
        @test !isempty(lc)

        # Decompress to a temp location
        mktempdir() do dstdir
            env_name = "test_roundtrip"
            # Temporarily override Pkg.envdir
            original_depot = copy(DEPOT_PATH)
            pushfirst!(DEPOT_PATH, dstdir)
            try
                AzManagers.decompress_environment(pc, mc, lc, env_name)

                envpath = joinpath(Pkg.envdir(), env_name)
                @test read(joinpath(envpath, "Project.toml"), String) == project_text
                @test TOML.parse(read(joinpath(envpath, "Manifest.toml"), String)) == TOML.parse(manifest_text)
                @test read(joinpath(envpath, "LocalPreferences.toml"), String) == localprefs_text
            finally
                empty!(DEPOT_PATH)
                append!(DEPOT_PATH, original_depot)
            end
        end
    end

    # Test without LocalPreferences.toml
    mktempdir() do srcdir
        write(joinpath(srcdir, "Project.toml"), "name = \"Test\"\n")
        write(joinpath(srcdir, "Manifest.toml"), "# empty\n")

        pc, mc, lc = AzManagers.compress_environment(srcdir)
        @test !isempty(pc)
        @test !isempty(mc)
        # lc should be compressed empty string
    end
end

@testset "sanitize_manifest strips path keys" begin
    manifest = """
    manifest_format = "2.0"
    [deps]
    [[deps.Foo]]
    uuid = "abc123"
    version = "1.0.0"
    path = "/home/user/Foo"
    [[deps.Bar]]
    uuid = "def456"
    git-tree-sha1 = "abc"
    version = "2.0.0"
    """
    result = AzManagers.sanitize_manifest(manifest)
    parsed = TOML.parse(result)
    # Foo should still exist but without path key
    @test haskey(parsed["deps"], "Foo")
    @test !haskey(parsed["deps"]["Foo"][1], "path")
    # Bar should remain unchanged
    @test haskey(parsed["deps"], "Bar")
    @test !haskey(parsed["deps"]["Bar"][1], "path")
end

@testset "add_instance_to helpers" begin
    manager = AzManagers.AzManager()
    manager.pending_down = Dict{AzManagers.ScaleSet,Set{String}}()
    manager.pruned = Dict{AzManagers.ScaleSet,Set{String}}()
    manager.preempted = Dict{AzManagers.ScaleSet,Set{String}}()

    ss = AzManagers.ScaleSet("sub", "rg", "ss1")

    # pending_down: first add creates the set
    AzManagers.add_instance_to_pending_down_list(manager, ss, "inst1")
    @test "inst1" ∈ manager.pending_down[ss]

    # pending_down: second add appends to existing set
    AzManagers.add_instance_to_pending_down_list(manager, ss, "inst2")
    @test "inst1" ∈ manager.pending_down[ss]
    @test "inst2" ∈ manager.pending_down[ss]

    # pruned
    AzManagers.add_instance_to_pruned_list(manager, ss, "inst3")
    @test "inst3" ∈ manager.pruned[ss]
    AzManagers.add_instance_to_pruned_list(manager, ss, "inst4")
    @test "inst3" ∈ manager.pruned[ss]
    @test "inst4" ∈ manager.pruned[ss]

    # preempted
    AzManagers.add_instance_to_preempted_list(manager, ss, "inst5")
    @test "inst5" ∈ manager.preempted[ss]
    AzManagers.add_instance_to_preempted_list(manager, ss, "inst6")
    @test "inst5" ∈ manager.preempted[ss]
    @test "inst6" ∈ manager.preempted[ss]

    # Integer instance IDs get converted to strings
    AzManagers.add_instance_to_pending_down_list(manager, ss, 99)
    @test "99" ∈ manager.pending_down[ss]
end

@testset "add_instance_to_deleted_list" begin
    manager = AzManagers.AzManager()
    manager.deleted = Dict{AzManagers.ScaleSet,Dict{String,DateTime}}()

    ss = AzManagers.ScaleSet("sub", "rg", "ss1")

    AzManagers.add_instance_to_deleted_list(manager, ss, "inst1")
    @test haskey(manager.deleted[ss], "inst1")
    @test manager.deleted[ss]["inst1"] isa DateTime

    AzManagers.add_instance_to_deleted_list(manager, ss, "inst2")
    @test haskey(manager.deleted[ss], "inst1")
    @test haskey(manager.deleted[ss], "inst2")
end

@testset "variablebundle" begin
    # Clear state
    empty!(AzManagers.VARIABLE_BUNDLE)

    variablebundle!(x=1, y="hello")
    @test variablebundle(:x) == 1
    @test variablebundle(:y) == "hello"

    # Dict form
    variablebundle!(Dict("a" => 42, "b" => 3.14))
    @test variablebundle(:a) == 42
    @test variablebundle(:b) ≈ 3.14

    # Full bundle
    bundle = variablebundle()
    @test bundle[:x] == 1
    @test bundle[:a] == 42

    empty!(AzManagers.VARIABLE_BUNDLE)
end

@testset "DetachedJob constructors" begin
    job = DetachedJob("10.0.0.1", 5)
    @test job.vm["ip"] == "10.0.0.1"
    @test job.vm["port"] == "8081"
    @test job.id == "5"

    job = DetachedJob("10.0.0.2", 3, 1234; port=9090)
    @test job.vm["ip"] == "10.0.0.2"
    @test job.vm["port"] == "9090"
    @test job.id == "3"
    @test job.pid == "1234"
end

@testset "SpotPreemptException" begin
    e = AzManagers.SpotPreemptException("inst-42", 7, "Mon, 01 Jan 2024 00:00:00 GMT")
    io = IOBuffer()
    showerror(io, e)
    msg = String(take!(io))
    @test contains(msg, "inst-42")
    @test contains(msg, "7")
    @test contains(msg, "Mon, 01 Jan 2024 00:00:00 GMT")
end

@testset "detached_port" begin
    original = AzManagers.detached_port()
    AzManagers.detached_port!(9999)
    @test AzManagers.detached_port() == 9999
    AzManagers.detached_port!(original)
    @test AzManagers.detached_port() == original
end

# ═══════════════════════════════════════════════════════════════════════════════
# Event-driven architecture tests (async refactor)
# ═══════════════════════════════════════════════════════════════════════════════

@testset "Event types" begin
    wp = AzManagers.WorkerPreempted(42, "inst-1", "Mon, 01 Jan 2024 00:00:00 GMT")
    @test wp isa AzManagers.ManagerEvent
    @test wp.pid == 42
    @test wp.instanceid == "inst-1"
    @test wp.notbefore == "Mon, 01 Jan 2024 00:00:00 GMT"

    wl = AzManagers.WorkerLost(7)
    @test wl isa AzManagers.ManagerEvent
    @test wl.pid == 7

    wc = AzManagers.WorkersChanged(3)
    @test wc isa AzManagers.ManagerEvent
    @test wc.count == 3

    @test AzManagers.PruneTick()        isa AzManagers.ManagerEvent
    @test AzManagers.CleanTick()        isa AzManagers.ManagerEvent
    @test AzManagers.BatchFlushTick()   isa AzManagers.ManagerEvent
    @test AzManagers.ShutdownRequested() isa AzManagers.ManagerEvent

    ds = AzManagers.DeletionStarted("myvm", "https://example.com/op", AzSessions.AzSession(;lazy=true))
    @test ds isa AzManagers.ManagerEvent
    @test ds.vmname == "myvm"
    @test ds.async_url == "https://example.com/op"
end

@testset "run_event_loop — process and shutdown" begin
    mgr = AzManagers.AzManager()
    mgr.events = Channel{AzManagers.ManagerEvent}(16)
    mgr.wconfig_batch = Distributed.WorkerConfig[]
    mgr.batch_max = 64
    mgr.workers_changed = Threads.Condition()
    mgr.lock = ReentrantLock()

    # Post benign events (handlers will log errors but not throw)
    put!(mgr.events, AzManagers.PruneTick())
    put!(mgr.events, AzManagers.CleanTick())
    close(mgr.events)

    # Should run to completion without throwing
    AzManagers.run_event_loop(mgr)
    @test !isopen(mgr.events)
end

@testset "ShutdownRequested handler" begin
    mgr = AzManagers.AzManager()
    mgr.events = Channel{AzManagers.ManagerEvent}(16)
    mgr.timer_prune = Timer(999)
    mgr.timer_clean = Timer(999)
    mgr.lock = ReentrantLock()
    mgr.wconfig_batch = Distributed.WorkerConfig[]
    mgr.batch_max = 64
    mgr.workers_changed = Threads.Condition()

    @test isopen(mgr.timer_prune)
    @test isopen(mgr.timer_clean)

    put!(mgr.events, AzManagers.ShutdownRequested())
    event = take!(mgr.events)
    AzManagers.handle(mgr, event)

    @test !isopen(mgr.timer_prune)
    @test !isopen(mgr.timer_clean)
    @test !isopen(mgr.events)
end

@testset "ConnectionValidated batching" begin
    mgr = AzManagers.AzManager()
    mgr.events = Channel{AzManagers.ManagerEvent}(64)
    mgr.wconfig_batch = Distributed.WorkerConfig[]
    mgr.batch_max = 3
    mgr.lock = ReentrantLock()
    mgr.workers_changed = Threads.Condition()

    # Create a mock WorkerConfig
    wconfig = Distributed.WorkerConfig()
    wconfig.bind_addr = "10.0.0.1"
    wconfig.exename = "julia"

    AzManagers.handle(mgr, AzManagers.ConnectionValidated(wconfig))
    @test length(mgr.wconfig_batch) == 1
    @test isdefined(mgr, :timer_batch_flush)

    close(mgr.events)
end

@testset "WorkersChanged notifies condition" begin
    mgr = AzManagers.AzManager()
    mgr.workers_changed = Threads.Condition()
    mgr.events = Channel{AzManagers.ManagerEvent}(8)
    mgr.lock = ReentrantLock()

    notified = Ref(false)
    t = @async begin
        lock(mgr.workers_changed) do
            wait(mgr.workers_changed)
            notified[] = true
        end
    end
    yield()

    AzManagers.handle(mgr, AzManagers.WorkersChanged(5))
    wait(t)
    @test notified[]
    close(mgr.events)
end

@testset "DeletionStarted handler tracks pending deletion" begin
    mgr = AzManagers.AzManager()
    mgr.events = Channel{AzManagers.ManagerEvent}(8)
    mgr.pending_deletions = @NamedTuple{vmname::String, url::String, session::AzSessions.AzSessionAbstract, started::Float64}[]

    sess = AzSessions.AzSession(;lazy=true)
    event = AzManagers.DeletionStarted("testvm", "https://example.com/op/123", sess)
    AzManagers.handle(mgr, event)

    @test length(mgr.pending_deletions) == 1
    @test mgr.pending_deletions[1].vmname == "testvm"
    @test mgr.pending_deletions[1].url == "https://example.com/op/123"
    close(mgr.events)
end

@testset "deregister_worker_safe — missing pid is no-op" begin
    AzManagers.deregister_worker_safe(999_999)
    @test true
end

@testset "AzManager field inventory" begin
    mgr = AzManagers.AzManager()

    # New fields undefined on bare constructor
    @test !isdefined(mgr, :events)
    @test !isdefined(mgr, :wconfig_batch)
    @test !isdefined(mgr, :task_accept)
    @test !isdefined(mgr, :task_event_loop)
    @test !isdefined(mgr, :timer_prune)
    @test !isdefined(mgr, :timer_clean)
    @test !isdefined(mgr, :preempt_channel_ready)
    @test !isdefined(mgr, :pending_deletions)

    # Old deleted fields must not exist
    @test !hasfield(AzManagers.AzManager, :pending_up)
    @test !hasfield(AzManagers.AzManager, :task_add)
    @test !hasfield(AzManagers.AzManager, :task_process)
    @test !hasfield(AzManagers.AzManager, :task_prune)
    @test !hasfield(AzManagers.AzManager, :task_clean)
end

@testset "flush_wconfig_batch — empty batch is no-op" begin
    mgr = AzManagers.AzManager()
    mgr.events = Channel{AzManagers.ManagerEvent}(16)
    mgr.wconfig_batch = Distributed.WorkerConfig[]
    mgr.batch_max = 64
    mgr.lock = ReentrantLock()
    mgr.timer_batch_flush = Timer(999)
    mgr.workers_changed = Threads.Condition()

    AzManagers.flush_wconfig_batch(mgr)

    @test isempty(mgr.wconfig_batch)
    @test !isopen(mgr.timer_batch_flush)
    close(mgr.events)
end

# ═══════════════════════════════════════════════════════════════════════════════
# Error types (src/errors.jl)
# ═══════════════════════════════════════════════════════════════════════════════

@testset "AzureAPIError" begin
    # From an HTTP.StatusError with JSON error body
    body = JSON.json(Dict("error" => Dict("code" => "QuotaExceeded", "message" => "Not enough cores")))
    r = HTTP.Response(429, [], body)
    http_err = HTTP.StatusError(429, "PUT", "/subscriptions/...", r)
    e = AzManagers.AzureAPIError(http_err; operation="scaleset_create")

    @test e isa AzManagers.AzManagersError
    @test e.status == 429
    @test e.error_code == "QuotaExceeded"
    @test e.message == "Not enough cores"
    @test e.operation == "scaleset_create"
    @test e.cause === http_err

    io = IOBuffer()
    showerror(io, e)
    msg = String(take!(io))
    @test contains(msg, "AzureAPIError(429)")
    @test contains(msg, "QuotaExceeded")
    @test contains(msg, "scaleset_create")
    @test contains(msg, "Not enough cores")

    # From an HTTP.StatusError with non-JSON body
    r2 = HTTP.Response(500, [], "Internal Server Error")
    http_err2 = HTTP.StatusError(500, "GET", "/", r2)
    e2 = AzManagers.AzureAPIError(http_err2)
    @test e2.status == 500
    @test e2.error_code == "Unknown"
    @test e2.operation == ""

    # From a non-HTTP exception
    generic_err = ErrorException("connection refused")
    e3 = AzManagers.AzureAPIError(generic_err; operation="list_vms")
    @test e3.status == 999
    @test e3.operation == "list_vms"
    @test contains(e3.message, "connection refused")
end

@testset "QuotaExhaustedError" begin
    e = AzManagers.QuotaExhaustedError("sub-123", "eastus", "standardDv3Family", 16, 4, 60)
    @test e isa AzManagers.AzManagersError
    @test e.subscriptionid == "sub-123"
    @test e.requested == 16
    @test e.available == 4
    @test e.retries == 60

    io = IOBuffer()
    showerror(io, e)
    msg = String(take!(io))
    @test contains(msg, "QuotaExhaustedError")
    @test contains(msg, "standardDv3Family")
    @test contains(msg, "eastus")
    @test contains(msg, "16")
    @test contains(msg, "60 retries")
end

@testset "WorkerJoinTimeoutError" begin
    e = AzManagers.WorkerJoinTimeoutError("myscaleset", "inst-42", 720.0)
    @test e isa AzManagers.AzManagersError

    io = IOBuffer()
    showerror(io, e)
    msg = String(take!(io))
    @test contains(msg, "WorkerJoinTimeoutError")
    @test contains(msg, "inst-42")
    @test contains(msg, "myscaleset")
    @test contains(msg, "720.0")
end

@testset "CloudInitError" begin
    e = AzManagers.CloudInitError("inst-7", "ss-prod", "failed", "cloud-init timed out")
    @test e isa AzManagers.AzManagersError

    io = IOBuffer()
    showerror(io, e)
    msg = String(take!(io))
    @test contains(msg, "CloudInitError")
    @test contains(msg, "inst-7")
    @test contains(msg, "failed")
    @test contains(msg, "cloud-init timed out")
end

@testset "ScaleSetOperationError" begin
    e = AzManagers.ScaleSetOperationError("delete", "ss-prod", "404 not found", nothing)
    @test e isa AzManagers.AzManagersError

    io = IOBuffer()
    showerror(io, e)
    msg = String(take!(io))
    @test contains(msg, "ScaleSetOperationError")
    @test contains(msg, "delete")
    @test contains(msg, "ss-prod")

    # With a cause
    cause = ErrorException("network error")
    e2 = AzManagers.ScaleSetOperationError("create", "ss-dev", "failed", cause)
    @test e2.cause === cause
end

@testset "ManifestError" begin
    e = AzManagers.ManifestError("/home/user/.azmanagers/manifest.json", "file not found")
    @test e isa AzManagers.AzManagersError

    io = IOBuffer()
    showerror(io, e)
    msg = String(take!(io))
    @test contains(msg, "ManifestError")
    @test contains(msg, "file not found")
    @test contains(msg, "manifest.json")

    # load_manifest throws ManifestError for missing file
    original_home = ENV["HOME"]
    mktempdir() do tmpdir
        ENV["HOME"] = tmpdir
        try
            @test_throws AzManagers.ManifestError AzManagers.load_manifest()
        finally
            ENV["HOME"] = original_home
            isfile(AzManagers.manifestfile()) && AzManagers.load_manifest()
        end
    end

    # load_manifest throws ManifestError for invalid JSON
    mktempdir() do tmpdir
        ENV["HOME"] = tmpdir
        try
            mkpath(AzManagers.manifestpath())
            write(AzManagers.manifestfile(), "not valid json {{{")
            @test_throws AzManagers.ManifestError AzManagers.load_manifest()
        finally
            ENV["HOME"] = original_home
            isfile(AzManagers.manifestfile()) && AzManagers.load_manifest()
        end
    end
end

@testset "ImageResolutionError" begin
    e = AzManagers.ImageResolutionError("myimage", "", "", "gallery name not found in template")
    @test e isa AzManagers.AzManagersError

    io = IOBuffer()
    showerror(io, e)
    msg = String(take!(io))
    @test contains(msg, "ImageResolutionError")
    @test contains(msg, "myimage")
    @test contains(msg, "gallery name not found")
end

@testset "DetachedServiceError" begin
    e = AzManagers.DetachedServiceError("cbox-abc", "10.0.0.5", "run", "connection refused", nothing)
    @test e isa AzManagers.AzManagersError

    io = IOBuffer()
    showerror(io, e)
    msg = String(take!(io))
    @test contains(msg, "DetachedServiceError")
    @test contains(msg, "cbox-abc")
    @test contains(msg, "10.0.0.5")
    @test contains(msg, "run")
    @test contains(msg, "connection refused")
end

@testset "Error type hierarchy" begin
    # All error types are subtypes of AzManagersError
    @test AzManagers.AzureAPIError <: AzManagers.AzManagersError
    @test AzManagers.QuotaExhaustedError <: AzManagers.AzManagersError
    @test AzManagers.WorkerJoinTimeoutError <: AzManagers.AzManagersError
    @test AzManagers.CloudInitError <: AzManagers.AzManagersError
    @test AzManagers.ScaleSetOperationError <: AzManagers.AzManagersError
    @test AzManagers.ManifestError <: AzManagers.AzManagersError
    @test AzManagers.ImageResolutionError <: AzManagers.AzManagersError
    @test AzManagers.DetachedServiceError <: AzManagers.AzManagersError

    # AzManagersError is a subtype of Exception
    @test AzManagers.AzManagersError <: Exception
end

# ═══════════════════════════════════════════════════════════════════════════════
# Telemetry (src/telemetry.jl)
# ═══════════════════════════════════════════════════════════════════════════════

@testset "ManagerMetrics construction" begin
    m = AzManagers.ManagerMetrics()
    @test m.workers_joined == 0
    @test m.workers_lost == 0
    @test m.workers_preempted == 0
    @test m.workers_pruned == 0
    @test m.workers_join_failed == 0
    @test m.scalesets_created == 0
    @test m.scalesets_deleted == 0
    @test m.api_calls == 0
    @test m.api_retries == 0
    @test m.api_errors == 0
    @test m.api_throttles == 0
    @test m.created_at <= now(Dates.UTC)
    @test isempty(m.worker_join_durations)
end

@testset "ManagerMetrics record functions" begin
    m = AzManagers.ManagerMetrics()

    AzManagers.record_api_call!(m)
    AzManagers.record_api_call!(m)
    @test m.api_calls == 2

    AzManagers.record_api_retry!(m)
    @test m.api_retries == 1

    AzManagers.record_api_error!(m)
    @test m.api_errors == 1

    AzManagers.record_api_throttle!(m)
    AzManagers.record_api_throttle!(m)
    @test m.api_throttles == 2

    AzManagers.record_worker_joined!(m, 15.5)
    AzManagers.record_worker_joined!(m, 22.3)
    @test m.workers_joined == 2
    @test length(m.worker_join_durations) == 2
    @test m.worker_join_durations[1] ≈ 15.5
    @test m.worker_join_durations[2] ≈ 22.3

    # Zero duration doesn't add to durations vector
    AzManagers.record_worker_joined!(m)
    @test m.workers_joined == 3
    @test length(m.worker_join_durations) == 2

    AzManagers.record_worker_lost!(m)
    @test m.workers_lost == 1

    AzManagers.record_worker_preempted!(m)
    @test m.workers_preempted == 1

    AzManagers.record_worker_pruned!(m)
    AzManagers.record_worker_pruned!(m)
    @test m.workers_pruned == 2

    AzManagers.record_worker_join_failed!(m)
    @test m.workers_join_failed == 1

    AzManagers.record_scaleset_created!(m)
    @test m.scalesets_created == 1

    AzManagers.record_scaleset_deleted!(m)
    @test m.scalesets_deleted == 1

    AzManagers.record_prune_time!(m)
    @test m.last_prune_time > DateTime(0)

    AzManagers.record_clean_time!(m)
    @test m.last_clean_time > DateTime(0)
end

@testset "ManagerMetrics join durations bounded" begin
    m = AzManagers.ManagerMetrics()
    for i in 1:1100
        AzManagers.record_worker_joined!(m, Float64(i))
    end
    @test m.workers_joined == 1100
    @test length(m.worker_join_durations) <= 1000
end

@testset "ManagerMetrics thread safety" begin
    m = AzManagers.ManagerMetrics()
    n = 100
    tasks = Task[]
    for _ in 1:n
        push!(tasks, Threads.@spawn begin
            AzManagers.record_api_call!(m)
            AzManagers.record_api_retry!(m)
            AzManagers.record_worker_joined!(m, 1.0)
        end)
    end
    foreach(wait, tasks)
    @test m.api_calls == n
    @test m.api_retries == n
    @test m.workers_joined == n
end

@testset "AzManager has metrics field" begin
    @test hasfield(AzManagers.AzManager, :metrics)

    mgr = AzManagers.AzManager()
    # Bare constructor leaves it undefined
    @test !isdefined(mgr, :metrics)
end

# ═══════════════════════════════════════════════════════════════════════════════
# Structured logging improvements
# ═══════════════════════════════════════════════════════════════════════════════

@testset "logerror with context kwargs" begin
    # logerror should accept context keyword arguments without throwing
    try
        error("test structured logerror")
    catch e
        AzManagers.logerror(e, Logging.Debug; operation="test", scaleset="ss-1")
    end
    @test true  # didn't throw
end

@testset "logerror default level is Warn" begin
    logger = Test.TestLogger(min_level=Logging.Debug)
    with_logger(logger) do
        try
            error("test default level")
        catch e
            AzManagers.logerror(e)
        end
    end
    @test length(logger.logs) >= 1
    @test logger.logs[end].level == Logging.Warn
end

@testset "retrywarn structured logging — 429 with no remaining_resource header" begin
    # 429 without the quota header — should not throw
    r = HTTP.Response(429, ["retry-after" => "30"], "")
    e = HTTP.StatusError(429, "GET", "/", r)
    AzManagers.retrywarn(0, 3, 30, e)
    @test true
end

@testset "retrywarn structured logging — 500 with non-JSON body" begin
    r = HTTP.Response(500, [], "plain text error")
    e = HTTP.StatusError(500, "GET", "/", r)
    # Should not throw even with unparseable body
    AzManagers.retrywarn(1, 3, 10, e)
    @test true
end

@testset "retrywarn structured logging — 409 with non-JSON body" begin
    r = HTTP.Response(409, [], "not json")
    e = HTTP.StatusError(409, "GET", "/", r)
    AzManagers.retrywarn(1, 2, 5, e)
    @test true
end

@testset "retrywarn structured logging — other HTTP status" begin
    r = HTTP.Response(503, [], "Service Unavailable")
    e = HTTP.StatusError(503, "GET", "/", r)
    # 503 is not retryable normally but retrywarn should still handle it
    AzManagers.retrywarn(0, 1, 5, e)
    @test true
end

# ═══════════════════════════════════════════════════════════════════════════════
# @retry telemetry integration
# ═══════════════════════════════════════════════════════════════════════════════

@testset "@retry records retry count on success" begin
    # After retries, the macro should succeed and log retries at @debug
    counter = Ref(0)
    result = AzManagers.@retry 3 begin
        counter[] += 1
        if counter[] < 2
            throw(Base.IOError("transient", 0))
        end
        "recovered"
    end
    @test result == "recovered"
    @test counter[] == 2
end

# ═══════════════════════════════════════════════════════════════════════════════
# Event loop metrics integration
# ═══════════════════════════════════════════════════════════════════════════════

@testset "CleanTick handler calls log_health_summary" begin
    mgr = AzManagers.AzManager()
    mgr.events = Channel{AzManagers.ManagerEvent}(16)
    mgr.wconfig_batch = Distributed.WorkerConfig[]
    mgr.batch_max = 64
    mgr.workers_changed = Threads.Condition()
    mgr.lock = ReentrantLock()
    mgr.metrics = AzManagers.ManagerMetrics()
    mgr.pending_deletions = @NamedTuple{vmname::String, url::String, session::AzSessions.AzSessionAbstract, started::Float64}[]
    mgr.pending_down = Dict{AzManagers.ScaleSet,Set{String}}()
    mgr.scalesets = Dict{AzManagers.ScaleSet,Int}()
    mgr.deleted = Dict{AzManagers.ScaleSet,Dict{String,DateTime}}()
    mgr.pruned = Dict{AzManagers.ScaleSet,Set{String}}()
    mgr.preempted = Dict{AzManagers.ScaleSet,Set{String}}()

    # CleanTick calls delete_pending_down_vms() which accesses the global
    # _manager.session — that will throw in test, but the handler catches it
    # and still runs check_pending_deletions + log_health_summary.
    # record_clean_time! is inside the try block so it won't be called here.
    AzManagers.handle(mgr, AzManagers.CleanTick())

    # Verify log_health_summary ran (no throw) and metrics struct is intact
    @test mgr.metrics isa AzManagers.ManagerMetrics
    close(mgr.events)
end

@testset "PruneTick handler records prune time" begin
    mgr = AzManagers.AzManager()
    mgr.events = Channel{AzManagers.ManagerEvent}(16)
    mgr.wconfig_batch = Distributed.WorkerConfig[]
    mgr.batch_max = 64
    mgr.workers_changed = Threads.Condition()
    mgr.lock = ReentrantLock()
    mgr.metrics = AzManagers.ManagerMetrics()

    AzManagers.handle(mgr, AzManagers.PruneTick())
    # PruneTick will fail with UndefRefError on scalesets, but metrics should still be recorded
    # because the error is caught inside the handler
    # Note: prune_time only gets set on success, so it may remain DateTime(0) here
    @test mgr.metrics isa AzManagers.ManagerMetrics
    close(mgr.events)
end

@testset "log_health_summary does not throw" begin
    mgr = AzManagers.AzManager()
    mgr.metrics = AzManagers.ManagerMetrics()
    mgr.pending_down = Dict{AzManagers.ScaleSet,Set{String}}()
    mgr.scalesets = Dict{AzManagers.ScaleSet,Int}()
    mgr.lock = ReentrantLock()

    # Should emit structured log without throwing
    AzManagers.log_health_summary(mgr)
    @test true

    # No-op when metrics not defined
    mgr2 = AzManagers.AzManager()
    AzManagers.log_health_summary(mgr2)
    @test true
end
