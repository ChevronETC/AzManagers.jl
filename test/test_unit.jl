using AzManagers, Test, HTTP, JSON, Dates, Distributed, Logging, Pkg, TOML, Sockets, AzSessions

@testset "spin" begin
    s = AzManagers.spin(1, 12.345)
    @test startswith(s, "◐")
    @test contains(s, "12.35")
    @test contains(s, "seconds")

    s = AzManagers.spin(5, 0.0)
    @test startswith(s, "✓")
    @test contains(s, "0.00")

    # All spinner characters
    for i in 1:4
        s = AzManagers.spin(i, 1.0)
        @test s[1] ∈ ('◐', '◓', '◑', '◒')
    end
end

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
    mgr.socket_batch = Sockets.TCPSocket[]
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
    mgr.socket_batch = Sockets.TCPSocket[]
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

@testset "SocketAccepted batching" begin
    mgr = AzManagers.AzManager()
    mgr.events = Channel{AzManagers.ManagerEvent}(64)
    mgr.socket_batch = Sockets.TCPSocket[]
    mgr.batch_max = 3
    mgr.lock = ReentrantLock()
    mgr.workers_changed = Threads.Condition()

    # Create a real socket pair for type correctness
    server = listen(getipaddr(), 0)
    port = getsockname(server)[2]
    client = connect(getipaddr(), port)
    accepted = accept(server)

    AzManagers.handle(mgr, AzManagers.SocketAccepted(client))
    @test length(mgr.socket_batch) == 1
    @test isdefined(mgr, :timer_batch_flush)

    close(client)
    close(accepted)
    close(server)
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
    @test !isdefined(mgr, :socket_batch)
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

@testset "flush_socket_batch — empty batch is no-op" begin
    mgr = AzManagers.AzManager()
    mgr.events = Channel{AzManagers.ManagerEvent}(16)
    mgr.socket_batch = Sockets.TCPSocket[]
    mgr.batch_max = 64
    mgr.lock = ReentrantLock()
    mgr.timer_batch_flush = Timer(999)
    mgr.workers_changed = Threads.Condition()

    AzManagers.flush_socket_batch(mgr)

    @test isempty(mgr.socket_batch)
    @test !isopen(mgr.timer_batch_flush)
    close(mgr.events)
end
