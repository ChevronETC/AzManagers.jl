using Test, AzManagers, HTTP, JSON, Distributed, Dates, Logging, Pkg

# ── Helper: create a minimal AzManager for state tests ────────────────────────
function make_test_manager()
    mgr = AzManagers.AzManager()
    mgr.pending_down = Dict{AzManagers.ScaleSet, Set{String}}()
    mgr.deleted      = Dict{AzManagers.ScaleSet, Dict{String, DateTime}}()
    mgr.pruned        = Dict{AzManagers.ScaleSet, Set{String}}()
    mgr.preempted     = Dict{AzManagers.ScaleSet, Set{String}}()
    mgr.scalesets     = Dict{AzManagers.ScaleSet, Int}()
    mgr.lock          = ReentrantLock()
    mgr
end

# ═══════════════════════════════════════════════════════════════════════════════
# 1. Error classification
# ═══════════════════════════════════════════════════════════════════════════════

@testset "isretryable" begin
    # Retryable HTTP status codes
    for code in (409, 429, 500)
        r = HTTP.Response(code)
        e = HTTP.Exceptions.StatusError(code, "GET", "/test", r)
        @test AzManagers.isretryable(e) == true
    end

    # Non-retryable HTTP status codes
    for code in (400, 401, 403, 404, 422)
        r = HTTP.Response(code)
        e = HTTP.Exceptions.StatusError(code, "GET", "/test", r)
        @test AzManagers.isretryable(e) == false
    end

    # Retryable non-HTTP errors
    @test AzManagers.isretryable(Base.IOError("test", 0)) == true
    @test AzManagers.isretryable(Base.EOFError()) == true

    # Non-retryable generic errors
    @test AzManagers.isretryable(ErrorException("nope")) == false
    @test AzManagers.isretryable(ArgumentError("bad")) == false
end

# ═══════════════════════════════════════════════════════════════════════════════
# 2. Retry warning formatting
# ═══════════════════════════════════════════════════════════════════════════════

@testset "retrywarn" begin
    # 429 with retry-after header
    r = HTTP.Response(
        429,
        ["retry-after" => "60", "x-ms-ratelimit-remaining-resource" => "foo"],
        "")
    e = HTTP.Exceptions.StatusError(429, "GET", "/test", r)
    # Should not throw
    @test (AzManagers.retrywarn(1, 2, 60, e); true)

    # 500 with error body
    body_500 = JSON.json(Dict("error" => Dict("code" => "InternalError")))
    r = HTTP.Response(500, ["Content-Type" => "application/json"], body_500)
    e = HTTP.Exceptions.StatusError(500, "GET", "/test", r)
    @test (AzManagers.retrywarn(1, 3, 5, e); true)

    # 409 with error body
    body_409 = JSON.json(Dict("error" => Dict("code" => "Conflict", "message" => "resource exists")))
    r = HTTP.Response(409, ["Content-Type" => "application/json"], body_409)
    e = HTTP.Exceptions.StatusError(409, "GET", "/test", r)
    @test (AzManagers.retrywarn(1, 3, 5, e); true)

    # Generic non-HTTP error
    @test (AzManagers.retrywarn(1, 2, 1, ErrorException("boom")); true)
end

# ═══════════════════════════════════════════════════════════════════════════════
# 3. status() helper
# ═══════════════════════════════════════════════════════════════════════════════

@testset "status helper" begin
    r = HTTP.Response(404)
    e = HTTP.Exceptions.StatusError(404, "GET", "/", r)
    @test AzManagers.status(e) == 404

    # Non-HTTP error returns 999
    @test AzManagers.status(ErrorException("x")) == 999
end

# ═══════════════════════════════════════════════════════════════════════════════
# 4. remaining_resource header extraction
# ═══════════════════════════════════════════════════════════════════════════════

@testset "remaining_resource" begin
    r = HTTP.Response(200, ["x-ms-ratelimit-remaining-resource" => "Microsoft.Compute/GetVMScaleSet30Min;237"])
    @test AzManagers.remaining_resource(r) == "Microsoft.Compute/GetVMScaleSet30Min;237"

    # No matching header
    r = HTTP.Response(200, ["Content-Type" => "application/json"])
    @test AzManagers.remaining_resource(r) == ""
end

# ═══════════════════════════════════════════════════════════════════════════════
# 5. nthreads_filter
# ═══════════════════════════════════════════════════════════════════════════════

@testset "nthreads_filter" begin
    @test AzManagers.nthreads_filter("4") == "4"
    @test AzManagers.nthreads_filter("4,0") == "4"
    @test AzManagers.nthreads_filter("4,2") == "4,2"
    @test AzManagers.nthreads_filter("1,1") == "1,1"
    @test AzManagers.nthreads_filter(4) == "4"
    @test AzManagers.nthreads_filter("3,0") == "3"
end

# ═══════════════════════════════════════════════════════════════════════════════
# 6. spin formatting
# ═══════════════════════════════════════════════════════════════════════════════

@testset "spin" begin
    s = AzManagers.spin(1, 12.345)
    @test startswith(s, "◐")
    @test contains(s, "12.35")

    s = AzManagers.spin(5, 0.0)
    @test startswith(s, "✓")
end

# ═══════════════════════════════════════════════════════════════════════════════
# 7. build_envstring
# ═══════════════════════════════════════════════════════════════════════════════

@testset "build_envstring" begin
    env = Dict("FOO" => "bar", "BAZ" => "42")
    s = AzManagers.build_envstring(env)
    @test contains(s, "export FOO=bar")
    @test contains(s, "export BAZ=42")

    @test AzManagers.build_envstring(Dict()) == ""
end

# ═══════════════════════════════════════════════════════════════════════════════
# 8. logerror (should not throw)
# ═══════════════════════════════════════════════════════════════════════════════

@testset "logerror" begin
    @test (AzManagers.logerror(ErrorException("test error")); true)
    @test (AzManagers.logerror(ArgumentError("bad arg"), Logging.Debug); true)
end

# ═══════════════════════════════════════════════════════════════════════════════
# 9. ScaleSet struct
# ═══════════════════════════════════════════════════════════════════════════════

@testset "ScaleSet" begin
    ss = AzManagers.ScaleSet("SUB-123", "MyGroup", "MyScaleSet")

    # Constructor lowercases all fields
    @test ss.subscriptionid == "sub-123"
    @test ss.resourcegroup == "mygroup"
    @test ss.scalesetname == "myscaleset"

    # Dict conversion
    d = Dict(ss)
    @test d["subscriptionid"] == "sub-123"
    @test d["resourcegroup"] == "mygroup"
    @test d["name"] == "myscaleset"

    # Equality (used as Dict key)
    ss2 = AzManagers.ScaleSet("sub-123", "mygroup", "myscaleset")
    @test ss == ss2
    @test hash(ss) == hash(ss2)
end

# ═══════════════════════════════════════════════════════════════════════════════
# 10. Instance tracking lists
# ═══════════════════════════════════════════════════════════════════════════════

@testset "instance tracking" begin
    mgr = make_test_manager()
    ss = AzManagers.ScaleSet("sub", "rg", "ss1")

    # pending_down
    AzManagers.add_instance_to_pending_down_list(mgr, ss, "inst1")
    @test "inst1" ∈ mgr.pending_down[ss]
    AzManagers.add_instance_to_pending_down_list(mgr, ss, "inst2")
    @test length(mgr.pending_down[ss]) == 2

    # pruned
    AzManagers.add_instance_to_pruned_list(mgr, ss, "inst1")
    @test "inst1" ∈ mgr.pruned[ss]
    AzManagers.add_instance_to_pruned_list(mgr, ss, "inst2")
    @test length(mgr.pruned[ss]) == 2

    # preempted
    AzManagers.add_instance_to_preempted_list(mgr, ss, "inst3")
    @test "inst3" ∈ mgr.preempted[ss]

    # deleted (stores DateTime)
    AzManagers.add_instance_to_deleted_list(mgr, ss, "inst1")
    @test haskey(mgr.deleted[ss], "inst1")
    @test mgr.deleted[ss]["inst1"] isa DateTime

    # New scaleset creates new entry
    ss2 = AzManagers.ScaleSet("sub", "rg", "ss2")
    AzManagers.add_instance_to_pending_down_list(mgr, ss2, "a")
    @test haskey(mgr.pending_down, ss2)
end

# ═══════════════════════════════════════════════════════════════════════════════
# 11. ispreempted
# ═══════════════════════════════════════════════════════════════════════════════

@testset "ispreempted" begin
    mgr = make_test_manager()
    ss = AzManagers.ScaleSet("sub", "rg", "ss1")

    config = Distributed.WorkerConfig()
    config.userdata = Dict(
        "subscriptionid" => "sub",
        "resourcegroup" => "rg",
        "scalesetname" => "ss1",
        "instanceid" => "inst5"
    )

    # Not preempted
    @test AzManagers.ispreempted(mgr, config) == false

    # Mark as preempted
    AzManagers.add_instance_to_preempted_list(mgr, ss, "inst5")
    @test AzManagers.ispreempted(mgr, config) == true

    # Different instance
    config2 = Distributed.WorkerConfig()
    config2.userdata = Dict(
        "subscriptionid" => "sub",
        "resourcegroup" => "rg",
        "scalesetname" => "ss1",
        "instanceid" => "inst999"
    )
    @test AzManagers.ispreempted(mgr, config2) == false
end

# ═══════════════════════════════════════════════════════════════════════════════
# 12. scalesets accessor
# ═══════════════════════════════════════════════════════════════════════════════

@testset "scalesets accessor" begin
    mgr = make_test_manager()
    @test AzManagers.scalesets(mgr) == Dict{AzManagers.ScaleSet, Int}()

    ss = AzManagers.ScaleSet("sub", "rg", "ss1")
    mgr.scalesets[ss] = 4
    @test AzManagers.scalesets(mgr)[ss] == 4

    # Uninitialized manager
    mgr2 = AzManagers.AzManager()
    @test AzManagers.scalesets(mgr2) == Dict{AzManagers.ScaleSet, Int}()
end

# ═══════════════════════════════════════════════════════════════════════════════
# 13. Template building — scale set
# ═══════════════════════════════════════════════════════════════════════════════

@testset "build_sstemplate" begin
    t = AzManagers.build_sstemplate("cbox04";
        subscriptionid = "sub-1",
        admin_username = "testuser",
        location = "eastus",
        resourcegroup = "rg-1",
        imagegallery = "gallery1",
        imagename = "image1",
        vnet = "vnet1",
        subnet = "default",
        skuname = "Standard_D4s_v5")

    @test t["subscriptionid"] == "sub-1"
    @test t["resourcegroup"] == "rg-1"
    @test t["value"]["sku"]["name"] == "Standard_D4s_v5"
    @test t["value"]["location"] == "eastus"

    vmp = t["value"]["properties"]["virtualMachineProfile"]
    @test vmp["osProfile"]["adminUsername"] == "testuser"
    @test contains(vmp["storageProfile"]["imageReference"]["id"], "gallery1")
    @test contains(vmp["storageProfile"]["imageReference"]["id"], "image1")
    @test vmp["storageProfile"]["osDisk"]["diskSizeGB"] == 60

    # Subnet reference constructed correctly
    subnet_id = vmp["networkProfile"]["networkInterfaceConfigurations"][1]["properties"]["ipConfigurations"][1]["properties"]["subnet"]["id"]
    @test contains(subnet_id, "sub-1")
    @test contains(subnet_id, "vnet1")
    @test contains(subnet_id, "default")

    # No tags by default
    @test !haskey(t["value"], "tags")

    # With tags
    t2 = AzManagers.build_sstemplate("cbox04";
        subscriptionid = "sub-1", admin_username = "u", location = "eastus",
        resourcegroup = "rg-1", imagegallery = "g", imagename = "i",
        vnet = "v", subnet = "s", skuname = "Standard_D2s_v5",
        tags = Dict("env" => "test"))
    @test t2["value"]["tags"]["env"] == "test"

    # Custom osdisksize
    t3 = AzManagers.build_sstemplate("cbox04";
        subscriptionid = "sub-1", admin_username = "u", location = "eastus",
        resourcegroup = "rg-1", imagegallery = "g", imagename = "i",
        vnet = "v", subnet = "s", skuname = "Standard_D2s_v5",
        osdisksize = 128)
    @test t3["value"]["properties"]["virtualMachineProfile"]["storageProfile"]["osDisk"]["diskSizeGB"] == 128

    # Datadisks
    t4 = AzManagers.build_sstemplate("cbox04";
        subscriptionid = "sub-1", admin_username = "u", location = "eastus",
        resourcegroup = "rg-1", imagegallery = "g", imagename = "i",
        vnet = "v", subnet = "s", skuname = "Standard_D2s_v5",
        datadisks = [Dict("diskSizeGB" => 512)])
    disks = t4["value"]["properties"]["virtualMachineProfile"]["storageProfile"]["dataDisks"]
    @test length(disks) == 1
    @test disks[1]["diskSizeGB"] == 512

    # Encryption at host
    t5 = AzManagers.build_sstemplate("cbox04";
        subscriptionid = "sub-1", admin_username = "u", location = "eastus",
        resourcegroup = "rg-1", imagegallery = "g", imagename = "i",
        vnet = "v", subnet = "s", skuname = "Standard_D2s_v5",
        encryption_at_host = true)
    @test t5["value"]["properties"]["virtualMachineProfile"]["securityProfile"]["encryptionAtHost"] == true

    # Cross-subscription image
    t6 = AzManagers.build_sstemplate("cbox04";
        subscriptionid = "sub-1", subscriptionid_image = "sub-img",
        admin_username = "u", location = "eastus",
        resourcegroup = "rg-1", resourcegroup_image = "rg-img",
        imagegallery = "g", imagename = "i",
        vnet = "v", subnet = "s", skuname = "Standard_D2s_v5")
    img_id = t6["value"]["properties"]["virtualMachineProfile"]["storageProfile"]["imageReference"]["id"]
    @test contains(img_id, "sub-img")
    @test contains(img_id, "rg-img")
end

# ═══════════════════════════════════════════════════════════════════════════════
# 14. Template building — VM
# ═══════════════════════════════════════════════════════════════════════════════

@testset "build_vmtemplate" begin
    t = AzManagers.build_vmtemplate("testvm";
        subscriptionid = "sub-1",
        admin_username = "testuser",
        location = "eastus",
        resourcegroup = "rg-1",
        imagegallery = "gallery1",
        imagename = "image1",
        vmsize = "Standard_D4s_v5")

    @test t["subscriptionid"] == "sub-1"
    @test t["resourcegroup"] == "rg-1"
    @test t["value"]["properties"]["hardwareProfile"]["vmSize"] == "Standard_D4s_v5"
    @test t["value"]["properties"]["osProfile"]["computerName"] == "testvm"
    @test t["value"]["properties"]["osProfile"]["adminUsername"] == "testuser"

    # Image reference
    img_id = t["value"]["properties"]["storageProfile"]["imageReference"]["id"]
    @test contains(img_id, "gallery1")
    @test contains(img_id, "image1")

    # No tags by default
    @test !haskey(t["value"], "tags")

    # With tags
    t2 = AzManagers.build_vmtemplate("testvm";
        subscriptionid = "s", admin_username = "u", location = "eastus",
        resourcegroup = "rg", imagegallery = "g", imagename = "i",
        vmsize = "Standard_D2s_v5", tags = Dict("team" => "cofii"))
    @test t2["value"]["tags"]["team"] == "cofii"

    # Datadisks with UltraSSD
    t3 = AzManagers.build_vmtemplate("testvm";
        subscriptionid = "s", admin_username = "u", location = "eastus",
        resourcegroup = "rg", imagegallery = "g", imagename = "i",
        vmsize = "Standard_D2s_v5",
        datadisks = [Dict("diskSizeGB" => 1023, "managedDisk" => Dict("storageAccountType" => "UltraSSD_LRS"))])
    @test t3["value"]["properties"]["additionalCapabilities"]["ultraSSDEnabled"] == true
end

# ═══════════════════════════════════════════════════════════════════════════════
# 15. Template building — NIC
# ═══════════════════════════════════════════════════════════════════════════════

@testset "build_nictemplate" begin
    t = AzManagers.build_nictemplate("cbox-nic";
        subscriptionid = "sub-1",
        resourcegroup_vnet = "rg-vnet",
        vnet = "vnet1",
        subnet = "default",
        location = "eastus")

    @test t["location"] == "eastus"
    @test t["properties"]["enableAcceleratedNetworking"] == true

    subnet_id = t["properties"]["ipConfigurations"][1]["properties"]["subnet"]["id"]
    @test contains(subnet_id, "sub-1")
    @test contains(subnet_id, "vnet1")
    @test contains(subnet_id, "default")

    # Accelerated networking disabled
    t2 = AzManagers.build_nictemplate("cbox-nic";
        subscriptionid = "s", resourcegroup_vnet = "rg",
        vnet = "v", subnet = "s", location = "eastus",
        accelerated = false)
    @test t2["properties"]["enableAcceleratedNetworking"] == false
end

# ═══════════════════════════════════════════════════════════════════════════════
# 16. Manifest file I/O
# ═══════════════════════════════════════════════════════════════════════════════

@testset "write_manifest and load_manifest" begin
    mktempdir() do tmpdir
        # Override the manifest path
        original_home = ENV["HOME"]
        ENV["HOME"] = tmpdir

        try
            AzManagers.write_manifest(;
                resourcegroup = "test-rg",
                subscriptionid = "test-sub",
                ssh_user = "testuser")

            # File should exist
            mpath = joinpath(tmpdir, ".azmanagers", "manifest.json")
            @test isfile(mpath)

            # Permissions should be restrictive
            @test filemode(mpath) & 0o777 == 0o600

            # Parse and verify
            data = JSON.parse(read(mpath, String))
            @test data["resourcegroup"] == "test-rg"
            @test data["subscriptionid"] == "test-sub"
            @test data["ssh_user"] == "testuser"

            # load_manifest should populate the internal dict
            AzManagers.load_manifest()
            @test AzManagers._manifest["resourcegroup"] == "test-rg"
            @test AzManagers._manifest["subscriptionid"] == "test-sub"
        finally
            ENV["HOME"] = original_home
            # Reset manifest state
            for k in keys(AzManagers._manifest)
                AzManagers._manifest[k] = ""
            end
        end
    end
end

# ═══════════════════════════════════════════════════════════════════════════════
# 17. Template save/load (file I/O)
# ═══════════════════════════════════════════════════════════════════════════════

@testset "save_template and template filenames" begin
    mktempdir() do tmpdir
        original_home = ENV["HOME"]
        ENV["HOME"] = tmpdir

        try
            template = AzManagers.build_sstemplate("test01";
                subscriptionid = "s", admin_username = "u", location = "eastus",
                resourcegroup = "rg", imagegallery = "g", imagename = "i",
                vnet = "v", subnet = "s", skuname = "Standard_D2s_v5")

            AzManagers.save_template_scaleset("test01", template)

            fname = AzManagers.templates_filename_scaleset()
            @test isfile(fname)

            saved = JSON.parse(read(fname, String))
            @test haskey(saved, "test01")
            @test saved["test01"]["subscriptionid"] == "s"

            # Save a second template to the same file
            AzManagers.save_template_scaleset("test02", template)
            saved2 = JSON.parse(read(fname, String))
            @test haskey(saved2, "test01")
            @test haskey(saved2, "test02")
        finally
            ENV["HOME"] = original_home
        end
    end
end

# ═══════════════════════════════════════════════════════════════════════════════
# 18. compress/decompress environment round-trip
# ═══════════════════════════════════════════════════════════════════════════════

@testset "compress/decompress environment" begin
    mktempdir() do tmpdir
        # Create a fake environment
        write(joinpath(tmpdir, "Project.toml"), "[deps]\nTest = \"8dfed614-e22c-5e08-85e1-65c5234f0b40\"\n")
        write(joinpath(tmpdir, "Manifest.toml"), "# This file is machine-generated\n")
        write(joinpath(tmpdir, "LocalPreferences.toml"), "[Foo]\nbar = \"baz\"\n")

        pc, mc, lpc = AzManagers.compress_environment(tmpdir)
        @test pc isa String
        @test mc isa String
        @test lpc isa String
        @test !isempty(pc)

        # Decompress into a new temp env
        envname = "test_env_$(rand(1000:9999))"
        AzManagers.decompress_environment(pc, mc, lpc, envname)

        envdir = joinpath(Pkg.envdir(), envname)
        @test isfile(joinpath(envdir, "Project.toml"))
        @test isfile(joinpath(envdir, "Manifest.toml"))
        @test isfile(joinpath(envdir, "LocalPreferences.toml"))

        @test contains(read(joinpath(envdir, "Project.toml"), String), "Test")
        @test contains(read(joinpath(envdir, "LocalPreferences.toml"), String), "bar")

        # Cleanup
        rm(envdir; recursive=true, force=true)
    end

    # Without LocalPreferences
    mktempdir() do tmpdir
        write(joinpath(tmpdir, "Project.toml"), "[deps]\n")
        write(joinpath(tmpdir, "Manifest.toml"), "# manifest\n")

        pc, mc, lpc = AzManagers.compress_environment(tmpdir)
        @test lpc isa String  # empty string compressed

        envname = "test_env_nolp_$(rand(1000:9999))"
        AzManagers.decompress_environment(pc, mc, lpc, envname)

        envdir = joinpath(Pkg.envdir(), envname)
        @test isfile(joinpath(envdir, "Project.toml"))
        @test !isfile(joinpath(envdir, "LocalPreferences.toml"))

        rm(envdir; recursive=true, force=true)
    end
end

# ═══════════════════════════════════════════════════════════════════════════════
# 19. _replace compatibility shim
# ═══════════════════════════════════════════════════════════════════════════════

@testset "_replace" begin
    @test AzManagers._replace("a+b/c", "+" => "plus", "/" => "-") == "aplusb-c"
    @test AzManagers._replace("hello", "x" => "y") == "hello"
    @test AzManagers._replace("foo/bar", "/" => "-") == "foo-bar"
end

# ═══════════════════════════════════════════════════════════════════════════════
# 20. timestamp_metaformatter
# ═══════════════════════════════════════════════════════════════════════════════

@testset "timestamp_metaformatter" begin
    color, prefix, suffix = AzManagers.timestamp_metaformatter(
        Logging.Info, @__MODULE__, :default, :test_id, @__FILE__, @__LINE__)
    @test prefix isa String
    @test !isempty(prefix)
    @test contains(prefix, "Info")
    @test suffix == ""
end

# ═══════════════════════════════════════════════════════════════════════════════
# 21. cloudcfg_nvme_scratch
# ═══════════════════════════════════════════════════════════════════════════════

@testset "cloudcfg_nvme_scratch" begin
    cfg = AzManagers.cloudcfg_nvme_scratch()
    @test contains(cfg, "#cloud-config")
    @test contains(cfg, "nvme")
    @test contains(cfg, "scratch")
end

# ═══════════════════════════════════════════════════════════════════════════════
# 22. @retry macro
# ═══════════════════════════════════════════════════════════════════════════════

@testset "@retry macro" begin
    # Succeeds on first try
    result = AzManagers.@retry 3 begin
        42
    end
    @test result == 42

    # Retries on retryable error, then succeeds
    attempt = Ref(0)
    result = AzManagers.@retry 3 begin
        attempt[] += 1
        if attempt[] < 3
            throw(Base.IOError("transient", 0))
        end
        "ok"
    end
    @test result == "ok"
    @test attempt[] == 3

    # Non-retryable error propagates immediately
    attempt2 = Ref(0)
    @test_throws ArgumentError AzManagers.@retry 5 begin
        attempt2[] += 1
        throw(ArgumentError("bad"))
    end
    @test attempt2[] == 1

    # Exhausts retries and throws
    @test_throws Base.IOError AzManagers.@retry 2 begin
        throw(Base.IOError("persistent", 0))
    end
end

# ═══════════════════════════════════════════════════════════════════════════════
# 23. variablebundle
# ═══════════════════════════════════════════════════════════════════════════════

@testset "variablebundle" begin
    # Clear any existing state
    empty!(AzManagers.VARIABLE_BUNDLE)

    # Set via kwargs
    AzManagers.variablebundle!(x=1.0, y="hello")
    @test AzManagers.variablebundle(:x) == 1.0
    @test AzManagers.variablebundle(:y) == "hello"

    # Set via Dict
    AzManagers.variablebundle!(Dict("z" => 42))
    @test AzManagers.variablebundle(:z) == 42

    # Full bundle
    bundle = AzManagers.variablebundle()
    @test bundle isa Dict
    @test haskey(bundle, :x)
    @test haskey(bundle, :z)

    # String key access (variablebundle(key) converts to Symbol)
    @test AzManagers.variablebundle("x") == 1.0

    # Cleanup
    empty!(AzManagers.VARIABLE_BUNDLE)
end
