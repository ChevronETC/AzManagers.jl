using Test, Sockets, Distributed, Base64, JSON, Dates, Logging, CodecZlib, Pkg, HTTP
using AzManagers

@testset "Local Unit Tests" begin

    # ===== Connection Pipeline =====

    @testset "_read_worker_config: Phase 1 handshake" begin
        cookie = Distributed.cluster_cookie()

        vm = Dict(
            "bind_addr" => "10.0.0.5",
            "ppi" => 1,
            "exeflags" => "--threads=2",
            "userdata" => Dict(
                "subscriptionid" => "sub-123",
                "resourcegroup" => "rg-test",
                "scalesetname" => "ss-test",
                "instanceid" => "42",
                "name" => "worker_042",
                "priority" => "",
                "localid" => 1,
                "mpi" => false,
                "mpi_size" => 0,
                "physical_hostname" => "host42"
            )
        )
        payload = base64encode(JSON.json(vm))

        server = listen(IPv4("127.0.0.1"), 0)
        port = getsockname(server)[2]

        writer = connect(IPv4("127.0.0.1"), port)
        reader = accept(server)

        padded_cookie = rpad(cookie, Distributed.HDR_COOKIE_LEN)[1:Distributed.HDR_COOKIE_LEN]
        write(writer, padded_cookie)
        write(writer, payload * "\n")
        flush(writer)

        wconfig = AzManagers._read_worker_config(reader)

        @test wconfig.bind_addr == "10.0.0.5"
        @test wconfig.count == 1
        @test wconfig.exename == "julia"
        @test wconfig.userdata["instanceid"] == "42"
        @test wconfig.userdata["name"] == "worker_042"
        @test haskey(wconfig.userdata, "_validated_at")
        @test wconfig.userdata["_validated_at"] isa Float64
        @test time() - wconfig.userdata["_validated_at"] < 5.0

        close(writer)
        close(reader)
        close(server)
    end

    @testset "_read_worker_config: bad cookie rejected" begin
        vm = Dict(
            "bind_addr" => "10.0.0.5", "ppi" => 1, "exeflags" => "",
            "userdata" => Dict(
                "subscriptionid" => "s", "resourcegroup" => "r",
                "scalesetname" => "ss", "instanceid" => "1",
                "name" => "w1", "priority" => "", "localid" => 1,
                "mpi" => false, "mpi_size" => 0, "physical_hostname" => "h"
            )
        )
        payload = base64encode(JSON.json(vm))

        server = listen(IPv4("127.0.0.1"), 0)
        port = getsockname(server)[2]
        writer = connect(IPv4("127.0.0.1"), port)
        reader = accept(server)

        bad_cookie = rpad("WRONG_COOKIE_XXX", Distributed.HDR_COOKIE_LEN)[1:Distributed.HDR_COOKIE_LEN]
        write(writer, bad_cookie)
        write(writer, payload * "\n")
        flush(writer)

        @test_throws ErrorException AzManagers._read_worker_config(reader)

        close(writer)
        close(reader)
        close(server)
    end

    @testset "_read_worker_config: ppi > 1" begin
        cookie = Distributed.cluster_cookie()
        vm = Dict(
            "bind_addr" => "10.0.0.10", "ppi" => 4, "exeflags" => "--threads=8",
            "userdata" => Dict(
                "subscriptionid" => "sub", "resourcegroup" => "rg",
                "scalesetname" => "ss", "instanceid" => "7",
                "name" => "worker_007", "priority" => "spot",
                "localid" => 1, "mpi" => false, "mpi_size" => 0,
                "physical_hostname" => "host7"
            )
        )
        payload = base64encode(JSON.json(vm))

        server = listen(IPv4("127.0.0.1"), 0)
        port = getsockname(server)[2]
        writer = connect(IPv4("127.0.0.1"), port)
        reader = accept(server)

        padded_cookie = rpad(cookie, Distributed.HDR_COOKIE_LEN)[1:Distributed.HDR_COOKIE_LEN]
        write(writer, padded_cookie)
        write(writer, payload * "\n")
        flush(writer)

        wconfig = AzManagers._read_worker_config(reader)
        @test wconfig.count == 4
        @test wconfig.exeflags == `--threads=8`
        @test wconfig.userdata["priority"] == "spot"

        close(writer)
        close(reader)
        close(server)
    end

    @testset "validate_connection: timeout on slow socket" begin
        server = listen(IPv4("127.0.0.1"), 0)
        port = getsockname(server)[2]
        writer = connect(IPv4("127.0.0.1"), port)
        reader = accept(server)

        mgr = AzManagers._manager

        result = withenv("JULIA_AZMANAGERS_VALIDATION_TIMEOUT" => "1.0") do
            AzManagers.validate_connection(mgr, reader)
        end

        @test result === nothing

        close(writer)
        close(reader)
        close(server)
    end

    @testset "validate_connection: success path" begin
        cookie = Distributed.cluster_cookie()
        vm = Dict(
            "bind_addr" => "10.0.0.20", "ppi" => 1, "exeflags" => "",
            "userdata" => Dict(
                "subscriptionid" => "s", "resourcegroup" => "r",
                "scalesetname" => "ss", "instanceid" => "20",
                "name" => "w20", "priority" => "", "localid" => 1,
                "mpi" => false, "mpi_size" => 0, "physical_hostname" => "h20"
            )
        )
        payload = base64encode(JSON.json(vm))

        server = listen(IPv4("127.0.0.1"), 0)
        port = getsockname(server)[2]
        writer = connect(IPv4("127.0.0.1"), port)
        reader = accept(server)

        padded_cookie = rpad(cookie, Distributed.HDR_COOKIE_LEN)[1:Distributed.HDR_COOKIE_LEN]
        write(writer, padded_cookie)
        write(writer, payload * "\n")
        flush(writer)

        mgr = AzManagers._manager
        result = AzManagers.validate_connection(mgr, reader)

        @test result !== nothing
        @test result.bind_addr == "10.0.0.20"
        @test result.userdata["instanceid"] == "20"
        @test haskey(result.userdata, "_validated_at")

        close(writer)
        close(reader)
        close(server)
    end

    @testset "_validated_at timestamp pipeline" begin
        cookie = Distributed.cluster_cookie()
        vm = Dict(
            "bind_addr" => "10.0.0.99", "ppi" => 2, "exeflags" => "",
            "userdata" => Dict(
                "subscriptionid" => "sub", "resourcegroup" => "rg",
                "scalesetname" => "ss", "instanceid" => "99",
                "name" => "worker_099", "priority" => "spot",
                "localid" => 1, "mpi" => false, "mpi_size" => 0,
                "physical_hostname" => "host99"
            )
        )
        payload = base64encode(JSON.json(vm))

        server = listen(IPv4("127.0.0.1"), 0)
        port = getsockname(server)[2]
        writer = connect(IPv4("127.0.0.1"), port)
        reader = accept(server)

        padded_cookie = rpad(cookie, Distributed.HDR_COOKIE_LEN)[1:Distributed.HDR_COOKIE_LEN]
        write(writer, padded_cookie)
        write(writer, payload * "\n")
        flush(writer)

        before = time()
        wconfig = AzManagers._read_worker_config(reader)
        after = time()

        validated_at = wconfig.userdata["_validated_at"]
        @test validated_at >= before
        @test validated_at <= after

        # Simulate what setup_launched_worker does
        staleness = time() - validated_at
        @test staleness >= 0
        @test staleness < 5.0

        # Didn't clobber other userdata
        @test wconfig.userdata["instanceid"] == "99"
        @test wconfig.userdata["priority"] == "spot"
        @test wconfig.count == 2

        close(writer)
        close(reader)
        close(server)
    end

    # ===== Worker Cookie Validation =====

    @testset "worker cookie validation logic" begin
        function check_cookie(cookie_from_master, expected_cookie)
            if cookie_from_master[1] == 0x00
                null_count = count(==(0x00), cookie_from_master)
                hex = join(string.(cookie_from_master, base=16, pad=2), " ")
                return (status=:null_cookie, null_count=null_count, hex=hex)
            end
            if String(cookie_from_master) != expected_cookie
                return (status=:invalid_cookie,)
            end
            return (status=:ok,)
        end

        cookie = "TestCookie123456"

        # Valid cookie
        result = check_cookie(Vector{UInt8}(cookie), cookie)
        @test result.status == :ok

        # All null bytes
        result = check_cookie(zeros(UInt8, 16), cookie)
        @test result.status == :null_cookie
        @test result.null_count == 16
        @test result.hex == "00 " ^ 15 * "00"

        # Partial nulls — first byte null
        partial = Vector{UInt8}(cookie)
        partial[1] = 0x00
        result = check_cookie(partial, cookie)
        @test result.status == :null_cookie
        @test result.null_count == 1

        # Wrong cookie, no nulls
        result = check_cookie(Vector{UInt8}("WrongCookie12345"), cookie)
        @test result.status == :invalid_cookie

        # 3-byte null probe
        probe = UInt8[0x00, 0x00, 0x00, 0x41, 0x41, 0x41, 0x41, 0x41,
                      0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41]
        result = check_cookie(probe, cookie)
        @test result.status == :null_cookie
        @test result.null_count == 3
    end

    # ===== Variable Scoping (the tic bug) =====

    @testset "setup_launched_worker: variable scoping" begin
        function scoping_test_success()
            timeout = 2.0
            interrupted = false
            local pid

            u = Dict("instanceid" => "1", "name" => "w1", "scalesetname" => "ss",
                      "_validated_at" => time(), "subscriptionid" => "s", "resourcegroup" => "r")
            validated_at = get(u, "_validated_at", NaN)
            staleness = isnan(validated_at) ? NaN : time() - validated_at

            tic = time()
            try
                pid = 42
            catch e
                elapsed = time() - tic
                return (status=:failed, elapsed=elapsed, staleness=staleness)
            end

            elapsed = time() - tic
            return (status=:ok, pid=pid, elapsed=elapsed, staleness=staleness)
        end

        function scoping_test_failure()
            timeout = 2.0
            interrupted = false
            local pid

            u = Dict("instanceid" => "1", "name" => "w1", "scalesetname" => "ss",
                      "_validated_at" => time(), "subscriptionid" => "s", "resourcegroup" => "r")
            validated_at = get(u, "_validated_at", NaN)
            staleness = isnan(validated_at) ? NaN : time() - validated_at

            tic = time()
            try
                error("simulated failure")
            catch e
                elapsed = time() - tic
                return (status=:failed, elapsed=elapsed, staleness=staleness)
            end

            elapsed = time() - tic
            return (status=:ok, pid=pid, elapsed=elapsed, staleness=staleness)
        end

        result_ok = scoping_test_success()
        @test result_ok.status == :ok
        @test result_ok.pid == 42
        @test result_ok.elapsed >= 0
        @test result_ok.staleness >= 0
        @test result_ok.staleness < 5.0

        result_fail = scoping_test_failure()
        @test result_fail.status == :failed
        @test result_fail.elapsed >= 0
        @test result_fail.staleness >= 0
    end

    # ===== Prune Timeout Arithmetic =====

    @testset "prune timeout alignment" begin
        # Case 1: join_timeout dominates
        timeout1 = withenv("JULIA_AZMANAGERS_VM_JOIN_TIMEOUT" => "720") do
            _join_timeout = parse(Int, get(ENV, "JULIA_AZMANAGERS_VM_JOIN_TIMEOUT", "720"))
            Second(max(_join_timeout, ceil(Int, Distributed.worker_timeout()) + 30))
        end
        @test timeout1 >= Second(720)

        # Case 2: worker_timeout + 30 dominates
        timeout2 = withenv("JULIA_AZMANAGERS_VM_JOIN_TIMEOUT" => "50",
                           "JULIA_WORKER_TIMEOUT" => "800") do
            _join_timeout = parse(Int, get(ENV, "JULIA_AZMANAGERS_VM_JOIN_TIMEOUT", "50"))
            wt = parse(Float64, get(ENV, "JULIA_WORKER_TIMEOUT", "60"))
            Second(max(_join_timeout, ceil(Int, wt) + 30))
        end
        @test timeout2 == Second(830)
        @test timeout2 > Second(50)

        # Case 3: production config where old code was wrong
        timeout3 = withenv("JULIA_AZMANAGERS_VM_JOIN_TIMEOUT" => "720",
                           "JULIA_WORKER_TIMEOUT" => "800") do
            _join_timeout = parse(Int, get(ENV, "JULIA_AZMANAGERS_VM_JOIN_TIMEOUT", "720"))
            wt = parse(Float64, get(ENV, "JULIA_WORKER_TIMEOUT", "800"))
            Second(max(_join_timeout, ceil(Int, wt) + 30))
        end
        @test timeout3 == Second(830)
        @test timeout3 > Second(720)
    end

    # ===== ScaleSet struct =====

    @testset "ScaleSet: lowercasing" begin
        ss = AzManagers.ScaleSet("SUB-123", "RG-Test", "SS-Name")
        @test ss.subscriptionid == "sub-123"
        @test ss.resourcegroup == "rg-test"
        @test ss.scalesetname == "ss-name"
    end

    @testset "ScaleSet: equality and hashing" begin
        ss1 = AzManagers.ScaleSet("sub", "rg", "ss")
        ss2 = AzManagers.ScaleSet("SUB", "RG", "SS")
        @test ss1.subscriptionid == ss2.subscriptionid
        @test ss1.resourcegroup == ss2.resourcegroup
        @test ss1.scalesetname == ss2.scalesetname
    end

    # ===== Instance List Management =====

    @testset "add_instance_to_pending_down_list" begin
        mgr = AzManagers.AzManager()
        mgr.pending_down = Dict{AzManagers.ScaleSet,Set{String}}()
        ss = AzManagers.ScaleSet("sub", "rg", "ss1")

        AzManagers.add_instance_to_pending_down_list(mgr, ss, "42")
        @test haskey(mgr.pending_down, ss)
        @test "42" ∈ mgr.pending_down[ss]

        AzManagers.add_instance_to_pending_down_list(mgr, ss, "43")
        @test "42" ∈ mgr.pending_down[ss]
        @test "43" ∈ mgr.pending_down[ss]
        @test length(mgr.pending_down[ss]) == 2

        # Duplicate — set semantics
        AzManagers.add_instance_to_pending_down_list(mgr, ss, "42")
        @test length(mgr.pending_down[ss]) == 2

        # Different scaleset
        ss2 = AzManagers.ScaleSet("sub", "rg", "ss2")
        AzManagers.add_instance_to_pending_down_list(mgr, ss2, "1")
        @test haskey(mgr.pending_down, ss2)
        @test length(mgr.pending_down[ss2]) == 1
    end

    @testset "add_instance_to_pruned_list" begin
        mgr = AzManagers.AzManager()
        mgr.pruned = Dict{AzManagers.ScaleSet,Set{String}}()
        ss = AzManagers.ScaleSet("sub", "rg", "ss1")

        AzManagers.add_instance_to_pruned_list(mgr, ss, "10")
        @test "10" ∈ mgr.pruned[ss]

        AzManagers.add_instance_to_pruned_list(mgr, ss, "11")
        @test length(mgr.pruned[ss]) == 2
    end

    @testset "add_instance_to_deleted_list" begin
        mgr = AzManagers.AzManager()
        mgr.deleted = Dict{AzManagers.ScaleSet,Dict{String,DateTime}}()
        ss = AzManagers.ScaleSet("sub", "rg", "ss1")

        AzManagers.add_instance_to_deleted_list(mgr, ss, "5")
        @test haskey(mgr.deleted, ss)
        @test haskey(mgr.deleted[ss], "5")
        @test mgr.deleted[ss]["5"] isa DateTime

        t1 = mgr.deleted[ss]["5"]
        sleep(0.01)
        AzManagers.add_instance_to_deleted_list(mgr, ss, "6")
        @test mgr.deleted[ss]["6"] >= t1
    end

    @testset "add_instance_to_preempted_list" begin
        mgr = AzManagers.AzManager()
        mgr.preempted = Dict{AzManagers.ScaleSet,Set{String}}()
        ss = AzManagers.ScaleSet("sub", "rg", "ss1")

        AzManagers.add_instance_to_preempted_list(mgr, ss, "7")
        @test "7" ∈ mgr.preempted[ss]

        AzManagers.add_instance_to_preempted_list(mgr, ss, "8")
        @test length(mgr.preempted[ss]) == 2
    end

    @testset "ispreempted" begin
        mgr = AzManagers.AzManager()
        mgr.preempted = Dict{AzManagers.ScaleSet,Set{String}}()
        ss = AzManagers.ScaleSet("sub", "rg", "ss1")
        mgr.preempted[ss] = Set(["10", "20"])

        wconfig = WorkerConfig()
        wconfig.userdata = Dict(
            "subscriptionid" => "sub", "resourcegroup" => "rg",
            "scalesetname" => "ss1", "instanceid" => "10"
        )
        @test AzManagers.ispreempted(mgr, wconfig) == true

        wconfig2 = WorkerConfig()
        wconfig2.userdata = Dict(
            "subscriptionid" => "sub", "resourcegroup" => "rg",
            "scalesetname" => "ss1", "instanceid" => "99"
        )
        @test AzManagers.ispreempted(mgr, wconfig2) == false

        # Non-existent scaleset
        wconfig3 = WorkerConfig()
        wconfig3.userdata = Dict(
            "subscriptionid" => "sub", "resourcegroup" => "rg",
            "scalesetname" => "other-ss", "instanceid" => "10"
        )
        @test AzManagers.ispreempted(mgr, wconfig3) == false
    end

    # ===== Retry / Error Classification =====

    @testset "isretryable" begin
        for code in (409, 429, 500)
            req = HTTP.Request("GET", "/")
            resp = HTTP.Response(code)
            e = HTTP.Exceptions.StatusError(code, req.method, req.target, resp)
            @test AzManagers.isretryable(e) == true
        end

        for code in (400, 401, 403, 404)
            req = HTTP.Request("GET", "/")
            resp = HTTP.Response(code)
            e = HTTP.Exceptions.StatusError(code, req.method, req.target, resp)
            @test AzManagers.isretryable(e) == false
        end

        @test AzManagers.isretryable(Base.IOError("test", 0)) == true
        @test AzManagers.isretryable(Base.EOFError()) == true

        @test AzManagers.isretryable(ErrorException("nope")) == false
        @test AzManagers.isretryable(ArgumentError("bad")) == false
    end

    @testset "status helper" begin
        req = HTTP.Request("GET", "/")
        resp = HTTP.Response(429)
        e = HTTP.Exceptions.StatusError(429, req.method, req.target, resp)
        @test AzManagers.status(e) == 429

        @test AzManagers.status(ErrorException("test")) == 999
        @test AzManagers.status(nothing) == 999
    end

    # ===== Utility Functions =====

    @testset "nthreads_filter" begin
        @test AzManagers.nthreads_filter(4) == "4"
        @test AzManagers.nthreads_filter(1) == "1"
        @test AzManagers.nthreads_filter("4,2") == "4,2"
        @test AzManagers.nthreads_filter("8,0") == "8"
        @test AzManagers.nthreads_filter("16") == "16"
    end

    @testset "spin" begin
        @test startswith(AzManagers.spin(1, 0.0), "◐")
        @test startswith(AzManagers.spin(2, 0.0), "◓")
        @test startswith(AzManagers.spin(3, 0.0), "◑")
        @test startswith(AzManagers.spin(4, 0.0), "◒")
        @test startswith(AzManagers.spin(5, 0.0), "✓")
        @test contains(AzManagers.spin(1, 12.345), "12.35")
        @test contains(AzManagers.spin(5, 99.0), "99.00")
        @test endswith(AzManagers.spin(1, 0.0), "seconds")
    end

    @testset "logerror" begin
        try
            error("test error for logerror")
        catch e
            @test nothing === AzManagers.logerror(e, Logging.Debug)
        end
    end

    @testset "build_envstring" begin
        @test AzManagers.build_envstring(Dict()) == ""
        result = AzManagers.build_envstring(Dict("FOO" => "bar"))
        @test contains(result, "export FOO=bar")
        @test endswith(result, "\n")

        result2 = AzManagers.build_envstring(Dict("A" => "1", "B" => "2"))
        @test contains(result2, "export A=1\n")
        @test contains(result2, "export B=2\n")
    end

    @testset "remaining_resource" begin
        r = HTTP.Response(200)
        push!(r.headers, "Content-Type" => "application/json")
        push!(r.headers, "x-ms-ratelimit-remaining-resource" => "Microsoft.Compute/GetVMScaleSet3Min;238")
        @test AzManagers.remaining_resource(r) == "Microsoft.Compute/GetVMScaleSet3Min;238"

        r2 = HTTP.Response(200)
        push!(r2.headers, "Content-Type" => "application/json")
        @test AzManagers.remaining_resource(r2) == ""

        r3 = HTTP.Response(200)
        push!(r3.headers, "x-ms-ratelimit-remaining-resource" => "first")
        push!(r3.headers, "x-ms-ratelimit-remaining-resource" => "second")
        @test AzManagers.remaining_resource(r3) == "second"
    end

    @testset "timestamp_metaformatter" begin
        color, prefix, suffix = AzManagers.timestamp_metaformatter(
            Logging.Info, AzManagers, :test, :test_id, "test.jl", 1)
        @test contains(prefix, "Info:")
        @test occursin(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}", prefix)
        @test suffix == ""

        _, warn_prefix, _ = AzManagers.timestamp_metaformatter(
            Logging.Warn, AzManagers, :test, :test_id, "test.jl", 1)
        @test contains(warn_prefix, "Warning:")

        _, error_prefix, _ = AzManagers.timestamp_metaformatter(
            Logging.Error, AzManagers, :test, :test_id, "test.jl", 1)
        @test contains(error_prefix, "Error:")
    end

    # ===== Manifest =====

    @testset "manifest read/write roundtrip" begin
        mktempdir() do tmpdir
            test_manifest_path = joinpath(tmpdir, ".azmanagers")
            test_manifest_file = joinpath(test_manifest_path, "manifest.json")

            mkpath(test_manifest_path)
            manifest_data = Dict(
                "resourcegroup" => "test-rg",
                "subscriptionid" => "test-sub-123",
                "ssh_user" => "testuser",
                "ssh_private_key_file" => "/tmp/fake_key",
                "ssh_public_key_file" => "/tmp/fake_key.pub"
            )
            write(test_manifest_file, JSON.json(manifest_data))

            parsed = JSON.parse(read(test_manifest_file, String))
            @test parsed["resourcegroup"] == "test-rg"
            @test parsed["subscriptionid"] == "test-sub-123"
            @test parsed["ssh_user"] == "testuser"
        end
    end

    # ===== Template Builders =====

    @testset "build_sstemplate" begin
        template = AzManagers.build_sstemplate("test-ss";
            subscriptionid="sub-123",
            admin_username="testuser",
            location="eastus",
            resourcegroup="rg-test",
            imagegallery="gallery1",
            imagename="myimage",
            vnet="vnet1",
            subnet="subnet1",
            skuname="Standard_HB120rs_v3"
        )

        @test template["subscriptionid"] == "sub-123"
        @test template["resourcegroup"] == "rg-test"
        @test template["value"]["location"] == "eastus"
        @test template["value"]["sku"]["name"] == "Standard_HB120rs_v3"
        @test template["value"]["sku"]["tier"] == "Standard"

        profile = template["value"]["properties"]["virtualMachineProfile"]
        @test profile["storageProfile"]["osDisk"]["diskSizeGB"] == 60
        @test profile["storageProfile"]["dataDisks"] == []
        @test profile["osProfile"]["adminUsername"] == "testuser"
        @test profile["securityProfile"]["encryptionAtHost"] == false

        imgref = profile["storageProfile"]["imageReference"]["id"]
        @test contains(imgref, "sub-123")
        @test contains(imgref, "gallery1")
        @test contains(imgref, "myimage")

        subnet_id = profile["networkProfile"]["networkInterfaceConfigurations"][1]["properties"]["ipConfigurations"][1]["properties"]["subnet"]["id"]
        @test contains(subnet_id, "vnet1")
        @test contains(subnet_id, "subnet1")
    end

    @testset "build_sstemplate: with datadisks" begin
        template = AzManagers.build_sstemplate("test-dd";
            subscriptionid="sub", admin_username="user", location="westus",
            resourcegroup="rg", imagegallery="gal", imagename="img",
            vnet="v", subnet="s", skuname="Standard_D4s_v3",
            datadisks=[Dict("diskSizeGB" => 512)]
        )
        profile = template["value"]["properties"]["virtualMachineProfile"]
        @test length(profile["storageProfile"]["dataDisks"]) == 1
        @test profile["storageProfile"]["dataDisks"][1]["diskSizeGB"] == 512
        @test profile["storageProfile"]["dataDisks"][1]["lun"] == 1
    end

    @testset "build_sstemplate: with UltraSSD" begin
        template = AzManagers.build_sstemplate("test-ultra";
            subscriptionid="sub", admin_username="user", location="westus",
            resourcegroup="rg", imagegallery="gal", imagename="img",
            vnet="v", subnet="s", skuname="Standard_L8s_v2",
            datadisks=[Dict("diskSizeGB" => 2048, "managedDisk" => Dict("storageAccountType" => "UltraSSD_LRS"))]
        )
        @test template["value"]["properties"]["additionalCapabilities"]["ultraSSDEnabled"] == true
    end

    @testset "build_sstemplate: with tags" begin
        template = AzManagers.build_sstemplate("test-tags";
            subscriptionid="sub", admin_username="user", location="westus",
            resourcegroup="rg", imagegallery="gal", imagename="img",
            vnet="v", subnet="s", skuname="Standard_D4s_v3",
            tags=Dict("env" => "test", "team" => "hpc")
        )
        @test template["value"]["tags"]["env"] == "test"
        @test template["value"]["tags"]["team"] == "hpc"
    end

    @testset "build_sstemplate: no tags → no tags key" begin
        template = AzManagers.build_sstemplate("test-notags";
            subscriptionid="sub", admin_username="user", location="westus",
            resourcegroup="rg", imagegallery="gal", imagename="img",
            vnet="v", subnet="s", skuname="Standard_D4s_v3"
        )
        @test !haskey(template["value"], "tags")
    end

    @testset "build_sstemplate: cross-subscription image" begin
        template = AzManagers.build_sstemplate("test-cross";
            subscriptionid="sub-main",
            subscriptionid_image="sub-images",
            admin_username="user", location="eastus",
            resourcegroup="rg",
            resourcegroup_image="rg-images",
            imagegallery="gal", imagename="img",
            vnet="v", subnet="s", skuname="Standard_D4s_v3"
        )
        profile = template["value"]["properties"]["virtualMachineProfile"]
        imgref = profile["storageProfile"]["imageReference"]["id"]
        @test contains(imgref, "sub-images")
        @test contains(imgref, "rg-images")
    end

    @testset "build_sstemplate: encryption_at_host" begin
        template = AzManagers.build_sstemplate("test-enc";
            subscriptionid="sub", admin_username="user", location="westus",
            resourcegroup="rg", imagegallery="gal", imagename="img",
            vnet="v", subnet="s", skuname="Standard_D4s_v3",
            encryption_at_host=true
        )
        profile = template["value"]["properties"]["virtualMachineProfile"]
        @test profile["securityProfile"]["encryptionAtHost"] == true
    end

    @testset "build_sstemplate: multiple datadisks" begin
        template = AzManagers.build_sstemplate("test-multi-dd";
            subscriptionid="sub", admin_username="user", location="westus",
            resourcegroup="rg", imagegallery="gal", imagename="img",
            vnet="v", subnet="s", skuname="Standard_D4s_v3",
            datadisks=[Dict("diskSizeGB" => 256), Dict("diskSizeGB" => 512)]
        )
        profile = template["value"]["properties"]["virtualMachineProfile"]
        dds = profile["storageProfile"]["dataDisks"]
        @test length(dds) == 2
        @test dds[1]["lun"] == 1
        @test dds[2]["lun"] == 2
        @test dds[1]["diskSizeGB"] == 256
        @test dds[2]["diskSizeGB"] == 512
    end

    @testset "build_nictemplate" begin
        nic = AzManagers.build_nictemplate("test-nic";
            subscriptionid="sub-123",
            resourcegroup_vnet="rg-vnet",
            vnet="myvnet",
            subnet="mysubnet",
            location="eastus2"
        )

        @test nic["location"] == "eastus2"
        @test nic["properties"]["enableAcceleratedNetworking"] == true

        subnet_id = nic["properties"]["ipConfigurations"][1]["properties"]["subnet"]["id"]
        @test contains(subnet_id, "myvnet")
        @test contains(subnet_id, "mysubnet")
    end

    @testset "build_nictemplate: accelerated=false" begin
        nic = AzManagers.build_nictemplate("test-nic-slow";
            subscriptionid="sub", resourcegroup_vnet="rg",
            vnet="v", subnet="s", location="westus",
            accelerated=false
        )
        @test nic["properties"]["enableAcceleratedNetworking"] == false
    end

    @testset "build_vmtemplate" begin
        template = AzManagers.build_vmtemplate("test-vm";
            subscriptionid="sub-123",
            admin_username="testuser",
            location="eastus",
            resourcegroup="rg-test",
            imagegallery="gallery1",
            imagename="myimage",
            vmsize="Standard_D8s_v3"
        )

        @test template["subscriptionid"] == "sub-123"
        @test template["resourcegroup"] == "rg-test"
        @test template["value"]["location"] == "eastus"

        props = template["value"]["properties"]
        @test props["hardwareProfile"]["vmSize"] == "Standard_D8s_v3"
        @test props["storageProfile"]["osDisk"]["diskSizeGB"] == 60
        @test props["osProfile"]["computerName"] == "test-vm"
        @test props["osProfile"]["adminUsername"] == "testuser"
    end

    @testset "build_vmtemplate: with datadisks and tags" begin
        template = AzManagers.build_vmtemplate("test-vm-dd";
            subscriptionid="sub", admin_username="user", location="westus",
            resourcegroup="rg", imagegallery="gal", imagename="img",
            vmsize="Standard_D4s_v3",
            datadisks=[Dict("diskSizeGB" => 256)],
            tags=Dict("project" => "test")
        )
        props = template["value"]["properties"]
        @test length(props["storageProfile"]["dataDisks"]) == 1
        @test props["storageProfile"]["dataDisks"][1]["name"] == "scratch1"
        @test template["value"]["tags"]["project"] == "test"
    end

    @testset "save_template and load" begin
        mktempdir() do tmpdir
            fname = joinpath(tmpdir, "test_templates.json")

            AzManagers.save_template(fname, "tmpl1", Dict("a" => 1))
            saved = JSON.parse(read(fname, String))
            @test saved["tmpl1"]["a"] == 1

            AzManagers.save_template(fname, "tmpl2", Dict("b" => 2))
            saved2 = JSON.parse(read(fname, String))
            @test saved2["tmpl1"]["a"] == 1
            @test saved2["tmpl2"]["b"] == 2

            # Overwrite existing
            AzManagers.save_template(fname, "tmpl1", Dict("a" => 99))
            saved3 = JSON.parse(read(fname, String))
            @test saved3["tmpl1"]["a"] == 99
        end
    end

    @testset "_replace" begin
        @test AzManagers._replace("foo+bar/baz", "+"=>"plus", "/"=>"-") == "fooplusbar-baz"
        @test AzManagers._replace("hello", "x"=>"y") == "hello"
        @test AzManagers._replace("a/b/c", "/"=>"-") == "a-b-c"
    end

    @testset "templates_folder and filenames" begin
        @test AzManagers.templates_folder() == joinpath(homedir(), ".azmanagers")
        @test endswith(AzManagers.templates_filename_scaleset(), "templates_scaleset.json")
        @test endswith(AzManagers.templates_filename_nic(), "templates_nic.json")
        @test endswith(AzManagers.templates_filename_vm(), "templates_vm.json")
    end

    # ===== Environment Compression =====

    @testset "compress/decompress roundtrip" begin
        mktempdir() do srcdir
            write(joinpath(srcdir, "Project.toml"), "[deps]\nFoo = \"abc-123\"\n")
            write(joinpath(srcdir, "Manifest.toml"), "# Manifest\n[[deps.Foo]]\nuuid = \"abc-123\"\n")
            write(joinpath(srcdir, "LocalPreferences.toml"), "[FooPackage]\nfoo = \"bar\"\n")

            pc, mc, lc = AzManagers.compress_environment(srcdir)

            @test pc isa String
            @test mc isa String
            @test lc isa String
            @test !isempty(pc)
            @test !isempty(mc)
            @test !isempty(lc)

            # Verify roundtrip via manual decompress
            text_p = String(CodecZlib.transcode(CodecZlib.ZlibDecompressor, Vector{UInt8}(base64decode(pc))))
            text_m = String(CodecZlib.transcode(CodecZlib.ZlibDecompressor, Vector{UInt8}(base64decode(mc))))
            text_l = String(CodecZlib.transcode(CodecZlib.ZlibDecompressor, Vector{UInt8}(base64decode(lc))))

            @test contains(text_p, "Foo")
            @test contains(text_m, "abc-123")
            @test contains(text_l, "bar")
        end
    end

    @testset "compress_environment: no LocalPreferences" begin
        mktempdir() do srcdir
            write(joinpath(srcdir, "Project.toml"), "[deps]\n")
            write(joinpath(srcdir, "Manifest.toml"), "# empty\n")

            pc, mc, lc = AzManagers.compress_environment(srcdir)
            @test !isempty(pc)
            @test !isempty(mc)

            text_l = String(CodecZlib.transcode(CodecZlib.ZlibDecompressor, Vector{UInt8}(base64decode(lc))))
            @test text_l == ""
        end
    end

    # ===== addprocs_with_timeout =====

    @testset "addprocs_with_timeout: empty wconfigs" begin
        mgr = AzManagers._manager
        pids = AzManagers.addprocs_with_timeout(mgr; wconfigs=WorkerConfig[])
        @test pids isa Vector
        @test isempty(pids)
    end

end
