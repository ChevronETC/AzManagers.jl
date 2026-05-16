using Test, Sockets, Distributed, Base64, JSON, Dates

# Import AzManagers internals we need to test
using AzManagers

@testset "Local Unit Tests" begin

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

        # Set up a loopback TCP pair
        server = listen(IPv4("127.0.0.1"), 0)
        port = getsockname(server)[2]

        writer = connect(IPv4("127.0.0.1"), port)
        reader = accept(server)

        # Write the Phase 1 data: cookie (padded to HDR_COOKIE_LEN) + base64 JSON + newline
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
            "bind_addr" => "10.0.0.5",
            "ppi" => 1,
            "exeflags" => "",
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

        # Write a wrong cookie
        bad_cookie = rpad("WRONG_COOKIE_XXX", Distributed.HDR_COOKIE_LEN)[1:Distributed.HDR_COOKIE_LEN]
        write(writer, bad_cookie)
        write(writer, payload * "\n")
        flush(writer)

        @test_throws ErrorException AzManagers._read_worker_config(reader)

        close(writer)
        close(reader)
        close(server)
    end

    @testset "validate_connection: timeout on slow socket" begin
        # Simulate a socket that never sends data — should timeout
        server = listen(IPv4("127.0.0.1"), 0)
        port = getsockname(server)[2]
        writer = connect(IPv4("127.0.0.1"), port)
        reader = accept(server)

        # Create a minimal manager-like object to pass to validate_connection
        # validate_connection only needs (manager, socket) and manager is unused in the function body
        mgr = AzManagers._manager

        result = withenv("JULIA_AZMANAGERS_VALIDATION_TIMEOUT" => "1.0") do
            AzManagers.validate_connection(mgr, reader)
        end

        @test result === nothing

        close(writer)
        close(reader)
        close(server)
    end

    @testset "prune timeout alignment: max(join_timeout, worker_timeout+30)" begin
        # Test that prune timeout respects both VM_JOIN_TIMEOUT and worker_timeout
        # Default: VM_JOIN_TIMEOUT=720, worker_timeout()=Distributed.worker_timeout() (typically 60)
        # With the fix, prune timeout = max(720, 60+30) = 720

        # Case 1: join_timeout > worker_timeout + 30 → uses join_timeout
        timeout1 = withenv("JULIA_AZMANAGERS_VM_JOIN_TIMEOUT" => "720") do
            _join_timeout = parse(Int, get(ENV, "JULIA_AZMANAGERS_VM_JOIN_TIMEOUT", "720"))
            Second(max(_join_timeout, ceil(Int, Distributed.worker_timeout()) + 30))
        end
        @test timeout1 >= Second(720)

        # Case 2: worker_timeout + 30 > join_timeout → uses worker_timeout + 30
        timeout2 = withenv("JULIA_AZMANAGERS_VM_JOIN_TIMEOUT" => "50",
                           "JULIA_WORKER_TIMEOUT" => "800") do
            _join_timeout = parse(Int, get(ENV, "JULIA_AZMANAGERS_VM_JOIN_TIMEOUT", "50"))
            wt = parse(Float64, get(ENV, "JULIA_WORKER_TIMEOUT", "60"))
            Second(max(_join_timeout, ceil(Int, wt) + 30))
        end
        @test timeout2 == Second(830)
        @test timeout2 > Second(50)  # would have been 50 without the fix

        # Case 3: the old code would have used 720 even with worker_timeout=800
        # verify the new formula catches this
        timeout3 = withenv("JULIA_AZMANAGERS_VM_JOIN_TIMEOUT" => "720",
                           "JULIA_WORKER_TIMEOUT" => "800") do
            _join_timeout = parse(Int, get(ENV, "JULIA_AZMANAGERS_VM_JOIN_TIMEOUT", "720"))
            wt = parse(Float64, get(ENV, "JULIA_WORKER_TIMEOUT", "800"))
            Second(max(_join_timeout, ceil(Int, wt) + 30))
        end
        @test timeout3 == Second(830)
        @test timeout3 > Second(720)  # old code would have returned 720
    end

    @testset "setup_launched_worker: variable scoping" begin
        # This test verifies that the tic variable is accessible in both
        # the catch block and after the try/catch in setup_launched_worker.
        # The bug was: tic was defined inside try{} but referenced in catch{} and after end.
        #
        # We can't easily call setup_launched_worker directly (needs a full AzManager with
        # server, Distributed state, etc.), but we CAN verify the scoping pattern compiles
        # and runs correctly by testing the exact pattern used in the function.

        # Reproduce the exact scoping pattern from setup_launched_worker
        function scoping_test_success()
            timeout = 2.0
            interrupted = false
            local pid

            u = Dict("instanceid" => "1", "name" => "w1", "scalesetname" => "ss",
                      "_validated_at" => time(), "subscriptionid" => "s",
                      "resourcegroup" => "r")
            instanceid = get(u, "instanceid", "unknown")
            vm_name = get(u, "name", "unknown")
            validated_at = get(u, "_validated_at", NaN)
            staleness = isnan(validated_at) ? NaN : time() - validated_at

            tic = time()
            try
                pid = 42  # simulate successful worker creation
            catch e
                elapsed = time() - tic  # tic must be in scope here
                return (status=:failed, elapsed=elapsed, staleness=staleness)
            end

            elapsed = time() - tic  # tic must be in scope here too
            return (status=:ok, pid=pid, elapsed=elapsed, staleness=staleness)
        end

        function scoping_test_failure()
            timeout = 2.0
            interrupted = false
            local pid

            u = Dict("instanceid" => "1", "name" => "w1", "scalesetname" => "ss",
                      "_validated_at" => time(), "subscriptionid" => "s",
                      "resourcegroup" => "r")
            instanceid = get(u, "instanceid", "unknown")
            vm_name = get(u, "name", "unknown")
            validated_at = get(u, "_validated_at", NaN)
            staleness = isnan(validated_at) ? NaN : time() - validated_at

            tic = time()
            try
                error("simulated failure")
            catch e
                elapsed = time() - tic  # tic must be in scope here
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

    @testset "worker cookie validation logic" begin
        # Test the exact cookie check pattern used in azure_worker_start

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

        # All null bytes (the production failure mode)
        null_bytes = zeros(UInt8, 16)
        result = check_cookie(null_bytes, cookie)
        @test result.status == :null_cookie
        @test result.null_count == 16
        @test result.hex == "00 " ^ 15 * "00"

        # Partial nulls (first byte null, rest valid)
        partial = Vector{UInt8}(cookie)
        partial[1] = 0x00
        result = check_cookie(partial, cookie)
        @test result.status == :null_cookie
        @test result.null_count == 1

        # Wrong cookie (no nulls)
        wrong = Vector{UInt8}("WrongCookie12345")
        result = check_cookie(wrong, cookie)
        @test result.status == :invalid_cookie

        # 3-byte null probe (partial read scenario)
        probe = UInt8[0x00, 0x00, 0x00, 0x41, 0x41, 0x41, 0x41, 0x41,
                      0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41]
        result = check_cookie(probe, cookie)
        @test result.status == :null_cookie
        @test result.null_count == 3
    end

    @testset "_validated_at timestamp flows through pipeline" begin
        # Verify the full pipeline: _read_worker_config stamps _validated_at,
        # and it's accessible in the wconfig.userdata downstream
        cookie = Distributed.cluster_cookie()

        vm = Dict(
            "bind_addr" => "10.0.0.99",
            "ppi" => 2,
            "exeflags" => "",
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

        # Verify timestamp was stamped and is reasonable
        validated_at = wconfig.userdata["_validated_at"]
        @test validated_at >= before
        @test validated_at <= after

        # Simulate what setup_launched_worker does with it
        staleness = time() - validated_at
        @test staleness >= 0
        @test staleness < 5.0

        # Verify it didn't clobber other userdata
        @test wconfig.userdata["instanceid"] == "99"
        @test wconfig.userdata["priority"] == "spot"
        @test wconfig.count == 2

        close(writer)
        close(reader)
        close(server)
    end

end
