using Test, Distributed, Sockets, Base64, JSON

# Load AzManagers without triggering full Azure init
using AzManagers

const COOKIE = Distributed.cluster_cookie()
const HDR_COOKIE_LEN = Distributed.HDR_COOKIE_LEN

"""
Simulate a worker connecting to the coordinator and sending the expected protocol:
1. Write cluster cookie (HDR_COOKIE_LEN bytes)
2. Write base64-encoded JSON connection string + newline
"""
function fake_worker_connect(port; delay=0.0, send_cookie=true, bad_cookie=false, send_connstr=true, ppi=1)
    sock = connect(getipaddr(), port)
    if delay > 0
        sleep(delay)
    end
    if send_cookie
        cookie = bad_cookie ? repeat("X", HDR_COOKIE_LEN) : COOKIE
        write(sock, cookie)
    end
    if send_connstr && send_cookie && !bad_cookie
        connstr = JSON.json(Dict(
            "bind_addr" => string(getipaddr()),
            "ppi" => ppi,
            "exeflags" => "--worker",
            "userdata" => Dict(
                "subscriptionid" => "test-sub",
                "resourcegroup" => "test-rg",
                "scalesetname" => "test-ss",
                "instanceid" => "test-$(rand(1000:9999))",
                "priority" => "regular"
            )
        ))
        write(sock, base64encode(connstr) * "\n")
    end
    sock
end

@testset "Connection Validation" begin
    # Start a local TCP server (simulating the coordinator)
    port, server = listenany(getipaddr(), 19000)

    @testset "validate_connection - good socket" begin
        # Spawn a fake worker that sends immediately
        client_task = @async fake_worker_connect(port; delay=0.0)

        sock = accept(server)
        # Use a mock manager (we only need it for the warn path)
        wconfig = AzManagers.validate_connection(nothing, sock)

        @test wconfig !== nothing
        @test wconfig.bind_addr == string(getipaddr())
        @test wconfig.count == 1
        @test wconfig.exename == "julia"
        @test wconfig.userdata["scalesetname"] == "test-ss"

        # Clean up client
        close(fetch(client_task))
    end

    @testset "validate_connection - slow socket (2s delay)" begin
        # Worker that delays 2s before sending — should still succeed (timeout is 30s)
        client_task = @async fake_worker_connect(port; delay=2.0)

        sock = accept(server)
        t0 = time()
        wconfig = AzManagers.validate_connection(nothing, sock)
        elapsed = time() - t0

        @test wconfig !== nothing
        @test elapsed >= 1.5  # confirm it actually waited
        @test wconfig.bind_addr == string(getipaddr())

        close(fetch(client_task))
    end

    @testset "validate_connection - dead socket (never sends)" begin
        # Worker connects but never sends anything — should timeout and return nothing
        # Use a short timeout for testing
        withenv("JULIA_AZMANAGERS_VALIDATION_TIMEOUT" => "2.0") do
            client_task = @async begin
                sock = connect(getipaddr(), port)
                sleep(10)  # hold connection open but never send
                sock
            end

            sock = accept(server)
            t0 = time()
            wconfig = AzManagers.validate_connection(nothing, sock)
            elapsed = time() - t0

            @test wconfig === nothing
            @test elapsed >= 1.5
            @test elapsed < 5.0  # should timeout at ~2s, not wait forever

            close(fetch(client_task))
        end
    end

    @testset "validate_connection - bad cookie" begin
        client_task = @async fake_worker_connect(port; bad_cookie=true)

        sock = accept(server)
        wconfig = AzManagers.validate_connection(nothing, sock)

        @test wconfig === nothing

        close(fetch(client_task))
    end

    @testset "validate_connection - ppi > 1" begin
        client_task = @async fake_worker_connect(port; ppi=4)

        sock = accept(server)
        wconfig = AzManagers.validate_connection(nothing, sock)

        @test wconfig !== nothing
        @test wconfig.count == 4

        close(fetch(client_task))
    end

    @testset "concurrent validations (32 workers, 4 bad)" begin
        n_good = 28
        n_bad = 4
        n_total = n_good + n_bad

        withenv("JULIA_AZMANAGERS_VALIDATION_TIMEOUT" => "3.0") do
            # Spawn all fake workers
            client_tasks = []
            for i in 1:n_good
                push!(client_tasks, @async fake_worker_connect(port; delay=rand()*0.5))
            end
            for i in 1:n_bad
                push!(client_tasks, @async begin
                    sock = connect(getipaddr(), port)
                    sleep(10)  # dead socket
                    sock
                end)
            end

            # Accept and validate all concurrently (as the real code does)
            results = Channel{Union{Nothing, WorkerConfig}}(n_total)
            t0 = time()
            @sync for i in 1:n_total
                @async begin
                    sock = accept(server)
                    wconfig = AzManagers.validate_connection(nothing, sock)
                    put!(results, wconfig)
                end
            end
            elapsed = time() - t0
            close(results)

            validated = filter(!isnothing, collect(results))

            @test length(validated) == n_good
            # All 32 should complete within timeout + margin, NOT 32 * timeout
            @test elapsed < 8.0  # concurrent, not serial

            # Clean up
            for t in client_tasks
                try close(fetch(t)) catch end
            end
        end
    end

    close(server)
end

@testset "Pipeline Integration" begin
    # Test the full pipeline: accept → @async validate → pending_validated channel
    # This mirrors add_pending_connections exactly, but with a test-local server and channel.

    port, server = listenany(getipaddr(), 19100)
    pending_validated = Channel{WorkerConfig}(64)

    n_good = 16
    n_slow = 4     # 1.5s delay — should still succeed
    n_dead = 4     # never send — should timeout and be discarded
    n_bad_cookie = 2
    n_total = n_good + n_slow + n_dead + n_bad_cookie
    n_expected = n_good + n_slow  # only good + slow should make it through

    withenv("JULIA_AZMANAGERS_VALIDATION_TIMEOUT" => "3.0") do
        # Start the accept loop — this is the real add_pending_connections logic
        accept_task = @async begin
            for _ in 1:n_total
                try
                    s = accept(server)
                    @async try
                        wconfig = AzManagers.validate_connection(nothing, s)
                        if wconfig !== nothing
                            put!(pending_validated, wconfig)
                        end
                    catch e
                        @error "pipeline test: validation error" exception=(e, catch_backtrace())
                    end
                catch e
                    @error "pipeline test: accept error" exception=(e, catch_backtrace())
                end
            end
        end

        # Fire all fake workers concurrently
        client_tasks = Task[]
        for i in 1:n_good
            push!(client_tasks, @async fake_worker_connect(port; delay=rand()*0.1))
        end
        for i in 1:n_slow
            push!(client_tasks, @async fake_worker_connect(port; delay=1.5))
        end
        for i in 1:n_dead
            push!(client_tasks, @async begin
                sock = connect(getipaddr(), port)
                sleep(15)
                sock
            end)
        end
        for i in 1:n_bad_cookie
            push!(client_tasks, @async fake_worker_connect(port; bad_cookie=true))
        end

        # Drain the channel and collect validated configs
        validated = WorkerConfig[]
        t0 = time()

        # We expect n_expected configs. Dead sockets timeout at 3s.
        # Use a generous deadline: timeout + margin
        deadline = 8.0
        while length(validated) < n_expected && (time() - t0) < deadline
            if isready(pending_validated)
                push!(validated, take!(pending_validated))
            else
                sleep(0.05)
            end
        end

        # Wait a bit more to confirm no extras sneak through
        sleep(0.5)
        while isready(pending_validated)
            push!(validated, take!(pending_validated))
        end

        elapsed = time() - t0

        @testset "correct number of validated connections" begin
            @test length(validated) == n_expected
        end

        @testset "all validated configs have expected fields" begin
            for wc in validated
                @test wc.bind_addr == string(getipaddr())
                @test wc.exename == "julia"
                @test wc.userdata["scalesetname"] == "test-ss"
                @test wc.io isa TCPSocket
            end
        end

        @testset "pipeline completes within bounded time (not serial)" begin
            # If validations were serial: n_dead * 3s timeout + n_slow * 1.5s = 18s minimum
            # Concurrent: should finish within timeout + margin ≈ 5-6s
            @test elapsed < 8.0
        end

        @testset "dead and bad-cookie sockets were discarded" begin
            # pending_validated should be empty — no extras
            @test !isready(pending_validated)
        end

        # Wait for dead socket timeouts to finish, then clean up
        wait(accept_task)
        for t in client_tasks
            try close(fetch(t)) catch end
        end
    end

    close(pending_validated)
    close(server)
end
