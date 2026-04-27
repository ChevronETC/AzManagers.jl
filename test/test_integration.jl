using Test, AzManagers, HTTP, JSON, Distributed, Pkg, Sockets

# ═══════════════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════════════

"""Build a mock HTTP.Request with the given method, target, and optional JSON body."""
function mock_request(method, target; body=nothing)
    headers = ["Content-Type" => "application/json"]
    payload = body === nothing ? UInt8[] : Vector{UInt8}(JSON.json(body))
    HTTP.Request(method, target, headers, payload)
end

"""Clean up DETACHED_JOBS state between tests."""
function cleanup_detached_jobs!()
    for k in collect(keys(AzManagers.DETACHED_JOBS))
        job = AzManagers.DETACHED_JOBS[k]
        if haskey(job, "process")
            try kill(job["process"]) catch end
            try wait(job["process"]) catch end
        end
        for f in ("stdout", "stderr")
            haskey(job, f) && isfile(job[f]) && rm(job[f]; force=true)
        end
        delete!(AzManagers.DETACHED_JOBS, k)
    end
end

# ═══════════════════════════════════════════════════════════════════════════════
# 1. detachedping — trivial health check
# ═══════════════════════════════════════════════════════════════════════════════

@testset "detachedping" begin
    req = mock_request("GET", "/cofii/detached/ping")
    resp = AzManagers.detachedping(req)
    @test resp.status == 200
    @test String(resp.body) == "OK"
end

# ═══════════════════════════════════════════════════════════════════════════════
# 2. detachedvminfo — returns DETACHED_VM contents
# ═══════════════════════════════════════════════════════════════════════════════

@testset "detachedvminfo" begin
    # Set up mock VM info
    AzManagers.DETACHED_VM[] = Dict(
        "name" => "testvm",
        "ip" => "10.0.0.1",
        "port" => "8081",
        "subscriptionid" => "sub-123",
        "resourcegroup" => "rg-test"
    )

    req = mock_request("GET", "/cofii/detached/vm")
    resp = AzManagers.detachedvminfo(req)
    @test resp.status == 200

    data = JSON.parse(String(resp.body))
    @test data["name"] == "testvm"
    @test data["ip"] == "10.0.0.1"
    @test data["subscriptionid"] == "sub-123"
end

# ═══════════════════════════════════════════════════════════════════════════════
# 3. detachedstatus — job status reporting
# ═══════════════════════════════════════════════════════════════════════════════

@testset "detachedstatus" begin
    cleanup_detached_jobs!()

    # Status for non-existent job returns 500
    req = mock_request("GET", "/cofii/detached/job/999/status")
    resp = AzManagers.detachedstatus(req)
    @test resp.status == 500
    @test contains(String(resp.body), "does not exist")

    # Create a real short-lived process and register it
    process = open(`julia -e 'sleep(60)'`)
    AzManagers.DETACHED_JOBS["1"] = Dict(
        "process" => process,
        "stdout" => tempname(),
        "stderr" => tempname(),
        "code" => "sleep(60)"
    )

    # Running job
    req = mock_request("GET", "/cofii/detached/job/1/status")
    resp = AzManagers.detachedstatus(req)
    @test resp.status == 200
    data = JSON.parse(String(resp.body))
    @test data["status"] == "running"

    # Kill and wait for exit
    kill(process)
    try wait(process) catch end

    # Completed (failed because killed)
    req = mock_request("GET", "/cofii/detached/job/1/status")
    resp = AzManagers.detachedstatus(req)
    @test resp.status == 200
    data = JSON.parse(String(resp.body))
    @test data["status"] ∈ ("done", "failed")

    cleanup_detached_jobs!()
end

# ═══════════════════════════════════════════════════════════════════════════════
# 4. detachedstdout / detachedstderr — log retrieval
# ═══════════════════════════════════════════════════════════════════════════════

@testset "detached stdout/stderr retrieval" begin
    cleanup_detached_jobs!()

    # Non-existent job
    req = mock_request("GET", "/cofii/detached/job/999/stdout")
    resp = AzManagers.detachedstdout(req)
    @test resp.status == 500

    # Create mock job with output files
    outfile = tempname()
    errfile = tempname()
    write(outfile, "hello stdout")
    write(errfile, "hello stderr")

    process = open(`julia -e 'nothing'`)
    wait(process)

    AzManagers.DETACHED_JOBS["2"] = Dict(
        "process" => process,
        "stdout" => outfile,
        "stderr" => errfile,
        "code" => "nothing"
    )

    # Read stdout
    req = mock_request("GET", "/cofii/detached/job/2/stdout")
    resp = AzManagers.detachedstdout(req)
    @test resp.status == 200
    @test String(resp.body) == "hello stdout"

    # Read stderr
    req = mock_request("GET", "/cofii/detached/job/2/stderr")
    resp = AzManagers.detachedstderr(req)
    @test resp.status == 200
    @test String(resp.body) == "hello stderr"

    cleanup_detached_jobs!()
    rm(outfile; force=true)
    rm(errfile; force=true)
end

# ═══════════════════════════════════════════════════════════════════════════════
# 5. detachedkill — process termination
# ═══════════════════════════════════════════════════════════════════════════════

@testset "detachedkill" begin
    cleanup_detached_jobs!()

    # Kill non-existent job
    req = mock_request("POST", "/cofii/detached/job/999/kill")
    resp = AzManagers.detachedkill(req)
    @test resp.status == 500

    # Create a long-running process
    process = open(`julia -e 'sleep(300)'`)
    AzManagers.DETACHED_JOBS["3"] = Dict(
        "process" => process,
        "stdout" => tempname(),
        "stderr" => tempname(),
        "code" => "sleep(300)"
    )

    # Kill it
    req = mock_request("POST", "/cofii/detached/job/3/kill")
    resp = AzManagers.detachedkill(req)
    @test resp.status == 200
    @test contains(String(resp.body), "killed")

    # Verify process is dead
    try wait(process) catch end
    @test process_exited(process)

    cleanup_detached_jobs!()
end

# ═══════════════════════════════════════════════════════════════════════════════
# 6. detachedrun — full local job submission (no Azure, just subprocess)
# ═══════════════════════════════════════════════════════════════════════════════

@testset "detachedrun" begin
    cleanup_detached_jobs!()

    # Missing "code" key returns 400
    req = mock_request("POST", "/cofii/detached/run"; body=Dict("persist" => true))
    resp = AzManagers.detachedrun(req)
    @test resp.status == 400
    @test contains(String(resp.body), "code")

    # Empty code block (just "begin\nend") returns 400
    req = mock_request("POST", "/cofii/detached/run"; body=Dict(
        "persist" => true,
        "code" => "begin\nend"
    ))
    resp = AzManagers.detachedrun(req)
    @test resp.status == 400
    @test contains(String(resp.body), "No code to execute")

    # Successful job submission — runs a trivial Julia script
    req = mock_request("POST", "/cofii/detached/run"; body=Dict(
        "persist" => true,
        "code" => """write(stdout, "unit_test_output")"""
    ))
    resp = AzManagers.detachedrun(req)
    @test resp.status == 200

    data = JSON.parse(String(resp.body))
    @test haskey(data, "id")
    @test haskey(data, "pid")
    job_id = string(data["id"])

    # Job should be registered
    @test haskey(AzManagers.DETACHED_JOBS, job_id)

    # Wait for the subprocess to finish
    process = AzManagers.DETACHED_JOBS[job_id]["process"]
    try wait(process) catch end

    # Check status is done
    req = mock_request("GET", "/cofii/detached/job/$job_id/status")
    resp = AzManagers.detachedstatus(req)
    @test resp.status == 200
    status_data = JSON.parse(String(resp.body))
    @test status_data["status"] == "done"

    # Check stdout was captured
    req = mock_request("GET", "/cofii/detached/job/$job_id/stdout")
    resp = AzManagers.detachedstdout(req)
    @test resp.status == 200
    @test String(resp.body) == "unit_test_output"

    cleanup_detached_jobs!()
end

# ═══════════════════════════════════════════════════════════════════════════════
# 7. DetachedJob struct and constructors
# ═══════════════════════════════════════════════════════════════════════════════

@testset "DetachedJob" begin
    # Full constructor
    vm = Dict("ip" => "10.0.0.1", "port" => "8081")
    job = AzManagers.DetachedJob(vm, "42", "1234", "")
    @test job.vm["ip"] == "10.0.0.1"
    @test job.id == "42"
    @test job.pid == "1234"
    @test job.logurl == ""

    # IP + id constructor
    job2 = AzManagers.DetachedJob("10.0.0.2", 7)
    @test job2.vm["ip"] == "10.0.0.2"
    @test job2.id == "7"
    @test job2.pid == "-1"

    # IP + id + pid constructor
    job3 = AzManagers.DetachedJob("10.0.0.3", 8, 999)
    @test job3.vm["ip"] == "10.0.0.3"
    @test job3.id == "8"
    @test job3.pid == "999"
end

# ═══════════════════════════════════════════════════════════════════════════════
# 8. Full detached lifecycle (submit → status → stdout → wait → kill)
# ═══════════════════════════════════════════════════════════════════════════════

@testset "detached lifecycle via HTTP handlers" begin
    cleanup_detached_jobs!()

    # Submit a job that writes to both stdout and stderr
    req = mock_request("POST", "/cofii/detached/run"; body=Dict(
        "persist" => true,
        "code" => """
        write(stdout, "lifecycle_out")
        write(stderr, "lifecycle_err")
        """
    ))
    resp = AzManagers.detachedrun(req)
    @test resp.status == 200
    job_id = string(JSON.parse(String(resp.body))["id"])

    # Wait for completion
    process = AzManagers.DETACHED_JOBS[job_id]["process"]
    try wait(process) catch end

    # Verify stdout
    req = mock_request("GET", "/cofii/detached/job/$job_id/stdout")
    resp = AzManagers.detachedstdout(req)
    @test String(resp.body) == "lifecycle_out"

    # Verify stderr
    req = mock_request("GET", "/cofii/detached/job/$job_id/stderr")
    resp = AzManagers.detachedstderr(req)
    @test String(resp.body) == "lifecycle_err"

    # Verify final status
    req = mock_request("GET", "/cofii/detached/job/$job_id/status")
    resp = AzManagers.detachedstatus(req)
    @test JSON.parse(String(resp.body))["status"] == "done"

    cleanup_detached_jobs!()
end

# ═══════════════════════════════════════════════════════════════════════════════
# 9. Detached service via local HTTP server (end-to-end without Azure)
# ═══════════════════════════════════════════════════════════════════════════════

@testset "detached service local server" begin
    cleanup_detached_jobs!()

    # Set up VM info
    AzManagers.DETACHED_VM[] = Dict(
        "name" => "localtest",
        "ip" => string(Sockets.getipaddr()),
        "port" => "0",
        "subscriptionid" => "test-sub",
        "resourcegroup" => "test-rg",
        "exename" => "julia"
    )

    # Register routes (same as detachedservice but without actually starting a blocking server)
    router = HTTP.Router()
    HTTP.register!(router, "POST", "/cofii/detached/run", AzManagers.detachedrun)
    HTTP.register!(router, "POST", "/cofii/detached/job/*/kill", AzManagers.detachedkill)
    HTTP.register!(router, "POST", "/cofii/detached/job/*/wait", AzManagers.detachedwait)
    HTTP.register!(router, "GET", "/cofii/detached/job/*/status", AzManagers.detachedstatus)
    HTTP.register!(router, "GET", "/cofii/detached/job/*/stdout", AzManagers.detachedstdout)
    HTTP.register!(router, "GET", "/cofii/detached/job/*/stderr", AzManagers.detachedstderr)
    HTTP.register!(router, "GET", "/cofii/detached/ping", AzManagers.detachedping)
    HTTP.register!(router, "GET", "/cofii/detached/vm", AzManagers.detachedvminfo)

    # Start on random port
    port, server = Sockets.listenany(Sockets.getipaddr(), 9100)
    task = @async HTTP.serve(router, Sockets.getipaddr(), port; server=server)
    sleep(1)  # let server start

    base = "http://$(Sockets.getipaddr()):$port"

    try
        # Ping
        r = HTTP.get("$base/cofii/detached/ping")
        @test r.status == 200

        # VM info
        r = HTTP.get("$base/cofii/detached/vm")
        @test r.status == 200
        vm = JSON.parse(String(r.body))
        @test vm["name"] == "localtest"

        # Submit job
        r = HTTP.post("$base/cofii/detached/run",
            ["Content-Type" => "application/json"],
            JSON.json(Dict("persist" => true, "code" => """write(stdout, "e2e_test")""")))
        @test r.status == 200
        job = JSON.parse(String(r.body))
        job_id = string(job["id"])

        # Wait for subprocess
        process = AzManagers.DETACHED_JOBS[job_id]["process"]
        try wait(process) catch end

        # Status
        r = HTTP.get("$base/cofii/detached/job/$job_id/status")
        @test JSON.parse(String(r.body))["status"] == "done"

        # Stdout
        r = HTTP.get("$base/cofii/detached/job/$job_id/stdout")
        @test String(r.body) == "e2e_test"
    finally
        close(server)
        cleanup_detached_jobs!()
    end
end
