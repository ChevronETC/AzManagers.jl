#
# Parallel integration test runner.
#
# Discovers test files under test/integration/, launches each as a separate
# Julia subprocess (all in parallel), captures stdout/stderr per process,
# and reports results sequentially after all complete. No output interleaving.
#
# Filter to specific tests via AZMANAGERS_TEST_NAMES (comma-separated):
#   AZMANAGERS_TEST_NAMES=addprocs,spot julia --project=. -e 'using Pkg; Pkg.test()'
#

using Dates

const INTEGRATION_DIR = joinpath(@__DIR__, "integration")
const PROJECT_DIR = get(ENV, "JULIA_PROJECT", joinpath(@__DIR__, ".."))

struct TestResult
    name::String
    file::String
    exitcode::Int
    stdout::String
    stderr::String
    elapsed::Float64
end

passed(r::TestResult) = r.exitcode == 0

function discover_tests()
    files = filter(f -> startswith(basename(f), "test_") && endswith(f, ".jl"),
                   readdir(INTEGRATION_DIR; join=true))
    sort!(files)

    # Optional filter: AZMANAGERS_TEST_NAMES=addprocs,spot,detached
    filter_str = get(ENV, "AZMANAGERS_TEST_NAMES", "")
    if !isempty(filter_str)
        names = Set(strip.(split(filter_str, ",")))
        files = filter(files) do f
            name = replace(basename(f), r"^test_" => "", r"\.jl$" => "")
            name ∈ names
        end
        if isempty(files)
            @warn "AZMANAGERS_TEST_NAMES=$filter_str matched no test files"
        end
    end

    return files
end

function run_test(file::String, logfile::String)
    name = replace(basename(file), r"^test_" => "", r"\.jl$" => "")
    @info "Starting integration test: $name ($(basename(file)))"

    # Propagate test env and config to subprocesses
    env = copy(ENV)
    for key in ("AZMANAGERS_TEST_ENV", "AZMANAGERS_TEST_TEMPLATE", "AZMANAGERS_TEST_SUITE")
        haskey(ENV, key) && (env[key] = ENV[key])
    end

    t0 = time()

    # Write combined stdout+stderr to a log file so the monitor can tail it
    open(logfile, "w") do io
        proc = run(pipeline(
            setenv(`$(Base.julia_cmd()) --project=$PROJECT_DIR --color=no $file`, env),
            stdout=io,
            stderr=io
        ); wait=false)

        # Wait for the main task to finish (background tasks may linger)
        for _ in 1:3600  # up to 30 min
            process_running(proc) || break
            sleep(0.5)
        end
        was_killed = false
        if process_running(proc)
            @warn "Test $name still running after 30 min — killing"
            kill(proc)
            was_killed = true
        end
        wait(proc)
        elapsed = time() - t0

        # If the process was killed, check the log for test failures.
        # A killed process that had test failures should NOT be reported as passed.
        exitcode = proc.exitcode
        if was_killed
            log_content = isfile(logfile) ? read(logfile, String) : ""
            has_test_failure = contains(log_content, "Test Failed") || contains(log_content, r"| *Fail")
            exitcode = has_test_failure ? 1 : 0
        end

        return TestResult(
            name,
            file,
            exitcode,
            "", # stdout captured in logfile
            "", # stderr captured in logfile
            elapsed
        )
    end
end

function print_new_output(name::String, logfile::String, offset::Int; indent="  ")
    sz = filesize(logfile)
    sz <= offset && return offset
    new_bytes = read(logfile)[offset+1:sz]
    text = String(new_bytes)
    # Handle \r-based progress lines: keep only the last segment after \r
    lines = split(text, '\n')
    for line in lines
        isempty(line) && continue
        # If line contains \r (carriage return progress), show only the final state
        if contains(line, '\r')
            parts = split(line, '\r')
            line = strip(parts[end])
            isempty(line) && continue
        end
        println("$(indent)│ $line")
    end
    return sz
end

function run_all_tests()
    files = discover_tests()

    if isempty(files)
        @warn "No integration test files found in $INTEGRATION_DIR"
        return true
    end

    @info "Discovered $(length(files)) integration test files — launching in parallel"

    # Set up log files and track state
    logdir = mktempdir()
    test_names = String[]
    logfiles = Dict{String, String}()
    offsets = Dict{String, Int}()

    for file in files
        name = replace(basename(file), r"^test_" => "", r"\.jl$" => "")
        push!(test_names, name)
        logfiles[name] = joinpath(logdir, "$(name).log")
        offsets[name] = 0
        touch(logfiles[name])
    end

    # Launch all tests as async tasks
    tasks = Dict{String, Task}()
    for file in files
        name = replace(basename(file), r"^test_" => "", r"\.jl$" => "")
        tasks[name] = Threads.@spawn run_test(file, logfiles[name])
    end

    # Monitor loop: every 15 seconds, print new output in test order
    println()
    while true
        all_done = all(name -> istaskdone(tasks[name]), test_names)

        # Print new output from each test in order
        any_output = false
        for name in test_names
            lf = logfiles[name]
            old_offset = offsets[name]
            sz = isfile(lf) ? filesize(lf) : 0
            if sz > old_offset
                if !any_output
                    println("─── Progress update $(Dates.format(now(), "HH:MM:SS")) ───")
                    any_output = true
                end
                done = istaskdone(tasks[name])
                status_icon = done ? (istaskfailed(tasks[name]) ? "✗" : "✓") : "⋯"
                println("$status_icon $name:")
                offsets[name] = print_new_output(name, lf, old_offset)
            end
        end
        any_output && println()

        all_done && break
        sleep(15)
    end

    # Collect results (preserves discovery order)
    results = TestResult[]
    for name in test_names
        push!(results, fetch(tasks[name]))
    end

    # Final summary
    println("\n", "="^80)
    println("INTEGRATION TEST RESULTS")
    println("="^80)

    for r in results
        status_str = passed(r) ? "PASS" : "FAIL"
        status_icon = passed(r) ? "✓" : "✗"
        elapsed_str = round(r.elapsed; digits=1)
        println("$status_icon [$status_str] $(r.name) ($(elapsed_str)s)")
    end

    # Summary
    n_pass = count(passed, results)
    n_fail = length(results) - n_pass
    total_time = sum(r -> r.elapsed, results)
    wall_time = maximum(r -> r.elapsed, results)

    println("\n", "="^80)
    println("SUMMARY: $n_pass passed, $n_fail failed out of $(length(results)) tests")
    println("Total CPU time: $(round(total_time; digits=1))s | Wall time: $(round(wall_time; digits=1))s")
    println("="^80)

    if n_fail > 0
        println("\nFailed tests:")
        for r in results
            if !passed(r)
                println("  ✗ $(r.name) ($(r.file))")
                lf = logfiles[r.name]
                if isfile(lf)
                    println("  --- full log ---")
                    println(read(lf, String))
                    println("  --- end log ---")
                end
            end
        end
    end

    # Clean up log files
    rm(logdir; force=true, recursive=true)

    return n_fail == 0
end

# Run and set exit code
all_passed = run_all_tests()
if !all_passed
    error("Integration tests failed")
end
