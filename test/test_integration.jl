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
const PROJECT_DIR = joinpath(@__DIR__, "..")

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

function run_test(file::String)
    name = replace(basename(file), r"^test_" => "", r"\.jl$" => "")
    @info "Starting integration test: $name ($(basename(file)))"

    stdout_buf = IOBuffer()
    stderr_buf = IOBuffer()

    # Propagate test env and config to subprocesses
    env = copy(ENV)
    for key in ("AZMANAGERS_TEST_ENV", "AZMANAGERS_TEST_TEMPLATE", "AZMANAGERS_TEST_SUITE")
        haskey(ENV, key) && (env[key] = ENV[key])
    end

    t0 = time()
    proc = run(pipeline(
        setenv(`$(Base.julia_cmd()) --project=$PROJECT_DIR --color=yes $file`, env),
        stdout=stdout_buf,
        stderr=stderr_buf
    ); wait=false)
    wait(proc)
    elapsed = time() - t0

    return TestResult(
        name,
        file,
        proc.exitcode,
        String(take!(stdout_buf)),
        String(take!(stderr_buf)),
        elapsed
    )
end

function run_all_tests()
    files = discover_tests()

    if isempty(files)
        @warn "No integration test files found in $INTEGRATION_DIR"
        return true
    end

    @info "Discovered $(length(files)) integration test files — launching in parallel"

    # Launch all tests as async tasks
    tasks = Dict{String, Task}()
    for file in files
        name = replace(basename(file), r"^test_" => "", r"\.jl$" => "")
        tasks[name] = Threads.@spawn run_test(file)
    end

    # Collect results (preserves discovery order for output)
    results = TestResult[]
    for file in files
        name = replace(basename(file), r"^test_" => "", r"\.jl$" => "")
        push!(results, fetch(tasks[name]))
    end

    # Print results sequentially — no interleaving
    println("\n", "="^80)
    println("INTEGRATION TEST RESULTS")
    println("="^80)

    for r in results
        status_str = passed(r) ? "PASS" : "FAIL"
        status_icon = passed(r) ? "✓" : "✗"
        elapsed_str = round(r.elapsed; digits=1)

        println("\n", "-"^80)
        println("$status_icon [$status_str] $(r.name) ($(elapsed_str)s)")
        println("-"^80)

        if !isempty(r.stdout)
            println("--- stdout ---")
            println(r.stdout)
        end

        if !isempty(r.stderr)
            println("--- stderr ---")
            println(r.stderr)
        end

        if !passed(r)
            println(">>> EXIT CODE: $(r.exitcode)")
        end
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
            passed(r) || println("  ✗ $(r.name) ($(r.file))")
        end
    end

    return n_fail == 0
end

# Run and set exit code
all_passed = run_all_tests()
if !all_passed
    error("Integration tests failed")
end
