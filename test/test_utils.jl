using Distributed, AzManagers, Random, TOML, Test, HTTP, AzSessions, JSON, Pkg
using MPI
using TestReports

const TEST_REPORT_DIR = get(ENV, "AZMANAGERS_TEST_REPORT_DIR", joinpath(homedir(), "test-reports"))
mkpath(TEST_REPORT_DIR)

"""
    run_group(name, f)

Run the test function `f` inside a ReportingTestSet and write a JUnit XML report
to `\$TEST_REPORT_DIR/<name>.xml`. Returns the testset.
"""
function run_group(f, name::String)
    ts = nothing
    try
        ts = @testset ReportingTestSet "$name" begin
            f()
        end
    finally
        if ts !== nothing
            report_path = joinpath(TEST_REPORT_DIR, "$name.xml")
            try
                open(report_path, "w") do io
                    print(io, report(ts))
                end
                @info "Wrote JUnit report to $report_path"
            catch e
                @warn "Failed to write JUnit report" exception=e
            end
        end
    end
    ts
end

function with_timeout(f, seconds; msg="operation")
    t = @async f()
    deadline = time() + seconds
    while !istaskdone(t) && time() < deadline
        sleep(1)
    end
    if !istaskdone(t)
        error("$msg timed out after $(seconds)s")
    end
    fetch(t)
end

function quiet_pkg(f)
    try
        redirect_stdout(devnull) do
            redirect_stderr(devnull) do
                f()
            end
        end
    catch
        rethrow()
    end
end

const test_start_time = time()
elapsed() = round(time() - test_start_time; digits=1)

const session = AzSession(;protocal=AzClientCredentials)

const azmanagers_pinfo = Pkg.project()
const _manifest_dir = dirname(azmanagers_pinfo.path)
const _manifest_path = if isfile(joinpath(_manifest_dir, "Manifest.toml"))
    joinpath(_manifest_dir, "Manifest.toml")
elseif isfile(joinpath(_manifest_dir, "JuliaManifest.toml"))
    joinpath(_manifest_dir, "JuliaManifest.toml")
else
    error("No Manifest.toml found in $_manifest_dir. Contents: $(readdir(_manifest_dir))")
end
const _pkgs = TOML.parse(read(_manifest_path, String))
const _pkg = VERSION < v"1.7.0" ? _pkgs["AzManagers"][1] : _pkgs["deps"]["AzManagers"][1]
const azmanagers_rev = get(_pkg, "repo-rev", "")

const templatename = "cbox02"
const _template_data = JSON.parse(read(AzManagers.templates_filename_scaleset(), String); dicttype=Dict)
const default_template = _template_data[templatename]
const subscriptionid = default_template["subscriptionid"]
const resourcegroup = default_template["resourcegroup"]
