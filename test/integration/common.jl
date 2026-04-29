#
# Shared setup for integration tests.
# Each integration test file includes this at the top.
#

using Distributed, AzManagers, Random, TOML, Test, HTTP, AzSessions, JSON, Pkg, Logging, Dates

# ── Environment detection ────────────────────────────────────────────────────
function _detect_test_env()
    for key in ("CI", "GITHUB_ACTIONS", "TF_BUILD", "JENKINS_URL")
        haskey(ENV, key) && return "ci"
    end
    return "local"
end

const TEST_ENV = lowercase(get(ENV, "AZMANAGERS_TEST_ENV", _detect_test_env()))

# ── Credentials ──────────────────────────────────────────────────────────────
# CI uses service-principal credentials injected via env vars.
# Local uses whatever AzClientCredentials resolves (e.g. cached tokens).
const SESSION = AzSession(;protocal=AzClientCredentials)

# ── Template configuration ───────────────────────────────────────────────────
# CI may override the template name and subscription/resource-group directly
# via env vars when templates aren't pre-baked on the runner.
const TEMPLATENAME = get(ENV, "AZMANAGERS_TEST_TEMPLATE", "cbox02")

function load_scaleset_template()
    templates = JSON.parse(read(AzManagers.templates_filename_scaleset(), String))
    haskey(templates, TEMPLATENAME) || error("Template '$TEMPLATENAME' not found in $(AzManagers.templates_filename_scaleset())")
    templates[TEMPLATENAME]
end

const TEMPLATE       = load_scaleset_template()
const SUBSCRIPTIONID = get(ENV, "AZMANAGERS_TEST_SUBSCRIPTION_ID", TEMPLATE["subscriptionid"])
const RESOURCEGROUP  = get(ENV, "AZMANAGERS_TEST_RESOURCE_GROUP",  TEMPLATE["resourcegroup"])

# ── Julia executable name ─────────────────────────────────────────────────────
# On dev boxes Julia may be installed via juliaup at a non-standard path
# (e.g. /opt/julia_depot/juliaup/...) that won't exist on gallery images.
# Default to plain "julia" so the worker uses PATH lookup on the remote VM.
const EXENAME = get(ENV, "AZMANAGERS_TEST_EXENAME", "julia")

# ── Worker timeout ────────────────────────────────────────────────────────────
# Custom environments need extra time for Pkg.add + instantiate + precompile
# before the detached service starts.  Default 60s is too short.
if !haskey(ENV, "JULIA_WORKER_TIMEOUT")
    ENV["JULIA_WORKER_TIMEOUT"] = "300"
end

@info "Integration test config" TEST_ENV TEMPLATENAME SUBSCRIPTIONID RESOURCEGROUP EXENAME

"""
    unique_group() -> String

Generate a unique scale-set group name for a test.
"""
unique_group() = "test$(randstring('a':'z', 6))"

"""
    scaleset_url(group) -> String

Build the Azure REST URL for a scale set.
"""
scaleset_url(group) = "https://management.azure.com/subscriptions/$SUBSCRIPTIONID/resourceGroups/$RESOURCEGROUP/providers/Microsoft.Compute/virtualMachineScaleSets/$group?api-version=2023-03-01"

"""
    wait_for_scaleset_deletion(group; timeout=120)

Poll until the scale set is confirmed deleted (404) or timeout.
"""
function wait_for_scaleset_deletion(group; timeout=120)
    url = scaleset_url(group)
    tic = time()
    while time() - tic < timeout
        try
            HTTP.request("GET", url, ["Authorization" => "Bearer $(token(SESSION))"]; verbose=0)
        catch e
            if isa(e, HTTP.StatusError) && e.status == 404
                @info "Scale set '$group' confirmed deleted ($(round(time()-tic, digits=1))s)"
                return true
            end
        end
        sleep(5)
    end
    @warn "Scale set '$group' not confirmed deleted after $(timeout)s"
    return false
end

"""
    cleanup_workers()

Remove all workers, suppressing errors.
"""
function cleanup_workers()
    try
        wrkrs = workers()
        if length(wrkrs) > 0 && !(length(wrkrs) == 1 && wrkrs[1] == 1)
            rmprocs(wrkrs)
        end
    catch e
        @warn "cleanup_workers failed" exception=e
    end
end

"""
    cleanup_vm(vm)

Remove a detached VM, suppressing errors.
"""
function cleanup_vm(vm)
    try
        rmproc(vm; session=SESSION)
    catch e
        @warn "cleanup_vm failed" exception=e
    end
end
