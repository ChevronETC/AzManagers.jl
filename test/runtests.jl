using Test

function _detect_test_env()
    for key in ("CI", "GITHUB_ACTIONS", "TF_BUILD", "JENKINS_URL")
        haskey(ENV, key) && return "ci"
    end
    return "local"
end

const TEST_SUITE = lowercase(get(ENV, "AZMANAGERS_TEST_SUITE", "all"))
const TEST_ENV   = lowercase(get(ENV, "AZMANAGERS_TEST_ENV", _detect_test_env()))

@info "Test environment: $TEST_ENV | Test suite: $TEST_SUITE"

if TEST_SUITE ∈ ("unit", "all")
    @testset "AzManagers — unit" begin
        include("test_unit.jl")
    end
end

if TEST_SUITE ∈ ("integration", "all")
    @info "Running integration tests (parallel)..."
    include("test_integration.jl")
end

