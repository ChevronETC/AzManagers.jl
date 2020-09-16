using Distributed, AzManagers, Test

@testset "AzManagers" begin
    @test true
    print(">>> ", ENV["subscription_id"], " <<<")
    print(">>> ", ENV["resource_group"], " <<<")
end
