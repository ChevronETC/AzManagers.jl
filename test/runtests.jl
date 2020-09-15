using Distributed, AzManagers, Test

@testset "AzManagers" begin
    @test true
    print(">>> ", ENV.FIRST_NAME, " <<<")
    # write(stdout, "env.FIRST_NAME")
    # write(stdout, "$FIRST_NAME")
end
