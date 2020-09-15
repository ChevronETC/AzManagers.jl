using Distributed, AzManagers, Test

@testset "AzManagers" begin
    @test true
    write(stdout, "env.FIRST_NAME")
    write(stdout, "$FIRST_NAME")
end
