include(joinpath(@__DIR__, "common.jl"))

@testset "scale set tag propagation" begin
    group = unique_group()
    @info "Tag propagation: 1 VM with custom tag foo=bar → expect tag on scale set resource" group
    try
        templates_scaleset = JSON.parse(read(AzManagers.templates_filename_scaleset(), String))
        template = Dict{Any,Any}(deepcopy(templates_scaleset[TEMPLATENAME]))

        _template = template["value"]
        if haskey(_template, "tags")
            _template["tags"]["foo"] = "bar"
        else
            _template["tags"] = Dict("foo" => "bar")
        end

        addprocs(AzManager(), template, 1; waitfor=true, group=group, session=SESSION, exename=EXENAME, overprovision=false)

        url = scaleset_url(group)
        _r = HTTP.request("GET", url, ["Authorization" => "Bearer $(token(SESSION))"])
        r = JSON.parse(String(_r.body))
        @test r["tags"]["foo"] == "bar"
    finally
        cleanup_workers()
        wait_for_scaleset_deletion(group)
    end
end
