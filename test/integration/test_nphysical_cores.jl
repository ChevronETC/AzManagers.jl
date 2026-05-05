include(joinpath(@__DIR__, "common.jl"))

@testset "nphysical_cores" begin
    # String-based lookup (queries Azure SKU API via VM template file)
    @testset "by name — $TEMPLATENAME" begin
        ncores = nphysical_cores(TEMPLATENAME; session=SESSION)
        @test ncores > 0
        @test ncores isa Int
        @info "nphysical_cores($TEMPLATENAME) = $ncores"
    end

    # Dict-based lookup using VM template file
    @testset "by template dict" begin
        vm_templates_file = AzManagers.templates_filename_vm()
        if isfile(vm_templates_file)
            templates_vm = JSON.parse(read(vm_templates_file, String); dicttype=Dict)
            if haskey(templates_vm, TEMPLATENAME)
                template = templates_vm[TEMPLATENAME]
                ncores = nphysical_cores(template; session=SESSION)
                @test ncores > 0
                @test ncores isa Int
            else
                @warn "VM template '$TEMPLATENAME' not found in $vm_templates_file — skipping dict test"
                @test_skip true
            end
        else
            @warn "VM template file not found at $vm_templates_file — skipping dict test"
            @test_skip true
        end
    end
end
