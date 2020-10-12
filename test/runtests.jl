using Distributed, AzManagers, Random, Test, HTTP, AzSessions, JSON

ss_template_json = JSON.parse(ENV["SS_TEMPLATE_JSON"])

myscaleset = AzManagers.build_sstemplate(
name                 = ss_template_json["name"],
subscriptionid       = ss_template_json["subscriptionid"],
location             = ss_template_json["location"],
resourcegroup        = ss_template_json["resourcegroup"],
resourcegroup_vnet   = ss_template_json["resourcegroup_vnet"],
vnet                 = ss_template_json["vnet"],
subnet               = ss_template_json["subnet"],
imagegallery         = ss_template_json["imagegallery"],
imagename            = ss_template_json["imagename"],
skuname              = ss_template_json["skuname"])

AzManagers.save_template_scaleset("cbox02", myscaleset)

template = "cbox02"
# credentials = JSON.parse(ENV["AZURE_CREDENTIALS"])
# subscriptionid = credentials["subscriptionId"]
# resourcegroup = ENV["RESOURCE_GROUP"]

# @testset "AzManagers, addprocs" for kwargs in (
#     (subscriptionid = subscriptionid, resourcegroup = resourcegroup,          ninstances = 1, group = "test$(randstring('a':'z',4))"),
#     (subscriptionid = subscriptionid, resourcegroup = resourcegroup, ppi = 2, ninstances = 2, group = "test$(randstring('a':'z',4))") )
    
#     # Set up iteration vars
#     url = "https://management.azure.com/subscriptions/$subscriptionid/resourceGroups/$resourcegroup/providers/Microsoft.Compute/virtualMachineScaleSets/$(kwargs.group)?api-version=2019-12-01"
#     ninstances = kwargs.ninstances              # Number of new scale set instances to be added to the scale set
#     ppi = haskey(kwargs, :ppi) ? kwargs.ppi : 1 # Number of Julia processes to be present on each scale set instance
#     tppi = ppi*ninstances                       # Total number of Julia processes in the entire scale set

#     #
#     # Unit Test 1 - Create scale set and start Julia processes
#     #
#     addprocs(template, ninstances; kwargs...)
    
#     # Verify that the scale set is present
#     session = AzSession()
#     _r = HTTP.request("GET", url, Dict("Authorization"=>"Bearer $(token(session))"); verbose=0)
#     @test _r.status == 200

#     #
#     # Unit Test 2 - Verify that (Total # of Julia processes specified) == (Total # of Julia processes actual)
#     #
#     @test nworkers() == tppi

#     #
#     # Unit Test 3 - Verify that there is a healthy connection to each node
#     #
#     myworkers = [remotecall_fetch(gethostname, workers()[i]) for i=1:tppi]

#     #
#     # Unit Test 4 - Verify that none of the created Julia processes are the master Julia process
#     #
#     master = gethostname()
#     unique_workers = unique(myworkers)
#     @test length(unique_workers) == ninstances
#     for worker in myworkers
#         @test master != worker
#     end

#     #
#     # Unit Test 5 - Verify that the cloud-init startup script ran successfully
#     #
#     for i = 1:tppi
#         @test remotecall_fetch(isfile, workers()[i], ".git-credentials")
#     end

#     #
#     # Unit Test 6 - Delete the Julia processes, scale set instances and the scale set itself
#     #

#     # First, verify that the scale set is present
#     session = AzSession()
#     _r = HTTP.request("GET", url, Dict("Authorization"=>"Bearer $(token(session))"); verbose=0)
#     @test _r.status == 200

#     @info "Deleting cluster..."
#     rmprocs(workers())

#     # Last, verify that the scale set has been deleted
#     while true
#         try
#             session = AzSession()    
#             HTTP.request("GET", url, Dict("Authorization"=>"Bearer $(token(session))"); verbose=0)
#         catch _e
#             e = JSON.parse(String(_e.response.body))
#             if _e.status == 404 && e["error"]["code"] == "ResourceNotFound"
#                 @info "Cluster deleted!"
#                 break
#             end
#         end
#         sleep(5)
#     end
# end

# # Needs work - Default VM sku causes a failure here
# # @testset "AzManagers, addproc" for kwargs in ( (basename = "test$(randstring('a':'z',4))"), )
# #     addproc(template; basename=kwargs.basename)
# # end

# @testset "AzManagers, detach" for kwargs in ( (session = AzSession()), )

#     #
#     # Unit Test 1 - Create a detached job and persist the server
#     #
#     job1 = @detach vm(;session=kwargs.session, persist=true) begin
#         write(stdout, "job1 - stdout string")
#         write(stderr, "job1 - stderr string")
#     end

#     #
#     # Unit Test 2 - Send a new job to the server started above
#     #
#     job2 = @detachat job1.vm["ip"] begin
#         write(stdout, "job2 - stdout string")
#         write(stderr, "job2 - stderr string")
#     end

#     # Wait for jobs to finish
#     wait(job1)
#     wait(job2)

#     @test status(job1) == "done"
#     @test read(job1;stdio=stdout) == "job1 - stdout string"
#     @test read(job1;stdio=stderr) == "job1 - stderr string"

#     @test status(job2) == "done"
#     @test read(job2;stdio=stdout) == "job2 - stdout string"
#     @test read(job2;stdio=stderr) == "job2 - stderr string"

#     #
#     # Unit Test 3 - shut-down the detached server
#     #
#     rmproc(job1.vm)

#     #
#     # Unit Test 4 - create a new job on a new detached server that auto-destructs upon completion of its work
#     #
#     job3 = @detach vm(;session=kwargs.session, persist=false) begin
#     end
# end
