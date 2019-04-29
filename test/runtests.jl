using Distributed, AzManagers, Random, Test

import GCPManagers:baseurl_meta,gcprequest_meta,header_meta,instancegroup_listmanagedinstances

template = "jbox02"
subscriptionid = ENV["subscriptionid"]
resourcegroup = ENV["resourcegroup"]

@testset "AzManagers, addprocs" for kwargs in ( (subscriptionid=subscriptionid,resourcegroup=resourcegroup), (subscriptionid=subscriptionid, resourcegroup=resourcegroup, ppi=2) )
    addprocs(template, 2; scalesetname="test$(randstring('a':'z',4))", kwargs...)

    ppid = haskey(kwargs, :ppi) ? kwargs.ppi : 1

    @test nworkers() == 2*ppi
    myworkers = [remotecall_fetch(gethostname, workers()[i]) for i=1:(2*ppi)]
    master = gethostname()
    unique_workers = unique(myworkers)
    @test length(unique_workers) == 2
    for worker in myworkers
        @test master != worker
    end

    rmprocs(workers())
end
