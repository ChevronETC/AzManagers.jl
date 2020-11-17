# Examples

## Julia cluster

```julia
using Distributed, AzManagers

# add 10 instances to an Azure scale set with the "cbox08" scale-set template.
addprocs("cbox08", 10)

# monitor the workers as they join the Julia cluster.
while nprocs() == nworkers() || nworkers() < 10
    sleep(10)
    @show workers()
end
length(workers()) # 10

# add 10 more instances, waiting for the cluster to be stable before returning control to the caller.
addprocs("cbox08", 10; waitfor=true)
length(workers()) # 20

# remove the first 5 instances
rmprocs(workers()[1:5])

# remove remaining instances
rmprocs(workers())

# list self-doc for AzManagers addprocs method:
?addprocs

# make a scale-set from SPOT VMs
addprocs("cbox08", 10; group="myspotgroup", spot=true)

# wait for at-least one worker to be available
while nprocs() - nworkers() == 0; yield(); end

# check to see if Azure is shutting down (pre-empt'ing) the worker
remotecall_fetch(preempted, workers()[1])
```