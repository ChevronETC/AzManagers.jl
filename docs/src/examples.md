# Examples

## Julia cluster

```julia
using Distributed, AzManagers

# add 10 instances to an Azure scale set.  "cbox08" scale-set template.
addprocs("cbox08", 10)
length(workers()) # 10

# add 10 more instances
addprocs("cbox08", 10)
length(workers()) # 20

# remove the first 5 instances
rmprocs(workers()[1:5])

# remove remaining instances
rmprocs(workers())

# list self-doc for AzManagers addprocs method:
?addprocs

# make a scale-set from SPOT VMs
addprocs("cbox08", 10; group="myspotgroup", spot=true)
remotecall_fetch(preempted, workers()[1]) # check to see if Azure is shutting down your machine 
```