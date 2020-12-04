# AzManagers

AzManagers is a Julia resource/cluster manager for Azure scale-sets. In turn, An Azure
scale-set is a collection of virtual machines (VMs).  To describe the scale-set, AzManagers
uses a user-defined template.  For example, we can create a new julia cluster consisting of
5 VMs, and where the scale-set is described by the template `"myscaleset"` as follows,
```julia
using AzManagers, Distributed
addprocs("myscaleset", 5)
```
Note that `addprocs` will return as soon as the provisioning is initialized.  Subsequently, workers
will add themselves to the Julia cluster as they become available.  This is similar to the "elastic.jl" 
cluster manager in [ClusterManagers.jl](https://github.com/JuliaParallel/ClusterManagers.jl), and allows
AzManagers to behave dynamically.  To wait for the cluster to be completely up use the `waitfor` argument.
For example,
```julia
using AzManagers, Distributed
addprocs("myscaleset", 5; waitfor=true)
```
In this case `addprocs` will return only once the 5 workers have joined the cluster.

The scaleset template, `"myscaleset"' describes the compute infrastructure.  Importantly,
this includes the image that is attached to the VMs.  The user is responsible for creating
this image, and ensuring that it contains both julia and the AzManagers package.  In the
future, we will work to provide standard images.  Please see the section [VM images](# VM images)
for more information.

AzManagers does not provide scale-set templates since they will depend on your specific Azure
setup.  However, we provide a means to create the templates.  Please see the section
[Scale-set templates](# Scale-set templates) for more information. 

AzManagers requires a user provided Azure resource group and subscription, as well as information
about the ssh user for the scale-set VMs.  AzManagers uses a manifest file to store this information.
See the section [AzManagers manifest](# AzManagers manifest) for more information.

The consequence of the above is that, at preset, we place the burden on the user to correctly, 1)
create an image, 2) create templates, and 3) create a manifest file.

In addition to the julia cluster/scale-set functionality, AzManagers provides a method for
running and monitoring arbitrary julia code on an Azure VM.  This is useful for long running
processes in a way that is completely independent of your personal computer.  See the section
[Detached service](# Detached service) for more information.

# Scale-set templates
To create a scale-set template, use `AzManagers.build_sstemplate` and
`AzManagers.save_template_scaleset` methods.  For example:
```julia
using AzManagers
myscaleset = AzManagers.build_sstemplate("myvm",
    subscriptionid       = "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
    admin_username       = "linuxuser",
    location             = "southcentralus",
    resourcegroup        = "my-resource-group",
    vnet                 = "my-vnet",
    subnet               = "my-vnets-subnet",
    imagegallery         = "my-image-gallery",
    imagename            = "my-image-name",
    skuname              = "Standard_D2s_v3")
AzManagers.save_template_scaleset("myscaleset", myscaleset)
```
The above code will save the template to the json file, `~/.azmanagers/templates_scaleset.json`.
Subsequently, `addprocs("myscaleset", 5)` will query the json file for the VM template.  One can
repeat this process, populating `~/.azmanagers/templates_scaleset.json` with a variety of templates
for a variety of machine types.

# VM images
In the previous section, notice that `AzManagers.build_sstemplate` includes the arguments
`imagename` and `imagegallery`.  This specifies the Azure image that will be attached to each
VM in the scale-set.  In order to build an Azure image and place it in an image gallery,  we
recommend using packer (https://www.packer.io/).  It is important that the image should contain
the software that you want to run on the machines in the Julia cluster, and that should include
both Julia and the AzManagers package.

# Azure manifest
The manifest is stored in `~/.azmanagers/manifest.json`, and contains information that is specific
to your Azure setup.  We create the manifest using the `AzManagers.write_manifest` method.  For
example,
```julia
using AzManagers
AzManagers.write_manifest(;
    resourcegroup  = "my-resource-group",
    subscriptionid = "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
    ssh_user = "username")
``` 
One can also specify the locations of the public and private ssh keys which AzManagers will use
to establish ssh connections to the cluster machines.  This connection is used for the initial
set-up of the cluster, and for sending log messages back to the master process.  By default,
the private key is `~/.ssh/azmanagers_rsa` and the public key is `~/.ssh/azmanagers_rsa.pub`.
Create the key-pair via the linux command,
```
ssh-keygen -f /home/cvx/.ssh/azmanagers_rsa -N ''
```

# Logging
By default, logs in AzManagers behaves the same as Julia's `SSHManager`.  In particular, log
messages generated by `@info` and-the-like are sent to the master process over ssh.  However,
it is useful to note that a custom logger that sends worker messages to a cloud logger services
such as azure log analytics might be useful.  At this time, AzManagers does not provide such a
logger; but, if one had such a logger (e.g. MyAzureLogger), then one would do:
```
using AzManagers, Distributed
addprocs("myscaleset",5)
@everywhere using Logging, MyAzureLogger
@everywhere global_logger(MyAzureLogger())
```

# Detached service
For long running processes it is convenient to use an Azure VM for the master process, and where that
Azure VM is independent of your personal computer.  To accomplish this, AzManagers
provides an `addproc` method that creates an azure VM (i.e. a detached VM), and also starts a very
small web service to allow for the querying of the detached VM.  A convenience macro `@detachat` is
provided for running code on the detached VM. In a way this is similar to an
Azure function but without some of its limitation (e.g. time limits).  Here is an example:
```julia
using AzManagers

myvm = addproc("myvm")
detached_job = @detachat myvm begin
    @info "this is running on the detached vm"
end

read(detached_job) # returns the job's stdout
read(detached_job; stdio=stderr) # returns the job's stderr
status(detached_job) # returns the status of the detached job
wait(detached_job) # blocks unitl the detached job is completed
rmproc(myvm)
```
In the above example, `"myvm"` is a template similar to a scale-set template, and
can be created in a similar way using the `AzManagers.build_vmtemplate` and
`AzManagers.save_template_vm` methods. In addition, you will need to create a template
for a network interface card (NIC) using the `AzManagers.build_nictemplate` and
`AzManagers.save_template_nic` methods.

In a more involved example, one might want to serialize and send variables to the detached
VM.  In the following example, we use the `variablebundle!` and `variablebundle` methods
to accomplish this task.
```julia
using AzManagers, AzSessions

variablebundle!(session = AzSession())

myvm = addproc("myvm")
detached_job = @detachat myvm begin
    using Distributed, AzManagers
    addprocs("myscaleset", 5; session=variablebundle(:session))
    for pid in workers()
        remotecall_fetch(println, "hello from pid=$(myid())")
    end
    rmprocs(workers())
end
wait(detached_job)
rmproc(vm)
```

# A note about MPI (experimental)
We have experimental support for inter-node MPI communication.  In other words, we allow for each
Julia worker to have its own MPI communicator.  For example, this is useful for interacting with
Devito.jl, and where there are performance benefits to parallel work within a many-core
VM via a mix of MPI and OpenMP.

# Custom environments
AzManagers can create an on-the-fly custom Julia software environment for the workers.
This is managed via Julia environments.  If you update your Julia environment, then
you can, commit those updates to a branch and push that branch to a remote.  Subsequently, 
when you create a cluster, the worker nodes will, at boot time, instantiate the environment.
For example:
```sh
julia -e `using Pkg; pkg"add FFTW"`
cd /home/cvx/.julia/environments/v1.5
git branch custom_environment
git add Manifest.toml
git add Project.toml
git commit -m "custom environment"
git push origin custom_environment
```
Now, when worker VMs are initialized, they will have the software stack
defined in `custom_environment`.  Please note that this can add significant
overhead to the boot-time of the VMs.