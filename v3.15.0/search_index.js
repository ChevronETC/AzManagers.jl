var documenterSearchIndex = {"docs":
[{"location":"examples/#Examples","page":"Examples","title":"Examples","text":"","category":"section"},{"location":"examples/#Julia-cluster","page":"Examples","title":"Julia cluster","text":"","category":"section"},{"location":"examples/","page":"Examples","title":"Examples","text":"using Distributed, AzManagers\n\n# add 10 instances to an Azure scale set with the \"cbox08\" scale-set template.\naddprocs(\"cbox08\", 10)\n\n# monitor the workers as they join the Julia cluster.\nwhile nprocs() == nworkers() || nworkers() < 10\n    sleep(10)\n    @show workers()\nend\nlength(workers()) # 10\n\n# add 10 more instances, waiting for the cluster to be stable before returning control to the caller.\naddprocs(\"cbox08\", 10; waitfor=true)\nlength(workers()) # 20\n\n# remove the first 5 instances\nrmprocs(workers()[1:5])\n\n# remove remaining instances\nrmprocs(workers())\n\n# list self-doc for AzManagers addprocs method:\n?addprocs\n\n# make a scale-set from SPOT VMs\naddprocs(\"cbox08\", 10; group=\"myspotgroup\", spot=true)\n\n# wait for at-least one worker to be available\nwhile nprocs() - nworkers() == 0; yield(); end\n\n# check to see if Azure is shutting down (pre-empt'ing) the worker\nremotecall_fetch(preempted, workers()[1])","category":"page"},{"location":"reference/#Reference","page":"Reference","title":"Reference","text":"","category":"section"},{"location":"reference/#Julia-clusters","page":"Reference","title":"Julia clusters","text":"","category":"section"},{"location":"reference/","page":"Reference","title":"Reference","text":"addprocs\npreempted","category":"page"},{"location":"reference/#Distributed.addprocs","page":"Reference","title":"Distributed.addprocs","text":"addprocs(template, ninstances[; kwargs...])\n\nAdd Azure scale set instances where template is either a dictionary produced via the AzManagers.build_sstemplate method or a string corresponding to a template stored in ~/.azmanagers/templates_scaleset.json.\n\nkey word arguments:\n\nsubscriptionid=template[\"subscriptionid\"] if exists, or AzManagers._manifest[\"subscriptionid\"] otherwise.\nresourcegroup=template[\"resourcegroup\"] if exists, or AzManagers._manifest[\"resourcegroup\"] otherwise.\nsigimagename=\"\" The name of the SIG image[1].\nsigimageversion=\"\" The version of the sigimagename[1].\nimagename=\"\" The name of the image (alternative to sigimagename and sigimageversion used for development work).\nosdisksize=60 The size of the OS disk in GB.\ncustomenv=false If true, then send the current project environment to the workers where it will be instantiated.\nsession=AzSession(;lazy=true) The Azure session used for authentication.\ngroup=\"cbox\" The name of the Azure scale set.  If the scale set does not yet exist, it will be created.\noverprovision=true Use Azure scle-set overprovisioning?\nppi=1 The number of Julia processes to start per Azure scale set instance.\njulia_num_threads=\"$(Threads.nthreads(),$(Threads.nthreads(:interactive))\" set the number of julia threads for the detached process.[2]\nomp_num_threads=get(ENV, \"OMP_NUM_THREADS\", 1) set the number of OpenMP threads to run on each worker\nexename=\"$(Sys.BINDIR)/julia\" name of the julia executable.\nexeflags=\"\" set additional command line start-up flags for Julia workers.  For example, --heap-size-hint=1G.\nenv=Dict() each dictionary entry is an environment variable set on the worker before Julia starts. e.g. env=Dict(\"OMP_PROC_BIND\"=>\"close\")\nnretry=20 Number of retries for HTTP REST calls to Azure services.\nverbose=0 verbose flag used in HTTP requests.\nsave_cloud_init_failures=false set to true to copy cloud init logs (/var/log/clout-init-output.log) from workers that fail to join the cluster.\nshow_quota=false after various operation, show the \"x-ms-rate-remaining-resource\" response header.  Useful for debugging/understanding Azure quota's.\nuser=AzManagers._manifest[\"ssh_user\"] ssh user.\nspot=false use Azure SPOT VMs for the scale-set\nmaxprice=-1 set maximum price per hour for a VM in the scale-set.  -1 uses the market price.\nspot_base_regular_priority_count=0 If spot is true, only start adding spot machines once there are this many non-spot machines added.\nspot_regular_percentage_above_base If spot is true, then when ading new machines (above spot_base_reqular_priority_count) use regular (non-spot) priority for this percent of new machines.\nwaitfor=false wait for the cluster to be provisioned before returning, or return control to the caller immediately[3]\nmpi_ranks_per_worker=0 set the number of MPI ranks per Julia worker[4]\nmpi_flags=\"-bind-to core:$(ENV[\"OMP_NUM_THREADS\"]) -map-by numa\" extra flags to pass to mpirun (has effect when mpi_ranks_per_worker>0)\nnvidia_enable_ecc=true on NVIDIA machines, ensure that ECC is set to true or false for all GPUs[5]\nnvidia_enable_mig=false on NVIDIA machines, ensure that MIG is set to true or false for all GPUs[5]\nhyperthreading=nothing Turn on/off hyperthreading on supported machine sizes.  The default uses the setting in the template.  To override the template setting, use true (on) or false (off).\nuse_lvm=false For SKUs that have 1 or more nvme disks, combines all disks as a single mount point /scratch vs /scratch, /scratch1, /scratch2, etc..\n\nNotes\n\n[1] If addprocs is called from an Azure VM, then the default imagename,imageversion are the image/version the VM was built with; otherwise, it is the latest version of the image specified in the scale-set template. [2] Interactive threads are supported beginning in version 1.9 of Julia.  For earlier versions, the default for julia_num_threads is Threads.nthreads(). [3] waitfor=false reflects the fact that the cluster manager is dynamic.  After the call to addprocs returns, use workers() to monitor the size of the cluster. [4] This is inteneded for use with Devito.  In particular, it allows Devito to gain performance by using MPI to do domain decomposition using MPI within a single VM.  If mpi_ranks_per_worker=0, then MPI is not used on the Julia workers.  This feature makes use of package extensions, meaning that you need to ensure that using MPI is somewhere in your calling script. [5] This may result in a re-boot of the VMs\n\n\n\n\n\n","category":"function"},{"location":"reference/#AzManagers.preempted","page":"Reference","title":"AzManagers.preempted","text":"ispreempted,notbefore = preempted([id=myid()|id=\"instanceid\"])\n\nCheck to see if the machine id::Int has received an Azure spot preempt message.  Returns (true, notbefore) if a preempt message is received and (false,\"\") otherwise.  notbefore is the date/time before which the machine is guaranteed to still exist.\n\n\n\n\n\n","category":"function"},{"location":"reference/#Detached-service","page":"Reference","title":"Detached service","text":"","category":"section"},{"location":"reference/","page":"Reference","title":"Reference","text":"addproc\n@detachat\nvariablebundle\nvariablebundle!\nread\nrmproc\nstatus\nwait","category":"page"},{"location":"reference/#AzManagers.addproc","page":"Reference","title":"AzManagers.addproc","text":"addproc(template[; name=\"\", basename=\"cbox\", subscriptionid=\"myid\", resourcegroup=\"mygroup\", nretry=10, verbose=0, session=AzSession(;lazy=true), sigimagename=\"\", sigimageversion=\"\", imagename=\"\", detachedservice=true])\n\nCreate a VM, and returns a named tuple (name,ip,resourcegrup,subscriptionid) where name is the name of the VM, and ip is the ip address of the VM. resourcegroup and subscriptionid denote where the VM resides on Azure.\n\nParameters\n\nname=\"\" name for the VM.  If it is not an empty string, then the next paramter (basename) is ignored\nbasename=\"cbox\" base name for the VM, we append a random suffix to ensure uniqueness\nsubscriptionid=template[\"subscriptionid\"] if exists, or AzManagers._manifest[\"subscriptionid\"] otherwise.\nresourcegroup=template[\"resourcegroup\"] if exists, or AzManagers._manifest[\"resourcegroup\"] otherwise.\nsession=AzSession(;lazy=true) Session used for OAuth2 authentication\nsigimagename=\"\" Azure shared image gallery image to use for the VM (defaults to the template's image)\nsigimageversion=\"\" Azure shared image gallery image version to use for the VM (defaults to latest)\nimagename=\"\" Azure image name used as an alternative to sigimagename and sigimageversion (used for development work)\nosdisksize=60 Disk size of the OS disk in GB\ncustomenv=false If true, then send the current project environment to the workers where it will be instantiated.\nnretry=10 Max retries for re-tryable REST call failures\nverbose=0 Verbosity flag passes to HTTP.jl methods\nshow_quota=false after various operation, show the \"x-ms-rate-remaining-resource\" response header.  Useful for debugging/understanding Azure quota's.\njulia_num_threads=\"$(Threads.nthreads(),$(Threads.nthreads(:interactive))\" set the number of julia threads for the workers.[1]\nomp_num_threads = get(ENV, \"OMP_NUM_THREADS\", 1) set OMP_NUM_THREADS environment variable before starting the detached process\nexename=\"$(Sys.BINDIR)/julia\" name of the julia executable.\nenv=Dict() Dictionary of environemnt variables that will be exported before starting the detached process\ndetachedservice=true start the detached service allowing for RESTful remote code execution\nuse_lvm=false For SKUs that have 1 or more nvme disks, combines all disks as a single mount point /scratch vs /scratch, /scratch1, /scratch2, etc..\n\nNotes\n\n[1] Interactive threads are supported beginning in version 1.9 of Julia.  For earlier versions, the default for julia_num_threads is Threads.nthreads().\n\n\n\n\n\n","category":"function"},{"location":"reference/#AzManagers.@detachat","page":"Reference","title":"AzManagers.@detachat","text":"@detachat myvm begin ... end\n\nRun code on an Azure VM.\n\nExample\n\nusing AzManagers\nmyvm = addproc(\"myvm\")\njob = @detachat myvm begin\n    @info \"I'm running detached\"\nend\nread(job)\nwait(job)\nrmproc(myvm)\n\n\n\n\n\n","category":"macro"},{"location":"reference/#AzManagers.variablebundle","page":"Reference","title":"AzManagers.variablebundle","text":"variablebundle(:key)\n\nRetrieve a variable from a variable bundle.  See variablebundle! for more information.\n\n\n\n\n\n","category":"function"},{"location":"reference/#AzManagers.variablebundle!","page":"Reference","title":"AzManagers.variablebundle!","text":"variablebundle!(;kwargs...)\n\nDefine variables that will be passed to a detached job.\n\nExample\n\nusing AzManagers\nvariablebundle(;x=1)\nmyvm = addproc(\"myvm\")\nmyjob = @detachat myvm begin\n    write(stdout, \"my variable is $(variablebundle(:x))\n\")\nend\nwait(myjob)\nread(myjob)\n\n\n\n\n\n","category":"function"},{"location":"reference/#Base.read","page":"Reference","title":"Base.read","text":"read(job[;stdio=stdout])\n\nreturns the stdout from a detached job.\n\n\n\n\n\n","category":"function"},{"location":"reference/#AzManagers.rmproc","page":"Reference","title":"AzManagers.rmproc","text":"rmproc(vm[; session=AzSession(;lazy=true), verbose=0, nretry=10])\n\nDelete the VM that was created using the addproc method.\n\nParameters\n\nsession=AzSession(;lazy=true) Azure session for OAuth2 authentication\nverbose=0 verbosity flag passed to HTTP.jl methods\nnretry=10 max number of retries for retryable REST calls\nshow_quota=false after various operation, show the \"x-ms-rate-remaining-resource\" response header.  Useful for debugging/understanding Azure quota's.\n\n\n\n\n\n","category":"function"},{"location":"reference/#AzManagers.status","page":"Reference","title":"AzManagers.status","text":"status(job)\n\nreturns the status of a detached job.\n\n\n\n\n\n","category":"function"},{"location":"reference/#Base.wait","page":"Reference","title":"Base.wait","text":"wait(job[;stdio=stdout])\n\nblocks until the detached job, job, is complete.\n\n\n\n\n\n","category":"function"},{"location":"reference/#Configuration","page":"Reference","title":"Configuration","text":"","category":"section"},{"location":"reference/","page":"Reference","title":"Reference","text":"AzManagers.build_nictemplate\nAzManagers.build_sstemplate\nAzManagers.build_vmtemplate\nAzManagers.write_manifest\nAzManagers.save_template_nic\nAzManagers.save_template_scaleset\nAzManagers.save_template_vm","category":"page"},{"location":"reference/#AzManagers.build_nictemplate","page":"Reference","title":"AzManagers.build_nictemplate","text":"AzManagers.build_nictemplate(nic_name; kwargs...)\n\nReturns a dictionary for a NIC template, and that can be passed to the addproc method, or written to AzManagers.jl configuration files.\n\nRequired keyword arguments\n\nsubscriptionid Azure subscription\nresourcegroup_vnet Azure resource group that holds the virtual network that the NIC is attaching to.\nvnet Azure virtual network for the NIC to attach to.\nsubnet Azure sub-network name.\nlocation location of the Azure data center where the NIC correspond to.\n\nOptional keyword arguments\n\naccelerated=true use accelerated networking (not all VM sizes support accelerated networking).\n\n\n\n\n\n","category":"function"},{"location":"reference/#AzManagers.build_sstemplate","page":"Reference","title":"AzManagers.build_sstemplate","text":"AzManagers.build_sstemplate(name; kwargs...)\n\nreturns a dictionary that is an Azure scaleset template for use in addprocs or for saving to the ~/.azmanagers folder.\n\nrequired key-word arguments\n\nsubscriptionid Azure subscription\nadmin_username ssh user for the scaleset virtual machines\nlocation Azure data-center location\nresourcegroup Azure resource-group\nimagegallery Azure image gallery that contains the VM image\nimagename Azure image\nvnet Azure virtual network for the scaleset\nsubnet Azure virtual subnet for the scaleset\nskuname Azure VM type\n\noptional key-word arguments\n\nsubscriptionid_image Azure subscription corresponding to the image gallery, defaults to subscriptionid\nresourcegroup_vnet Azure resource group corresponding to the virtual network, defaults to resourcegroup\nresourcegroup_image Azure resource group correcsponding to the image gallery, defaults to resourcegroup\nosdisksize=60 Disk size in GB for the operating system disk\nskutier = \"Standard\" Azure SKU tier.\ndatadisks=[] list of data disks to create and attach [1]\ntempdisk = \"sudo mkdir -m 777 /mnt/scratch; ln -s /mnt/scratch /scratch\" cloud-init commands used to mount or link to temporary disk\ntags = Dict(\"azure_tag_name\" => \"some_tag_value\") Optional tags argument for resource\nencryption_at_host = false Optional argument for enabling encryption at host\n\nNotes\n\n[1] Each datadisk is a Dictionary. For example,\n\nDict(\"createOption\"=>\"Empty\", \"diskSizeGB\"=>1023, \"managedDisk\"=>Dict(\"storageAccountType\"=>\"PremiumSSD_LRS\"))\n\nor, to accept the defaults,\n\nDict(\"diskSizeGB\"=>1023)\n\nThe above example is populated with the default options.  So, if datadisks=[Dict()], then the default options will be included.\n\n\n\n\n\n","category":"function"},{"location":"reference/#AzManagers.build_vmtemplate","page":"Reference","title":"AzManagers.build_vmtemplate","text":"AzManagers.build_vmtemplate(vm_name; kwargs...)\n\nReturns a dictionary for a virtual machine template, and that can be passed to the addproc method or written to AzManagers.jl configuration files.\n\nRequired keyword arguments\n\nsubscriptionid Azure subscription\nadmin_username ssh user for the scaleset virtual machines\nlocation Azure data center location\nresourcegroup Azure resource group where the VM will reside\nimagegallery Azure shared image gallery name\nimagename Azure image name that is in the shared image gallery \nvmsize Azure vm type, e.g. \"StandardD8sv3\"\n\nOptional keyword arguments\n\nresourcegroup_vnet Azure resource group containing the virtual network, defaults to resourcegroup\nsubscriptionid_image Azure subscription containing the image gallery, defaults to subscriptionid\nresourcegroup_image Azure resource group containing the image gallery, defaults to subscriptionid\nnicname = \"cbox-nic\" Name of the NIC for this VM\nosdisksize = 60 size in GB of the OS disk\ndatadisks=[] additional data disks to attach\n`tempdisk = \"sudo mkdir -m 777 /mnt/scratch\n\nln -s /mnt/scratch /scratch\"`  cloud-init commands used to mount or link to temporary disk\n\ntags = Dict(\"azure_tag_name\" => \"some_tag_value\") Optional tags argument for resource\nencryption_at_host = false Optional argument for enabling encryption at host\ndefault_nic = \"\" Optional argument for inserting \"default_nic\" as a key \n\nNotes\n\n[1] Each datadisk is a Dictionary. For example,\n\nDict(\"createOption\"=>\"Empty\", \"diskSizeGB\"=>1023, \"managedDisk\"=>Dict(\"storageAccountType\"=>\"PremiumSSD_LRS\"))\n\nThe above example is populated with the default options.  So, if datadisks=[Dict()], then the default options will be included.\n\n\n\n\n\n","category":"function"},{"location":"reference/#AzManagers.write_manifest","page":"Reference","title":"AzManagers.write_manifest","text":"AzManagers.write_manifest(;resourcegroup=\"\", subscriptionid=\"\", ssh_user=\"\", ssh_public_key_file=\"~/.ssh/azmanagers_rsa.pub\", ssh_private_key_file=\"~/.ssh/azmanagers_rsa\")\n\nWrite an AzManagers manifest file (~/.azmanagers/manifest.json).  The manifest file contains information specific to your Azure account.\n\n\n\n\n\n","category":"function"},{"location":"reference/#AzManagers.save_template_nic","page":"Reference","title":"AzManagers.save_template_nic","text":"AzManagers.save_template_nic(nic_name, template)\n\nSave template::Dict generated by AzManagers.buildnictmplate to /home/runner/.azmanagers/templatesnic.json.\n\n\n\n\n\n","category":"function"},{"location":"reference/#AzManagers.save_template_scaleset","page":"Reference","title":"AzManagers.save_template_scaleset","text":"AzManagers.save_template_scaleset(scalesetname, template)\n\nSave template::Dict generated by AzManagers.buildsstemplate to /home/runner/.azmanagers/templatesscaleset.json.\n\n\n\n\n\n","category":"function"},{"location":"reference/#AzManagers.save_template_vm","page":"Reference","title":"AzManagers.save_template_vm","text":"AzManagers.save_template_vm(vm_name, template)\n\nSave template::Dict generated by AzManagers.buildvmtmplate to /home/runner/.azmanagers/templatesvm.json.\n\n\n\n\n\n","category":"function"},{"location":"#AzManagers","page":"AzManagers","title":"AzManagers","text":"","category":"section"},{"location":"","page":"AzManagers","title":"AzManagers","text":"AzManagers is a Julia resource/cluster manager for Azure scale-sets. In turn, An Azure scale-set is a collection of virtual machines (VMs).  To describe the scale-set, AzManagers uses a user-defined template.  For example, we can create a new julia cluster consisting of 5 VMs, and where the scale-set is described by the template \"myscaleset\" as follows,","category":"page"},{"location":"","page":"AzManagers","title":"AzManagers","text":"using AzManagers, Distributed\naddprocs(\"myscaleset\", 5)","category":"page"},{"location":"","page":"AzManagers","title":"AzManagers","text":"Note that addprocs will return as soon as the provisioning is initialized.  Subsequently, workers will add themselves to the Julia cluster as they become available.  This is similar to the \"elastic.jl\"  cluster manager in ClusterManagers.jl, and allows AzManagers to behave dynamically.  To wait for the cluster to be completely up use the waitfor argument. For example,","category":"page"},{"location":"","page":"AzManagers","title":"AzManagers","text":"using AzManagers, Distributed\naddprocs(\"myscaleset\", 5; waitfor=true)","category":"page"},{"location":"","page":"AzManagers","title":"AzManagers","text":"In this case addprocs will return only once the 5 workers have joined the cluster.","category":"page"},{"location":"","page":"AzManagers","title":"AzManagers","text":"The scaleset template, `\"myscaleset\"' describes the compute infrastructure.  Importantly, this includes the image that is attached to the VMs.  The user is responsible for creating this image, and ensuring that it contains both julia and the AzManagers package.  In the future, we will work to provide standard images.  Please see the section VM images for more information.","category":"page"},{"location":"","page":"AzManagers","title":"AzManagers","text":"AzManagers does not provide scale-set templates since they will depend on your specific Azure setup.  However, we provide a means to create the templates.  Please see the section Scale-set templates for more information. ","category":"page"},{"location":"","page":"AzManagers","title":"AzManagers","text":"AzManagers requires a user provided Azure resource group and subscription, as well as information about the ssh user for the scale-set VMs.  AzManagers uses a manifest file to store this information. See the section AzManagers manifest for more information.","category":"page"},{"location":"","page":"AzManagers","title":"AzManagers","text":"The consequence of the above is that, at preset, we place the burden on the user to correctly, 1) create an image, 2) create templates, and 3) create a manifest file.","category":"page"},{"location":"","page":"AzManagers","title":"AzManagers","text":"In addition to the julia cluster/scale-set functionality, AzManagers provides a method for running and monitoring arbitrary julia code on an Azure VM.  This is useful for long running processes in a way that is completely independent of your personal computer.  See the section Detached service for more information.","category":"page"},{"location":"#Scale-set-templates","page":"AzManagers","title":"Scale-set templates","text":"","category":"section"},{"location":"","page":"AzManagers","title":"AzManagers","text":"To create a scale-set template, use AzManagers.build_sstemplate and AzManagers.save_template_scaleset methods.  For example:","category":"page"},{"location":"","page":"AzManagers","title":"AzManagers","text":"using AzManagers\nmyscaleset = AzManagers.build_sstemplate(\"myvm\",\n    subscriptionid       = \"xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx\",\n    admin_username       = \"linuxuser\",\n    location             = \"southcentralus\",\n    resourcegroup        = \"my-resource-group\",\n    vnet                 = \"my-vnet\",\n    subnet               = \"my-vnets-subnet\",\n    imagegallery         = \"my-image-gallery\",\n    imagename            = \"my-image-name\",\n    skuname              = \"Standard_D2s_v3\",\n    encryption_at_host   = false)\nAzManagers.save_template_scaleset(\"myscaleset\", myscaleset)","category":"page"},{"location":"","page":"AzManagers","title":"AzManagers","text":"The above code will save the template to the json file, ~/.azmanagers/templates_scaleset.json. Subsequently, addprocs(\"myscaleset\", 5) will query the json file for the VM template.  One can repeat this process, populating ~/.azmanagers/templates_scaleset.json with a variety of templates for a variety of machine types.","category":"page"},{"location":"#VM-images","page":"AzManagers","title":"VM images","text":"","category":"section"},{"location":"","page":"AzManagers","title":"AzManagers","text":"In the previous section, notice that AzManagers.build_sstemplate includes the arguments imagename and imagegallery.  This specifies the Azure image that will be attached to each VM in the scale-set.  In order to build an Azure image and place it in an image gallery,  we recommend using packer (https://www.packer.io/).  It is important that the image should contain the software that you want to run on the machines in the Julia cluster, and that should include both Julia and the AzManagers package.","category":"page"},{"location":"#Azure-manifest","page":"AzManagers","title":"Azure manifest","text":"","category":"section"},{"location":"","page":"AzManagers","title":"AzManagers","text":"The manifest is stored in ~/.azmanagers/manifest.json, and contains information that is specific to your Azure setup.  We create the manifest using the AzManagers.write_manifest method.  For example,","category":"page"},{"location":"","page":"AzManagers","title":"AzManagers","text":"using AzManagers\nAzManagers.write_manifest(;\n    resourcegroup  = \"my-resource-group\",\n    subscriptionid = \"xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx\",\n    ssh_user = \"username\")","category":"page"},{"location":"","page":"AzManagers","title":"AzManagers","text":"One can also specify the locations of the public and private ssh keys which AzManagers will use to establish ssh connections to the cluster machines.  This connection is used for the initial set-up of the cluster, and for sending log messages back to the master process.  By default, the private key is ~/.ssh/azmanagers_rsa and the public key is ~/.ssh/azmanagers_rsa.pub. Create the key-pair via the linux command,","category":"page"},{"location":"","page":"AzManagers","title":"AzManagers","text":"ssh-keygen -f /home/cvx/.ssh/azmanagers_rsa -N ''","category":"page"},{"location":"#Logging","page":"AzManagers","title":"Logging","text":"","category":"section"},{"location":"","page":"AzManagers","title":"AzManagers","text":"By default, logs in AzManagers behaves the same as Julia's SSHManager.  In particular, log messages generated by @info and-the-like are sent to the master process over ssh.  However, it is useful to note that a custom logger that sends worker messages to a cloud logger services such as azure log analytics might be useful.  At this time, AzManagers does not provide such a logger; but, if one had such a logger (e.g. MyAzureLogger), then one would do:","category":"page"},{"location":"","page":"AzManagers","title":"AzManagers","text":"using AzManagers, Distributed\naddprocs(\"myscaleset\",5)\n@everywhere using Logging, MyAzureLogger\n@everywhere global_logger(MyAzureLogger())","category":"page"},{"location":"#Detached-service","page":"AzManagers","title":"Detached service","text":"","category":"section"},{"location":"","page":"AzManagers","title":"AzManagers","text":"For long running processes it is convenient to use an Azure VM for the master process, and where that Azure VM is independent of your personal computer.  To accomplish this, AzManagers provides an addproc method that creates an azure VM (i.e. a detached VM), and also starts a very small web service to allow for the querying of the detached VM.  A convenience macro @detachat is provided for running code on the detached VM. In a way this is similar to an Azure function but without some of its limitation (e.g. time limits).  Here is an example:","category":"page"},{"location":"","page":"AzManagers","title":"AzManagers","text":"using AzManagers\n\nmyvm = addproc(\"myvm\")\ndetached_job = @detachat myvm begin\n    @info \"this is running on the detached vm\"\nend\n\nread(detached_job) # returns the job's stdout\nread(detached_job; stdio=stderr) # returns the job's stderr\nstatus(detached_job) # returns the status of the detached job\nwait(detached_job) # blocks unitl the detached job is completed\nrmproc(myvm)","category":"page"},{"location":"","page":"AzManagers","title":"AzManagers","text":"In the above example, \"myvm\" is a template similar to a scale-set template, and can be created in a similar way using the AzManagers.build_vmtemplate and AzManagers.save_template_vm methods. In addition, you will need to create a template for a network interface card (NIC) using the AzManagers.build_nictemplate and AzManagers.save_template_nic methods.","category":"page"},{"location":"","page":"AzManagers","title":"AzManagers","text":"In a more involved example, one might want to serialize and send variables to the detached VM.  In the following example, we use the variablebundle! and variablebundle methods to accomplish this task.","category":"page"},{"location":"","page":"AzManagers","title":"AzManagers","text":"using AzManagers, AzSessions\n\nvariablebundle!(session = AzSession())\n\nmyvm = addproc(\"myvm\")\ndetached_job = @detachat myvm begin\n    using Distributed, AzManagers\n    addprocs(\"myscaleset\", 5; session=variablebundle(:session))\n    for pid in workers()\n        remotecall_fetch(println, \"hello from pid=$(myid())\")\n    end\n    rmprocs(workers())\nend\nwait(detached_job)\nrmproc(vm)","category":"page"},{"location":"#A-note-about-MPI-(experimental)","page":"AzManagers","title":"A note about MPI (experimental)","text":"","category":"section"},{"location":"","page":"AzManagers","title":"AzManagers","text":"We have experimental support for inter-node MPI communication.  In other words, we allow for each Julia worker to have its own MPI communicator.  For example, this is useful for interacting with Devito.jl, and where there are performance benefits to parallel work within a many-core VM via a mix of MPI and OpenMP.","category":"page"},{"location":"#Custom-environments","page":"AzManagers","title":"Custom environments","text":"","category":"section"},{"location":"","page":"AzManagers","title":"AzManagers","text":"AzManagers can create an on-the-fly custom Julia software environment for the workers. This is managed via Julia environments.  If you use the customenv=true keyword argument, then when you create a cluster, the worker nodes will, at boot time, instantiate the environment. For example:","category":"page"},{"location":"","page":"AzManagers","title":"AzManagers","text":"using Pkg\nPkg.instantiate(\".\")\nPkg.add(\"AzManagers\")\nPkg.add(\"Jets\")\naddprocs(\"cbox16\",2;customenv=true)","category":"page"},{"location":"","page":"AzManagers","title":"AzManagers","text":"Now, when worker VMs are initialized, they will have the software stack defined by the current project.  Please note that this can add significant overhead to the boot-time of the VMs.","category":"page"}]
}
