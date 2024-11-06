templates_folder() = joinpath(homedir(), ".azmanagers")

function save_template(templates_filename::AbstractString, name::AbstractString, template::Dict)
    templates = isfile(templates_filename) ? JSON.parse(read(templates_filename, String)) : Dict{String,Any}()
    templates[name] = template
    if !ispath(templates_folder())
        mkdir(templates_folder())
    end
    write(templates_filename, json(templates, 1))
    nothing
end

function _replace(x::AbstractString, r::Pair...)
    local y
    if VERSION >= v"1.7"
        y = replace(x, r...)
    else
        y = x
        for _r in r
            y = replace(y, _r)
        end
    end
    y
end

#
# scale-set templates
#
"""
    AzManagers.build_sstemplate(name; kwargs...)

returns a dictionary that is an Azure scaleset template for use in `addprocs` or for saving
to the `~/.azmanagers` folder.

# required key-word arguments
* `subscriptionid` Azure subscription
* `admin_username` ssh user for the scaleset virtual machines
* `location` Azure data-center location
* `resourcegroup` Azure resource-group
* `imagegallery` Azure image gallery that contains the VM image
* `imagename` Azure image
* `vnet` Azure virtual network for the scaleset
* `subnet` Azure virtual subnet for the scaleset
* `skuname` Azure VM type

# optional key-word arguments
* `subscriptionid_image` Azure subscription corresponding to the image gallery, defaults to `subscriptionid`
* `resourcegroup_vnet` Azure resource group corresponding to the virtual network, defaults to `resourcegroup`
* `resourcegroup_image` Azure resource group correcsponding to the image gallery, defaults to `resourcegroup`
* `osdisksize=60` Disk size in GB for the operating system disk
* `skutier = "Standard"` Azure SKU tier.
* `datadisks=[]` list of data disks to create and attach [1]
* `tempdisk = "sudo mkdir -m 777 /mnt/scratch; ln -s /mnt/scratch /scratch"` cloud-init commands used to mount or link to temporary disk
* `tags = Dict("azure_tag_name" => "some_tag_value")` Optional tags argument for resource
* `encryption_at_host = false` Optional argument for enabling encryption at host

# Notes
[1] Each datadisk is a Dictionary. For example,
```julia
Dict("createOption"=>"Empty", "diskSizeGB"=>1023, "managedDisk"=>Dict("storageAccountType"=>"PremiumSSD_LRS"))
```
or, to accept the defaults,
```julia
Dict("diskSizeGB"=>1023)
```
The above example is populated with the default options.  So, if `datadisks=[Dict()]`, then the default options
will be included.
"""
function build_sstemplate(name;
        subscriptionid,
        subscriptionid_image = "",
        admin_username,
        location,
        resourcegroup,
        resourcegroup_vnet = "",
        resourcegroup_image = "",
        imagegallery,
        imagename,
        vnet,
        subnet,
        skutier="Standard",
        osdisksize=60,
        datadisks=[],
        tempdisk="sudo mkdir -m 777 /mnt/scratch\nln -s /mnt/scratch /scratch",
        skuname,
        tags=Dict(),
        encryption_at_host=false)
    resourcegroup_vnet == "" && (resourcegroup_vnet = resourcegroup)
    resourcegroup_image == "" && (resourcegroup_image = resourcegroup)
    subscriptionid_image == "" && (subscriptionid_image = subscriptionid)
    subnetid = "/subscriptions/$subscriptionid/resourceGroups/$resourcegroup_vnet/providers/Microsoft.Network/virtualNetworks/$vnet/subnets/$subnet"
    image = "/subscriptions/$subscriptionid_image/resourceGroups/$resourcegroup_image/providers/Microsoft.Compute/galleries/$imagegallery/images/$imagename"

    _datadisks = Dict{String,Any}[]
    ultrassdenabled = false
    for (idisk,datadisk) in enumerate(datadisks)
        datadisk_template = Dict{String,Any}("createOption"=>"Empty", "diskSizeGB"=>1023, "lun"=>1, "managedDisk"=>Dict("storageAccountType"=>"Premium_LRS"))
        merge!(datadisk_template, datadisk)
        merge!(datadisk_template, Dict("lun"=>idisk))
        push!(_datadisks, datadisk_template)
        if datadisk_template["managedDisk"]["storageAccountType"] ∈ ("UltraSSD_LRS",)
            ultrassdenabled = true
        end
    end

    template = Dict{Any,Any}(
        "subscriptionid" => subscriptionid,
        "resourcegroup" => resourcegroup,
        "tempdisk" => tempdisk,
        "value" => Dict(
            "sku" => Dict(
                "tier" => skutier,
                "capacity" => 2,
                "name" => skuname
            ),
            "location" => location,
            "properties" => Dict(
                "overprovision" => true,
                "singlePlacementGroup" => false,
                "additionalCapabilities" => Dict(
                    "ultraSSDEnabled" => ultrassdenabled
                ),
                "virtualMachineProfile" => Dict{String,Any}(
                    "storageProfile" => Dict(
                        "imageReference" => Dict(
                            "id" => image
                        ),
                        "osDisk" => Dict(
                            "caching" => "ReadWrite",
                            "managedDisk" => Dict(
                                "storageAccountType" => "Standard_LRS"
                            ),
                            "createOption" => "FromImage",
                            "diskSizeGB" => osdisksize
                        ),
                        "dataDisks" => _datadisks
                    ),
                    "osProfile" => Dict(
                        "computerNamePrefix" => _replace(split(name,'/')[end], "+"=>"plus", "/"=>"-"),
                        "adminUsername" => admin_username,
                        "linuxConfiguration" => Dict(
                            "ssh" => Dict(
                                "publicKeys" => []
                                ),
                            "disablePasswordAuthentication" => true
                        )
                    ),
                    "securityProfile" => Dict(
                        "encryptionAtHost" => encryption_at_host
                    ),
                    "networkProfile" => Dict(
                        "networkInterfaceConfigurations" => [
                            Dict(
                                "name" => _replace(split(name,'/')[end], "+"=>"plus", "/"=>"-"),
                                "properties" => Dict(
                                    "primary" => true,
                                    "ipConfigurations" => [
                                        Dict(
                                            "name" => _replace(split(name,'/')[end], "+"=>"plus", "/"=>"-"),
                                            "properties" => Dict(
                                                "subnet" => Dict(
                                                    "id" => subnetid
                                                )
                                            )
                                        )
                                    ] # ipConfigurations
                                )
                            )
                        ] # networkInterfaceConfigurations
                    ), # networkProfile
                ), # virtualMachineProfile
                "upgradePolicy" => Dict(
                    "mode" => "Manual"
                )
            ) # properties
        )
    )
    if !isempty(tags)
        template["value"]["tags"] = tags
    end
    template
end

templates_filename_scaleset() = joinpath(templates_folder(), "templates_scaleset.json")

"""
    AzManagers.save_template_scaleset(scalesetname, template)

Save `template::Dict` generated by AzManagers.build_sstemplate to $(templates_filename_scaleset()).
"""
save_template_scaleset(name::AbstractString, template::Dict) = save_template(templates_filename_scaleset(), name, template)

#
# NIC templates
#
"""
    AzManagers.build_nictemplate(nic_name; kwargs...)

Returns a dictionary for a NIC template, and that can be passed to the `addproc` method, or written
to AzManagers.jl configuration files.

# Required keyword arguments
* `subscriptionid` Azure subscription
* `resourcegroup_vnet` Azure resource group that holds the virtual network that the NIC is attaching to.
* `vnet` Azure virtual network for the NIC to attach to.
* `subnet` Azure sub-network name.
* `location` location of the Azure data center where the NIC correspond to.

# Optional keyword arguments
* `accelerated=true` use accelerated networking (not all VM sizes support accelerated networking).
"""
function build_nictemplate(name;
        subscriptionid,
        resourcegroup_vnet,
        vnet,
        subnet,
        accelerated = true,
        location)
    subnetid = "/subscriptions/$subscriptionid/resourceGroups/$resourcegroup_vnet/providers/Microsoft.Network/virtualNetworks/$vnet/subnets/$subnet"

    body = Dict(
        "properties" => Dict(
            "enableAcceleratedNetworking" => accelerated,
            "ipConfigurations" => [
                Dict(
                    "name" => "ipConfig1",
                    "properties" => Dict(
                        "subnet" => Dict(
                            "id" => subnetid
                        )
                    )
                )
            ]
        ),
        "location" => location
    )
end

templates_filename_nic() = joinpath(templates_folder(), "templates_nic.json")

"""
    AzManagers.save_template_nic(nic_name, template)

Save `template::Dict` generated by AzManagers.build_nictmplate to $(templates_filename_nic()).
"""
save_template_nic(name::AbstractString, template::Dict) = save_template(templates_filename_nic(), name, template)

#
# VM templates
#
"""
    AzManagers.build_vmtemplate(vm_name; kwargs...)

Returns a dictionary for a virtual machine template, and that can be passed to the `addproc` method
or written to AzManagers.jl configuration files.

# Required keyword arguments
* `subscriptionid` Azure subscription
* `admin_username` ssh user for the scaleset virtual machines
* `location` Azure data center location
* `resourcegroup` Azure resource group where the VM will reside
* `imagegallery` Azure shared image gallery name
* `imagename` Azure image name that is in the shared image gallery 
* `vmsize` Azure vm type, e.g. "Standard_D8s_v3"

# Optional keyword arguments
* `resourcegroup_vnet` Azure resource group containing the virtual network, defaults to `resourcegroup`
* `subscriptionid_image` Azure subscription containing the image gallery, defaults to `subscriptionid`
* `resourcegroup_image` Azure resource group containing the image gallery, defaults to `subscriptionid`
* `nicname = "cbox-nic"` Name of the NIC for this VM
* `osdisksize = 60` size in GB of the OS disk
* `datadisks=[]` additional data disks to attach
* `tempdisk = "sudo mkdir -m 777 /mnt/scratch\nln -s /mnt/scratch /scratch"`  cloud-init commands used to mount or link to temporary disk
* `tags = Dict("azure_tag_name" => "some_tag_value")` Optional tags argument for resource
* `encryption_at_host = false` Optional argument for enabling encryption at host

# Notes
[1] Each datadisk is a Dictionary. For example,
```julia
Dict("createOption"=>"Empty", "diskSizeGB"=>1023, "managedDisk"=>Dict("storageAccountType"=>"PremiumSSD_LRS"))
```
The above example is populated with the default options.  So, if `datadisks=[Dict()]`, then the default options
will be included.
"""
function build_vmtemplate(name;
        subscriptionid,
        admin_username,
        subscriptionid_image = "",
        location,
        resourcegroup,
        resourcegroup_vnet = "",
        resourcegroup_image = "",
        imagegallery,
        imagename,
        vmsize,
        osdisksize = 60,
        datadisks = [],
        tempdisk = "sudo mkdir -m 777 /mnt/scratch\nln -s /mnt/scratch /scratch",
        nicname = "cbox-nic",
        tags = Dict(),
        encryption_at_host=false)
    resourcegroup_vnet == "" && (resourcegroup_vnet = resourcegroup)
    resourcegroup_image == "" && (resourcegroup_image = resourcegroup)
    subscriptionid_image == "" && (subscriptionid_image = subscriptionid)

    image = "/subscriptions/$subscriptionid_image/resourceGroups/$resourcegroup_image/providers/Microsoft.Compute/galleries/$imagegallery/images/$imagename"

    ultrassdenabled = false
    _datadisks = Dict{String,Any}[]
    for (idisk,datadisk) in enumerate(datadisks)
        datadisk_template = Dict{String,Any}("createOption"=>"Empty", "diskSizeGB"=>1023, "lun"=>1, "managedDisk"=>Dict("storageAccountType"=>"Premium_LRS"))
        merge!(datadisk_template, datadisk)
        merge!(datadisk_template, Dict("lun"=>idisk, "name"=>"scratch$idisk"))
        push!(_datadisks, datadisk_template)
        if datadisk_template["managedDisk"]["storageAccountType"] ∈ ("UltraSSD_LRS",)
            ultrassdenabled = true
        end
    end

    template = Dict(
        "subscriptionid" => subscriptionid,
        "resourcegroup" => resourcegroup,
        "tempdisk" => tempdisk,
        "value" => Dict(
            "location" => location,
            "properties" => Dict(
                "additionalCapabilities" => Dict(
                    "ultraSSDEnabled"=>ultrassdenabled
                ),
                "hardwareProfile" => Dict(
                    "vmSize" => vmsize
                ),
                "storageProfile" => Dict(
                    "imageReference" => Dict(
                        "id" => image
                    ),
                    "osDisk" => Dict(
                        "caching" => "ReadWrite",
                        "managedDisk" => Dict(
                            "storageAccountType" => "Standard_LRS"
                        ),
                        "createOption" => "FromImage",
                        "diskSizeGB" => osdisksize
                    ),
                    "dataDisks" => _datadisks
                ),
                "osProfile" => Dict(
                    "computerName" => name,
                    "adminUsername" => admin_username,
                    "linuxConfiguration" => Dict(
                        "ssh" => Dict(
                            "publicKeys" => []
                        ),
                        "disablePasswordAuthentication" => true
                    )
                ),
                "securityProfile" => Dict(
                    "encryptionAtHost" => encryption_at_host
                ),
                "networkProfile" => Dict(
                    "networkInterfaces" => [
                        Dict(
                            "id" => "/subscription/$subscriptionid/resourceGroups/$resourcegroup_vnet/providers/Microsoft.Network/networkInterfaces/$nicname",
                            "properties" => Dict{Any,Any}(
                                "primary" => true
                            )
                        )
                    ]
                )
            )
        )
    )
    if !isempty(tags)
        template["value"]["tags"] = tags
    end
    template
end

function cloudcfg_nvme_scratch()
    cloud_cfg = raw"""
    #cloud-config
    write_files:
    - path: /usr/sbin/azure_nvme.sh
        permissions: '0777'
        owner: root:root
        content: |
            #!/bin/bash
            # This script creates a NVMe scratch LVM and replaces the existing /mnt/resource scratch
            export PATH=/bin:/sbin:/usr/bin:/usr/sbin

            # Build associative array of nvme devices and their sizes
            declare -A NVME_DEVICES=()
            while read -r device size
            do
                NVME_DEVICES[$device]=$(($size/1024/1024-10))
            done < <(lsblk -pbln -o name,size,mountpoint | awk '/^\/dev\/nvme/ && $3 == "" { print $0 }')
            NVME_DEVICE_SIZE=$((${NVME_DEVICES[@]/%/+}0))

            SCRATCH_VG="scratch"
            SCRATCH_LV="storage"

            echo "Number of NVMe Devices:" "${#NVME_DEVICES[@]}"
            echo "NVMe Device List:" "${!NVME_DEVICES[@]}"
            echo "NVMe LVM Size (MB):" $NVME_DEVICE_SIZE

            if [[ "${#NVME_DEVICES[@]}" -gt 0 ]]
            then
                sed -i '/^\/dev\/disk\/cloud\/azure_resource-part1/s/^\(.*\)$/#\1/' /etc/fstab
                umount /scratch
                mkdir -m 777 -p /scratch
                for x in "${!NVME_DEVICES[@]}"
                do
                    pvcreate $x
                done
                vgcreate -q -s 1M $SCRATCH_VG ${!NVME_DEVICES[@]} --force
                lvcreate -q -Wy --yes -I 128k -i ${#NVME_DEVICES[@]} -L $NVME_DEVICE_SIZE -v $SCRATCH_VG -n $SCRATCH_LV
                mkfs.xfs -q -f /dev/$SCRATCH_VG/$SCRATCH_LV
                mount /dev/$SCRATCH_VG/$SCRATCH_LV /scratch
                # querk with 777 dropping writes
                chmod ugo+rwx -Rf /scratch
            fi
    - path: /etc/systemd/system/scratch-nvme.service
        permissions: '0644'
        owner: root:root
        content: |
            [Unit]
            Description=Create nvme scratch directory

            [Service]
            ExecStart=/usr/bin/bash /usr/sbin/azure_nvme_scratch.sh
            User=root
            Type=oneshot
            RemainAfterExit=no

            [Install]
            WantedBy=cloud-init.target
    runcmd:
    - [ systemctl, enable, scratch-nvme.service ]
    - [ systemctl, daemon-reload ]
    """
    cloud_cfg
end

templates_filename_vm() = joinpath(templates_folder(), "templates_vm.json")

"""
    AzManagers.save_template_vm(vm_name, template)

Save `template::Dict` generated by AzManagers.build_vmtmplate to $(templates_filename_vm()).
"""
save_template_vm(name::AbstractString, template::Dict) = save_template(templates_filename_vm(), name, template)
