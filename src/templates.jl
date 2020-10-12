templates_folder() = joinpath(homedir(), ".azmanagers")

function save_template(templates_filename::AbstractString, name::AbstractString, template::Dict)
    templates = isfile(templates_filename) ? JSON.parse(read(templates_filename, String)) : Dict{String,Any}()
    templates[name] = template
    if isfile(templates_filename)
        write(templates_filename, json(templates))
    else
        run(`pwd`)
        run(`ls -al`)
        run(`mkdir /home/runner/.azmanagers/`)
        run(`ls /home/runner/.azmanagers/`)
        # run(`touch $templates_filename`)
        io = open(templates_filename, "w")
        write(io, json(templates))
        close(io)
    end
    nothing
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
* `skutier="Standard"` Azure SKU tier.
"""
function build_sstemplate(name;
        subscriptionid,
        subscriptionid_image = "",
        location,
        resourcegroup,
        resourcegroup_vnet = "",
        resourcegroup_image = "",
        imagegallery,
        imagename,
        vnet,
        subnet,
        skutier="Standard",
        skuname)
    resourcegroup_vnet == "" && (resourcegroup_vnet = resourcegroup)
    resourcegroup_image == "" && (resourcegroup_image = resourcegroup)
    subscriptionid_image == "" && (subscriptionid_image = subscriptionid)
    subnetid = "/subscriptions/$subscriptionid/resourceGroups/$resourcegroup_vnet/providers/Microsoft.Network/virtualNetworks/$vnet/subnets/$subnet"
    image = "/subscriptions/$subscriptionid_image/resourceGroups/$resourcegroup_image/providers/Microsoft.Compute/galleries/$imagegallery/images/$imagename"

    body = Dict(
        "sku" => Dict(
            "tier" => skutier,
            "capacity" => 2,
            "name" => skuname
        ),
        "location" => location,
        "properties" => Dict(
            "overprovision" => true,
            "singlePlacementGroup" => false,
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
                        "createOption" => "FromImage"
                    )
                ),
                "osProfile" => Dict(
                    "computerNamePrefix" => name,
                    "adminUsername" => "cvx",
                    "linuxConfiguration" => Dict(
                        "ssh" => Dict(
                            "publicKeys" => []
                            ),
                        "disablePasswordAuthentication" => true
                    )
                ),
                "networkProfile" => Dict(
                    "networkInterfaceConfigurations" => [
                        Dict(
                            "name" => name,
                            "properties" => Dict(
                                "primary" => true,
                                "ipConfigurations" => [
                                    Dict(
                                        "name" => name,
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
"""
function build_nictemplate(name;
        subscriptionid,
        resourcegroup_vnet,
        vnet,
        subnet,
        location)
    subnetid = "/subscriptions/$subscriptionid/resourceGroups/$resourcegroup_vnet/providers/Microsoft.Network/virtualNetworks/$vnet/subnets/$subnet"

    body = Dict(
        "properties" => Dict(
            "enableAcceleratedNetworking" => true,
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
* `location` Azure data center location
* `resourcegroup` Azure resource group where the VM will reside
* `imagegallery` Azure shared image gallery name
* `imagename` Azure image name that is in the shared image gallery 
* `vnet` Azure virtual network
* `subnet` Azure subnet
* `vmsize` Azure vm type, e.g. "Standard_D8s_v3"

# Optional keyword arguments
* resourcegroup_vnet` Azure resource group containing the virtual network, defaults to `resourcegroup`
* subscriptionid_image` Azure subscription containing the image gallery, defaults to `subscriptionid`
* resourcegroup_image` Azure resource group containing the image gallery, defaults to `subscriptionid`
* nicname = "cbox-nic" Name of the NIC for this VM
"""
function build_vmtemplate(name;
        subscriptionid,
        subscriptionid_image = "",
        location,
        resourcegroup,
        resourcegroup_vnet = "",
        resourcegroup_image = "",
        imagegallery,
        imagename,
        vnet,
        subnet,
        vmsize,
        nicname = "cbox-nic")
    resourcegroup_vnet == "" && (resourcegroup_vnet = resourcegroup)
    resourcegroup_image == "" && (resourcegroup_image = resourcegroup)
    subscriptionid_image == "" && (subscriptionid_image = subscriptionid)

    image = "/subscriptions/$subscriptionid_image/resourceGroups/$resourcegroup_image/providers/Microsoft.Compute/galleries/$imagegallery/images/$imagename"
    subnetid = "/subscriptions/$subscriptionid/resourceGroups/$resourcegroup_vnet/providers/Microsoft.Network/virtualNetworks/$vnet/subnets/$subnet"

    body = Dict(
        "location" => location,
        "properties" => Dict(
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
                    "createOption" => "FromImage"
                )
            ),
            "osProfile" => Dict(
                "computerName" => name,
                "adminUsername" => "cvx",
                "linuxConfiguration" => Dict(
                    "ssh" => Dict(
                        "publicKeys" => []
                    ),
                    "disablePasswordAuthentication" => true
                )
            ),
            "networkProfile" => Dict(
                "networkInterfaces" => [
                    Dict(
                        "id" => "/subscription/$subscriptionid/resourceGroups/$resourcegroup_vnet/providers/Microsoft.Network/networkInterfaces/$nicname",
                        "properties" => Dict(
                            "primary" => true
                        )
                    )
                ]
            )
        )
    )
end

templates_filename_vm() = joinpath(templates_folder(), "templates_vm.json")

"""
    AzManagers.save_template_vm(vm_name, template)

Save `template::Dict` generated by AzManagers.build_vmtmplate to $(templates_filename_vm()).
"""
save_template_vm(name::AbstractString, template::Dict) = save_template(templates_filename_vm(), name, template)
