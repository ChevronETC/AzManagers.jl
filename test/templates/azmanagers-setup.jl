using AzManagers

sstemplate = AzManagers.build_sstemplate(
        templatename         = "cbox02",
        subscriptionid       = ENV["SUBSCRIPTION_ID"],
        admin_username       = "cvx",
        location             = "southcentralus",
        resourcegroup        = ENV["RESOURCE_GROUP"],
        resourcegroup_vnet   = ENV["RESOURCE_GROUP"],
        vnet                 = ENV["VNET_NAME"],
        subnet               = ENV["SUBNET_NAME"],
        imagegallery         = ENV["GALLERY_NAME"],
        imagename            = ENV["IMAGE_NAME"],
        skuname              = "Standard_D2s_v3")

vmtemplate = AzManagers.build_vmtemplate(
        templatename         = "cbox02",
        subscriptionid       = ENV["SUBSCRIPTION_ID"],
        admin_username       = "cvx",
        location             = "southcentralus",
        resourcegroup        = ENV["RESOURCE_GROUP"],
        resourcegroup_vnet   = ENV["RESOURCE_GROUP"],
        imagegallery         = ENV["GALLERY_NAME"],
        imagename            = ENV["IMAGE_NAME"],
        vmsize              = "Standard_D3_v2")

nictemplate = AzManagers.build_nictemplate(
        templatename         = "cbox02",
        accelerated          = false,
        subscriptionid       = ENV["SUBSCRIPTION_ID"],
        location             = "southcentralus",
        resourcegroup_vnet   = ENV["RESOURCE_GROUP"],
        vnet                 = ENV["VNET_NAME"],
        subnet               = ENV["SUBNET_NAME"])
        
AzManagers.save_template_scaleset(templatename, sstemplate)
AzManagers.save_template_vm(templatename, vmtemplate)
AzManagers.save_template_nic(templatename, nictemplate)

AzSessions.write_manifest(;client_id=client_id, client_secret=client_secret, tenant=tenant_id)
AzManagers.write_manifest(;resourcegroup=resourcegroup, subscriptionid=subscriptionid, ssh_user="cvx")
