using AzManagers

sstemplate = AzManagers.build_sstemplate(
        templatename         = "cbox02",
        subscriptionid       = ENV["SUBSCRIPTION_ID"],
        admin_username       = "cvx",
        location             = "southcentralus",
        resourcegroup        = ENV["RESOURCE_GROUP"],
        resourcegroup_vnet   = ENV["RESOURCE_GROUP"],
        vnet                 = ENV["VNET_NAME"],
        subnet               = "default",
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
        subnet               = "default")
        
AzManagers.save_template_scaleset(templatename, sstemplate)
AzManagers.save_template_vm(templatename, vmtemplate)
AzManagers.save_template_nic(templatename, nictemplate)

AzSessions.write_manifest(;client_id=ENV["CLIENT_ID"], client_secret=ENV["CLIENT_SECRET"], tenant=ENV["TENANT_ID"])
AzManagers.write_manifest(;resourcegroup=ENV["RESOURCE_GROUP"], subscriptionid=ENV["SUBSCRIPTION_ID"], ssh_user="cvx")
