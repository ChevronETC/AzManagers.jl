using Distributed, AzManagers, Random, Test, HTTP, AzSessions, JSON

tenant_id = ENV["TENANT_ID"]
subscriptionid = ENV["SUBSCRIPTION_ID"]
resourcegroup = ENV["RESOURCE_GROUP"]
client_id = ENV["CLIENT_ID"]
client_secret = ENV["CLIENT_SECRET"]
imagename = ENV["IMAGE_NAME"]

templatename = "cbox02"

sstemplate = AzManagers.build_sstemplate(
        templatename,
        subscriptionid       = subscriptionid,
        admin_username       = "cvx",
        location             = "southcentralus",
        resourcegroup        = resourcegroup,
        resourcegroup_vnet   = resourcegroup,
        vnet                 = ENV["VNET_NAME"],
        subnet               = ENV["SUBNET_NAME"],
        imagegallery         = ENV["GALLERY_NAME"],
        imagename            = imagename,
        skuname              = "Standard_D2s_v5")

# For another PR
# vmtemplate = AzManagers.build_vmtemplate(
#         templatename,
#         subscriptionid       = subscriptionid,
#         admin_username       = "cvx",
#         location             = "southcentralus",
#         resourcegroup        = resourcegroup,
#         resourcegroup_vnet   = resourcegroup,
#         imagegallery         = ENV["GALLERY_NAME"],
#         imagename            = imagename,
#         vmsize               = "Standard_D2s_v5",
#         default_nic          = templatename)

vmtemplate = AzManagers.build_vmtemplate(
        templatename,
        subscriptionid       = subscriptionid,
        admin_username       = "cvx",
        location             = "southcentralus",
        resourcegroup        = resourcegroup,
        resourcegroup_vnet   = resourcegroup,
        imagegallery         = ENV["GALLERY_NAME"],
        imagename            = imagename,
        vmsize               = "Standard_D2s_v5")

nictemplate = AzManagers.build_nictemplate(
        templatename,
        accelerated          = false,
        subscriptionid       = subscriptionid,
        location             = "southcentralus",
        resourcegroup_vnet   = resourcegroup,
        vnet                 = ENV["VNET_NAME"],
        subnet               = ENV["SUBNET_NAME"])
        
AzManagers.save_template_scaleset(templatename, sstemplate)
AzManagers.save_template_vm(templatename, vmtemplate)
AzManagers.save_template_nic(templatename, nictemplate)

AzSessions.write_manifest(;client_id=client_id, client_secret=client_secret, tenant=tenant_id)
AzManagers.write_manifest(;resourcegroup=resourcegroup, subscriptionid=subscriptionid, ssh_user="cvx")
