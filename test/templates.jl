using Distributed, AzManagers, Random, Test, HTTP, AzSessions, JSON

tenant_id = ENV["TENANT_ID"]
subscriptionid = ENV["SUBSCRIPTION_ID"]
resourcegroup = ENV["RESOURCE_GROUP"]
client_id = ENV["CLIENT_ID"]
client_secret = ENV["CLIENT_SECRET"]
imagename = ENV["IMAGE_NAME"]

# Region the test scale set / VM / NIC templates point at, and the VM SKU
# they request. Defaulted here so a local `julia /tmp/templates.jl` works
# stand-alone, but ci.yml overrides both via Packer vars -> env so changing
# region or SKU only requires editing the workflow.
location = get(ENV, "LOCATION", "southcentralus")
# Standard_D4s_v3: 4 vCPU, HT enabled -> 2 physical cores. The test
# `nphysical_cores` testset in runtests.jl asserts ncores == 2 for cbox02,
# so the SKU must report two physical cores. D2s_v3 reports only one
# (HT halves the vCPU count) and breaks that test.
vm_size  = get(ENV, "VM_SIZE",  "Standard_D4s_v3")

templatename = "cbox02"

sstemplate = AzManagers.build_sstemplate(
        templatename,
        subscriptionid       = subscriptionid,
        admin_username       = "cvx",
        location             = location,
        resourcegroup        = resourcegroup,
        resourcegroup_vnet   = resourcegroup,
        vnet                 = ENV["VNET_NAME"],
        subnet               = ENV["SUBNET_NAME"],
        imagegallery         = ENV["GALLERY_NAME"],
        imagename            = imagename,
        skuname              = vm_size)

# For another PR
# vmtemplate = AzManagers.build_vmtemplate(
#         templatename,
#         subscriptionid       = subscriptionid,
#         admin_username       = "cvx",
#         location             = location,
#         resourcegroup        = resourcegroup,
#         resourcegroup_vnet   = resourcegroup,
#         imagegallery         = ENV["GALLERY_NAME"],
#         imagename            = imagename,
#         vmsize               = "Standard_D2s_v3",
#         default_nic          = templatename)

vmtemplate = AzManagers.build_vmtemplate(
        templatename,
        subscriptionid       = subscriptionid,
        admin_username       = "cvx",
        location             = location,
        resourcegroup        = resourcegroup,
        resourcegroup_vnet   = resourcegroup,
        imagegallery         = ENV["GALLERY_NAME"],
        imagename            = imagename,
        vmsize               = vm_size)

nictemplate = AzManagers.build_nictemplate(
        templatename,
        accelerated          = false,
        subscriptionid       = subscriptionid,
        location             = location,
        resourcegroup_vnet   = resourcegroup,
        vnet                 = ENV["VNET_NAME"],
        subnet               = ENV["SUBNET_NAME"])
        
AzManagers.save_template_scaleset(templatename, sstemplate)
AzManagers.save_template_vm(templatename, vmtemplate)
AzManagers.save_template_nic(templatename, nictemplate)

AzSessions.write_manifest(;client_id=client_id, client_secret=client_secret, tenant=tenant_id)
AzManagers.write_manifest(;resourcegroup=resourcegroup, subscriptionid=subscriptionid, ssh_user="cvx")
