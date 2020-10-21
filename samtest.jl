using Revise

using Distributed, AzManagers, AzSessions, Base64, JSON, Random

myscaleset = AzManagers.build_sstemplate("oss2",
    subscriptionid       = "461a0ce0-0ca0-44fa-b7d0-29bb0711dadc",
    location             = "southcentralus",
    resourcegroup        = "azmanagers-unittesting",
    vnet                 = "dummy-vnet",
    subnet               = "dummy-subnet",
    imagegallery         = "dummysig",
    imagename            = "unit-test-image",
    skuname              = "Standard_D2s_v3",
    publicipname         = "oss3")

AzManagers.save_template_scaleset("oss3", myscaleset)

AzManagers.load_manifest()
ssh_key = AzManagers._manifest["ssh_public_key_file"]

templates_scaleset = JSON.parse(read(AzManagers.templates_filename_scaleset(), String))
template = templates_scaleset["oss3"]

_template = deepcopy(template)

_template["properties"]["virtualMachineProfile"]["osProfile"]["computerNamePrefix"] = string("foobar", "-", randstring('a':'z', 4), "-")

user="cvx"
key = Dict("path" => "/home/$user/.ssh/authorized_keys", "keyData" => read(ssh_key, String))
push!(_template["properties"]["virtualMachineProfile"]["osProfile"]["linuxConfiguration"]["ssh"]["publicKeys"], key)

_template["sku"]["capacity"] = 0

session = AzSession(;
    protocal=AzClientCredentials,
    client_id="7f4e2cd1-20a8-47fd-ab49-d3df30121c9a",
    client_secret="***")

subscriptionid = "461a0ce0-0ca0-44fa-b7d0-29bb0711dadc"
scalesetname = "matt2"
resourcegroup = "azmanagers-unittesting"

# publicipname = "oss"
# _template["properties"]["virtualMachineProfile"]["networkProfile"]["networkInterfaceConfigurations"][1]["properties"]["ipConfigurations"][1]["properties"]["publicIPAddressConfiguration"] = Dict("name" => publicipname, "properties" => Dict("idleTimeoutInMinutes" => 15))

AzManagers.azrequest(
    "PUT",
    2,
    "https://management.azure.com/subscriptions/$subscriptionid/resourceGroups/$resourcegroup/providers/Microsoft.Compute/virtualMachineScaleSets/$scalesetname?api-version=2020-06-01",
    Dict("Content-type"=>"application/json", "Authorization"=>"Bearer $(token(session))"),
    json(_template,1))

addprocs("oss3", 2; 
    group = "newmachine",
    session = session,
    subscription = "461a0ce0-0ca0-44fa-b7d0-29bb0711dadc",
    resourcegroup = "azmanagers-unittesting",
    sigimagename = "")

