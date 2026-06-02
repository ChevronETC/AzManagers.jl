# Single-stage Packer build. Sources from the Ubuntu 22.04 marketplace
# image, installs apt deps + Julia + AzManagers + templates_scaleset.json,
# and pushes one per-run SIG image. SIG replication is the dominant cost
# of any CI run, so splitting this into a base image + per-run swap layer
# saves no real wall-clock and is not worth the chicken-and-egg.

variable "subscription_id" {
    default = "subscriptionid"
}

variable "tenant_id" {
    default = "tenantid"
}

variable "client_id" {
    default = "clientid"
}

variable "client_secret" {
    default = "secret"
}

variable "resource_group" {
    default = "resourcegroup"
}

variable "image_name" {
    default = "imagename"
}

variable "gallery" {
    default = "gallery"
}

variable "image_version" {
    default = "1.0.0"
}

variable "virtual_network" {
    default = "virtualnetwork"
}

variable "virtual_subnet" {
    default = "subnet"
}

variable "julia_version_major" {
    default = "1"
}

variable "julia_version_minor" {
    default = "12"
}

variable "julia_version_patch" {
    default = "0"
}

variable "azmanagers_version" {
    default = "master"
}

# Git URL the build VM clones AzManagers from. CI workflows override this
# via `-var` so the URL matches `${{ github.repository }}` of the run.
variable "azmanagers_repo" {
    default = "https://github.com/ChevronETC/AzManagers.jl.git"
}

# Region and VM SKU the baked-in test/templates.jl request. Override via
# `-var` so the same Packer file builds images for any region/SKU pair.
variable "location" {
    default = "southcentralus"
}

variable "vm_size" {
    default = "Standard_D4s_v3"
}

# SIG regions to replicate the produced image to. Comma-separated string
# (HCL splits internally). ci.yml passes a single region matching
# $LOCATION; multi-worker-test.yml passes its matrix regions.
variable "replication_regions" {
    default = "South Central US"
}

packer {
    required_plugins {
        azure = {
            source = "github.com/hashicorp/azure"
            version = "~> 1"
        }
    }
}

source "azure-arm" "cofii" {
    subscription_id = var.subscription_id
    tenant_id = var.tenant_id
    client_id = var.client_id
    client_secret = var.client_secret
    os_type = "Linux"
    vm_size = var.vm_size
    image_publisher = "canonical"
    image_offer = "0001-com-ubuntu-server-jammy"
    image_sku = "22_04-lts-gen2"
    shared_image_gallery_destination {
        resource_group = var.resource_group
        gallery_name = var.gallery
        image_name = var.image_name
        image_version = var.image_version
        replication_regions = split(",", var.replication_regions)
    }
    shared_image_gallery_timeout = "120m"
    build_resource_group_name = var.resource_group
    managed_image_resource_group_name = var.resource_group
    managed_image_name = var.image_name
    managed_image_storage_account_type = "Premium_LRS"
    virtual_network_name = var.virtual_network
    virtual_network_subnet_name = var.virtual_subnet
    virtual_network_resource_group_name = var.resource_group
    private_virtual_network_with_public_ip = true
    ssh_username = "cvx"
}

build {
    sources = [
        "source.azure-arm.cofii"
    ]

    provisioner "shell" {
        inline = [
            "echo \"Host *\" > ~/.ssh/config",
            "echo \"    StrictHostKeyChecking    no\" >> ~/.ssh/config",
            "echo \"    LogLevel                 ERROR\" >> ~/.ssh/config",
            "echo \"    UserKnownHostsFile       /dev/null\" >> ~/.ssh/config"
        ]
    }

    provisioner "shell" {
        inline = [
            "sudo apt-get -y update",
            "sudo DEBIAN_FRONTEND=noninteractive apt-get -y -o Dpkg::Options::=\"--force-confdef\" -o Dpkg::Options::=\"--force-confold\" upgrade",
            "sudo apt-get -y install git"
        ]
        max_retries = 5
    }

    provisioner "shell" {
        inline = [
            "echo \"**** generating AzManagers ssh key-pair ****\"",
            "ssh-keygen -f /home/cvx/.ssh/azmanagers_rsa -N ''"
        ]
    }

    provisioner "shell" {
        inline = [
            "echo \"**** installing Julia ****\"",
            "sudo wget https://julialang-s3.julialang.org/bin/linux/x64/${var.julia_version_major}.${var.julia_version_minor}/julia-${var.julia_version_major}.${var.julia_version_minor}.${var.julia_version_patch}-linux-x86_64.tar.gz",
            "sudo mkdir -p /opt/julia",
            "sudo tar --strip-components=1 -xzvf julia-${var.julia_version_major}.${var.julia_version_minor}.${var.julia_version_patch}-linux-x86_64.tar.gz -C /opt/julia",
            "sudo rm -f julia-${var.julia_version_major}.${var.julia_version_minor}.${var.julia_version_patch}-linux-x86_64.tar.gz",
            "sed -i '1 i export PATH=\"/opt/julia/bin:$${PATH}\"' ~/.bashrc",
            "sed -i '1 i export JULIA_WORKER_TIMEOUT=\"720\"' ~/.bashrc"
        ]
    }

    provisioner "shell" {
        inline = [
            "echo \"**** installing julia packages ****\"",
            "julia -e 'using Pkg; Pkg.add([\"AzSessions\", \"Coverage\", \"Distributed\", \"HTTP\", \"JSON\", \"MPI\", \"MPIPreferences\", \"Random\", \"Test\"])'",
            "julia -e 'using MPIPreferences; MPIPreferences.use_jll_binary(\"MPICH_jll\")'",
            "julia -e 'using Pkg; Pkg.add(PackageSpec(url=\"${var.azmanagers_repo}\", rev=\"${var.azmanagers_version}\"))'"
        ]
    }

    provisioner "file" {
        source = "test/templates.jl"
        destination = "/tmp/templates.jl"
    }

    provisioner "shell" {
        inline = [
            "echo \"**** building AzManagers manifest and templates ****\"",
            "export TENANT_ID=\"${var.tenant_id}\"",
            "export SUBSCRIPTION_ID=\"${var.subscription_id}\"",
            "export RESOURCE_GROUP=\"${var.resource_group}\"",
            "export CLIENT_ID=\"${var.client_id}\"",
            "export CLIENT_SECRET=\"${var.client_secret}\"",
            "export IMAGE_NAME=\"${var.image_name}\"",
            "export VNET_NAME=\"${var.virtual_network}\"",
            "export SUBNET_NAME=\"${var.virtual_subnet}\"",
            "export GALLERY_NAME=\"${var.gallery}\"",
            "export LOCATION=\"${var.location}\"",
            "export VM_SIZE=\"${var.vm_size}\"",
            "julia /tmp/templates.jl"
        ]
    }
}
