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
    default = "8"
}

variable "julia_version_patch" {
    default = "0"
}

variable "azmanagers_version" {
    default = "master"
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
    vm_size = "Standard_D8s_v3"
    image_publisher = "canonical"
    image_offer = "0001-com-ubuntu-server-jammy"
    image_sku = "22_04-lts-gen2"
    shared_image_gallery_destination {
        resource_group = var.resource_group
        gallery_name = var.gallery
        image_name = var.image_name
        image_version = var.image_version
        replication_regions = ["South Central US"]
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
            "julia -e 'using Pkg; pkg\"add AzSessions AzManagers#${var.azmanagers_version} Coverage Distributed HTTP JSON MPI MPIPreferences Random Test\"'",
            "julia -e 'using MPIPreferences; MPIPreferences.use_jll_binary(\"MPICH_jll\")'"
        ]
    }

    provisioner "file" {
        source = "test/templates/azmanagers-setup.jl"
        destination = "/tmp/azmanagers-setup.jl"
    }

    provisioner "shell" {
        inline = [
            "export TENANT_ID=\"${var.tenant_id}\"",
            "export SUBSCRIPTION_ID=\"${var.subscription_id}\"",
            "export RESOURCE_GROUP=\"${var.resource_group}\"",
            "export CLIENT_ID=\"${var.client_id}\"",
            "export CLIENT_SECRET=\"${var.client_secret}\"",
            "export IMAGE_NAME=\"${var.image_name}\"",
            "export VENT_NAME=\"${var.virtual_network}\"",
            "export SUBNET_NAME=\"${var.virtual_subnet}\"",
            "export GALLERY_NAME=\"${var.gallery}\"",
            "julia /tmp/azmanagers-setup.jl"
        ]
    }
}