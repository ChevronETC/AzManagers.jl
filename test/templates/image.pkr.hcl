variable "tenant_id" {
  default = "tenant_id"
}
variable "subscription_id" {
  default = "subscription_id"
}
variable "client_id" {
  default = "client_id"
}
variable "client_secret" {
  default = "client_secret"
}
variable "resource_group" {
  default = "resource_group"
}
variable "gallery_name" {
  default = "gallery_name"
}
variable "virtual_network_name" {
  default = "virtual_network_name"
}
variable "image_name" {
  default = "image_name"
}
variable "image_version" {
  default = "1.0.0"
}
variable "julia_version" {
  default = "1.8"
}
variable "commit_sha" {
  default = "sha"
}

source "azure-arm" "cofii" {
  build_resource_group_name              = var.resource_group
  client_id                              = var.client_id
  client_secret                          = var.client_secret
  image_offer                            = "0001-com-ubuntu-server-jammy"
  image_publisher                        = "canonical"
  image_sku                              = "22_04-lts-gen2"
  managed_image_name                     = var.image_name
  managed_image_resource_group_name      = var.resource_group
  managed_image_storage_account_type     = "Premium_LRS"
  os_disk_size_gb                        = "128"
  os_type                                = "Linux"
  private_virtual_network_with_public_ip = true
  shared_image_gallery_destination {
    gallery_name        = var.gallery_name
    image_name          = var.image_name
    image_version       = var.image_version
    replication_regions = ["South Central US"]
    resource_group      = var.resource_group
  }
  shared_image_gallery_timeout        = "120m"
  ssh_username                        = "cvx"
  subscription_id                     = var.tenant_id
  tenant_id                           = "fd799da1-bfc1-4234-a91c-72b3a1cb9e26"
  virtual_network_name                = var.virtual_network_name
  virtual_network_resource_group_name = var.resource_group
  virtual_network_subnet_name         = "default"
  vm_size                             = "Standard_D8s_v5"
}

build {
  sources = [
    "source.azure-arm.cofii"
  ]

  provisioner "shell" {
    inline = [
      "sed -i '1 i export SUBSCRIPTION_ID=\"${var.subscription_id}\"' .bashrc",
      "sed -i '1 i export RESOURCE_GROUP=\"${var.resource_group}\"' .bashrc",
      "sed -i '1 i export VNET_NAME=\"${var.virtual_network_name}\"' .bashrc",
      "sed -i '1 i export GALLERY_NAME=\"${var.gallery_name}\"' .bashrc",
      "sed -i '1 i export CLIENT_ID=\"${var.client_id}\"' .bashrc",
      "sed -i '1 i export CLIENT_SECRET=\"${var.client_secret}\"' .bashrc",
    ]
  }

  provisioner "shell" {
    inline = [
      "echo \"Host *\" > ~/.ssh/config",
      "echo \"    StrictHostKeyChecking    no\" >> ~/.ssh/config",
      "echo \"    LogLevel                 ERROR\" >> ~/.ssh/config",
      "echo \"    UserKnownHostsFile       /dev/null\" >> ~/.ssh/config",
      "echo \"ssl_verify: false\" >> ~/.condarc"
    ]
  }

  provisioner "shell" {
    inline = [
      "sudo apt-get -y update",
      "sudo DEBIAN_FRONTEND=noninteractive apt-get -y -o Dpkg::Options::=\"--force-confdef\" -o Dpkg::Options::=\"--force-confold\" upgrade",
      "sudo apt-get -y install mpich git",
      "sudo mkdir -p /opt/julia",
      "cd /tmp"
    ]
  }

  provisioner "shell" {
    inline = [
      "ssh-keygen -f /home/cvx/.ssh/azmanagers_rsa -N ''"
    ]
  }

  provisioner "file" {
    source = "test/templates/azmanagers-setup.jl"
    destination = "/home/cvx/azmanagers-setup.jl"
  }

  provisioner "shell" {
    inline = [
      "echo \"installing Julia\"",
      "sudo wget https://julialang-s3.julialang.org/bin/linux/x64/${var.julia_version}/julia-${var.julia_version}.0-linux-x86_64.tar.gz",
      "mkdir ~/.julia",
      "sudo tar --strip-components=1 -xzvf julia-${var.julia_version}.0-linux-x86_64.tar.gz -C /opt/julia",
      "sudo rm -f julia-${var.julia_version}.0-linux-x86_64.tar.gz",
      "cd",
      "sed -i '1 i export PATH=\"${PATH}:/opt/julia/bin\"' .bashrc",
      "sed -i '1 i export JULIA_WORKER_TIMEOUT=\"720\"' .bashrc",
      "export PATH=\"${PATH}:/opt/julia/bin\"",
      "echo \"installing julia packages\"",
      "export JULIA_MPI_BINARY=\"system\"",
      "julia -e 'using Pkg; pkg\"add AzSessions AzManagers#${var.commit_sha} Distributed JSON HTTP Test Random Coverage\"'",
      "julia -e 'using Pkg; pkg\"precompile\"'"
    ]
  }
}
