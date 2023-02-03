package redhatopenshift_test

import (
	"context"
	"fmt"
	"github.com/hashicorp/go-azure-sdk/resource-manager/redhatopenshift/2022-09-04/openshiftclusters"
	"testing"

	"github.com/Azure/azure-sdk-for-go/services/redhatopenshift/mgmt/2022-04-01/redhatopenshift"
	"github.com/hashicorp/terraform-provider-azurerm/internal/acceptance"
	"github.com/hashicorp/terraform-provider-azurerm/internal/acceptance/check"
	"github.com/hashicorp/terraform-provider-azurerm/internal/clients"
	"github.com/hashicorp/terraform-provider-azurerm/internal/tf/pluginsdk"
	"github.com/hashicorp/terraform-provider-azurerm/utils"
)

type OpenShiftClusterResource struct{}

func TestAccOpenShiftCluster_basic(t *testing.T) {
	data := acceptance.BuildTestData(t, "azurerm_redhat_openshift_cluster", "test")
	r := OpenShiftClusterResource{}

	data.ResourceTest(t, r, []acceptance.TestStep{
		{
			Config: r.basic(data),
			Check: acceptance.ComposeTestCheckFunc(
				check.That(data.ResourceName).ExistsInAzure(r),
				check.That(data.ResourceName).Key("main_profile.0.vm_size").HasValue("Standard_D8s_v3"),
				check.That(data.ResourceName).Key("worker_profile.0.vm_size").HasValue("Standard_D4s_v3"),
				check.That(data.ResourceName).Key("worker_profile.0.disk_size_gb").HasValue("128"),
				check.That(data.ResourceName).Key("worker_profile.0.node_count").HasValue("3"),
				check.That(data.ResourceName).Key("api_server_profile.0.visibility").HasValue(string(redhatopenshift.VisibilityPublic)),
				check.That(data.ResourceName).Key("ingress_profile.0.visibility").HasValue(string(redhatopenshift.VisibilityPublic)),
			),
		},
	})
}

func TestAccOpenShiftCluster_private(t *testing.T) {
	data := acceptance.BuildTestData(t, "azurerm_redhat_openshift_cluster", "test")
	r := OpenShiftClusterResource{}

	data.ResourceTest(t, r, []acceptance.TestStep{
		{
			Config: r.private(data),
			Check: acceptance.ComposeTestCheckFunc(
				check.That(data.ResourceName).ExistsInAzure(r),
				check.That(data.ResourceName).Key("main_profile.0.vm_size").HasValue("Standard_D8s_v3"),
				check.That(data.ResourceName).Key("worker_profile.0.vm_size").HasValue("Standard_D4s_v3"),
				check.That(data.ResourceName).Key("worker_profile.0.disk_size_gb").HasValue("128"),
				check.That(data.ResourceName).Key("worker_profile.0.node_count").HasValue("3"),
				check.That(data.ResourceName).Key("api_server_profile.0.visibility").HasValue(string(redhatopenshift.VisibilityPrivate)),
				check.That(data.ResourceName).Key("ingress_profile.0.visibility").HasValue(string(redhatopenshift.VisibilityPrivate)),
			),
		},
	})
}

func TestAccOpenShiftCluster_customDomain(t *testing.T) {
	data := acceptance.BuildTestData(t, "azurerm_redhat_openshift_cluster", "test")
	r := OpenShiftClusterResource{}

	data.ResourceTest(t, r, []acceptance.TestStep{
		{
			Config: r.customDomain(data),
			Check: acceptance.ComposeTestCheckFunc(
				check.That(data.ResourceName).ExistsInAzure(r),
				check.That(data.ResourceName).Key("main_profile.0.vm_size").HasValue("Standard_D8s_v3"),
				check.That(data.ResourceName).Key("worker_profile.0.vm_size").HasValue("Standard_D4s_v3"),
				check.That(data.ResourceName).Key("worker_profile.0.disk_size_gb").HasValue("128"),
				check.That(data.ResourceName).Key("worker_profile.0.node_count").HasValue("3"),
				check.That(data.ResourceName).Key("api_server_profile.0.visibility").HasValue(string(redhatopenshift.VisibilityPublic)),
				check.That(data.ResourceName).Key("ingress_profile.0.visibility").HasValue(string(redhatopenshift.VisibilityPublic)),
				check.That(data.ResourceName).Key("cluster_profile.0.domain").HasValue("foo.example.com"),
			),
		},
	})
}

func TestAccOpenShiftCluster_encryptionAtHost(t *testing.T) {
	data := acceptance.BuildTestData(t, "azurerm_redhat_openshift_cluster", "test")
	r := OpenShiftClusterResource{}

	data.ResourceTest(t, r, []acceptance.TestStep{
		{
			Config: r.encryptionAtHost(data),
			Check: acceptance.ComposeTestCheckFunc(
				check.That(data.ResourceName).ExistsInAzure(r),
				check.That(data.ResourceName).Key("main_profile.0.encryption_at_host_enabled").HasValue("true"),
				check.That(data.ResourceName).Key("main_profile.0.disk_encryption_set_id").IsSet(),
				check.That(data.ResourceName).Key("worker_profile.0.encryption_at_host_enabled").HasValue("true"),
				check.That(data.ResourceName).Key("worker_profile.0.disk_encryption_set_id").IsSet(),
			),
		},
	})
}

func TestAccOpenShiftCluster_basicWithFipsEnabled(t *testing.T) {
	data := acceptance.BuildTestData(t, "azurerm_redhat_openshift_cluster", "test")
	r := OpenShiftClusterResource{}

	data.ResourceTest(t, r, []acceptance.TestStep{
		{
			Config: r.basicWithFipsEnabled(data),
			Check: acceptance.ComposeTestCheckFunc(
				check.That(data.ResourceName).ExistsInAzure(r),
				check.That(data.ResourceName).Key("cluster_profile.0.fips_enabled").HasValue("true"),
				check.That(data.ResourceName).Key("main_profile.0.vm_size").HasValue("Standard_D8s_v3"),
				check.That(data.ResourceName).Key("worker_profile.0.vm_size").HasValue("Standard_D4s_v3"),
				check.That(data.ResourceName).Key("worker_profile.0.disk_size_gb").HasValue("128"),
				check.That(data.ResourceName).Key("worker_profile.0.node_count").HasValue("3"),
				check.That(data.ResourceName).Key("api_server_profile.0.visibility").HasValue(string(redhatopenshift.VisibilityPublic)),
				check.That(data.ResourceName).Key("ingress_profile.0.visibility").HasValue(string(redhatopenshift.VisibilityPublic)),
			),
		},
	})
}

func (t OpenShiftClusterResource) Exists(ctx context.Context, clients *clients.Client, state *pluginsdk.InstanceState) (*bool, error) {
	id, err := openshiftclusters.ParseProviderOpenShiftClusterID(state.ID)
	if err != nil {
		return nil, err
	}

	resp, err := clients.RedHatOpenshift.OpenShiftClustersClient.Get(ctx, *id)
	if err != nil {
		return nil, fmt.Errorf("reading Red Hat Openshift Cluster (%s): %+v", id.String(), err)
	}

	return utils.Bool(resp.Model != nil), nil
}

func (OpenShiftClusterResource) basic(data acceptance.TestData) string {
	return fmt.Sprintf(`
provider "azurerm" {
  features {}
}

provider "azuread" {}

data "azuread_client_config" "test" {}

resource "azuread_application" "test" {
  display_name = "acctest-aro-%d"
}

resource "azuread_service_principal" "test" {
  application_id = azuread_application.test.application_id
}

resource "azuread_service_principal_password" "test" {
  service_principal_id = azuread_service_principal.test.object_id
}

resource "azurerm_resource_group" "test" {
  name     = "acctestRG-aro-%d"
  location = "%s"
}

resource "azurerm_resource_group" "test1" {
  name     = "acctestRG-aro-%d-2"
  location = "%s"
}


resource "azurerm_virtual_network" "test" {
  name                = "acctestvirtnet%d"
  address_space       = ["10.0.0.0/22"]
  location            = azurerm_resource_group.test.location
  resource_group_name = azurerm_resource_group.test.name
}

resource "azurerm_subnet" "main_subnet" {
  name                                           = "main-subnet-%d"
  resource_group_name                            = azurerm_resource_group.test.name
  virtual_network_name                           = azurerm_virtual_network.test.name
  address_prefixes                               = ["10.0.0.0/23"]
  service_endpoints                              = ["Microsoft.Storage", "Microsoft.ContainerRegistry"]
  enforce_private_link_service_network_policies  = true
  enforce_private_link_endpoint_network_policies = true
}

resource "azurerm_subnet" "worker_subnet" {
  name                 = "worker-subnet-%d"
  resource_group_name  = azurerm_resource_group.test.name
  virtual_network_name = azurerm_virtual_network.test.name
  address_prefixes     = ["10.0.2.0/23"]
  service_endpoints    = ["Microsoft.Storage", "Microsoft.ContainerRegistry"]
}

resource "azurerm_redhat_openshift_cluster" "test" {
  name                = "acctestaro%d"
  location            = azurerm_resource_group.test1.location
  resource_group_name = azurerm_resource_group.test1.name

  main_profile {
    vm_size   = "Standard_D8s_v3"
    subnet_id = azurerm_subnet.main_subnet.id
  }

  worker_profile {
    vm_size      = "Standard_D4s_v3"
    disk_size_gb = 128
    node_count   = 3
    subnet_id    = azurerm_subnet.worker_subnet.id
  }

  service_principal {
    client_id     = azuread_application.test.application_id
    client_secret = azuread_service_principal_password.test.value
  }
}
  `, data.RandomInteger, data.RandomInteger, data.Locations.Primary, data.RandomInteger, data.Locations.Primary, data.RandomInteger, data.RandomInteger, data.RandomInteger, data.RandomInteger)
}

func (OpenShiftClusterResource) private(data acceptance.TestData) string {
	return fmt.Sprintf(`
provider "azurerm" {
  features {}
}

provider "azuread" {}

data "azuread_client_config" "test" {}

resource "azuread_application" "test" {
  display_name = "acctest-aro-%d"
}

resource "azuread_service_principal" "test" {
  application_id = azuread_application.test.application_id
}

resource "azuread_service_principal_password" "test" {
  service_principal_id = azuread_service_principal.test.object_id
}

resource "azurerm_resource_group" "test" {
  name     = "acctestRG-aro-%d"
  location = "%s"
}

resource "azurerm_virtual_network" "test" {
  name                = "acctestvirtnet%d"
  address_space       = ["10.0.0.0/22"]
  location            = azurerm_resource_group.test.location
  resource_group_name = azurerm_resource_group.test.name
}

resource "azurerm_subnet" "main_subnet" {
  name                                           = "main-subnet-%d"
  resource_group_name                            = azurerm_resource_group.test.name
  virtual_network_name                           = azurerm_virtual_network.test.name
  address_prefixes                               = ["10.0.0.0/23"]
  service_endpoints                              = ["Microsoft.Storage", "Microsoft.ContainerRegistry"]
  enforce_private_link_service_network_policies  = true
  enforce_private_link_endpoint_network_policies = true
}

resource "azurerm_subnet" "worker_subnet" {
  name                 = "worker-subnet-%d"
  resource_group_name  = azurerm_resource_group.test.name
  virtual_network_name = azurerm_virtual_network.test.name
  address_prefixes     = ["10.0.2.0/23"]
  service_endpoints    = ["Microsoft.Storage", "Microsoft.ContainerRegistry"]
}

resource "azurerm_redhat_openshift_cluster" "test" {
  name                = "acctestaro%d"
  location            = azurerm_resource_group.test.location
  resource_group_name = azurerm_resource_group.test.name

  main_profile {
    vm_size   = "Standard_D8s_v3"
    subnet_id = azurerm_subnet.main_subnet.id
  }

  worker_profile {
    vm_size      = "Standard_D4s_v3"
    disk_size_gb = 128
    node_count   = 3
    subnet_id    = azurerm_subnet.worker_subnet.id
  }

  api_server_profile {
    visibility = "Private"
  }

  ingress_profile {
    visibility = "Private"
  }

  service_principal {
    client_id     = azuread_application.test.application_id
    client_secret = azuread_service_principal_password.test.value
  }
}
  `, data.RandomInteger, data.RandomInteger, data.Locations.Primary, data.RandomInteger, data.RandomInteger, data.RandomInteger, data.RandomInteger)
}

func (OpenShiftClusterResource) customDomain(data acceptance.TestData) string {
	return fmt.Sprintf(`
provider "azurerm" {
  features {}
}

provider "azuread" {}

data "azuread_client_config" "test" {}

resource "azuread_application" "test" {
  display_name = "acctest-aro-%d"
}

resource "azuread_service_principal" "test" {
  application_id = azuread_application.test.application_id
}

resource "azuread_service_principal_password" "test" {
  service_principal_id = azuread_service_principal.test.object_id
}

resource "azurerm_resource_group" "test" {
  name     = "acctestRG-aro-%d"
  location = "%s"
}

resource "azurerm_virtual_network" "test" {
  name                = "acctestvirtnet%d"
  address_space       = ["10.0.0.0/22"]
  location            = azurerm_resource_group.test.location
  resource_group_name = azurerm_resource_group.test.name
}

resource "azurerm_subnet" "main_subnet" {
  name                                           = "main-subnet-%d"
  resource_group_name                            = azurerm_resource_group.test.name
  virtual_network_name                           = azurerm_virtual_network.test.name
  address_prefixes                               = ["10.0.0.0/23"]
  service_endpoints                              = ["Microsoft.Storage", "Microsoft.ContainerRegistry"]
  enforce_private_link_service_network_policies  = true
  enforce_private_link_endpoint_network_policies = true
}

resource "azurerm_subnet" "worker_subnet" {
  name                 = "worker-subnet-%d"
  resource_group_name  = azurerm_resource_group.test.name
  virtual_network_name = azurerm_virtual_network.test.name
  address_prefixes     = ["10.0.2.0/23"]
  service_endpoints    = ["Microsoft.Storage", "Microsoft.ContainerRegistry"]
}

resource "azurerm_redhat_openshift_cluster" "test" {
  name                = "acctestaro%d"
  location            = azurerm_resource_group.test.location
  resource_group_name = azurerm_resource_group.test.name

  main_profile {
    vm_size   = "Standard_D8s_v3"
    subnet_id = azurerm_subnet.main_subnet.id
  }

  worker_profile {
    vm_size      = "Standard_D4s_v3"
    disk_size_gb = 128
    node_count   = 3
    subnet_id    = azurerm_subnet.worker_subnet.id
  }

  cluster_profile {
    domain = "foo.example.com"
  }

  service_principal {
    client_id     = azuread_application.test.application_id
    client_secret = azuread_service_principal_password.test.value
  }
}
  `, data.RandomInteger, data.RandomInteger, data.Locations.Primary, data.RandomInteger, data.RandomInteger, data.RandomInteger, data.RandomInteger)
}

func (OpenShiftClusterResource) basicWithFipsEnabled(data acceptance.TestData) string {
	return fmt.Sprintf(`
provider "azurerm" {
  features {}
}

provider "azuread" {}

data "azuread_client_config" "test" {}

resource "azuread_application" "test" {
  display_name = "acctest-aro-%d"
}

resource "azuread_service_principal" "test" {
  application_id = azuread_application.test.application_id
}

resource "azuread_service_principal_password" "test" {
  service_principal_id = azuread_service_principal.test.object_id
}

resource "azurerm_resource_group" "test" {
  name     = "acctestRG-aro-%d"
  location = "%s"
}

resource "azurerm_virtual_network" "test" {
  name                = "acctestvirtnet%d"
  address_space       = ["10.0.0.0/22"]
  location            = azurerm_resource_group.test.location
  resource_group_name = azurerm_resource_group.test.name
}

resource "azurerm_subnet" "main_subnet" {
  name                                           = "main-subnet-%d"
  resource_group_name                            = azurerm_resource_group.test.name
  virtual_network_name                           = azurerm_virtual_network.test.name
  address_prefixes                               = ["10.0.0.0/23"]
  service_endpoints                              = ["Microsoft.Storage", "Microsoft.ContainerRegistry"]
  enforce_private_link_service_network_policies  = true
  enforce_private_link_endpoint_network_policies = true
}

resource "azurerm_subnet" "worker_subnet" {
  name                 = "worker-subnet-%d"
  resource_group_name  = azurerm_resource_group.test.name
  virtual_network_name = azurerm_virtual_network.test.name
  address_prefixes     = ["10.0.2.0/23"]
  service_endpoints    = ["Microsoft.Storage", "Microsoft.ContainerRegistry"]
}

resource "azurerm_redhat_openshift_cluster" "test" {
  name                = "acctestaro%d"
  location            = azurerm_resource_group.test.location
  resource_group_name = azurerm_resource_group.test.name

  cluster_profile {
    fips_enabled = true
  }

  main_profile {
    vm_size   = "Standard_D8s_v3"
    subnet_id = azurerm_subnet.main_subnet.id
  }

  worker_profile {
    vm_size      = "Standard_D4s_v3"
    disk_size_gb = 128
    node_count   = 3
    subnet_id    = azurerm_subnet.worker_subnet.id
  }

  service_principal {
    client_id     = azuread_application.test.application_id
    client_secret = azuread_service_principal_password.test.value
  }
}
  `, data.RandomInteger, data.RandomInteger, data.Locations.Primary, data.RandomInteger, data.RandomInteger, data.RandomInteger, data.RandomInteger)
}

func (OpenShiftClusterResource) encryptionAtHost(data acceptance.TestData) string {
	return fmt.Sprintf(`
provider "azurerm" {
  features {
    key_vault {
      recover_soft_deleted_key_vaults    = false
      purge_soft_delete_on_destroy       = false
      purge_soft_deleted_keys_on_destroy = false
    }
  }
}

provider "azuread" {}

data "azuread_client_config" "test" {}

resource "azuread_application" "test" {
  display_name = "acctest-aro-%d"
}

resource "azuread_service_principal" "test" {
  application_id = azuread_application.test.application_id
}

resource "azuread_service_principal_password" "test" {
  service_principal_id = azuread_service_principal.test.object_id
}

resource "azurerm_resource_group" "test" {
  name     = "acctestRG-aro-%d"
  location = "%s"
}

resource "azurerm_virtual_network" "test" {
  name                = "acctestvirtnet%d"
  address_space       = ["10.0.0.0/22"]
  location            = azurerm_resource_group.test.location
  resource_group_name = azurerm_resource_group.test.name
}

resource "azurerm_subnet" "main_subnet" {
  name                                           = "main-subnet-%d"
  resource_group_name                            = azurerm_resource_group.test.name
  virtual_network_name                           = azurerm_virtual_network.test.name
  address_prefixes                               = ["10.0.0.0/23"]
  service_endpoints                              = ["Microsoft.Storage", "Microsoft.ContainerRegistry"]
  enforce_private_link_service_network_policies  = true
  enforce_private_link_endpoint_network_policies = true
}

resource "azurerm_subnet" "worker_subnet" {
  name                 = "worker-subnet-%d"
  resource_group_name  = azurerm_resource_group.test.name
  virtual_network_name = azurerm_virtual_network.test.name
  address_prefixes     = ["10.0.2.0/23"]
  service_endpoints    = ["Microsoft.Storage", "Microsoft.ContainerRegistry"]
}

resource "azurerm_key_vault" "test" {
  name                        = "acctestKV-%4s"
  location                    = azurerm_resource_group.test.location
  resource_group_name         = azurerm_resource_group.test.name
  tenant_id                   = data.azurerm_client_config.current.tenant_id
  sku_name                    = "premium"
  enabled_for_disk_encryption = true
  purge_protection_enabled    = true
}

resource "azurerm_key_vault_access_policy" "service-principal" {
  key_vault_id = azurerm_key_vault.test.id

  tenant_id = data.azurerm_client_config.current.tenant_id
  object_id = data.azurerm_client_config.current.object_id

  key_permissions = [
    "Create",
    "Delete",
    "Get",
    "Purge",
    "Update",
  ]
}

resource "azurerm_key_vault_key" "test" {
  name         = "acctestkvkey%s"
  key_vault_id = azurerm_key_vault.test.id
  key_type     = "RSA"
  key_size     = 2048

  key_opts = [
    "decrypt",
    "encrypt",
    "sign",
    "unwrapKey",
    "verify",
    "wrapKey",
  ]

  depends_on = [
    azurerm_key_vault_access_policy.service-principal
  ]
}

resource "azurerm_disk_encryption_set" "test" {
  name                = "acctestdes-%d"
  resource_group_name = azurerm_resource_group.test.name
  location            = azurerm_resource_group.test.location
  key_vault_key_id    = azurerm_key_vault_key.test.id

  identity {
    type = "SystemAssigned"
  }
}

resource "azurerm_key_vault_access_policy" "disk-encryption" {
  key_vault_id = azurerm_key_vault.test.id
  tenant_id    = azurerm_disk_encryption_set.test.identity.0.tenant_id
  object_id    = azurerm_disk_encryption_set.test.identity.0.principal_id

  key_permissions = [
    "Get",
    "WrapKey",
    "UnwrapKey"
  ]
}

resource "azurerm_redhat_openshift_cluster" "test" {
  name                = "acctestaro%d"
  location            = azurerm_resource_group.test.location
  resource_group_name = azurerm_resource_group.test.name

  main_profile {
    vm_size                    = "Standard_D8s_v3"
    subnet_id                  = azurerm_subnet.main_subnet.id
    encryption_at_host_enabled = true
    disk_encryption_set_id     = azurerm_disk_encryption_set.test.id
  }

  worker_profile {
    vm_size                    = "Standard_D4s_v3"
    disk_size_gb               = 128
    node_count                 = 3
    subnet_id                  = azurerm_subnet.worker_subnet.id
    encryption_at_host_enabled = true
    disk_encryption_set_id     = azurerm_disk_encryption_set.test.id
  }

  service_principal {
    client_id     = azuread_application.test.application_id
    client_secret = azuread_service_principal_password.test.value
  }

  depends_on = [
    azurerm_key_vault_access_policy.disk-encryption
  ]
}
  `, data.RandomInteger, data.RandomInteger, data.Locations.Primary, data.RandomInteger, data.RandomInteger, data.RandomInteger, data.RandomString, data.RandomString, data.RandomInteger, data.RandomInteger)
}