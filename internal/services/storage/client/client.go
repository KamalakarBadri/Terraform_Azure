// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package client

import (
	"context"
	"fmt"

	"github.com/Azure/azure-sdk-for-go/services/storage/mgmt/2021-09-01/storage" // nolint: staticcheck
	"github.com/Azure/go-autorest/autorest"
	"github.com/Azure/go-autorest/autorest/azure"
	storage_v2023_01_01 "github.com/hashicorp/go-azure-sdk/resource-manager/storage/2023-01-01"
	"github.com/hashicorp/go-azure-sdk/resource-manager/storagesync/2020-03-01/cloudendpointresource"
	"github.com/hashicorp/go-azure-sdk/resource-manager/storagesync/2020-03-01/storagesyncservicesresource"
	"github.com/hashicorp/go-azure-sdk/resource-manager/storagesync/2020-03-01/syncgroupresource"
	"github.com/hashicorp/go-azure-sdk/sdk/auth"
	"github.com/hashicorp/go-azure-sdk/sdk/client/resourcemanager"
	"github.com/hashicorp/go-azure-sdk/sdk/environments"
	"github.com/hashicorp/terraform-provider-azurerm/internal/common"
	"github.com/hashicorp/terraform-provider-azurerm/internal/services/storage/shim"
	"github.com/tombuildsstuff/giovanni/storage/2023-11-03/blob/accounts"
	"github.com/tombuildsstuff/giovanni/storage/2023-11-03/blob/blobs"
	"github.com/tombuildsstuff/giovanni/storage/2023-11-03/blob/containers"
	"github.com/tombuildsstuff/giovanni/storage/2023-11-03/datalakestore/filesystems"
	"github.com/tombuildsstuff/giovanni/storage/2023-11-03/datalakestore/paths"
	"github.com/tombuildsstuff/giovanni/storage/2023-11-03/file/directories"
	"github.com/tombuildsstuff/giovanni/storage/2023-11-03/file/files"
	"github.com/tombuildsstuff/giovanni/storage/2023-11-03/file/shares"
	"github.com/tombuildsstuff/giovanni/storage/2023-11-03/queue/queues"
	"github.com/tombuildsstuff/giovanni/storage/2023-11-03/table/entities"
	"github.com/tombuildsstuff/giovanni/storage/2023-11-03/table/tables"
)

type Client struct {
	SubscriptionId string

	ADLSGen2PathsClient         *paths.Client
	AccountsClient              *storage.AccountsClient
	BlobInventoryPoliciesClient *storage.BlobInventoryPoliciesClient
	BlobServicesClient          *storage.BlobServicesClient
	EncryptionScopesClient      *storage.EncryptionScopesClient
	FileServicesClient          *storage.FileServicesClient
	FileSystemsClient           *filesystems.Client
	SyncCloudEndpointsClient    *cloudendpointresource.CloudEndpointResourceClient
	SyncGroupsClient            *syncgroupresource.SyncGroupResourceClient
	SyncServiceClient           *storagesyncservicesresource.StorageSyncServicesResourceClient

	ResourceManager *storage_v2023_01_01.Client

	Environment         environments.Environment
	AzureEnvironment    azure.Environment
	StorageDomainSuffix string

	resourceManagerAuthorizer autorest.Authorizer
	authorizerForAad          auth.Authorizer
	autorestAuthorizerForAad  *autorest.Authorizer
}

func NewClient(o *common.ClientOptions) (*Client, error) {
	storageSuffix, ok := o.Environment.Storage.DomainSuffix()
	if !ok {
		return nil, fmt.Errorf("determining domain suffix for storage in environment: %s", o.Environment.Name)
	}

	accountsClient := storage.NewAccountsClientWithBaseURI(o.ResourceManagerEndpoint, o.SubscriptionId)
	o.ConfigureClient(&accountsClient.Client, o.ResourceManagerAuthorizer)

	fileSystemsClient, err := filesystems.NewWithBaseUri(*storageSuffix)
	if err != nil {
		return nil, fmt.Errorf("building Data Lake Store Filesystems client: %+v", err)
	}
	o.Configure(fileSystemsClient.Client, o.Authorizers.Storage)

	adlsGen2PathsClient, err := paths.NewWithBaseUri(*storageSuffix)
	if err != nil {
		return nil, fmt.Errorf("building Data Lake Storage Path client: %+v", err)
	}
	o.Configure(adlsGen2PathsClient.Client, o.Authorizers.Storage)

	blobServicesClient := storage.NewBlobServicesClientWithBaseURI(o.ResourceManagerEndpoint, o.SubscriptionId)
	o.ConfigureClient(&blobServicesClient.Client, o.ResourceManagerAuthorizer)

	blobInventoryPoliciesClient := storage.NewBlobInventoryPoliciesClientWithBaseURI(o.ResourceManagerEndpoint, o.SubscriptionId)
	o.ConfigureClient(&blobInventoryPoliciesClient.Client, o.ResourceManagerAuthorizer)

	encryptionScopesClient := storage.NewEncryptionScopesClientWithBaseURI(o.ResourceManagerEndpoint, o.SubscriptionId)
	o.ConfigureClient(&encryptionScopesClient.Client, o.ResourceManagerAuthorizer)

	fileServicesClient := storage.NewFileServicesClientWithBaseURI(o.ResourceManagerEndpoint, o.SubscriptionId)
	o.ConfigureClient(&fileServicesClient.Client, o.ResourceManagerAuthorizer)

	resourceManager, err := storage_v2023_01_01.NewClientWithBaseURI(o.Environment.ResourceManager, func(c *resourcemanager.Client) {
		o.Configure(c, o.Authorizers.ResourceManager)
	})
	if err != nil {
		return nil, fmt.Errorf("building ResourceManager clients: %+v", err)
	}

	syncCloudEndpointsClient, err := cloudendpointresource.NewCloudEndpointResourceClientWithBaseURI(o.Environment.ResourceManager)
	if err != nil {
		return nil, fmt.Errorf("building CloudEndpoint client: %+v", err)
	}
	o.Configure(syncCloudEndpointsClient.Client, o.Authorizers.ResourceManager)
	syncServiceClient, err := storagesyncservicesresource.NewStorageSyncServicesResourceClientWithBaseURI(o.Environment.ResourceManager)
	if err != nil {
		return nil, fmt.Errorf("building StorageSyncService client: %+v", err)
	}
	o.Configure(syncServiceClient.Client, o.Authorizers.ResourceManager)

	syncGroupsClient, err := syncgroupresource.NewSyncGroupResourceClientWithBaseURI(o.Environment.ResourceManager)
	if err != nil {
		return nil, fmt.Errorf("building StorageSyncGroups client: %+v", err)
	}
	o.Configure(syncGroupsClient.Client, o.Authorizers.ResourceManager)

	// TODO: switch Storage Containers to using the storage.BlobContainersClient
	// (which should fix #2977) when the storage clients have been moved in here
	client := Client{
		AccountsClient:              &accountsClient,
		FileSystemsClient:           fileSystemsClient,
		ADLSGen2PathsClient:         adlsGen2PathsClient,
		BlobServicesClient:          &blobServicesClient,
		BlobInventoryPoliciesClient: &blobInventoryPoliciesClient,
		EncryptionScopesClient:      &encryptionScopesClient,
		FileServicesClient:          &fileServicesClient,
		ResourceManager:             resourceManager,
		SubscriptionId:              o.SubscriptionId,
		SyncCloudEndpointsClient:    syncCloudEndpointsClient,
		SyncServiceClient:           syncServiceClient,
		SyncGroupsClient:            syncGroupsClient,

		Environment:         o.Environment,
		AzureEnvironment:    o.AzureEnvironment,
		StorageDomainSuffix: *storageSuffix,

		resourceManagerAuthorizer: o.ResourceManagerAuthorizer,
	}

	if o.StorageUseAzureAD {
		client.authorizerForAad = o.Authorizers.Storage
		client.autorestAuthorizerForAad = &o.StorageAuthorizer
	}

	return &client, nil
}

func (client Client) AccountsDataPlaneClient(ctx context.Context, account accountDetails) (*accounts.Client, error) {
	if client.authorizerForAad != nil {
		accountsClient, err := accounts.NewWithBaseUri(client.StorageDomainSuffix)
		if err != nil {
			return nil, fmt.Errorf("building Blob Storage Accounts client: %+v", err)
		}

		accountsClient.Client.SetAuthorizer(client.authorizerForAad)

		return accountsClient, nil
	}

	accountKey, err := account.AccountKey(ctx, client)
	if err != nil {
		return nil, fmt.Errorf("retrieving Storage Account Key: %s", err)
	}

	storageAuth, err := auth.NewSharedKeyAuthorizer(account.name, *accountKey, auth.SharedKey)
	if err != nil {
		return nil, fmt.Errorf("building Shared Key Authorizer for Blob Storage client: %+v", err)
	}

	accountsClient, err := accounts.NewWithBaseUri(client.StorageDomainSuffix)
	if err != nil {
		return nil, fmt.Errorf("building Blob Storage Accounts client: %+v", err)
	}

	accountsClient.Client.SetAuthorizer(storageAuth)

	return accountsClient, nil
}

func (client Client) BlobsClient(ctx context.Context, account accountDetails) (*blobs.Client, error) {
	if client.authorizerForAad != nil {
		blobsClient, err := blobs.NewWithBaseUri(client.StorageDomainSuffix)
		if err != nil {
			return nil, fmt.Errorf("building Blob Storage Blobs client: %+v", err)
		}

		blobsClient.Client.SetAuthorizer(client.authorizerForAad)

		return blobsClient, nil
	}

	accountKey, err := account.AccountKey(ctx, client)
	if err != nil {
		return nil, fmt.Errorf("retrieving Storage Account Key: %s", err)
	}

	storageAuth, err := auth.NewSharedKeyAuthorizer(account.name, *accountKey, auth.SharedKey)
	if err != nil {
		return nil, fmt.Errorf("building Shared Key Authorizer for Blob Storage client: %+v", err)
	}

	blobsClient, err := blobs.NewWithBaseUri(client.StorageDomainSuffix)
	if err != nil {
		return nil, fmt.Errorf("building Blob Storage Blobs client: %+v", err)
	}

	blobsClient.Client.SetAuthorizer(storageAuth)

	return blobsClient, nil
}

func (client Client) ContainersClient(ctx context.Context, account accountDetails) (shim.StorageContainerWrapper, error) {
	if client.authorizerForAad != nil {
		containersClient, err := containers.NewWithBaseUri(client.StorageDomainSuffix)
		if err != nil {
			return nil, fmt.Errorf("building Blob Storage Containers client: %+v", err)
		}

		containersClient.Client.SetAuthorizer(client.authorizerForAad)

		return shim.NewDataPlaneStorageContainerWrapper(containersClient), nil
	}

	accountKey, err := account.AccountKey(ctx, client)
	if err != nil {
		return nil, fmt.Errorf("retrieving Storage Account Key: %s", err)
	}

	storageAuth, err := auth.NewSharedKeyAuthorizer(account.name, *accountKey, auth.SharedKey)
	if err != nil {
		return nil, fmt.Errorf("building Shared Key Authorizer for Blob Storage client: %+v", err)
	}

	containersClient, err := containers.NewWithBaseUri(client.StorageDomainSuffix)
	if err != nil {
		return nil, fmt.Errorf("building Blob Storage Containers client: %+v", err)
	}

	containersClient.Client.SetAuthorizer(storageAuth)

	return shim.NewDataPlaneStorageContainerWrapper(containersClient), nil
}

func (client Client) FileShareDirectoriesClient(ctx context.Context, account accountDetails) (*directories.Client, error) {
	if client.authorizerForAad != nil {
		directoriesClient, err := directories.NewWithBaseUri(client.StorageDomainSuffix)
		if err != nil {
			return nil, fmt.Errorf("building File Storage Share Directories client: %+v", err)
		}

		directoriesClient.Client.SetAuthorizer(client.authorizerForAad)

		return directoriesClient, nil
	}

	accountKey, err := account.AccountKey(ctx, client)
	if err != nil {
		return nil, fmt.Errorf("retrieving Storage Account Key: %s", err)
	}

	storageAuth, err := auth.NewSharedKeyAuthorizer(account.name, *accountKey, auth.SharedKey)
	if err != nil {
		return nil, fmt.Errorf("building Shared Key Authorizer for File Storage Shares client: %+v", err)
	}

	directoriesClient, err := directories.NewWithBaseUri(client.StorageDomainSuffix)
	if err != nil {
		return nil, fmt.Errorf("building File Storage Share Directories client: %+v", err)
	}

	directoriesClient.Client.SetAuthorizer(storageAuth)

	return directoriesClient, nil
}

func (client Client) FileShareFilesClient(ctx context.Context, account accountDetails) (*files.Client, error) {
	if client.authorizerForAad != nil {
		filesClient, err := files.NewWithBaseUri(client.StorageDomainSuffix)
		if err != nil {
			return nil, fmt.Errorf("building File Storage Share Files client: %+v", err)
		}

		filesClient.Client.SetAuthorizer(client.authorizerForAad)

		return filesClient, nil
	}

	accountKey, err := account.AccountKey(ctx, client)
	if err != nil {
		return nil, fmt.Errorf("retrieving Storage Account Key: %s", err)
	}

	storageAuth, err := auth.NewSharedKeyAuthorizer(account.name, *accountKey, auth.SharedKey)
	if err != nil {
		return nil, fmt.Errorf("building Shared Key Authorizer for File Storage Shares client: %+v", err)
	}

	filesClient, err := files.NewWithBaseUri(client.StorageDomainSuffix)
	if err != nil {
		return nil, fmt.Errorf("building File Storage Share Files client: %+v", err)
	}

	filesClient.Client.SetAuthorizer(storageAuth)

	return filesClient, nil
}

func (client Client) FileSharesClient(ctx context.Context, account accountDetails) (shim.StorageShareWrapper, error) {
	if client.authorizerForAad != nil {
		sharesClient, err := shares.NewWithBaseUri(client.StorageDomainSuffix)
		if err != nil {
			return nil, fmt.Errorf("building File Storage Share Shares client: %+v", err)
		}

		sharesClient.Client.SetAuthorizer(client.authorizerForAad)

		return shim.NewDataPlaneStorageShareWrapper(sharesClient), nil
	}

	accountKey, err := account.AccountKey(ctx, client)
	if err != nil {
		return nil, fmt.Errorf("retrieving Storage Account Key: %s", err)
	}

	storageAuth, err := auth.NewSharedKeyAuthorizer(account.name, *accountKey, auth.SharedKey)
	if err != nil {
		return nil, fmt.Errorf("building Shared Key Authorizer for File Storage Shares client: %+v", err)
	}

	sharesClient, err := shares.NewWithBaseUri(client.StorageDomainSuffix)
	if err != nil {
		return nil, fmt.Errorf("building File Storage Share Shares client: %+v", err)
	}

	sharesClient.Client.SetAuthorizer(storageAuth)

	return shim.NewDataPlaneStorageShareWrapper(sharesClient), nil
}

func (client Client) QueuesClient(ctx context.Context, account accountDetails) (shim.StorageQueuesWrapper, error) {
	if client.authorizerForAad != nil {
		queuesClient, err := queues.NewWithBaseUri(client.StorageDomainSuffix)
		if err != nil {
			return nil, fmt.Errorf("building File Storage Queue Queues client: %+v", err)
		}

		queuesClient.Client.SetAuthorizer(client.authorizerForAad)

		return shim.NewDataPlaneStorageQueueWrapper(queuesClient), nil
	}

	accountKey, err := account.AccountKey(ctx, client)
	if err != nil {
		return nil, fmt.Errorf("retrieving Storage Account Key: %s", err)
	}

	storageAuth, err := auth.NewSharedKeyAuthorizer(account.name, *accountKey, auth.SharedKey)
	if err != nil {
		return nil, fmt.Errorf("building Queued Key Authorizer for File Storage Queues client: %+v", err)
	}

	queuesClient, err := queues.NewWithBaseUri(client.StorageDomainSuffix)
	if err != nil {
		return nil, fmt.Errorf("building File Storage Queue Queues client: %+v", err)
	}

	queuesClient.Client.SetAuthorizer(storageAuth)

	return shim.NewDataPlaneStorageQueueWrapper(queuesClient), nil
}

func (client Client) TableEntityClient(ctx context.Context, account accountDetails) (*entities.Client, error) {
	if client.authorizerForAad != nil {
		entitiesClient, err := entities.NewWithBaseUri(client.StorageDomainSuffix)
		if err != nil {
			return nil, fmt.Errorf("building Table Storage Share Entities client: %+v", err)
		}

		entitiesClient.Client.SetAuthorizer(client.authorizerForAad)

		return entitiesClient, nil
	}

	accountKey, err := account.AccountKey(ctx, client)
	if err != nil {
		return nil, fmt.Errorf("retrieving Storage Account Key: %s", err)
	}

	storageAuth, err := auth.NewSharedKeyAuthorizer(account.name, *accountKey, auth.SharedKey)
	if err != nil {
		return nil, fmt.Errorf("building Shared Key Authorizer for Table Storage Shares client: %+v", err)
	}

	entitiesClient, err := entities.NewWithBaseUri(client.StorageDomainSuffix)
	if err != nil {
		return nil, fmt.Errorf("building Table Storage Share Entities client: %+v", err)
	}

	entitiesClient.Client.SetAuthorizer(storageAuth)

	return entitiesClient, nil
}

func (client Client) TablesClient(ctx context.Context, account accountDetails) (shim.StorageTableWrapper, error) {
	if client.authorizerForAad != nil {
		tablesClient, err := tables.NewWithBaseUri(client.StorageDomainSuffix)
		if err != nil {
			return nil, fmt.Errorf("building Table Storage Share Tables client: %+v", err)
		}

		tablesClient.Client.SetAuthorizer(client.authorizerForAad)

		return shim.NewDataPlaneStorageTableWrapper(tablesClient), nil
	}

	accountKey, err := account.AccountKey(ctx, client)
	if err != nil {
		return nil, fmt.Errorf("retrieving Storage Account Key: %s", err)
	}

	storageAuth, err := auth.NewSharedKeyAuthorizer(account.name, *accountKey, auth.SharedKey)
	if err != nil {
		return nil, fmt.Errorf("building Shared Key Authorizer for Table Storage Shares client: %+v", err)
	}

	tablesClient, err := tables.NewWithBaseUri(client.StorageDomainSuffix)
	if err != nil {
		return nil, fmt.Errorf("building Table Storage Share Tables client: %+v", err)
	}

	tablesClient.Client.SetAuthorizer(storageAuth)

	return shim.NewDataPlaneStorageTableWrapper(tablesClient), nil
}
