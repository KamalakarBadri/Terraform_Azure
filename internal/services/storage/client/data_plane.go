package client

import (
	"context"
	"fmt"
	"strings"

	"github.com/hashicorp/go-azure-sdk/sdk/auth"
	"github.com/hashicorp/terraform-provider-azurerm/internal/services/storage/shim"
	"github.com/tombuildsstuff/giovanni/storage/2023-11-03/blob/accounts"
	"github.com/tombuildsstuff/giovanni/storage/2023-11-03/blob/blobs"
	"github.com/tombuildsstuff/giovanni/storage/2023-11-03/blob/containers"
	"github.com/tombuildsstuff/giovanni/storage/2023-11-03/file/directories"
	"github.com/tombuildsstuff/giovanni/storage/2023-11-03/file/files"
	"github.com/tombuildsstuff/giovanni/storage/2023-11-03/file/shares"
	"github.com/tombuildsstuff/giovanni/storage/2023-11-03/queue/queues"
	"github.com/tombuildsstuff/giovanni/storage/2023-11-03/table/entities"
	"github.com/tombuildsstuff/giovanni/storage/2023-11-03/table/tables"
)

func (client Client) AccountsDataPlaneClient(ctx context.Context, account accountDetails) (*accounts.Client, error) {
	if account.Properties == nil {
		return nil, fmt.Errorf("storage account %q has no properties", account.name)
	}
	if account.Properties.PrimaryEndpoints == nil {
		return nil, fmt.Errorf("storage account %q has missing endpoints", account.name)
	}
	if account.Properties.PrimaryEndpoints.Blob == nil {
		return nil, fmt.Errorf("determing Blob endpoint for storage account %q", account.name)
	}

	baseUri := strings.TrimSuffix(*account.Properties.PrimaryEndpoints.Blob, "/")

	if client.authorizerForAad != nil {
		accountsClient, err := accounts.NewWithBaseUri(baseUri)
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

	accountsClient, err := accounts.NewWithBaseUri(baseUri)
	if err != nil {
		return nil, fmt.Errorf("building Blob Storage Accounts client: %+v", err)
	}

	accountsClient.Client.SetAuthorizer(storageAuth)

	return accountsClient, nil
}

func (client Client) BlobsDataPlaneClient(ctx context.Context, account accountDetails) (*blobs.Client, error) {
	if account.Properties == nil {
		return nil, fmt.Errorf("storage account %q has no properties", account.name)
	}
	if account.Properties.PrimaryEndpoints == nil {
		return nil, fmt.Errorf("storage account %q has missing endpoints", account.name)
	}
	if account.Properties.PrimaryEndpoints.Blob == nil {
		return nil, fmt.Errorf("determing Blob endpoint for storage account %q", account.name)
	}

	baseUri := strings.TrimSuffix(*account.Properties.PrimaryEndpoints.Blob, "/")

	if client.authorizerForAad != nil {
		blobsClient, err := blobs.NewWithBaseUri(baseUri)
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

	blobsClient, err := blobs.NewWithBaseUri(baseUri)
	if err != nil {
		return nil, fmt.Errorf("building Blob Storage Blobs client: %+v", err)
	}

	blobsClient.Client.SetAuthorizer(storageAuth)

	return blobsClient, nil
}

func (client Client) ContainersDataPlaneClient(ctx context.Context, account accountDetails) (shim.StorageContainerWrapper, error) {
	if account.Properties == nil {
		return nil, fmt.Errorf("storage account %q has no properties", account.name)
	}
	if account.Properties.PrimaryEndpoints == nil {
		return nil, fmt.Errorf("storage account %q has missing endpoints", account.name)
	}
	if account.Properties.PrimaryEndpoints.Blob == nil {
		return nil, fmt.Errorf("determing Blob endpoint for storage account %q", account.name)
	}

	baseUri := strings.TrimSuffix(*account.Properties.PrimaryEndpoints.Blob, "/")

	if client.authorizerForAad != nil {
		containersClient, err := containers.NewWithBaseUri(baseUri)
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

	containersClient, err := containers.NewWithBaseUri(baseUri)
	if err != nil {
		return nil, fmt.Errorf("building Blob Storage Containers client: %+v", err)
	}

	containersClient.Client.SetAuthorizer(storageAuth)

	return shim.NewDataPlaneStorageContainerWrapper(containersClient), nil
}

func (client Client) FileShareDirectoriesDataPlaneClient(ctx context.Context, account accountDetails) (*directories.Client, error) {
	if account.Properties == nil {
		return nil, fmt.Errorf("storage account %q has no properties", account.name)
	}
	if account.Properties.PrimaryEndpoints == nil {
		return nil, fmt.Errorf("storage account %q has missing endpoints", account.name)
	}
	if account.Properties.PrimaryEndpoints.File == nil {
		return nil, fmt.Errorf("determing File endpoint for storage account %q", account.name)
	}

	baseUri := strings.TrimSuffix(*account.Properties.PrimaryEndpoints.File, "/")

	if client.authorizerForAad != nil {
		directoriesClient, err := directories.NewWithBaseUri(baseUri)
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

	directoriesClient, err := directories.NewWithBaseUri(baseUri)
	if err != nil {
		return nil, fmt.Errorf("building File Storage Share Directories client: %+v", err)
	}

	directoriesClient.Client.SetAuthorizer(storageAuth)

	return directoriesClient, nil
}

func (client Client) FileShareFilesDataPlaneClient(ctx context.Context, account accountDetails) (*files.Client, error) {
	if account.Properties == nil {
		return nil, fmt.Errorf("storage account %q has no properties", account.name)
	}
	if account.Properties.PrimaryEndpoints == nil {
		return nil, fmt.Errorf("storage account %q has missing endpoints", account.name)
	}
	if account.Properties.PrimaryEndpoints.File == nil {
		return nil, fmt.Errorf("determing File endpoint for storage account %q", account.name)
	}

	baseUri := strings.TrimSuffix(*account.Properties.PrimaryEndpoints.File, "/")

	if client.authorizerForAad != nil {
		filesClient, err := files.NewWithBaseUri(baseUri)
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

	filesClient, err := files.NewWithBaseUri(baseUri)
	if err != nil {
		return nil, fmt.Errorf("building File Storage Share Files client: %+v", err)
	}

	filesClient.Client.SetAuthorizer(storageAuth)

	return filesClient, nil
}

func (client Client) FileSharesDataPlaneClient(ctx context.Context, account accountDetails) (shim.StorageShareWrapper, error) {
	if account.Properties == nil {
		return nil, fmt.Errorf("storage account %q has no properties", account.name)
	}
	if account.Properties.PrimaryEndpoints == nil {
		return nil, fmt.Errorf("storage account %q has missing endpoints", account.name)
	}
	if account.Properties.PrimaryEndpoints.File == nil {
		return nil, fmt.Errorf("determing File endpoint for storage account %q", account.name)
	}

	baseUri := strings.TrimSuffix(*account.Properties.PrimaryEndpoints.File, "/")

	if client.authorizerForAad != nil {
		sharesClient, err := shares.NewWithBaseUri(baseUri)
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

	sharesClient, err := shares.NewWithBaseUri(baseUri)
	if err != nil {
		return nil, fmt.Errorf("building File Storage Share Shares client: %+v", err)
	}

	sharesClient.Client.SetAuthorizer(storageAuth)

	return shim.NewDataPlaneStorageShareWrapper(sharesClient), nil
}

func (client Client) QueuesDataPlaneClient(ctx context.Context, account accountDetails) (shim.StorageQueuesWrapper, error) {
	if account.Properties == nil {
		return nil, fmt.Errorf("storage account %q has no properties", account.name)
	}
	if account.Properties.PrimaryEndpoints == nil {
		return nil, fmt.Errorf("storage account %q has missing endpoints", account.name)
	}
	if account.Properties.PrimaryEndpoints.Queue == nil {
		return nil, fmt.Errorf("determing Queue endpoint for storage account %q", account.name)
	}

	baseUri := strings.TrimSuffix(*account.Properties.PrimaryEndpoints.Queue, "/")

	if client.authorizerForAad != nil {
		queuesClient, err := queues.NewWithBaseUri(baseUri)
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

	queuesClient, err := queues.NewWithBaseUri(baseUri)
	if err != nil {
		return nil, fmt.Errorf("building File Storage Queue Queues client: %+v", err)
	}

	queuesClient.Client.SetAuthorizer(storageAuth)

	return shim.NewDataPlaneStorageQueueWrapper(queuesClient), nil
}

func (client Client) TableEntityDataPlaneClient(ctx context.Context, account accountDetails) (*entities.Client, error) {
	if account.Properties == nil {
		return nil, fmt.Errorf("storage account %q has no properties", account.name)
	}
	if account.Properties.PrimaryEndpoints == nil {
		return nil, fmt.Errorf("storage account %q has missing endpoints", account.name)
	}
	if account.Properties.PrimaryEndpoints.Table == nil {
		return nil, fmt.Errorf("determing Table endpoint for storage account %q", account.name)
	}

	baseUri := strings.TrimSuffix(*account.Properties.PrimaryEndpoints.Table, "/")

	if client.authorizerForAad != nil {
		entitiesClient, err := entities.NewWithBaseUri(baseUri)
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

	entitiesClient, err := entities.NewWithBaseUri(baseUri)
	if err != nil {
		return nil, fmt.Errorf("building Table Storage Share Entities client: %+v", err)
	}

	entitiesClient.Client.SetAuthorizer(storageAuth)

	return entitiesClient, nil
}

func (client Client) TablesDataPlaneClient(ctx context.Context, account accountDetails) (shim.StorageTableWrapper, error) {
	if account.Properties == nil {
		return nil, fmt.Errorf("storage account %q has no properties", account.name)
	}
	if account.Properties.PrimaryEndpoints == nil {
		return nil, fmt.Errorf("storage account %q has missing endpoints", account.name)
	}
	if account.Properties.PrimaryEndpoints.Table == nil {
		return nil, fmt.Errorf("determing Table endpoint for storage account %q", account.name)
	}

	baseUri := strings.TrimSuffix(*account.Properties.PrimaryEndpoints.Table, "/")

	if client.authorizerForAad != nil {
		tablesClient, err := tables.NewWithBaseUri(baseUri)
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

	tablesClient, err := tables.NewWithBaseUri(baseUri)
	if err != nil {
		return nil, fmt.Errorf("building Table Storage Share Tables client: %+v", err)
	}

	tablesClient.Client.SetAuthorizer(storageAuth)

	return shim.NewDataPlaneStorageTableWrapper(tablesClient), nil
}
