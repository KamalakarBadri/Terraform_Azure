// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package storage

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/hashicorp/go-azure-helpers/lang/response"
	"github.com/hashicorp/terraform-provider-azurerm/helpers/tf"
	"github.com/hashicorp/terraform-provider-azurerm/internal/clients"
	"github.com/hashicorp/terraform-provider-azurerm/internal/services/storage/helpers"
	"github.com/hashicorp/terraform-provider-azurerm/internal/services/storage/validate"
	"github.com/hashicorp/terraform-provider-azurerm/internal/tf/pluginsdk"
	"github.com/hashicorp/terraform-provider-azurerm/internal/tf/validation"
	"github.com/hashicorp/terraform-provider-azurerm/internal/timeouts"
	"github.com/tombuildsstuff/giovanni/storage/2023-11-03/blob/accounts"
	"github.com/tombuildsstuff/giovanni/storage/2023-11-03/file/directories"
)

func resourceStorageShareDirectory() *pluginsdk.Resource {
	return &pluginsdk.Resource{
		Create: resourceStorageShareDirectoryCreate,
		Read:   resourceStorageShareDirectoryRead,
		Update: resourceStorageShareDirectoryUpdate,
		Delete: resourceStorageShareDirectoryDelete,

		Importer: helpers.ImporterValidatingStorageResourceId(func(id, storageDomainSuffix string) error {
			_, err := directories.ParseDirectoryID(id, storageDomainSuffix)
			return err
		}),

		Timeouts: &pluginsdk.ResourceTimeout{
			Create: pluginsdk.DefaultTimeout(30 * time.Minute),
			Read:   pluginsdk.DefaultTimeout(5 * time.Minute),
			Update: pluginsdk.DefaultTimeout(30 * time.Minute),
			Delete: pluginsdk.DefaultTimeout(30 * time.Minute),
		},

		Schema: map[string]*pluginsdk.Schema{
			"name": {
				Type:         pluginsdk.TypeString,
				Required:     true,
				ForceNew:     true,
				ValidateFunc: validate.StorageShareDirectoryName,
			},
			"share_name": {
				Type:         pluginsdk.TypeString,
				Required:     true,
				ForceNew:     true,
				ValidateFunc: validation.StringIsNotEmpty,
			},
			"storage_account_name": {
				Type:         pluginsdk.TypeString,
				Required:     true,
				ForceNew:     true,
				ValidateFunc: validation.StringIsNotEmpty,
			},

			"metadata": MetaDataSchema(),
		},
	}
}

func resourceStorageShareDirectoryCreate(d *pluginsdk.ResourceData, meta interface{}) error {
	ctx, cancel := timeouts.ForCreate(meta.(*clients.Client).StopContext, d)
	defer cancel()
	storageClient := meta.(*clients.Client).Storage

	accountName := d.Get("storage_account_name").(string)
	shareName := d.Get("share_name").(string)
	directoryName := d.Get("name").(string)

	metaDataRaw := d.Get("metadata").(map[string]interface{})
	metaData := ExpandMetaData(metaDataRaw)

	account, err := storageClient.FindAccount(ctx, accountName)
	if err != nil {
		return fmt.Errorf("retrieving Account %q for Directory %q (Share %q): %v", accountName, directoryName, shareName, err)
	}
	if account == nil {
		return fmt.Errorf("locating Storage Account %q", accountName)
	}

	accountId, err := accounts.ParseAccountID(accountName, storageClient.StorageDomainSuffix)
	if err != nil {
		return fmt.Errorf("parsing Account ID: %v", err)
	}

	id := directories.NewDirectoryID(*accountId, shareName, directoryName)

	client, err := storageClient.FileShareDirectoriesDataPlaneClient(ctx, *account)
	if err != nil {
		return fmt.Errorf("building File Share Directories Client: %v", err)
	}

	existing, err := client.Get(ctx, shareName, directoryName)
	if err != nil {
		if !response.WasNotFound(existing.HttpResponse) {
			return fmt.Errorf("checking for existing %s: %s", id, err)
		}
	}

	if !response.WasNotFound(existing.HttpResponse) {
		return tf.ImportAsExistsError("azurerm_storage_share_directory", id.ID())
	}

	input := directories.CreateDirectoryInput{
		MetaData: metaData,
	}
	if _, err = client.Create(ctx, shareName, directoryName, input); err != nil {
		return fmt.Errorf("creating %s: %v", id, err)
	}

	// Storage Share Directories are eventually consistent
	log.Printf("[DEBUG] Waiting for %s to become available", id)
	stateConf := &pluginsdk.StateChangeConf{
		Pending:                   []string{"404"},
		Target:                    []string{"200"},
		Refresh:                   storageShareDirectoryRefreshFunc(ctx, client, id),
		MinTimeout:                10 * time.Second,
		ContinuousTargetOccurence: 5,
		Timeout:                   d.Timeout(pluginsdk.TimeoutCreate),
	}

	if _, err = stateConf.WaitForStateContext(ctx); err != nil {
		return fmt.Errorf("waiting for %s to become available: %v", id, err)
	}

	d.SetId(id.ID())

	return resourceStorageShareDirectoryRead(d, meta)
}

func resourceStorageShareDirectoryUpdate(d *pluginsdk.ResourceData, meta interface{}) error {
	ctx, cancel := timeouts.ForUpdate(meta.(*clients.Client).StopContext, d)
	defer cancel()
	storageClient := meta.(*clients.Client).Storage

	id, err := directories.ParseDirectoryID(d.Id(), storageClient.StorageDomainSuffix)
	if err != nil {
		return err
	}

	metaDataRaw := d.Get("metadata").(map[string]interface{})
	metaData := ExpandMetaData(metaDataRaw)

	account, err := storageClient.FindAccount(ctx, id.AccountId.AccountName)
	if err != nil {
		return fmt.Errorf("retrieving Account %q for Directory %q (Share %q): %v", id.AccountId.AccountName, id.DirectoryPath, id.ShareName, err)
	}
	if account == nil {
		return fmt.Errorf("Unable to locate Storage Account %q!", id.AccountId.AccountName)
	}

	client, err := storageClient.FileShareDirectoriesDataPlaneClient(ctx, *account)
	if err != nil {
		return fmt.Errorf("building File Share Client: %v", err)
	}

	if _, err = client.SetMetaData(ctx, id.ShareName, id.DirectoryPath, directories.SetMetaDataInput{MetaData: metaData}); err != nil {
		return fmt.Errorf("updating Metadata for %s: %v", id, err)
	}

	return resourceStorageShareDirectoryRead(d, meta)
}

func resourceStorageShareDirectoryRead(d *pluginsdk.ResourceData, meta interface{}) error {
	ctx, cancel := timeouts.ForRead(meta.(*clients.Client).StopContext, d)
	defer cancel()
	storageClient := meta.(*clients.Client).Storage

	id, err := directories.ParseDirectoryID(d.Id(), storageClient.StorageDomainSuffix)
	if err != nil {
		return err
	}

	account, err := storageClient.FindAccount(ctx, id.AccountId.AccountName)
	if err != nil {
		return fmt.Errorf("retrieving Account %q for Directory %q (Share %q): %v", id.AccountId.AccountName, id.DirectoryPath, id.ShareName, err)
	}
	if account == nil {
		log.Printf("[WARN] Unable to determine Resource Group for Storage Share Directory %q (Share %s, Account %s) - assuming removed & removing from state", id.DirectoryPath, id.ShareName, id.AccountId.AccountName)
		d.SetId("")
		return nil
	}

	client, err := storageClient.FileShareDirectoriesDataPlaneClient(ctx, *account)
	if err != nil {
		return fmt.Errorf("building File Share Client: %v", err)
	}

	props, err := client.Get(ctx, id.ShareName, id.DirectoryPath)
	if err != nil {
		return fmt.Errorf("retrieving %s: %v", id, err)
	}

	d.Set("name", id.DirectoryPath)
	d.Set("share_name", id.ShareName)
	d.Set("storage_account_name", id.AccountId.AccountName)

	if err = d.Set("metadata", FlattenMetaData(props.MetaData)); err != nil {
		return fmt.Errorf("setting `metadata`: %v", err)
	}

	return nil
}

func resourceStorageShareDirectoryDelete(d *pluginsdk.ResourceData, meta interface{}) error {
	ctx, cancel := timeouts.ForDelete(meta.(*clients.Client).StopContext, d)
	defer cancel()
	storageClient := meta.(*clients.Client).Storage

	id, err := directories.ParseDirectoryID(d.Id(), storageClient.StorageDomainSuffix)
	if err != nil {
		return err
	}

	account, err := storageClient.FindAccount(ctx, id.AccountId.AccountName)
	if err != nil {
		return fmt.Errorf("retrieving Account %q for Directory %q (Share %q): %v", id.AccountId.AccountName, id.DirectoryPath, id.ShareName, err)
	}
	if account == nil {
		return fmt.Errorf("locating Storage Account %q", id.AccountId.AccountName)
	}

	client, err := storageClient.FileShareDirectoriesDataPlaneClient(ctx, *account)
	if err != nil {
		return fmt.Errorf("building File Share Client: %v", err)
	}

	if _, err = client.Delete(ctx, id.ShareName, id.DirectoryPath); err != nil {
		return fmt.Errorf("deleting %s: %v", id, err)
	}

	return nil
}

func storageShareDirectoryRefreshFunc(ctx context.Context, client *directories.Client, id directories.DirectoryId) pluginsdk.StateRefreshFunc {
	return func() (interface{}, string, error) {
		res, err := client.Get(ctx, id.ShareName, id.DirectoryPath)
		if err != nil {
			return nil, strconv.Itoa(res.HttpResponse.StatusCode), fmt.Errorf("retrieving %s: %v", id, err)
		}

		return res, strconv.Itoa(res.HttpResponse.StatusCode), nil
	}
}
