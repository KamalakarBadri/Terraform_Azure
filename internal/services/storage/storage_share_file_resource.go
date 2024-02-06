// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package storage

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/hashicorp/terraform-provider-azurerm/helpers/tf"
	"github.com/hashicorp/terraform-provider-azurerm/internal/clients"
	"github.com/hashicorp/terraform-provider-azurerm/internal/services/storage/parse"
	storageValidate "github.com/hashicorp/terraform-provider-azurerm/internal/services/storage/validate"
	"github.com/hashicorp/terraform-provider-azurerm/internal/tf/pluginsdk"
	"github.com/hashicorp/terraform-provider-azurerm/internal/tf/validation"
	"github.com/hashicorp/terraform-provider-azurerm/internal/timeouts"
	"github.com/hashicorp/terraform-provider-azurerm/utils"
	"github.com/tombuildsstuff/giovanni/storage/2023-11-03/blob/accounts"
	"github.com/tombuildsstuff/giovanni/storage/2023-11-03/file/files"
)

func resourceStorageShareFile() *pluginsdk.Resource {
	return &pluginsdk.Resource{
		Create: resourceStorageShareFileCreate,
		Read:   resourceStorageShareFileRead,
		Update: resourceStorageShareFileUpdate,
		Delete: resourceStorageShareFileDelete,

		Importer: pluginsdk.ImporterValidatingResourceId(func(id string) error {
			_, err := files.ParseFileID(id, "") // TODO: actual domain suffix needed here!
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
				Type:     pluginsdk.TypeString,
				Required: true,
				ForceNew: true,
			},
			"storage_share_id": {
				Type:         pluginsdk.TypeString,
				Required:     true,
				ForceNew:     true,
				ValidateFunc: storageValidate.StorageShareID,
			},
			"path": {
				Type:         pluginsdk.TypeString,
				ForceNew:     true,
				Optional:     true,
				Default:      "",
				ValidateFunc: storageValidate.StorageShareDirectoryName,
			},

			"content_type": {
				Type:     pluginsdk.TypeString,
				Optional: true,
				Default:  "application/octet-stream",
			},

			"content_encoding": {
				Type:         pluginsdk.TypeString,
				Optional:     true,
				ValidateFunc: validation.StringIsNotEmpty,
			},

			"content_md5": {
				Type:         pluginsdk.TypeString,
				Optional:     true,
				ForceNew:     true,
				ValidateFunc: validation.StringIsNotEmpty,
			},

			"content_disposition": {
				Type:         pluginsdk.TypeString,
				Optional:     true,
				ValidateFunc: validation.StringIsNotEmpty,
			},

			"source": {
				Type:         pluginsdk.TypeString,
				Optional:     true,
				ValidateFunc: validation.StringIsNotEmpty,
				ForceNew:     true,
			},

			"content_length": {
				Type:     pluginsdk.TypeInt,
				Computed: true,
			},

			"metadata": MetaDataSchema(),
		},
	}
}

func resourceStorageShareFileCreate(d *pluginsdk.ResourceData, meta interface{}) error {
	ctx, cancel := timeouts.ForCreate(meta.(*clients.Client).StopContext, d)
	defer cancel()
	storageClient := meta.(*clients.Client).Storage

	storageShareId, err := parse.StorageShareDataPlaneID(d.Get("storage_share_id").(string), storageClient.StorageDomainSuffix)
	if err != nil {
		return err
	}

	fileName := d.Get("name").(string)
	path := d.Get("path").(string)

	account, err := storageClient.FindAccount(ctx, storageShareId.AccountName)
	if err != nil {
		return fmt.Errorf("retrieving Account %q for File %q (Share %q): %v", storageShareId.AccountName, fileName, storageShareId.Name, err)
	}
	if account == nil {
		return fmt.Errorf("locating Storage Account %q", storageShareId.AccountName)
	}

	accountId, err := accounts.ParseAccountID(storageShareId.AccountName, storageClient.StorageDomainSuffix)
	if err != nil {
		return fmt.Errorf("parsing Account ID: %v", err)
	}

	id := files.NewFileID(*accountId, storageShareId.Name, path, fileName)

	fileSharesClient, err := storageClient.FileSharesClient(ctx, *account)
	if err != nil {
		return fmt.Errorf("building File Share Directories Client: %v", err)
	}

	share, err := fileSharesClient.Get(ctx, storageShareId.Name)
	if err != nil {
		return fmt.Errorf("retrieving Share %q for File %q: %v", storageShareId.Name, fileName, err)
	}
	if share == nil {
		return fmt.Errorf("unable to locate Storage Share %q", storageShareId.Name)
	}

	client, err := storageClient.FileShareFilesClient(ctx, *account)
	if err != nil {
		return fmt.Errorf("building File Share Directories Client: %s", err)
	}

	existing, err := client.GetProperties(ctx, storageShareId.Name, path, fileName)
	if err != nil {
		if existing.HttpResponse.StatusCode != http.StatusNotFound {
			return fmt.Errorf("checking for presence of existing File %q (File Share %q / Storage Account %q / Resource Group %q): %s", fileName, storageShareId.Name, storageShareId.AccountName, account.ResourceGroup, err)
		}
	}

	if existing.HttpResponse.StatusCode != http.StatusNotFound {
		return tf.ImportAsExistsError("azurerm_storage_share_file", id.ID())
	}

	input := files.CreateInput{
		MetaData:           ExpandMetaData(d.Get("metadata").(map[string]interface{})),
		ContentType:        utils.String(d.Get("content_type").(string)),
		ContentEncoding:    utils.String(d.Get("content_encoding").(string)),
		ContentDisposition: utils.String(d.Get("content_disposition").(string)),
	}

	if v, ok := d.GetOk("content_md5"); ok {
		input.ContentMD5 = utils.String(v.(string))
	}

	var file *os.File
	if v, ok := d.GetOk("source"); ok {
		file, err = os.Open(v.(string))
		if err != nil {
			return fmt.Errorf("opening file: %s", err)
		}

		info, err := file.Stat()
		if err != nil {
			return fmt.Errorf("'stat'-ing File %q (File Share %q / Account %q): %v", fileName, storageShareId.Name, storageShareId.AccountName, err)
		}

		if info.Size() == 0 {
			return fmt.Errorf("file %q (File Share %q / Account %q) is empty", fileName, storageShareId.Name, storageShareId.AccountName)
		}

		input.ContentLength = info.Size()
	}

	if _, err = client.Create(ctx, storageShareId.Name, path, fileName, input); err != nil {
		return fmt.Errorf("creating File %q (File Share %q / Account %q): %v", fileName, storageShareId.Name, storageShareId.AccountName, err)
	}

	if file != nil {
		if err = client.PutFile(ctx, storageShareId.Name, path, fileName, file, 4); err != nil {
			return fmt.Errorf("uploading File: %q (File Share %q / Account %q): %v", fileName, storageShareId.Name, storageShareId.AccountName, err)
		}
	}

	d.SetId(id.ID())

	return resourceStorageShareFileRead(d, meta)
}

func resourceStorageShareFileUpdate(d *pluginsdk.ResourceData, meta interface{}) error {
	ctx, cancel := timeouts.ForUpdate(meta.(*clients.Client).StopContext, d)
	defer cancel()
	storageClient := meta.(*clients.Client).Storage

	id, err := files.ParseFileID(d.Id(), storageClient.StorageDomainSuffix)
	if err != nil {
		return err
	}

	account, err := storageClient.FindAccount(ctx, id.AccountId.AccountName)
	if err != nil {
		return fmt.Errorf("retrieving Account %q for %s: %v", id.AccountId.AccountName, id, err)
	}
	if account == nil {
		return fmt.Errorf("locating Storage Account %q", id.AccountId.AccountName)
	}

	fileSharesClient, err := storageClient.FileSharesClient(ctx, *account)
	if err != nil {
		return fmt.Errorf("building File Share Directories Client: %v", err)
	}

	share, err := fileSharesClient.Get(ctx, id.ShareName)
	if err != nil {
		return fmt.Errorf("retrieving %s: %v", id, err)
	}
	if share == nil {
		return fmt.Errorf("unable to locate Storage Share %q", id.ShareName)
	}

	client, err := storageClient.FileShareFilesClient(ctx, *account)
	if err != nil {
		return fmt.Errorf("building File Share Files Client: %v", err)
	}

	existing, err := client.GetProperties(ctx, id.ShareName, id.DirectoryPath, id.FileName)
	if err != nil {
		if existing.HttpResponse.StatusCode != http.StatusNotFound {
			return fmt.Errorf("checking for presence of existing %s: %v", id, err)
		}
	}

	if d.HasChange("content_type") || d.HasChange("content_encoding") || d.HasChange("content_disposition") {
		input := files.SetPropertiesInput{
			ContentType:        utils.String(d.Get("content_type").(string)),
			ContentEncoding:    utils.String(d.Get("content_encoding").(string)),
			ContentDisposition: utils.String(d.Get("content_disposition").(string)),
			ContentLength:      int64(d.Get("content_length").(int)),
			MetaData:           ExpandMetaData(d.Get("metadata").(map[string]interface{})),
		}

		if v, ok := d.GetOk("content_md5"); ok {
			input.ContentMD5 = utils.String(v.(string))
		}

		if _, err = client.SetProperties(ctx, id.ShareName, id.DirectoryPath, id.FileName, input); err != nil {
			return fmt.Errorf("creating %s: %v", id, err)
		}
	}

	return resourceStorageShareFileRead(d, meta)
}

func resourceStorageShareFileRead(d *pluginsdk.ResourceData, meta interface{}) error {
	ctx, cancel := timeouts.ForRead(meta.(*clients.Client).StopContext, d)
	defer cancel()
	storageClient := meta.(*clients.Client).Storage

	id, err := files.ParseFileID(d.Id(), storageClient.StorageDomainSuffix)
	if err != nil {
		return err
	}

	account, err := storageClient.FindAccount(ctx, id.AccountId.AccountName)
	if err != nil {
		return fmt.Errorf("retrieving Account %q for File %q (Share %q): %s", id.AccountId.AccountName, id.FileName, id.ShareName, err)
	}
	if account == nil {
		log.Printf("[WARN] Unable to determine Storage Account for %s - assuming removed & removing from state", id)
		d.SetId("")
		return nil
	}

	fileSharesClient, err := storageClient.FileSharesClient(ctx, *account)
	if err != nil {
		return fmt.Errorf("building File Share Directories Client: %s", err)
	}

	share, err := fileSharesClient.Get(ctx, id.ShareName)
	if err != nil {
		return fmt.Errorf("retrieving Share %q for File %q: %s", id.ShareName, id.FileName, err)
	}
	if share == nil {
		log.Printf("[WARN] Unable to determine Storage Share for %s - assuming removed & removing from state", id)
		d.SetId("")
		return nil
	}

	client, err := storageClient.FileShareFilesClient(ctx, *account)
	if err != nil {
		return fmt.Errorf("building File Share Client for Storage Account %s: %s", id.AccountId, err)
	}

	props, err := client.GetProperties(ctx, id.ShareName, id.DirectoryPath, id.FileName)
	if err != nil {
		log.Printf("retrieving %s: %s", id, err)
		d.SetId("")
		return nil
	}

	d.Set("name", id.FileName)
	d.Set("path", id.DirectoryPath)
	d.Set("storage_share_id", parse.NewStorageShareDataPlaneId(id.AccountId.AccountName, storageClient.AzureEnvironment.StorageEndpointSuffix, id.ShareName).ID())

	if err = d.Set("metadata", FlattenMetaData(props.MetaData)); err != nil {
		return fmt.Errorf("setting `metadata`: %s", err)
	}
	d.Set("content_type", props.ContentType)
	d.Set("content_encoding", props.ContentEncoding)
	d.Set("content_md5", props.ContentMD5)
	d.Set("content_disposition", props.ContentDisposition)

	if props.ContentLength == nil {
		return fmt.Errorf("file share file properties %q returned no information about the content-length", id.FileName)
	}

	d.Set("content_length", int(*props.ContentLength))

	return nil
}

func resourceStorageShareFileDelete(d *pluginsdk.ResourceData, meta interface{}) error {
	ctx, cancel := timeouts.ForDelete(meta.(*clients.Client).StopContext, d)
	defer cancel()
	storageClient := meta.(*clients.Client).Storage

	id, err := files.ParseFileID(d.Id(), storageClient.StorageDomainSuffix)
	if err != nil {
		return err
	}

	account, err := storageClient.FindAccount(ctx, id.AccountId.AccountName)
	if err != nil {
		return fmt.Errorf("retrieving Account %q for File %q (Share %q): %v", id.AccountId.AccountName, id.FileName, id.ShareName, err)
	}
	if account == nil {
		return fmt.Errorf("locating Storage Account %q", id.AccountId.AccountName)
	}

	client, err := storageClient.FileShareFilesClient(ctx, *account)
	if err != nil {
		return fmt.Errorf("building File Share File Client for Storage Account %q (Resource Group %q): %v", id.AccountId.AccountName, account.ResourceGroup, err)
	}

	if _, err = client.Delete(ctx, id.ShareName, id.DirectoryPath, id.FileName); err != nil {
		return fmt.Errorf("deleting %s: %v", id, err)
	}

	return nil
}
