// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package storage

import (
	"fmt"
	"log"
	"time"

	"github.com/hashicorp/terraform-provider-azurerm/helpers/tf"
	"github.com/hashicorp/terraform-provider-azurerm/internal/clients"
	"github.com/hashicorp/terraform-provider-azurerm/internal/services/storage/migration"
	"github.com/hashicorp/terraform-provider-azurerm/internal/services/storage/parse"
	"github.com/hashicorp/terraform-provider-azurerm/internal/services/storage/validate"
	"github.com/hashicorp/terraform-provider-azurerm/internal/tf/pluginsdk"
	"github.com/hashicorp/terraform-provider-azurerm/internal/timeouts"
)

func resourceStorageQueue() *pluginsdk.Resource {
	return &pluginsdk.Resource{
		Create: resourceStorageQueueCreate,
		Read:   resourceStorageQueueRead,
		Update: resourceStorageQueueUpdate,
		Delete: resourceStorageQueueDelete,

		Importer: pluginsdk.ImporterValidatingResourceId(func(id string) error {
			_, err := parse.StorageQueueDataPlaneID(id, "") // TODO: actual domain suffix needed here!
			return err
		}),

		SchemaVersion: 1,
		StateUpgraders: pluginsdk.StateUpgrades(map[int]pluginsdk.StateUpgrade{
			0: migration.QueueV0ToV1{},
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
				ValidateFunc: validate.StorageQueueName,
			},

			"storage_account_name": {
				Type:         pluginsdk.TypeString,
				Required:     true,
				ForceNew:     true,
				ValidateFunc: validate.StorageAccountName,
			},

			"metadata": MetaDataSchema(),

			"resource_manager_id": {
				Type:     pluginsdk.TypeString,
				Computed: true,
			},
		},
	}
}

func resourceStorageQueueCreate(d *pluginsdk.ResourceData, meta interface{}) error {
	storageClient := meta.(*clients.Client).Storage
	ctx, cancel := timeouts.ForCreate(meta.(*clients.Client).StopContext, d)
	defer cancel()

	queueName := d.Get("name").(string)
	accountName := d.Get("storage_account_name").(string)

	metaDataRaw := d.Get("metadata").(map[string]interface{})
	metaData := ExpandMetaData(metaDataRaw)

	account, err := storageClient.FindAccount(ctx, accountName)
	if err != nil {
		return fmt.Errorf("retrieving Account %q for Queue %q: %v", accountName, queueName, err)
	}
	if account == nil {
		return fmt.Errorf("locating Storage Account %q", accountName)
	}

	client, err := storageClient.QueuesClient(ctx, *account)
	if err != nil {
		return fmt.Errorf("building Queues Client: %v", err)
	}

	id := parse.NewStorageQueueDataPlaneId(accountName, storageClient.AzureEnvironment.StorageEndpointSuffix, queueName).ID()

	exists, err := client.Exists(ctx, queueName)
	if err != nil {
		return fmt.Errorf("checking for presence of existing %s: %v", id, err)
	}
	if exists != nil && *exists {
		return tf.ImportAsExistsError("azurerm_storage_queue", id)
	}

	if err = client.Create(ctx, queueName, metaData); err != nil {
		return fmt.Errorf("creating %s: %+v", id, err)
	}

	d.SetId(id)

	return resourceStorageQueueRead(d, meta)
}

func resourceStorageQueueUpdate(d *pluginsdk.ResourceData, meta interface{}) error {
	storageClient := meta.(*clients.Client).Storage
	ctx, cancel := timeouts.ForUpdate(meta.(*clients.Client).StopContext, d)
	defer cancel()

	id, err := parse.StorageQueueDataPlaneID(d.Id(), storageClient.StorageDomainSuffix)
	if err != nil {
		return err
	}

	metaDataRaw := d.Get("metadata").(map[string]interface{})
	metaData := ExpandMetaData(metaDataRaw)

	account, err := storageClient.FindAccount(ctx, id.AccountName)
	if err != nil {
		return fmt.Errorf("retrieving Account %q for Queue %q: %v", id.AccountName, id.Name, err)
	}
	if account == nil {
		return fmt.Errorf("locating Storage Account %q", id.AccountName)
	}

	client, err := storageClient.QueuesClient(ctx, *account)
	if err != nil {
		return fmt.Errorf("building Queues Client: %v", err)
	}

	if err = client.UpdateMetaData(ctx, id.Name, metaData); err != nil {
		return fmt.Errorf("updating MetaData for %s: %v", id, err)
	}

	return resourceStorageQueueRead(d, meta)
}

func resourceStorageQueueRead(d *pluginsdk.ResourceData, meta interface{}) error {
	storageClient := meta.(*clients.Client).Storage
	subscriptionId := meta.(*clients.Client).Account.SubscriptionId
	ctx, cancel := timeouts.ForRead(meta.(*clients.Client).StopContext, d)
	defer cancel()

	id, err := parse.StorageQueueDataPlaneID(d.Id(), storageClient.StorageDomainSuffix)
	if err != nil {
		return err
	}

	account, err := storageClient.FindAccount(ctx, id.AccountName)
	if err != nil {
		return fmt.Errorf("retrieving Account %q for Queue %q: %v", id.AccountName, id.Name, err)
	}
	if account == nil {
		log.Printf("[WARN] Unable to determine Resource Group for Storage Queue %q (Account %s) - assuming removed & removing from state", id.Name, id.AccountName)
		d.SetId("")
		return nil
	}

	client, err := storageClient.QueuesClient(ctx, *account)
	if err != nil {
		return fmt.Errorf("building Queues Client: %v", err)
	}

	queue, err := client.Get(ctx, id.Name)
	if err != nil {
		return fmt.Errorf("retrieving %s: %v", id, err)
	}
	if queue == nil {
		log.Printf("[INFO] Storage Queue %q no longer exists, removing from state...", id.Name)
		d.SetId("")
		return nil
	}

	d.Set("name", id.Name)
	d.Set("storage_account_name", id.AccountName)

	if err := d.Set("metadata", FlattenMetaData(queue.MetaData)); err != nil {
		return fmt.Errorf("setting `metadata`: %s", err)
	}

	resourceManagerId := parse.NewStorageQueueResourceManagerID(subscriptionId, account.ResourceGroup, id.AccountName, "default", id.Name)
	d.Set("resource_manager_id", resourceManagerId.ID())

	return nil
}

func resourceStorageQueueDelete(d *pluginsdk.ResourceData, meta interface{}) error {
	storageClient := meta.(*clients.Client).Storage
	ctx, cancel := timeouts.ForDelete(meta.(*clients.Client).StopContext, d)
	defer cancel()

	id, err := parse.StorageQueueDataPlaneID(d.Id(), storageClient.StorageDomainSuffix)
	if err != nil {
		return err
	}

	account, err := storageClient.FindAccount(ctx, id.AccountName)
	if err != nil {
		return fmt.Errorf("retrieving Account %q for Queue %q: %s", id.AccountName, id.Name, err)
	}
	if account == nil {
		log.Printf("[WARN] Unable to determine Resource Group for Storage Queue %q (Account %s) - assuming removed & removing from state", id.Name, id.AccountName)
		d.SetId("")
		return nil
	}

	client, err := storageClient.QueuesClient(ctx, *account)
	if err != nil {
		return fmt.Errorf("building Queues Client: %v", err)
	}

	if err = client.Delete(ctx, id.Name); err != nil {
		return fmt.Errorf("deleting %s: %v", id, err)
	}

	return nil
}
