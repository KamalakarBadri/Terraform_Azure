// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package shim

import (
	"context"
	"net/http"

	"github.com/hashicorp/terraform-provider-azurerm/utils"
	"github.com/tombuildsstuff/giovanni/storage/2023-11-03/queue/queues"
)

type DataPlaneStorageQueueWrapper struct {
	client *queues.Client
}

func NewDataPlaneStorageQueueWrapper(client *queues.Client) StorageQueuesWrapper {
	return DataPlaneStorageQueueWrapper{
		client: client,
	}
}

func (w DataPlaneStorageQueueWrapper) Create(ctx context.Context, queueName string, metaData map[string]string) error {
	input := queues.CreateInput{
		MetaData: metaData,
	}
	_, err := w.client.Create(ctx, queueName, input)
	return err
}

func (w DataPlaneStorageQueueWrapper) Delete(ctx context.Context, queueName string) error {
	_, err := w.client.Delete(ctx, queueName)
	return err
}

func (w DataPlaneStorageQueueWrapper) Exists(ctx context.Context, queueName string) (*bool, error) {
	existing, err := w.client.GetMetaData(ctx, queueName)
	if err != nil {
		if existing.HttpResponse.StatusCode == http.StatusNotFound {
			return utils.Bool(false), nil
		}
		return nil, err
	}

	return utils.Bool(true), nil
}

func (w DataPlaneStorageQueueWrapper) Get(ctx context.Context, queueName string) (*StorageQueueProperties, error) {
	props, err := w.client.GetMetaData(ctx, queueName)
	if err != nil {
		if props.HttpResponse.StatusCode == http.StatusNotFound {
			return nil, nil
		}
		return nil, err
	}

	return &StorageQueueProperties{
		MetaData: props.MetaData,
	}, nil
}

func (w DataPlaneStorageQueueWrapper) GetServiceProperties(ctx context.Context) (*queues.StorageServiceProperties, error) {
	serviceProps, err := w.client.GetServiceProperties(ctx)
	if err != nil {
		if serviceProps.HttpResponse.StatusCode == http.StatusNotFound {
			return nil, nil
		}
		return nil, err
	}

	return &serviceProps.StorageServiceProperties, nil
}

func (w DataPlaneStorageQueueWrapper) UpdateMetaData(ctx context.Context, queueName string, metaData map[string]string) error {
	input := queues.SetMetaDataInput{
		MetaData: metaData,
	}
	_, err := w.client.SetMetaData(ctx, queueName, input)
	return err
}

func (w DataPlaneStorageQueueWrapper) UpdateServiceProperties(ctx context.Context, properties queues.StorageServiceProperties) error {
	input := queues.SetStorageServicePropertiesInput{
		Properties: properties,
	}
	_, err := w.client.SetServiceProperties(ctx, input)
	return err
}
