// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package shim

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/hashicorp/go-azure-helpers/lang/pointer"
	"github.com/hashicorp/go-azure-helpers/lang/response"
	"github.com/hashicorp/terraform-provider-azurerm/internal/tf/pluginsdk"
	"github.com/tombuildsstuff/giovanni/storage/2023-11-03/blob/containers"
)

type DataPlaneStorageContainerWrapper struct {
	client *containers.Client
}

func NewDataPlaneStorageContainerWrapper(client *containers.Client) StorageContainerWrapper {
	return DataPlaneStorageContainerWrapper{
		client: client,
	}
}

func (w DataPlaneStorageContainerWrapper) Create(ctx context.Context, containerName string, input containers.CreateInput) error {
	timeout, ok := ctx.Deadline()
	if !ok {
		return fmt.Errorf("context is missing a timeout")
	}

	if resp, err := w.client.Create(ctx, containerName, input); err != nil {
		// If we fail due to previous delete still in progress, then we can retry
		if response.WasConflict(resp.HttpResponse) && strings.Contains(err.Error(), "ContainerBeingDeleted") {
			stateConf := &pluginsdk.StateChangeConf{
				Pending:        []string{"waitingOnDelete"},
				Target:         []string{"succeeded"},
				Refresh:        w.createRefreshFunc(ctx, containerName, input),
				PollInterval:   10 * time.Second,
				NotFoundChecks: 180,
				Timeout:        time.Until(timeout),
			}

			if _, err := stateConf.WaitForStateContext(ctx); err != nil {
				return fmt.Errorf("failed creating container: %+v", err)
			}
		} else {
			return fmt.Errorf("failed creating container: %+v", err)
		}
	}
	return nil
}

func (w DataPlaneStorageContainerWrapper) Delete(ctx context.Context, containerName string) error {
	resp, err := w.client.Delete(ctx, containerName)
	if response.WasNotFound(resp.HttpResponse) {
		return nil
	}

	return err
}

func (w DataPlaneStorageContainerWrapper) Exists(ctx context.Context, containerName string) (*bool, error) {
	existing, err := w.client.GetProperties(ctx, containerName, containers.GetPropertiesInput{})
	if err != nil {
		if response.WasNotFound(existing.HttpResponse) {
			return pointer.To(false), nil
		}
		return nil, err
	}
	return pointer.To(true), nil
}

func (w DataPlaneStorageContainerWrapper) Get(ctx context.Context, containerName string) (*StorageContainerProperties, error) {
	props, err := w.client.GetProperties(ctx, containerName, containers.GetPropertiesInput{})
	if err != nil {
		if response.WasNotFound(props.HttpResponse) {
			return nil, nil
		}

		return nil, err
	}

	return &StorageContainerProperties{
		AccessLevel:           props.AccessLevel,
		MetaData:              props.MetaData,
		HasImmutabilityPolicy: props.HasImmutabilityPolicy,
		HasLegalHold:          props.HasLegalHold,
	}, nil
}

func (w DataPlaneStorageContainerWrapper) UpdateAccessLevel(ctx context.Context, containerName string, level containers.AccessLevel) error {
	input := containers.SetAccessControlInput{
		AccessLevel: level,
	}
	_, err := w.client.SetAccessControl(ctx, containerName, input)
	return err
}

func (w DataPlaneStorageContainerWrapper) UpdateMetaData(ctx context.Context, containerName string, metaData map[string]string) error {
	input := containers.SetMetaDataInput{
		MetaData: metaData,
	}
	_, err := w.client.SetMetaData(ctx, containerName, input)
	return err
}

func (w DataPlaneStorageContainerWrapper) createRefreshFunc(ctx context.Context, containerName string, input containers.CreateInput) pluginsdk.StateRefreshFunc {
	return func() (interface{}, string, error) {
		resp, err := w.client.Create(ctx, containerName, input)
		if err != nil {
			if !response.WasConflict(resp.HttpResponse) {
				return nil, "", err
			}

			if response.WasConflict(resp.HttpResponse) && strings.Contains(err.Error(), "ContainerBeingDeleted") {
				return nil, "waitingOnDelete", nil
			}
		}

		return "succeeded", "succeeded", nil
	}
}
