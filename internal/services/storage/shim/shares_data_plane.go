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
	"github.com/tombuildsstuff/giovanni/storage/2023-11-03/file/shares"
)

type DataPlaneStorageShareWrapper struct {
	client *shares.Client
}

func NewDataPlaneStorageShareWrapper(client *shares.Client) StorageShareWrapper {
	return DataPlaneStorageShareWrapper{
		client: client,
	}
}

func (w DataPlaneStorageShareWrapper) Create(ctx context.Context, shareName string, input shares.CreateInput) error {
	timeout, ok := ctx.Deadline()
	if !ok {
		return fmt.Errorf("context is missing a timeout")
	}

	resp, err := w.client.Create(ctx, shareName, input)
	if err == nil {
		return nil
	}

	// If we fail due to previous delete still in progress, then we can retry
	if response.WasConflict(resp.HttpResponse) && strings.Contains(err.Error(), "ShareBeingDeleted") {
		stateConf := &pluginsdk.StateChangeConf{
			Pending:        []string{"waitingOnDelete"},
			Target:         []string{"succeeded"},
			Refresh:        w.createRefreshFunc(ctx, shareName, input),
			PollInterval:   10 * time.Second,
			NotFoundChecks: 180,
			Timeout:        time.Until(timeout),
		}

		_, err := stateConf.WaitForStateContext(ctx)
		return err
	}

	// otherwise it's a legit error, so raise it
	return err
}

func (w DataPlaneStorageShareWrapper) Delete(ctx context.Context, shareName string) error {
	input := shares.DeleteInput{
		DeleteSnapshots: true,
	}
	_, err := w.client.Delete(ctx, shareName, input)
	return err
}

func (w DataPlaneStorageShareWrapper) Exists(ctx context.Context, shareName string) (*bool, error) {
	existing, err := w.client.GetProperties(ctx, shareName)
	if err != nil {
		if response.WasNotFound(existing.HttpResponse) {
			return pointer.To(false), nil
		}
		return nil, err
	}
	return pointer.To(true), nil
}

func (w DataPlaneStorageShareWrapper) Get(ctx context.Context, shareName string) (*StorageShareProperties, error) {
	props, err := w.client.GetProperties(ctx, shareName)
	if err != nil {
		if response.WasNotFound(props.HttpResponse) {
			return nil, nil
		}

		return nil, err
	}

	acls, err := w.client.GetACL(ctx, shareName)
	if err != nil {
		return nil, err
	}

	return &StorageShareProperties{
		MetaData:        props.MetaData,
		QuotaGB:         props.QuotaInGB,
		ACLs:            acls.SignedIdentifiers,
		EnabledProtocol: props.EnabledProtocol,
		AccessTier:      props.AccessTier,
	}, nil
}

func (w DataPlaneStorageShareWrapper) UpdateACLs(ctx context.Context, shareName string, input shares.SetAclInput) error {
	_, err := w.client.SetACL(ctx, shareName, input)
	return err
}

func (w DataPlaneStorageShareWrapper) UpdateMetaData(ctx context.Context, shareName string, metaData map[string]string) error {
	input := shares.SetMetaDataInput{
		MetaData: metaData,
	}
	_, err := w.client.SetMetaData(ctx, shareName, input)
	return err
}

func (w DataPlaneStorageShareWrapper) UpdateQuota(ctx context.Context, shareName string, quotaGB int) error {
	_, err := w.client.SetProperties(ctx, shareName, shares.ShareProperties{
		QuotaInGb: &quotaGB,
	})
	return err
}

func (w DataPlaneStorageShareWrapper) UpdateTier(ctx context.Context, shareName string, tier shares.AccessTier) error {
	props := shares.ShareProperties{
		AccessTier: &tier,
	}
	_, err := w.client.SetProperties(ctx, shareName, props)
	return err
}

func (w DataPlaneStorageShareWrapper) createRefreshFunc(ctx context.Context, shareName string, input shares.CreateInput) pluginsdk.StateRefreshFunc {
	return func() (interface{}, string, error) {
		resp, err := w.client.Create(ctx, shareName, input)
		if err != nil {
			if !response.WasConflict(resp.HttpResponse) {
				return nil, "", err
			}

			if response.WasConflict(resp.HttpResponse) && strings.Contains(err.Error(), "ShareBeingDeleted") {
				return nil, "waitingOnDelete", nil
			}
		}

		return "succeeded", "succeeded", nil
	}
}
