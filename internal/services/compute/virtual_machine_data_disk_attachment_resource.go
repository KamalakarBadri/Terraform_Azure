// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package compute

import (
	"fmt"
	"log"
	"time"

	"github.com/hashicorp/go-azure-helpers/lang/pointer"
	"github.com/hashicorp/go-azure-helpers/lang/response"
	"github.com/hashicorp/go-azure-sdk/resource-manager/compute/2022-03-02/disks"
	"github.com/hashicorp/go-azure-sdk/resource-manager/compute/2023-03-01/virtualmachines"
	"github.com/hashicorp/terraform-provider-azurerm/helpers/tf"
	"github.com/hashicorp/terraform-provider-azurerm/internal/clients"
	"github.com/hashicorp/terraform-provider-azurerm/internal/locks"
	"github.com/hashicorp/terraform-provider-azurerm/internal/services/compute/parse"
	"github.com/hashicorp/terraform-provider-azurerm/internal/services/compute/validate"
	"github.com/hashicorp/terraform-provider-azurerm/internal/tf/pluginsdk"
	"github.com/hashicorp/terraform-provider-azurerm/internal/tf/suppress"
	"github.com/hashicorp/terraform-provider-azurerm/internal/tf/validation"
	"github.com/hashicorp/terraform-provider-azurerm/internal/timeouts"
	"github.com/hashicorp/terraform-provider-azurerm/utils"
)

func resourceVirtualMachineDataDiskAttachment() *pluginsdk.Resource {
	return &pluginsdk.Resource{
		Create: resourceVirtualMachineDataDiskAttachmentCreateUpdate,
		Read:   resourceVirtualMachineDataDiskAttachmentRead,
		Update: resourceVirtualMachineDataDiskAttachmentCreateUpdate,
		Delete: resourceVirtualMachineDataDiskAttachmentDelete,
		Importer: pluginsdk.ImporterValidatingResourceId(func(id string) error {
			_, err := parse.DataDiskID(id)
			return err
		}),

		Timeouts: &pluginsdk.ResourceTimeout{
			Create: pluginsdk.DefaultTimeout(30 * time.Minute),
			Read:   pluginsdk.DefaultTimeout(5 * time.Minute),
			Update: pluginsdk.DefaultTimeout(30 * time.Minute),
			Delete: pluginsdk.DefaultTimeout(30 * time.Minute),
		},

		Schema: map[string]*pluginsdk.Schema{
			"managed_disk_id": {
				Type:             pluginsdk.TypeString,
				Required:         true,
				ForceNew:         true,
				DiffSuppressFunc: suppress.CaseDifference,
				ValidateFunc:     disks.ValidateDiskID,
			},

			"virtual_machine_id": {
				Type:         pluginsdk.TypeString,
				Required:     true,
				ForceNew:     true,
				ValidateFunc: validate.VirtualMachineID,
			},

			"lun": {
				Type:         pluginsdk.TypeInt,
				Required:     true,
				ForceNew:     true,
				ValidateFunc: validation.IntAtLeast(0),
			},

			"caching": {
				Type:     pluginsdk.TypeString,
				Required: true,
				ValidateFunc: validation.StringInSlice([]string{
					string(virtualmachines.CachingTypesNone),
					string(virtualmachines.CachingTypesReadOnly),
					string(virtualmachines.CachingTypesReadWrite),
				}, false),
			},

			"create_option": {
				Type:     pluginsdk.TypeString,
				Optional: true,
				ForceNew: true,
				Default:  string(virtualmachines.DiskCreateOptionTypesAttach),
				ValidateFunc: validation.StringInSlice([]string{
					string(virtualmachines.DiskCreateOptionTypesAttach),
					string(virtualmachines.DiskCreateOptionTypesEmpty),
				}, false),
			},

			"write_accelerator_enabled": {
				Type:     pluginsdk.TypeBool,
				Optional: true,
				Default:  false,
			},
		},
	}
}

func resourceVirtualMachineDataDiskAttachmentCreateUpdate(d *pluginsdk.ResourceData, meta interface{}) error {
	client := meta.(*clients.Client).Compute.VirtualMachinesClient
	ctx, cancel := timeouts.ForCreateUpdate(meta.(*clients.Client).StopContext, d)
	defer cancel()

	parsedVirtualMachineId, err := virtualmachines.ParseVirtualMachineID(d.Get("virtual_machine_id").(string))
	if err != nil {
		return fmt.Errorf("parsing Virtual Machine ID %q: %+v", parsedVirtualMachineId.ID(), err)
	}

	locks.ByName(parsedVirtualMachineId.VirtualMachineName, VirtualMachineResourceName)
	defer locks.UnlockByName(parsedVirtualMachineId.VirtualMachineName, VirtualMachineResourceName)

	virtualMachine, err := client.Get(ctx, *parsedVirtualMachineId, virtualmachines.DefaultGetOperationOptions())
	if err != nil {
		if response.WasNotFound(virtualMachine.HttpResponse) {
			return fmt.Errorf("Virtual Machine %q  was not found", parsedVirtualMachineId.String())
		}

		return fmt.Errorf("loading Virtual Machine %q : %+v", parsedVirtualMachineId.String(), err)
	}

	if virtualMachine.Model != nil && virtualMachine.Model.Properties != nil {
		return fmt.Errorf("reading Virtual Machine %q : `model/properties` was nil", parsedVirtualMachineId.String())
	}

	managedDiskId := d.Get("managed_disk_id").(string)
	managedDisk, err := retrieveDataDiskAttachmentManagedDisk(d, meta, managedDiskId)
	if err != nil {
		return fmt.Errorf("retrieving Managed Disk %q: %+v", managedDiskId, err)
	}

	if managedDisk.Sku == nil {
		return fmt.Errorf("Error: unable to determine Storage Account Type for Managed Disk %q: %+v", managedDiskId, err)
	}

	name := *managedDisk.Name
	resourceId := fmt.Sprintf("%s/dataDisks/%s", parsedVirtualMachineId.ID(), name)
	lun := int64(d.Get("lun").(int))
	caching := d.Get("caching").(string)
	createOption := virtualmachines.DiskCreateOptionTypes(d.Get("create_option").(string))
	writeAcceleratorEnabled := d.Get("write_accelerator_enabled").(bool)

	expandedDisk := virtualmachines.DataDisk{
		Name:         utils.String(name),
		Caching:      pointer.To(virtualmachines.CachingTypes(caching)),
		CreateOption: createOption,
		Lun:          lun,
		ManagedDisk: &virtualmachines.ManagedDiskParameters{
			Id:                 utils.String(managedDiskId),
			StorageAccountType: pointer.To(virtualmachines.StorageAccountTypes(pointer.From(managedDisk.Sku.Name))),
		},
		WriteAcceleratorEnabled: utils.Bool(writeAcceleratorEnabled),
	}

	disks := *virtualMachine.Model.Properties.StorageProfile.DataDisks
	existingIndex := -1
	for i, disk := range disks {
		if *disk.Name == name {
			existingIndex = i
			break
		}
	}

	if d.IsNewResource() {
		if existingIndex != -1 {
			return tf.ImportAsExistsError("azurerm_virtual_machine_data_disk_attachment", resourceId)
		}

		disks = append(disks, expandedDisk)
	} else {
		if existingIndex == -1 {
			return fmt.Errorf("Unable to find Disk %q attached to Virtual Machine %q ", name, parsedVirtualMachineId.String())
		}

		disks[existingIndex] = expandedDisk
	}

	virtualMachine.Model.Properties.StorageProfile.DataDisks = &disks

	// fixes #2485
	virtualMachine.Model.Identity = nil
	// fixes #1600
	virtualMachine.Model.Resources = nil

	// if there's too many disks we get a 409 back with:
	//   `The maximum number of data disks allowed to be attached to a VM of this size is 1.`
	// which we're intentionally not wrapping, since the errors good.
	if err = client.CreateOrUpdateThenPoll(ctx, *parsedVirtualMachineId, *virtualMachine.Model); err != nil {
		return fmt.Errorf("updating Virtual Machine %q  with Disk %q: %+v", parsedVirtualMachineId.String(), name, err)
	}

	d.SetId(resourceId)
	return resourceVirtualMachineDataDiskAttachmentRead(d, meta)
}

func resourceVirtualMachineDataDiskAttachmentRead(d *pluginsdk.ResourceData, meta interface{}) error {
	client := meta.(*clients.Client).Compute.VirtualMachinesClient
	ctx, cancel := timeouts.ForRead(meta.(*clients.Client).StopContext, d)
	defer cancel()

	id, err := parse.DataDiskID(d.Id())
	if err != nil {
		return err
	}

	vmId := virtualmachines.NewVirtualMachineID(id.SubscriptionId, id.ResourceGroup, id.VirtualMachineName)

	virtualMachine, err := client.Get(ctx, vmId, virtualmachines.DefaultGetOperationOptions())
	if err != nil {
		if response.WasNotFound(virtualMachine.HttpResponse) {
			log.Printf("[DEBUG] Virtual Machine %q was not found (Resource Group %q) therefore Data Disk Attachment cannot exist - removing from state", id.VirtualMachineName, id.ResourceGroup)
			d.SetId("")
			return nil
		}

		return fmt.Errorf("loading Virtual Machine %q : %+v", id.String(), err)
	}

	var disk *virtualmachines.DataDisk
	if model := virtualMachine.Model; model != nil {
		if props := model.Properties; props != nil {
			if profile := props.StorageProfile; profile != nil {
				if dataDisks := profile.DataDisks; dataDisks != nil {
					for _, dataDisk := range *dataDisks {
						// since this field isn't (and shouldn't be) case-sensitive; we're deliberately not using `strings.EqualFold`
						if *dataDisk.Name == id.Name {
							disk = &dataDisk
							break
						}
					}
				}
			}
		}
	}

	if disk == nil {
		log.Printf("[DEBUG] Data Disk %q was not found on Virtual Machine %q  - removing from state", id.Name, id.String())
		d.SetId("")
		return nil
	}

	d.Set("virtual_machine_id", vmId.ID())
	d.Set("caching", string(pointer.From(disk.Caching)))
	d.Set("create_option", string(disk.CreateOption))
	d.Set("write_accelerator_enabled", disk.WriteAcceleratorEnabled)
	d.Set("lun", int(disk.Lun))

	if managedDisk := disk.ManagedDisk; managedDisk != nil {
		d.Set("managed_disk_id", managedDisk.Id)
	}

	return nil
}

func resourceVirtualMachineDataDiskAttachmentDelete(d *pluginsdk.ResourceData, meta interface{}) error {
	client := meta.(*clients.Client).Compute.VirtualMachinesClient
	ctx, cancel := timeouts.ForDelete(meta.(*clients.Client).StopContext, d)
	defer cancel()

	id, err := parse.DataDiskID(d.Id())
	if err != nil {
		return err
	}

	vmId := virtualmachines.NewVirtualMachineID(id.SubscriptionId, id.ResourceGroup, id.VirtualMachineName)

	locks.ByName(id.VirtualMachineName, VirtualMachineResourceName)
	defer locks.UnlockByName(id.VirtualMachineName, VirtualMachineResourceName)

	virtualMachine, err := client.Get(ctx, vmId, virtualmachines.DefaultGetOperationOptions())
	if err != nil {
		if response.WasNotFound(virtualMachine.HttpResponse) {
			return fmt.Errorf("Virtual Machine %q was not found", id.String())
		}

		return fmt.Errorf("loading Virtual Machine %q : %+v", id.String(), err)
	}

	if virtualMachine.Model == nil || virtualMachine.Model.Properties == nil {
		return fmt.Errorf("reading Virtual Machine %q : `model/properties` was nil", vmId)
	}

	dataDisks := make([]virtualmachines.DataDisk, 0)
	for _, dataDisk := range *virtualMachine.Model.Properties.StorageProfile.DataDisks {
		// since this field isn't (and shouldn't be) case-sensitive; we're deliberately not using `strings.EqualFold`
		if *dataDisk.Name != id.Name {
			dataDisks = append(dataDisks, dataDisk)
		}
	}

	virtualMachine.Model.Properties.StorageProfile.DataDisks = &dataDisks

	// fixes #2485
	virtualMachine.Model.Identity = nil
	// fixes #1600
	virtualMachine.Model.Resources = nil

	if err = client.CreateOrUpdateThenPoll(ctx, vmId, *virtualMachine.Model); err != nil {
		return fmt.Errorf("removing Disk %q from Virtual Machine %q : %+v", id.Name, id.String(), err)
	}

	return nil
}

func retrieveDataDiskAttachmentManagedDisk(d *pluginsdk.ResourceData, meta interface{}, id string) (*disks.Disk, error) {
	client := meta.(*clients.Client).Compute.DisksClient
	ctx, cancel := timeouts.ForRead(meta.(*clients.Client).StopContext, d)
	defer cancel()

	parsedId, err := disks.ParseDiskID(id)
	if err != nil {
		return nil, fmt.Errorf("parsing Managed Disk ID %q: %+v", id, err)
	}

	resp, err := client.Get(ctx, *parsedId)
	if err != nil {
		if response.WasNotFound(resp.HttpResponse) {
			return nil, fmt.Errorf("Managed Disk %q  was not found!", parsedId.String())
		}

		return nil, fmt.Errorf("making Read request on Azure Managed Disk %q : %+v", parsedId.String(), err)
	}

	return resp.Model, nil
}
