package virtualmachinescalesets

// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See NOTICE.txt in the project root for license information.

type VirtualMachineScaleSetUpdateIPConfigurationProperties struct {
	ApplicationGatewayBackendAddressPools *[]SubResource                                            `json:"applicationGatewayBackendAddressPools,omitempty"`
	ApplicationSecurityGroups             *[]SubResource                                            `json:"applicationSecurityGroups,omitempty"`
	LoadBalancerBackendAddressPools       *[]SubResource                                            `json:"loadBalancerBackendAddressPools,omitempty"`
	LoadBalancerInboundNatPools           *[]SubResource                                            `json:"loadBalancerInboundNatPools,omitempty"`
	Primary                               *bool                                                     `json:"primary,omitempty"`
	PrivateIPAddressVersion               *IPVersion                                                `json:"privateIPAddressVersion,omitempty"`
	PublicIPAddressConfiguration          *VirtualMachineScaleSetUpdatePublicIPAddressConfiguration `json:"publicIPAddressConfiguration,omitempty"`
	Subnet                                *ApiEntityReference                                       `json:"subnet,omitempty"`
}
