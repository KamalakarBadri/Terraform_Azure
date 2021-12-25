package parse

// NOTE: this file is generated via 'go:generate' - manual changes will be overwritten

import (
	"testing"

	"github.com/hashicorp/go-azure-helpers/resourcemanager/resourceids"
)

var _ resourceids.Id = WorkspaceAADAdminId{}

func TestWorkspaceAADAdminIDFormatter(t *testing.T) {
	actual := NewWorkspaceAADAdminID("12345678-1234-9876-4563-123456789012", "resourceGroup1", "workspace1", "activeDirectory").ID()
	expected := "/subscriptions/12345678-1234-9876-4563-123456789012/resourceGroups/resourceGroup1/providers/Microsoft.Synapse/workspaces/workspace1/administrators/activeDirectory"
	if actual != expected {
		t.Fatalf("Expected %q but got %q", expected, actual)
	}
}

func TestWorkspaceAADAdminID(t *testing.T) {
	testData := []struct {
		Input    string
		Error    bool
		Expected *WorkspaceAADAdminId
	}{

		{
			// empty
			Input: "",
			Error: true,
		},

		{
			// missing SubscriptionId
			Input: "/",
			Error: true,
		},

		{
			// missing value for SubscriptionId
			Input: "/subscriptions/",
			Error: true,
		},

		{
			// missing ResourceGroup
			Input: "/subscriptions/12345678-1234-9876-4563-123456789012/",
			Error: true,
		},

		{
			// missing value for ResourceGroup
			Input: "/subscriptions/12345678-1234-9876-4563-123456789012/resourceGroups/",
			Error: true,
		},

		{
			// missing WorkspaceName
			Input: "/subscriptions/12345678-1234-9876-4563-123456789012/resourceGroups/resourceGroup1/providers/Microsoft.Synapse/",
			Error: true,
		},

		{
			// missing value for WorkspaceName
			Input: "/subscriptions/12345678-1234-9876-4563-123456789012/resourceGroups/resourceGroup1/providers/Microsoft.Synapse/workspaces/",
			Error: true,
		},

		{
			// missing AdministratorName
			Input: "/subscriptions/12345678-1234-9876-4563-123456789012/resourceGroups/resourceGroup1/providers/Microsoft.Synapse/workspaces/workspace1/",
			Error: true,
		},

		{
			// missing value for AdministratorName
			Input: "/subscriptions/12345678-1234-9876-4563-123456789012/resourceGroups/resourceGroup1/providers/Microsoft.Synapse/workspaces/workspace1/administrators/",
			Error: true,
		},

		{
			// valid
			Input: "/subscriptions/12345678-1234-9876-4563-123456789012/resourceGroups/resourceGroup1/providers/Microsoft.Synapse/workspaces/workspace1/administrators/activeDirectory",
			Expected: &WorkspaceAADAdminId{
				SubscriptionId:    "12345678-1234-9876-4563-123456789012",
				ResourceGroup:     "resourceGroup1",
				WorkspaceName:     "workspace1",
				AdministratorName: "activeDirectory",
			},
		},

		{
			// upper-cased
			Input: "/SUBSCRIPTIONS/12345678-1234-9876-4563-123456789012/RESOURCEGROUPS/RESOURCEGROUP1/PROVIDERS/MICROSOFT.SYNAPSE/WORKSPACES/WORKSPACE1/ADMINISTRATORS/ACTIVEDIRECTORY",
			Error: true,
		},
	}

	for _, v := range testData {
		t.Logf("[DEBUG] Testing %q", v.Input)

		actual, err := WorkspaceAADAdminID(v.Input)
		if err != nil {
			if v.Error {
				continue
			}

			t.Fatalf("Expect a value but got an error: %s", err)
		}
		if v.Error {
			t.Fatal("Expect an error but didn't get one")
		}

		if actual.SubscriptionId != v.Expected.SubscriptionId {
			t.Fatalf("Expected %q but got %q for SubscriptionId", v.Expected.SubscriptionId, actual.SubscriptionId)
		}
		if actual.ResourceGroup != v.Expected.ResourceGroup {
			t.Fatalf("Expected %q but got %q for ResourceGroup", v.Expected.ResourceGroup, actual.ResourceGroup)
		}
		if actual.WorkspaceName != v.Expected.WorkspaceName {
			t.Fatalf("Expected %q but got %q for WorkspaceName", v.Expected.WorkspaceName, actual.WorkspaceName)
		}
		if actual.AdministratorName != v.Expected.AdministratorName {
			t.Fatalf("Expected %q but got %q for AdministratorName", v.Expected.AdministratorName, actual.AdministratorName)
		}
	}
}
