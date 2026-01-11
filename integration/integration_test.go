package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"

	"github.com/tupyy/rvtools/models"
	"github.com/tupyy/rvtools/parser"

	_ "github.com/duckdb/duckdb-go/v2"
)

// MockValidator is a simple mock that returns concerns based on VM state.
// In production, you'd use the migration-planner OPA validator.
type MockValidator struct{}

func (v *MockValidator) Validate(ctx context.Context, vm models.VM) ([]models.Concern, error) {
	var concerns []models.Concern

	// Example validation rules similar to OPA policies
	if vm.PowerState != "poweredOn" {
		concerns = append(concerns, models.Concern{
			Id:         "vm-powered-off",
			Label:      "VM is not powered on",
			Category:   "Information",
			Assessment: "The VM is powered off and may need to be started before migration.",
		})
	}

	if vm.FaultToleranceEnabled {
		concerns = append(concerns, models.Concern{
			Id:         "fault-tolerance-enabled",
			Label:      "Fault tolerance is enabled",
			Category:   "Critical",
			Assessment: "VMs with fault tolerance enabled cannot be migrated.",
		})
	}

	if vm.ChangeTrackingEnabled == false && vm.PowerState == "poweredOn" {
		concerns = append(concerns, models.Concern{
			Id:         "cbt-disabled",
			Label:      "Changed Block Tracking is disabled",
			Category:   "Warning",
			Assessment: "CBT should be enabled for efficient migration.",
		})
	}

	return concerns, nil
}

func TestFullInventoryFlow(t *testing.T) {
	// Skip if no test file available
	rvtoolsFile := os.Getenv("RVTOOLS_FILE")
	if rvtoolsFile == "" {
		t.Skip("Set RVTOOLS_FILE environment variable to run this test")
	}

	ctx := context.Background()

	// Create in-memory DuckDB
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("Failed to open DuckDB: %v", err)
	}
	defer db.Close()

	// Create parser with mock validator
	p := parser.New(db, &MockValidator{})

	// Initialize schema
	if err := p.Init(); err != nil {
		t.Fatalf("Failed to initialize schema: %v", err)
	}

	// Ingest RVTools data (this also runs validation)
	result, err := p.IngestRvTools(ctx, rvtoolsFile)
	if err != nil {
		t.Fatalf("Failed to ingest RVTools: %v", err)
	}
	if result.HasErrors() {
		t.Fatalf("Schema validation failed: %v", result.Error())
	}
	if result.HasWarnings() {
		fmt.Printf("Schema validation warnings:\n")
		for _, w := range result.Warnings {
			fmt.Printf("  - [%s] %s\n", w.Code, w.Message)
		}
	}

	// Query individual data - caller is responsible for building their own inventory
	vms, err := p.VMs(ctx, parser.Filters{}, parser.Options{Limit: 10})
	if err != nil {
		t.Fatalf("Failed to get VMs: %v", err)
	}

	fmt.Printf("=== Sample VMs (first 10) ===\n")
	for _, vm := range vms {
		fmt.Printf("  VM: %s (Cluster: %s, PowerState: %s, Concerns: %d)\n",
			vm.Name, vm.Cluster, vm.PowerState, len(vm.Concerns))
		for _, c := range vm.Concerns {
			fmt.Printf("    - [%s] %s\n", c.Category, c.Label)
		}
	}

	// Get summary statistics
	vmCount, _ := p.VMCount(ctx, parser.Filters{})
	clusters, _ := p.Clusters(ctx)
	vcenterID, _ := p.VCenterID(ctx)

	fmt.Printf("\n=== Summary ===\n")
	fmt.Printf("VCenter ID: %s\n", vcenterID)
	fmt.Printf("Total VMs: %d\n", vmCount)
	fmt.Printf("Clusters (%d): %v\n", len(clusters), clusters)
}

func TestQueryMethods(t *testing.T) {
	rvtoolsFile := os.Getenv("RVTOOLS_FILE")
	if rvtoolsFile == "" {
		t.Skip("Set RVTOOLS_FILE environment variable to run this test")
	}

	ctx := context.Background()

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("Failed to open DuckDB: %v", err)
	}
	defer db.Close()

	p := parser.New(db, &MockValidator{})
	if err := p.Init(); err != nil {
		t.Fatalf("Failed to initialize: %v", err)
	}
	result, err := p.IngestRvTools(ctx, rvtoolsFile)
	if err != nil {
		t.Fatalf("Failed to ingest: %v", err)
	}
	if result.HasErrors() {
		t.Fatalf("Schema validation failed: %v", result.Error())
	}

	// Test various query methods
	t.Run("VMCount", func(t *testing.T) {
		count, err := p.VMCount(ctx, parser.Filters{})
		if err != nil {
			t.Errorf("VMCount failed: %v", err)
		}
		fmt.Printf("VM Count: %d\n", count)
	})

	t.Run("PowerStateCounts", func(t *testing.T) {
		states, err := p.PowerStateCounts(ctx, parser.Filters{})
		if err != nil {
			t.Errorf("PowerStateCounts failed: %v", err)
		}
		fmt.Printf("Power States: %v\n", states)
	})

	t.Run("OsSummary", func(t *testing.T) {
		summary, err := p.OsSummary(ctx, parser.Filters{})
		if err != nil {
			t.Errorf("OsSummary failed: %v", err)
		}
		fmt.Printf("OS Summary (%d types):\n", len(summary))
		for _, os := range summary {
			fmt.Printf("  %s: %d (supported: %v)\n", os.Name, os.Count, os.Supported)
		}
	})

	t.Run("Clusters", func(t *testing.T) {
		clusters, err := p.Clusters(ctx)
		if err != nil {
			t.Errorf("Clusters failed: %v", err)
		}
		fmt.Printf("Clusters: %v\n", clusters)
	})

	t.Run("MigratableVMCount", func(t *testing.T) {
		migratable, err := p.MigratableVMCount(ctx, parser.Filters{})
		if err != nil {
			t.Errorf("MigratableVMCount failed: %v", err)
		}
		fmt.Printf("Migratable VMs: %d\n", migratable)
	})

	t.Run("NotMigratableVMCount", func(t *testing.T) {
		notMigratable, err := p.NotMigratableVMCount(ctx, parser.Filters{})
		if err != nil {
			t.Errorf("NotMigratableVMCount failed: %v", err)
		}
		fmt.Printf("Not Migratable VMs: %d\n", notMigratable)
	})

	t.Run("MigrationIssues", func(t *testing.T) {
		issues, err := p.MigrationIssues(ctx, parser.Filters{}, "")
		if err != nil {
			t.Errorf("MigrationIssues failed: %v", err)
		}
		fmt.Printf("Migration Issues (%d):\n", len(issues))
		for _, issue := range issues {
			fmt.Printf("  [%s] %s: %d VMs\n", issue.Category, issue.Label, issue.Count)
		}
	})

	t.Run("TotalResources", func(t *testing.T) {
		resources, err := p.TotalResources(ctx, parser.Filters{})
		if err != nil {
			t.Errorf("TotalResources failed: %v", err)
		}
		fmt.Printf("Total Resources: CPU=%d, RAM=%dGB, Disks=%d, Storage=%dGB, NICs=%d\n",
			resources.TotalCPUCores, resources.TotalRAMGB, resources.TotalDiskCount,
			resources.TotalDiskGB, resources.TotalNICCount)
	})

	t.Run("FilterByCluster", func(t *testing.T) {
		clusters, _ := p.Clusters(ctx)
		if len(clusters) > 0 {
			filter := parser.Filters{Cluster: clusters[0]}
			count, err := p.VMCount(ctx, filter)
			if err != nil {
				t.Errorf("VMCount with filter failed: %v", err)
			}
			fmt.Printf("VMs in cluster %s: %d\n", clusters[0], count)
		}
	})
}
