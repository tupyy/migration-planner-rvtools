# rvtools

Go library for parsing VMware inventory data from RVTools Excel exports or forklift SQLite databases using DuckDB as the query engine.

## Installation

```bash
go get github.com/tupyy/rvtools@latest
```

## Usage

```go
package main

import (
    "context"
    "database/sql"

    "github.com/duckdb/duckdb-go/v2"
    "github.com/tupyy/rvtools/parser"
)

func main() {
    // Create DuckDB connection
    c, _ := duckdb.NewConnector("", nil)
    db := sql.OpenDB(c)
    defer db.Close()

    // Create parser and ingest data
    p := parser.New(db, nil)
    p.Init()
    p.IngestRvTools("/path/to/file.xlsx")
    // or: p.IngestSqlite("/path/to/file.sqlite")

    ctx := context.Background()

    // Query entities
    vms, _ := p.VMs(ctx, parser.Filters{}, parser.Options{})
    hosts, _ := p.Hosts(ctx, parser.Filters{}, parser.Options{})
    networks, _ := p.Networks(ctx, parser.Filters{}, parser.Options{})
    datastores, _ := p.Datastores(ctx, parser.Filters{}, parser.Options{})

    // Query with filters and pagination
    vms, _ = p.VMs(ctx, parser.Filters{Cluster: "MyCluster", PowerState: "poweredOn"}, parser.Options{Limit: 10})

    // Aggregations
    vmCount, _ := p.VMCount(ctx, parser.Filters{})
    powerStates, _ := p.PowerStateCounts(ctx, parser.Filters{})
    cpuTiers, _ := p.CPUTierDistribution(ctx, parser.Filters{})
    osSummary, _ := p.OsSummary(ctx, parser.Filters{})

    // Migration readiness (concern-based)
    migratable, _ := p.MigratableVMCount(ctx, parser.Filters{})
    withWarnings, _ := p.MigratableWithWarningsVMCount(ctx, parser.Filters{})
    breakdowns, _ := p.ResourceBreakdowns(ctx, parser.Filters{})
}
```

## API Reference

### Parser Methods

| Method | Description |
|--------|-------------|
| `VMs(ctx, filters, options)` | Query VMs with optional filters and pagination |
| `VMCount(ctx, filters)` | Get VM count |
| `Hosts(ctx, filters, options)` | Query hosts |
| `Networks(ctx, filters, options)` | Query networks |
| `Datastores(ctx, filters, options)` | Query datastores |
| `Clusters(ctx)` | Get cluster names |
| `VCenterID(ctx)` | Get vCenter UUID |

### Aggregations

| Method | Description |
|--------|-------------|
| `PowerStateCounts(ctx, filters)` | VM power state distribution |
| `OsSummary(ctx, filters)` | OS distribution |
| `CPUTierDistribution(ctx, filters)` | VMs by CPU tier |
| `MemoryTierDistribution(ctx, filters)` | VMs by memory tier |
| `TotalResources(ctx, filters)` | Aggregate resource totals |
| `ResourceBreakdowns(ctx, filters)` | Resources by migrability status |

### Migration Readiness

| Method | Description |
|--------|-------------|
| `MigratableVMCount(ctx, filters)` | VMs without critical concerns |
| `MigratableWithWarningsVMCount(ctx, filters)` | VMs with warnings only |
| `NotMigratableVMCount(ctx, filters)` | VMs with critical concerns |
| `MigrationIssues(ctx, filters, category)` | Aggregated issues by category |

### Filters and Options

```go
type Filters struct {
    Cluster    string // filter by cluster name
    OS         string // filter by OS name (VMs only)
    PowerState string // filter by power state (VMs only)
}

type Options struct {
    Limit  int // max results (0 = unlimited)
    Offset int // skip first N results
}
```

### Validation

Pass a `Validator` implementation to compute migration concerns:

```go
type Validator interface {
    Validate(ctx context.Context, vm models.VM) ([]Concern, error)
}

p := parser.New(db, myValidator)
p.Init()
p.IngestRvTools("/path/to/file.xlsx")
p.Validate(ctx) // stores concerns in DB

// Now queries include concern data
vms, _ := p.VMs(ctx, parser.Filters{}, parser.Options{})
// vms[i].Concerns contains validation results
```

## Architecture

Uses DuckDB as the query engine. Both data sources are ingested into RVTools-shaped tables (vinfo, vcpu, vmemory, vdisk, vnetwork, vhost, vdatastore), then SQL query templates extract the inventory data.

### Data Sources

- **RVTools Excel**: Direct table creation using DuckDB's `read_xlsx()`
- **Forklift SQLite**: Transforms normalized model into flat RVTools tables

## Testing

```bash
go test ./...
```
