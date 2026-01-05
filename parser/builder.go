package parser

import (
	"bytes"
	_ "embed"
	"fmt"
	"strings"
	"text/template"
)

//go:embed templates/create_schema.go.tmpl
var createSchemaTemplate string

//go:embed templates/ingest_rvtools.go.tmpl
var ingestRvtoolsTemplate string

//go:embed templates/ingest_sqlite.go.tmpl
var ingestSqliteTemplate string

//go:embed templates/vm_query.go.tmpl
var vmQueryTemplate string

//go:embed templates/datastore_query.go.tmpl
var datastoreQueryTemplate string

//go:embed templates/network_query.go.tmpl
var networkQueryTemplate string

//go:embed templates/os_query.go.tmpl
var osQueryTemplate string

//go:embed templates/host_query.go.tmpl
var hostQueryTemplate string

//go:embed templates/vcenter_query.go.tmpl
var vcenterQueryTemplate string

//go:embed templates/clusters_query.go.tmpl
var clustersQueryTemplate string

//go:embed templates/vm_count_query.go.tmpl
var vmCountQueryTemplate string

//go:embed templates/power_state_counts_query.go.tmpl
var powerStateCountsQueryTemplate string

//go:embed templates/host_power_state_counts_query.go.tmpl
var hostPowerStateCountsQueryTemplate string

//go:embed templates/cpu_tier_query.go.tmpl
var cpuTierQueryTemplate string

//go:embed templates/memory_tier_query.go.tmpl
var memoryTierQueryTemplate string

//go:embed templates/disk_size_tier_query.go.tmpl
var diskSizeTierQueryTemplate string

//go:embed templates/disk_type_summary_query.go.tmpl
var diskTypeSummaryQueryTemplate string

//go:embed templates/resource_totals_query.go.tmpl
var resourceTotalsQueryTemplate string

//go:embed templates/allocated_vcpus_query.go.tmpl
var allocatedVcpusQueryTemplate string

//go:embed templates/allocated_memory_query.go.tmpl
var allocatedMemoryQueryTemplate string

//go:embed templates/total_host_cpus_query.go.tmpl
var totalHostCpusQueryTemplate string

//go:embed templates/total_host_memory_query.go.tmpl
var totalHostMemoryQueryTemplate string

//go:embed templates/datastores_used_by_vms_query.go.tmpl
var datastoresUsedByVmsQueryTemplate string

//go:embed templates/networks_used_by_vms_query.go.tmpl
var networksUsedByVmsQueryTemplate string

//go:embed templates/vm_count_by_network_query.go.tmpl
var vmCountByNetworkQueryTemplate string

//go:embed templates/datacenter_count_query.go.tmpl
var datacenterCountQueryTemplate string

//go:embed templates/clusters_per_datacenter_query.go.tmpl
var clustersPerDatacenterQueryTemplate string

//go:embed templates/migratable_count_query.go.tmpl
var migratableCountQueryTemplate string

//go:embed templates/migratable_with_warnings_count_query.go.tmpl
var migratableWithWarningsCountQueryTemplate string

//go:embed templates/not_migratable_count_query.go.tmpl
var notMigratableCountQueryTemplate string

//go:embed templates/migration_issues_query.go.tmpl
var migrationIssuesQueryTemplate string

//go:embed templates/resource_breakdowns_query.go.tmpl
var resourceBreakdownsQueryTemplate string

// Type represents the type of query to build
type Type int

const (
	VM Type = iota
	Datastore
	Network
	Host
	Os
	VCenter
)

func (q Type) String() string {
	switch q {
	case VM:
		return "vm"
	case Datastore:
		return "datastore"
	case Network:
		return "network"
	case Host:
		return "host"
	case Os:
		return "os"
	case VCenter:
		return "vcenter"
	default:
		return "unknown"
	}
}

// QueryBuilder builds SQL queries from templates.
type QueryBuilder struct{}

// NewBuilder creates a new Builder.
func NewBuilder() *QueryBuilder {
	return &QueryBuilder{}
}

type ingestParams struct {
	FilePath string
}

// CreateSchemaQuery returns queries to create all RVTools tables with proper schema.
func (b *QueryBuilder) CreateSchemaQuery() (string, error) {
	return b.buildQuery("create_schema", createSchemaTemplate, nil)
}

// IngestRvtoolsQuery returns a query that inserts data from an RVTools Excel file into schema tables.
func (b *QueryBuilder) IngestRvtoolsQuery(filePath string) (string, error) {
	return b.buildQuery("ingest_rvtools", ingestRvtoolsTemplate, ingestParams{FilePath: filePath})
}

// IngestSqliteQuery returns a query that creates RVTools-shaped tables from a forklift SQLite database.
func (b *QueryBuilder) IngestSqliteQuery(filePath string) (string, error) {
	return b.buildQuery("ingest_sqlite", ingestSqliteTemplate, ingestParams{FilePath: filePath})
}

// Build generates all SQL queries based on the schema context.
func (b *QueryBuilder) Build() (map[Type]string, error) {
	queries := make(map[Type]string)

	q, err := b.buildVMQuery()
	if err != nil {
		return map[Type]string{}, fmt.Errorf("failed to build vm query: %v", err)
	}
	queries[VM] = q

	q, err = b.buildQuery("os_query", osQueryTemplate, nil)
	if err != nil {
		return map[Type]string{}, fmt.Errorf("failed to build os_query: %v", err)
	}
	queries[Os] = q

	q, err = b.buildQuery("vcenter_query", vcenterQueryTemplate, nil)
	if err != nil {
		return map[Type]string{}, fmt.Errorf("failed to build vcenter_query: %v", err)
	}
	queries[VCenter] = q

	q, err = b.buildDatastoreQuery()
	if err != nil {
		return map[Type]string{}, fmt.Errorf("failed to build datastore query: %v", err)
	}
	queries[Datastore] = q

	q, err = b.buildNetworkQuery()
	if err != nil {
		return map[Type]string{}, fmt.Errorf("fauled to build network query: %v", err)
	}
	queries[Network] = q

	q, err = b.buildQuery("host_query", hostQueryTemplate, nil)
	if err != nil {
		return map[Type]string{}, fmt.Errorf("fauled to build host_query: %v", err)
	}
	queries[Host] = q

	return queries, nil
}

// queryParams holds all template parameters for queries.
type queryParams struct {
	NetworkColumns   string
	ClusterFilter    string
	OSFilter         string
	PowerStateFilter string
	Category         string
	Limit            int
	Offset           int
}

func (b *QueryBuilder) buildVMQuery() (string, error) {
	return b.VMQuery(Filters{}, Options{})
}

// VMQuery builds the VM query with filters and pagination.
func (b *QueryBuilder) VMQuery(filters Filters, options Options) (string, error) {
	const maxNetworkNumbers = 25
	quoted := make([]string, 0, maxNetworkNumbers)
	for i := 1; i <= maxNetworkNumbers; i++ {
		quoted = append(quoted, fmt.Sprintf(`i."Network #%d"`, i))
	}
	networkColumns := strings.Join(quoted, ", ")

	params := queryParams{
		NetworkColumns:   networkColumns,
		ClusterFilter:    filters.Cluster,
		OSFilter:         filters.OS,
		PowerStateFilter: filters.PowerState,
		Limit:            options.Limit,
		Offset:           options.Offset,
	}
	return b.buildQuery("vm_query", vmQueryTemplate, params)
}

func (b *QueryBuilder) buildDatastoreQuery() (string, error) {
	return b.DatastoreQuery(Filters{}, Options{})
}

// DatastoreQuery builds the datastore query with filters and pagination.
func (b *QueryBuilder) DatastoreQuery(filters Filters, options Options) (string, error) {
	params := queryParams{
		ClusterFilter: filters.Cluster,
		Limit:         options.Limit,
		Offset:        options.Offset,
	}
	return b.buildQuery("datastore_query", datastoreQueryTemplate, params)
}

func (b *QueryBuilder) buildNetworkQuery() (string, error) {
	return b.NetworkQuery(Filters{}, Options{})
}

// NetworkQuery builds the network query with filters and pagination.
func (b *QueryBuilder) NetworkQuery(filters Filters, options Options) (string, error) {
	params := queryParams{
		ClusterFilter: filters.Cluster,
		Limit:         options.Limit,
		Offset:        options.Offset,
	}
	return b.buildQuery("network_query", networkQueryTemplate, params)
}

// HostQuery builds the host query with filters and pagination.
func (b *QueryBuilder) HostQuery(filters Filters, options Options) (string, error) {
	params := queryParams{
		ClusterFilter: filters.Cluster,
		Limit:         options.Limit,
		Offset:        options.Offset,
	}
	return b.buildQuery("host_query", hostQueryTemplate, params)
}

// OsQuery builds the OS summary query with filters.
func (b *QueryBuilder) OsQuery(filters Filters) (string, error) {
	params := queryParams{
		ClusterFilter: filters.Cluster,
	}
	return b.buildQuery("os_query", osQueryTemplate, params)
}

// ClustersQuery builds the clusters query.
func (b *QueryBuilder) ClustersQuery() (string, error) {
	return b.buildQuery("clusters_query", clustersQueryTemplate, nil)
}

// VMCountQuery builds the VM count query with filters.
func (b *QueryBuilder) VMCountQuery(filters Filters) (string, error) {
	params := queryParams{
		ClusterFilter:    filters.Cluster,
		PowerStateFilter: filters.PowerState,
	}
	return b.buildQuery("vm_count_query", vmCountQueryTemplate, params)
}

// PowerStateCountsQuery builds the power state counts query.
func (b *QueryBuilder) PowerStateCountsQuery(filters Filters) (string, error) {
	params := queryParams{
		ClusterFilter: filters.Cluster,
	}
	return b.buildQuery("power_state_counts_query", powerStateCountsQueryTemplate, params)
}

// HostPowerStateCountsQuery builds the host power state counts query.
func (b *QueryBuilder) HostPowerStateCountsQuery(filters Filters) (string, error) {
	params := queryParams{
		ClusterFilter: filters.Cluster,
	}
	return b.buildQuery("host_power_state_counts_query", hostPowerStateCountsQueryTemplate, params)
}

// CPUTierQuery builds the CPU tier distribution query.
func (b *QueryBuilder) CPUTierQuery(filters Filters) (string, error) {
	params := queryParams{
		ClusterFilter: filters.Cluster,
	}
	return b.buildQuery("cpu_tier_query", cpuTierQueryTemplate, params)
}

// MemoryTierQuery builds the memory tier distribution query.
func (b *QueryBuilder) MemoryTierQuery(filters Filters) (string, error) {
	params := queryParams{
		ClusterFilter: filters.Cluster,
	}
	return b.buildQuery("memory_tier_query", memoryTierQueryTemplate, params)
}

// DiskSizeTierQuery builds the disk size tier distribution query.
func (b *QueryBuilder) DiskSizeTierQuery(filters Filters) (string, error) {
	params := queryParams{
		ClusterFilter: filters.Cluster,
	}
	return b.buildQuery("disk_size_tier_query", diskSizeTierQueryTemplate, params)
}

// DiskTypeSummaryQuery builds the disk type summary query.
func (b *QueryBuilder) DiskTypeSummaryQuery(filters Filters) (string, error) {
	params := queryParams{
		ClusterFilter: filters.Cluster,
	}
	return b.buildQuery("disk_type_summary_query", diskTypeSummaryQueryTemplate, params)
}

// ResourceTotalsQuery builds the resource totals query.
func (b *QueryBuilder) ResourceTotalsQuery(filters Filters) (string, error) {
	params := queryParams{
		ClusterFilter: filters.Cluster,
	}
	return b.buildQuery("resource_totals_query", resourceTotalsQueryTemplate, params)
}

// AllocatedVCPUsQuery builds the allocated vCPUs query.
func (b *QueryBuilder) AllocatedVCPUsQuery(filters Filters) (string, error) {
	params := queryParams{
		ClusterFilter: filters.Cluster,
	}
	return b.buildQuery("allocated_vcpus_query", allocatedVcpusQueryTemplate, params)
}

// AllocatedMemoryQuery builds the allocated memory query.
func (b *QueryBuilder) AllocatedMemoryQuery(filters Filters) (string, error) {
	params := queryParams{
		ClusterFilter: filters.Cluster,
	}
	return b.buildQuery("allocated_memory_query", allocatedMemoryQueryTemplate, params)
}

// TotalHostCPUsQuery builds the total host CPUs query.
func (b *QueryBuilder) TotalHostCPUsQuery(filters Filters) (string, error) {
	params := queryParams{
		ClusterFilter: filters.Cluster,
	}
	return b.buildQuery("total_host_cpus_query", totalHostCpusQueryTemplate, params)
}

// TotalHostMemoryQuery builds the total host memory query.
func (b *QueryBuilder) TotalHostMemoryQuery(filters Filters) (string, error) {
	params := queryParams{
		ClusterFilter: filters.Cluster,
	}
	return b.buildQuery("total_host_memory_query", totalHostMemoryQueryTemplate, params)
}

// DatastoresUsedByVMsQuery builds the datastores used by VMs query.
func (b *QueryBuilder) DatastoresUsedByVMsQuery(cluster string) (string, error) {
	params := queryParams{
		ClusterFilter: cluster,
	}
	return b.buildQuery("datastores_used_by_vms_query", datastoresUsedByVmsQueryTemplate, params)
}

// NetworksUsedByVMsQuery builds the networks used by VMs query.
func (b *QueryBuilder) NetworksUsedByVMsQuery(cluster string) (string, error) {
	params := queryParams{
		ClusterFilter: cluster,
	}
	return b.buildQuery("networks_used_by_vms_query", networksUsedByVmsQueryTemplate, params)
}

// VMCountByNetworkQuery builds the VM count by network query.
func (b *QueryBuilder) VMCountByNetworkQuery(filters Filters) (string, error) {
	params := queryParams{
		ClusterFilter: filters.Cluster,
	}
	return b.buildQuery("vm_count_by_network_query", vmCountByNetworkQueryTemplate, params)
}

// DatacenterCountQuery builds the datacenter count query.
func (b *QueryBuilder) DatacenterCountQuery() (string, error) {
	return b.buildQuery("datacenter_count_query", datacenterCountQueryTemplate, nil)
}

// ClustersPerDatacenterQuery builds the clusters per datacenter query.
func (b *QueryBuilder) ClustersPerDatacenterQuery() (string, error) {
	return b.buildQuery("clusters_per_datacenter_query", clustersPerDatacenterQueryTemplate, nil)
}

// MigratableCountQuery builds the migratable VMs count query.
func (b *QueryBuilder) MigratableCountQuery(filters Filters) (string, error) {
	params := queryParams{
		ClusterFilter: filters.Cluster,
	}
	return b.buildQuery("migratable_count_query", migratableCountQueryTemplate, params)
}

// MigratableWithWarningsCountQuery builds the migratable with warnings count query.
func (b *QueryBuilder) MigratableWithWarningsCountQuery(filters Filters) (string, error) {
	params := queryParams{
		ClusterFilter: filters.Cluster,
	}
	return b.buildQuery("migratable_with_warnings_count_query", migratableWithWarningsCountQueryTemplate, params)
}

// NotMigratableCountQuery builds the not migratable VMs count query.
func (b *QueryBuilder) NotMigratableCountQuery(filters Filters) (string, error) {
	params := queryParams{
		ClusterFilter: filters.Cluster,
	}
	return b.buildQuery("not_migratable_count_query", notMigratableCountQueryTemplate, params)
}

// MigrationIssuesQuery builds the migration issues query.
func (b *QueryBuilder) MigrationIssuesQuery(filters Filters, category string) (string, error) {
	params := queryParams{
		ClusterFilter: filters.Cluster,
		Category:      category,
	}
	return b.buildQuery("migration_issues_query", migrationIssuesQueryTemplate, params)
}

// ResourceBreakdownsQuery builds the resource breakdowns query.
func (b *QueryBuilder) ResourceBreakdownsQuery(filters Filters) (string, error) {
	params := queryParams{
		ClusterFilter: filters.Cluster,
	}
	return b.buildQuery("resource_breakdowns_query", resourceBreakdownsQueryTemplate, params)
}

// VCenterQuery builds the vCenter ID query.
func (b *QueryBuilder) VCenterQuery() (string, error) {
	return b.buildQuery("vcenter_query", vcenterQueryTemplate, nil)
}

func (b *QueryBuilder) buildQuery(name, tmplContent string, params any) (string, error) {
	tmpl, err := template.New(name).Parse(tmplContent)
	if err != nil {
		return "", err
	}
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, params); err != nil {
		return "", err
	}
	return strings.TrimSpace(buf.String()), nil
}
