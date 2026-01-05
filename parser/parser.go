package parser

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"

	"github.com/georgysavva/scany/v2/sqlscan"
	"go.uber.org/zap"

	"github.com/tupyy/rvtools/models"
)

type Preprocessor interface {
	Process(db *sql.DB) error
}

type rvToolsPreprocessor struct {
	excelFile string
	builder   *QueryBuilder
}

var stmtRegex = regexp.MustCompile(`(?s)(CREATE|INSERT|UPDATE|DROP|ALTER|WITH|INSTALL|LOAD|ATTACH|DETACH).*?;`)

func (pp *rvToolsPreprocessor) Process(db *sql.DB) error {
	query, err := pp.builder.IngestRvtoolsQuery(pp.excelFile)
	if err != nil {
		return err
	}
	stmts := stmtRegex.FindAllString(query, -1)
	for _, stmt := range stmts {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		//		zap.S().Debugw("executing statement", "query", stmt)
		// Ignore errors for missing sheets
		if _, err := db.Exec(stmt); err != nil {
			zap.S().Debugw("statement failed", "error", err)
		}
	}
	return nil
}

type sqlitePreprocessor struct {
	sqliteFile string
	builder    *QueryBuilder
}

func (pp *sqlitePreprocessor) Process(db *sql.DB) error {
	query, err := pp.builder.IngestSqliteQuery(pp.sqliteFile)
	if err != nil {
		return err
	}
	stmts := stmtRegex.FindAllString(query, -1)
	for _, stmt := range stmts {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		zap.S().Debugw("executing statement", "query", stmt)
		if _, err := db.Exec(stmt); err != nil {
			zap.S().Debugw("statement failed", "error", err)
		}
	}
	return nil
}

func NewRvToolParser(db *sql.DB, excelFile string) *Parser {
	p := newParser(db)
	p.preprocessor = &rvToolsPreprocessor{
		builder:   NewBuilder(),
		excelFile: excelFile,
	}
	return p
}

func NewSqliteParser(db *sql.DB, sqliteFile string) *Parser {
	p := newParser(db)
	p.preprocessor = &sqlitePreprocessor{
		sqliteFile: sqliteFile,
		builder:    NewBuilder(),
	}
	return p
}

type Parser struct {
	db           *sql.DB
	builder      *QueryBuilder
	preprocessor Preprocessor
	validator    Validator
}

func newParser(db *sql.DB) *Parser {
	return &Parser{
		db:      db,
		builder: NewBuilder(),
	}
}

// New creates a new Parser with optional validator.
// The validator can be nil if no validation is needed.
func New(db *sql.DB, validator Validator) *Parser {
	return &Parser{
		db:        db,
		builder:   NewBuilder(),
		validator: validator,
	}
}

func (p *Parser) Init() error {
	if err := p.createSchema(); err != nil {
		return fmt.Errorf("creating schema: %w", err)
	}

	// For legacy constructors (NewRvToolParser, NewSqliteParser)
	if p.preprocessor != nil {
		if err := p.preprocessor.Process(p.db); err != nil {
			return fmt.Errorf("failed to preprocess: %v", err)
		}
	}

	return nil
}

// IngestRvTools ingests data from an RVTools Excel file.
func (p *Parser) IngestRvTools(excelFile string) error {
	pp := &rvToolsPreprocessor{
		builder:   p.builder,
		excelFile: excelFile,
	}
	return pp.Process(p.db)
}

// IngestSqlite ingests data from a forklift SQLite database.
func (p *Parser) IngestSqlite(sqliteFile string) error {
	pp := &sqlitePreprocessor{
		builder:    p.builder,
		sqliteFile: sqliteFile,
	}
	return pp.Process(p.db)
}

// Validate validates all VMs using the configured validator and stores concerns in the database.
// Returns nil if no validator is configured.
func (p *Parser) Validate(ctx context.Context) error {
	if p.validator == nil {
		return nil
	}

	// Get VMs without concerns for validation
	vms, err := p.VMs(ctx, Filters{}, Options{})
	if err != nil {
		return fmt.Errorf("getting VMs for validation: %w", err)
	}

	// Build bulk insert for concerns
	var values []string
	escape := func(s string) string {
		return strings.ReplaceAll(s, "'", "''")
	}

	for _, vm := range vms {
		concerns, err := p.validator.Validate(ctx, vm)
		if err != nil {
			zap.S().Warnw("validation failed for VM", "vm_id", vm.ID, "error", err)
			continue
		}
		for _, c := range concerns {
			value := fmt.Sprintf("('%s', '%s', '%s', '%s', '%s')",
				escape(vm.ID),
				escape(c.ID),
				escape(c.Label),
				escape(c.Category),
				escape(c.Assessment),
			)
			values = append(values, value)
		}
	}

	if len(values) == 0 {
		return nil
	}

	// Bulk insert concerns
	query := fmt.Sprintf(
		"INSERT INTO concerns (\"VM_ID\", \"Concern_ID\", \"Label\", \"Category\", \"Assessment\") VALUES %s;",
		strings.Join(values, ", "),
	)
	if _, err := p.db.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("inserting concerns: %w", err)
	}

	return nil
}

// VMs returns VMs with optional filters and pagination.
func (p *Parser) VMs(ctx context.Context, filters Filters, options Options) ([]models.VM, error) {
	q, err := p.builder.VMQuery(filters, options)
	if err != nil {
		return nil, fmt.Errorf("failed to build vm query: %v", err)
	}

	vms, err := p.readVMs(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("reading VMs: %w", err)
	}

	return vms, nil
}

// VMCount returns the count of VMs with optional filters.
func (p *Parser) VMCount(ctx context.Context, filters Filters) (int, error) {
	q, err := p.builder.VMCountQuery(filters)
	if err != nil {
		return 0, fmt.Errorf("building vm count query: %w", err)
	}
	var count int
	if err := p.db.QueryRowContext(ctx, q).Scan(&count); err != nil {
		return 0, fmt.Errorf("scanning vm count: %w", err)
	}
	return count, nil
}

// Datastores returns datastores with optional filters and pagination.
func (p *Parser) Datastores(ctx context.Context, filters Filters, options Options) ([]models.Datastore, error) {
	q, err := p.builder.DatastoreQuery(filters, options)
	if err != nil {
		return nil, fmt.Errorf("building datastore query: %w", err)
	}
	return p.readDatastores(ctx, q)
}

// DatastoresUsedByVMs returns datastores used by VMs in the specified cluster.
func (p *Parser) DatastoresUsedByVMs(ctx context.Context, cluster string) ([]models.Datastore, error) {
	q, err := p.builder.DatastoresUsedByVMsQuery(cluster)
	if err != nil {
		return nil, fmt.Errorf("building datastores used by vms query: %w", err)
	}
	return p.readDatastores(ctx, q)
}

// Networks returns networks with optional filters and pagination.
func (p *Parser) Networks(ctx context.Context, filters Filters, options Options) ([]models.Network, error) {
	q, err := p.builder.NetworkQuery(filters, options)
	if err != nil {
		return nil, fmt.Errorf("building network query: %w", err)
	}
	return p.readNetworks(ctx, q)
}

// NetworksUsedByVMs returns networks used by VMs in the specified cluster.
func (p *Parser) NetworksUsedByVMs(ctx context.Context, cluster string) ([]models.Network, error) {
	q, err := p.builder.NetworksUsedByVMsQuery(cluster)
	if err != nil {
		return nil, fmt.Errorf("building networks used by vms query: %w", err)
	}
	var results []models.Network
	if err := sqlscan.Select(ctx, p.db, &results, q); err != nil {
		return nil, fmt.Errorf("scanning networks: %w", err)
	}
	return results, nil
}

// Hosts returns hosts with optional filters and pagination.
func (p *Parser) Hosts(ctx context.Context, filters Filters, options Options) ([]models.Host, error) {
	q, err := p.builder.HostQuery(filters, options)
	if err != nil {
		return nil, fmt.Errorf("building host query: %w", err)
	}
	return p.readHosts(ctx, q)
}

// Clusters returns a list of unique cluster names.
func (p *Parser) Clusters(ctx context.Context) ([]string, error) {
	q, err := p.builder.ClustersQuery()
	if err != nil {
		return nil, fmt.Errorf("building clusters query: %w", err)
	}
	var clusters []string
	rows, err := p.db.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("querying clusters: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var cluster string
		if err := rows.Scan(&cluster); err != nil {
			return nil, fmt.Errorf("scanning cluster: %w", err)
		}
		clusters = append(clusters, cluster)
	}
	return clusters, rows.Err()
}

// OsSummary returns OS distribution with optional filters.
func (p *Parser) OsSummary(ctx context.Context, filters Filters) ([]models.Os, error) {
	q, err := p.builder.OsQuery(filters)
	if err != nil {
		return nil, fmt.Errorf("building os query: %w", err)
	}
	return p.readOs(ctx, q)
}

// PowerStateCounts returns VM power state distribution.
func (p *Parser) PowerStateCounts(ctx context.Context, filters Filters) (map[string]int, error) {
	q, err := p.builder.PowerStateCountsQuery(filters)
	if err != nil {
		return nil, fmt.Errorf("building power state counts query: %w", err)
	}
	return p.readStateCountMap(ctx, q)
}

// HostPowerStateCounts returns host power state distribution.
func (p *Parser) HostPowerStateCounts(ctx context.Context, filters Filters) (map[string]int, error) {
	q, err := p.builder.HostPowerStateCountsQuery(filters)
	if err != nil {
		return nil, fmt.Errorf("building host power state counts query: %w", err)
	}
	return p.readStateCountMap(ctx, q)
}

// VMCountByNetwork returns VM count per network.
func (p *Parser) VMCountByNetwork(ctx context.Context, filters Filters) (map[string]int, error) {
	q, err := p.builder.VMCountByNetworkQuery(filters)
	if err != nil {
		return nil, fmt.Errorf("building vm count by network query: %w", err)
	}
	result := make(map[string]int)
	rows, err := p.db.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("querying vm count by network: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var network string
		var count int
		if err := rows.Scan(&network, &count); err != nil {
			return nil, fmt.Errorf("scanning network count: %w", err)
		}
		result[network] = count
	}
	return result, rows.Err()
}

// CPUTierDistribution returns VM distribution by CPU tier.
func (p *Parser) CPUTierDistribution(ctx context.Context, filters Filters) (map[string]int, error) {
	q, err := p.builder.CPUTierQuery(filters)
	if err != nil {
		return nil, fmt.Errorf("building cpu tier query: %w", err)
	}
	return p.readTierCountMap(ctx, q)
}

// MemoryTierDistribution returns VM distribution by memory tier.
func (p *Parser) MemoryTierDistribution(ctx context.Context, filters Filters) (map[string]int, error) {
	q, err := p.builder.MemoryTierQuery(filters)
	if err != nil {
		return nil, fmt.Errorf("building memory tier query: %w", err)
	}
	return p.readTierCountMap(ctx, q)
}

// DiskSizeTierDistribution returns VM distribution by disk size tier.
func (p *Parser) DiskSizeTierDistribution(ctx context.Context, filters Filters) (map[string]DiskSizeTierSummary, error) {
	q, err := p.builder.DiskSizeTierQuery(filters)
	if err != nil {
		return nil, fmt.Errorf("building disk size tier query: %w", err)
	}
	result := make(map[string]DiskSizeTierSummary)
	rows, err := p.db.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("querying disk size tier: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var tier string
		var summary DiskSizeTierSummary
		if err := rows.Scan(&tier, &summary.VMCount, &summary.TotalSizeTB); err != nil {
			return nil, fmt.Errorf("scanning disk size tier: %w", err)
		}
		result[tier] = summary
	}
	return result, rows.Err()
}

// DiskTypeSummary returns disk usage aggregated by datastore type.
func (p *Parser) DiskTypeSummary(ctx context.Context, filters Filters) ([]DiskTypeSummary, error) {
	q, err := p.builder.DiskTypeSummaryQuery(filters)
	if err != nil {
		return nil, fmt.Errorf("building disk type summary query: %w", err)
	}
	var results []DiskTypeSummary
	rows, err := p.db.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("querying disk type summary: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var s DiskTypeSummary
		if err := rows.Scan(&s.Type, &s.VMCount, &s.TotalSizeTB); err != nil {
			return nil, fmt.Errorf("scanning disk type summary: %w", err)
		}
		results = append(results, s)
	}
	return results, rows.Err()
}

// TotalResources returns aggregated resource totals.
func (p *Parser) TotalResources(ctx context.Context, filters Filters) (ResourceTotals, error) {
	q, err := p.builder.ResourceTotalsQuery(filters)
	if err != nil {
		return ResourceTotals{}, fmt.Errorf("building resource totals query: %w", err)
	}
	var r ResourceTotals
	if err := p.db.QueryRowContext(ctx, q).Scan(
		&r.TotalCPUCores, &r.TotalRAMGB, &r.TotalDiskCount, &r.TotalDiskGB, &r.TotalNICCount,
	); err != nil {
		return ResourceTotals{}, fmt.Errorf("scanning resource totals: %w", err)
	}
	return r, nil
}

// AllocatedVCPUs returns sum of vCPUs for powered-on VMs.
func (p *Parser) AllocatedVCPUs(ctx context.Context, filters Filters) (int, error) {
	q, err := p.builder.AllocatedVCPUsQuery(filters)
	if err != nil {
		return 0, fmt.Errorf("building allocated vcpus query: %w", err)
	}
	var count int
	if err := p.db.QueryRowContext(ctx, q).Scan(&count); err != nil {
		return 0, fmt.Errorf("scanning allocated vcpus: %w", err)
	}
	return count, nil
}

// AllocatedMemoryMB returns sum of memory (MB) for powered-on VMs.
func (p *Parser) AllocatedMemoryMB(ctx context.Context, filters Filters) (int, error) {
	q, err := p.builder.AllocatedMemoryQuery(filters)
	if err != nil {
		return 0, fmt.Errorf("building allocated memory query: %w", err)
	}
	var count int
	if err := p.db.QueryRowContext(ctx, q).Scan(&count); err != nil {
		return 0, fmt.Errorf("scanning allocated memory: %w", err)
	}
	return count, nil
}

// TotalHostCPUCores returns sum of physical CPU cores across hosts.
func (p *Parser) TotalHostCPUCores(ctx context.Context, filters Filters) (int, error) {
	q, err := p.builder.TotalHostCPUsQuery(filters)
	if err != nil {
		return 0, fmt.Errorf("building total host cpus query: %w", err)
	}
	var count int
	if err := p.db.QueryRowContext(ctx, q).Scan(&count); err != nil {
		return 0, fmt.Errorf("scanning total host cpus: %w", err)
	}
	return count, nil
}

// TotalHostMemoryMB returns sum of host memory (MB).
func (p *Parser) TotalHostMemoryMB(ctx context.Context, filters Filters) (int, error) {
	q, err := p.builder.TotalHostMemoryQuery(filters)
	if err != nil {
		return 0, fmt.Errorf("building total host memory query: %w", err)
	}
	var count int
	if err := p.db.QueryRowContext(ctx, q).Scan(&count); err != nil {
		return 0, fmt.Errorf("scanning total host memory: %w", err)
	}
	return count, nil
}

// MigratableVMCount returns count of VMs without Critical concerns.
func (p *Parser) MigratableVMCount(ctx context.Context, filters Filters) (int, error) {
	q, err := p.builder.MigratableCountQuery(filters)
	if err != nil {
		return 0, fmt.Errorf("building migratable count query: %w", err)
	}
	var count int
	if err := p.db.QueryRowContext(ctx, q).Scan(&count); err != nil {
		return 0, fmt.Errorf("scanning migratable count: %w", err)
	}
	return count, nil
}

// MigratableWithWarningsVMCount returns count of VMs with Warning but no Critical concerns.
func (p *Parser) MigratableWithWarningsVMCount(ctx context.Context, filters Filters) (int, error) {
	q, err := p.builder.MigratableWithWarningsCountQuery(filters)
	if err != nil {
		return 0, fmt.Errorf("building migratable with warnings count query: %w", err)
	}
	var count int
	if err := p.db.QueryRowContext(ctx, q).Scan(&count); err != nil {
		return 0, fmt.Errorf("scanning migratable with warnings count: %w", err)
	}
	return count, nil
}

// NotMigratableVMCount returns count of VMs with Critical concerns.
func (p *Parser) NotMigratableVMCount(ctx context.Context, filters Filters) (int, error) {
	q, err := p.builder.NotMigratableCountQuery(filters)
	if err != nil {
		return 0, fmt.Errorf("building not migratable count query: %w", err)
	}
	var count int
	if err := p.db.QueryRowContext(ctx, q).Scan(&count); err != nil {
		return 0, fmt.Errorf("scanning not migratable count: %w", err)
	}
	return count, nil
}

// MigrationIssues returns aggregated migration issues by category.
func (p *Parser) MigrationIssues(ctx context.Context, filters Filters, category string) ([]MigrationIssue, error) {
	q, err := p.builder.MigrationIssuesQuery(filters, category)
	if err != nil {
		return nil, fmt.Errorf("building migration issues query: %w", err)
	}
	var results []MigrationIssue
	rows, err := p.db.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("querying migration issues: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var m MigrationIssue
		if err := rows.Scan(&m.ID, &m.Label, &m.Category, &m.Assessment, &m.Count); err != nil {
			return nil, fmt.Errorf("scanning migration issue: %w", err)
		}
		results = append(results, m)
	}
	return results, rows.Err()
}

// ResourceBreakdowns returns all resource breakdowns by migrability status.
func (p *Parser) ResourceBreakdowns(ctx context.Context, filters Filters) (AllResourceBreakdowns, error) {
	q, err := p.builder.ResourceBreakdownsQuery(filters)
	if err != nil {
		return AllResourceBreakdowns{}, fmt.Errorf("building resource breakdowns query: %w", err)
	}
	var result AllResourceBreakdowns
	if err := p.db.QueryRowContext(ctx, q).Scan(
		&result.CpuCores.Total, &result.CpuCores.TotalForMigratable,
		&result.CpuCores.TotalForMigratableWithWarnings, &result.CpuCores.TotalForNotMigratable,
		&result.RamGB.Total, &result.RamGB.TotalForMigratable,
		&result.RamGB.TotalForMigratableWithWarnings, &result.RamGB.TotalForNotMigratable,
		&result.DiskCount.Total, &result.DiskCount.TotalForMigratable,
		&result.DiskCount.TotalForMigratableWithWarnings, &result.DiskCount.TotalForNotMigratable,
		&result.DiskGB.Total, &result.DiskGB.TotalForMigratable,
		&result.DiskGB.TotalForMigratableWithWarnings, &result.DiskGB.TotalForNotMigratable,
		&result.NicCount.Total, &result.NicCount.TotalForMigratable,
		&result.NicCount.TotalForMigratableWithWarnings, &result.NicCount.TotalForNotMigratable,
	); err != nil {
		return AllResourceBreakdowns{}, fmt.Errorf("scanning resource breakdowns: %w", err)
	}
	return result, nil
}

// VCenterID returns the vCenter UUID.
func (p *Parser) VCenterID(ctx context.Context) (string, error) {
	q, err := p.builder.VCenterQuery()
	if err != nil {
		return "", fmt.Errorf("building vcenter query: %w", err)
	}
	return p.readVCenterID(ctx, q)
}

// DatacenterCount returns count of unique datacenters.
func (p *Parser) DatacenterCount(ctx context.Context) (int, error) {
	q, err := p.builder.DatacenterCountQuery()
	if err != nil {
		return 0, fmt.Errorf("building datacenter count query: %w", err)
	}
	var count int
	if err := p.db.QueryRowContext(ctx, q).Scan(&count); err != nil {
		return 0, fmt.Errorf("scanning datacenter count: %w", err)
	}
	return count, nil
}

// ClustersPerDatacenter returns cluster count per datacenter.
func (p *Parser) ClustersPerDatacenter(ctx context.Context) ([]int, error) {
	q, err := p.builder.ClustersPerDatacenterQuery()
	if err != nil {
		return nil, fmt.Errorf("building clusters per datacenter query: %w", err)
	}
	var counts []int
	rows, err := p.db.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("querying clusters per datacenter: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var dc string
		var count int
		if err := rows.Scan(&dc, &count); err != nil {
			return nil, fmt.Errorf("scanning clusters per datacenter: %w", err)
		}
		counts = append(counts, count)
	}
	return counts, rows.Err()
}

// Helper methods for reading common result types.

func (p *Parser) readStateCountMap(ctx context.Context, query string) (map[string]int, error) {
	result := make(map[string]int)
	rows, err := p.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("querying state counts: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var state string
		var count int
		if err := rows.Scan(&state, &count); err != nil {
			return nil, fmt.Errorf("scanning state count: %w", err)
		}
		result[state] = count
	}
	return result, rows.Err()
}

func (p *Parser) readTierCountMap(ctx context.Context, query string) (map[string]int, error) {
	result := make(map[string]int)
	rows, err := p.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("querying tier counts: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var tier string
		var count int
		if err := rows.Scan(&tier, &count); err != nil {
			return nil, fmt.Errorf("scanning tier count: %w", err)
		}
		result[tier] = count
	}
	return result, rows.Err()
}

func (p *Parser) Parse(ctx context.Context) (models.Inventory, error) {
	// Build queries based on available tables/columns
	queries, err := p.builder.Build()
	if err != nil {
		return models.Inventory{}, fmt.Errorf("building queries: %w", err)
	}

	// Read all data
	datastores, err := p.readDatastores(ctx, queries[Datastore])
	if err != nil {
		return models.Inventory{}, fmt.Errorf("reading datastores: %w", err)
	}

	hosts, err := p.readHosts(ctx, queries[Host])
	if err != nil {
		return models.Inventory{}, fmt.Errorf("reading hosts: %w", err)
	}

	networks, err := p.readNetworks(ctx, queries[Network])
	if err != nil {
		return models.Inventory{}, fmt.Errorf("reading networks: %w", err)
	}

	vms, err := p.readVMs(ctx, queries[VM])
	if err != nil {
		return models.Inventory{}, fmt.Errorf("reading VMs: %w", err)
	}

	osSummary, err := p.readOs(ctx, queries[Os])
	if err != nil {
		return models.Inventory{}, fmt.Errorf("reading OS summary: %w", err)
	}

	vcenterID, err := p.readVCenterID(ctx, queries[VCenter])
	if err != nil {
		return models.Inventory{}, fmt.Errorf("reading vCenter ID: %w", err)
	}

	// Build inventory grouped by cluster
	inventory := p.buildInventory(vcenterID, datastores, hosts, networks, vms, osSummary)

	return inventory, nil
}

func (p *Parser) createSchema() error {
	q, err := p.builder.CreateSchemaQuery()
	if err != nil {
		return err
	}
	_, err = p.db.Exec(q)
	return err
}

func (p *Parser) readDatastores(ctx context.Context, query string) ([]models.Datastore, error) {
	var results []models.Datastore
	if err := sqlscan.Select(ctx, p.db, &results, query); err != nil {
		return nil, fmt.Errorf("scanning datastores: %w", err)
	}
	return results, nil
}

func (p *Parser) readHosts(ctx context.Context, query string) ([]models.Host, error) {
	var results []models.Host
	if err := sqlscan.Select(ctx, p.db, &results, query); err != nil {
		return nil, fmt.Errorf("scanning hosts: %w", err)
	}
	return results, nil
}

func (p *Parser) readNetworks(ctx context.Context, query string) ([]models.Network, error) {
	var results []models.Network
	if err := sqlscan.Select(ctx, p.db, &results, query); err != nil {
		return nil, fmt.Errorf("scanning networks: %w", err)
	}
	return results, nil
}

func (p *Parser) readOs(ctx context.Context, query string) ([]models.Os, error) {
	var results []models.Os
	if err := sqlscan.Select(ctx, p.db, &results, query); err != nil {
		return nil, fmt.Errorf("scanning OS: %w", err)
	}
	return results, nil
}

func (p *Parser) readVCenterID(ctx context.Context, query string) (string, error) {
	var vcenterID string
	row := p.db.QueryRowContext(ctx, query)
	if err := row.Scan(&vcenterID); err != nil {
		return "", fmt.Errorf("failed to find vcenter ID. it should be present: %s", query)
	}
	return vcenterID, nil
}

func (p *Parser) readVMs(ctx context.Context, query string) ([]models.VM, error) {
	rows, err := p.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("querying VMs: %w", err)
	}
	defer rows.Close()

	var vms []models.VM
	for rows.Next() {
		var vm models.VM
		if err := rows.Scan(
			&vm.ID,
			&vm.Name,
			&vm.Folder,
			&vm.Host,
			&vm.UUID,
			&vm.Firmware,
			&vm.PowerState,
			&vm.ConnectionState,
			&vm.FaultToleranceEnabled,
			&vm.CpuCount,
			&vm.MemoryMB,
			&vm.GuestName,
			&vm.GuestNameFromVmwareTools,
			&vm.HostName,
			&vm.IpAddress,
			&vm.StorageUsed,
			&vm.IsTemplate,
			&vm.ChangeTrackingEnabled,
			&vm.DiskEnableUuid,
			&vm.Datacenter,
			&vm.Cluster,
			&vm.HWVersion,
			&vm.TotalDiskCapacityMiB,
			&vm.ProvisionedMiB,
			&vm.ResourcePool,
			&vm.CpuHotAddEnabled,
			&vm.CpuHotRemoveEnabled,
			&vm.CpuSockets,
			&vm.CoresPerSocket,
			&vm.MemoryHotAddEnabled,
			&vm.BalloonedMemory,
			&vm.Disks,
			&vm.NICs,
			&vm.Networks,
			&vm.Concerns,
		); err != nil {
			return nil, fmt.Errorf("scanning VM row: %w", err)
		}
		vms = append(vms, vm)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating VM rows: %w", err)
	}
	return vms, nil
}

func (p *Parser) buildInventory(vcenterID string, datastores []models.Datastore, hosts []models.Host, networks []models.Network, vms []models.VM, osSummary []models.Os) models.Inventory {
	clusterMap := make(map[string]*models.InventoryData)

	// Group datastores by cluster
	for _, ds := range datastores {
		cluster := strings.TrimSpace(ds.Cluster)
		if cluster == "" {
			continue
		}
		if _, ok := clusterMap[cluster]; !ok {
			clusterMap[cluster] = &models.InventoryData{}
		}
		clusterMap[cluster].Infra.Datastores = append(clusterMap[cluster].Infra.Datastores, ds)
	}

	// Group hosts by cluster
	for _, h := range hosts {
		cluster := strings.TrimSpace(h.Cluster)
		if cluster == "" {
			continue
		}
		if _, ok := clusterMap[cluster]; !ok {
			clusterMap[cluster] = &models.InventoryData{}
		}
		clusterMap[cluster].Infra.Hosts = append(clusterMap[cluster].Infra.Hosts, h)
		clusterMap[cluster].Infra.TotalHosts++
	}

	// Group networks by cluster
	for _, n := range networks {
		cluster := strings.TrimSpace(n.Cluster)
		if cluster == "" {
			continue
		}
		if _, ok := clusterMap[cluster]; !ok {
			clusterMap[cluster] = &models.InventoryData{}
		}
		clusterMap[cluster].Infra.Networks = append(clusterMap[cluster].Infra.Networks, n)
	}

	// Group VMs by cluster
	for _, vm := range vms {
		cluster := strings.TrimSpace(vm.Cluster)
		if cluster == "" {
			continue
		}
		if _, ok := clusterMap[cluster]; !ok {
			clusterMap[cluster] = &models.InventoryData{}
		}
		clusterMap[cluster].VMs = append(clusterMap[cluster].VMs, vm)
	}

	// Build final inventory
	clusters := make(map[string]models.InventoryData, len(clusterMap))
	for name, data := range clusterMap {
		clusters[name] = *data
	}

	return models.Inventory{
		VcenterId: vcenterID,
		Clusters:  clusters,
		OsSummary: osSummary,
	}
}
