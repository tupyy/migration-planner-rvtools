package parser

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/georgysavva/scany/v2/sqlscan"
	"github.com/tupyy/rvtools/models"
)

type Preprocessor interface {
	Process(db *sql.DB) error
}

type rvToolsPreprocessor struct {
	excelFile string
	builder   *QueryBuilder
}

func (pp *rvToolsPreprocessor) Process(db *sql.DB) error {
	_, err := db.Exec(pp.builder.IngestRvtoolsQuery(pp.excelFile))
	return err
}

type sqlitePreprocessor struct {
	sqliteFile string
	builder    *QueryBuilder
}

func (pp *sqlitePreprocessor) Process(db *sql.DB) error {
	_, err := db.Exec(pp.builder.IngestSqliteQuery(pp.sqliteFile))
	return err
}

func NewRvToolParser(db *sql.DB, excelFile string) *Parser {
	p := NewParser(db)
	return p.WithPreprocessor(&rvToolsPreprocessor{
		builder:   NewBuilder(),
		excelFile: excelFile,
	})
}
func NewSqliteParser(db *sql.DB, sqliteFile string) *Parser {
	p := NewParser(db)
	return p.WithPreprocessor(&sqlitePreprocessor{
		sqliteFile: sqliteFile,
		builder:    NewBuilder(),
	})
}

type Parser struct {
	db            *sql.DB
	builder       *QueryBuilder
	preprocessors []Preprocessor
}

func NewParser(db *sql.DB) *Parser {
	return &Parser{
		db:            db,
		builder:       NewBuilder(),
		preprocessors: []Preprocessor{},
	}
}

func (p *Parser) WithPreprocessor(pp Preprocessor) *Parser {
	p.preprocessors = append(p.preprocessors, pp)
	return p
}

func (p *Parser) Parse() (models.Inventory, error) {
	ctx := context.Background()

	for _, pp := range p.preprocessors {
		if err := pp.Process(p.db); err != nil {
			return models.Inventory{}, fmt.Errorf("failed to preprocess: %v", err)
		}
	}

	// Build schema context from actual data
	schemaCtx, err := p.buildSchemaContext()
	if err != nil {
		return models.Inventory{}, fmt.Errorf("building schema context: %w", err)
	}

	// Build queries based on available tables/columns
	queries, err := p.builder.Build(schemaCtx)
	if err != nil {
		return models.Inventory{}, fmt.Errorf("building queries: %w", err)
	}

	// Read all data
	datastores, err := p.readDatastores(ctx, queries)
	if err != nil {
		return models.Inventory{}, fmt.Errorf("reading datastores: %w", err)
	}

	hosts, err := p.readHosts(ctx, queries)
	if err != nil {
		return models.Inventory{}, fmt.Errorf("reading hosts: %w", err)
	}

	networks, err := p.readNetworks(ctx, queries)
	if err != nil {
		return models.Inventory{}, fmt.Errorf("reading networks: %w", err)
	}

	vms, err := p.readVMs(ctx, queries)
	if err != nil {
		return models.Inventory{}, fmt.Errorf("reading VMs: %w", err)
	}

	osSummary, err := p.readOs(ctx, queries)
	if err != nil {
		return models.Inventory{}, fmt.Errorf("reading OS summary: %w", err)
	}

	vcenterId, err := p.readVCenterId(ctx, queries)
	if err != nil {
		return models.Inventory{}, fmt.Errorf("reading vCenter ID: %w", err)
	}

	// Build inventory grouped by cluster
	inventory := p.buildInventory(vcenterId, datastores, hosts, networks, vms, osSummary)

	return inventory, nil
}

func (p *Parser) buildSchemaContext() (*SchemaContext, error) {
	ctx := &SchemaContext{
		Tables:  make(map[string]bool),
		Columns: make(map[string]map[string]bool),
	}

	// Query all tables
	rows, err := p.db.Query("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'")
	if err != nil {
		return nil, fmt.Errorf("querying tables: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, fmt.Errorf("scanning table name: %w", err)
		}
		ctx.Tables[tableName] = true
		ctx.Columns[tableName] = make(map[string]bool)
	}

	// Query all columns for each table
	colRows, err := p.db.Query("SELECT table_name, column_name FROM information_schema.columns WHERE table_schema = 'main'")
	if err != nil {
		return nil, fmt.Errorf("querying columns: %w", err)
	}
	defer colRows.Close()

	for colRows.Next() {
		var tableName, columnName string
		if err := colRows.Scan(&tableName, &columnName); err != nil {
			return nil, fmt.Errorf("scanning column: %w", err)
		}
		if ctx.Columns[tableName] != nil {
			ctx.Columns[tableName][columnName] = true
		}
	}

	return ctx, nil
}

func (p *Parser) readDatastores(ctx context.Context, queries map[Type]string) ([]models.Datastore, error) {
	q, ok := queries[Datastore]
	if !ok {
		return nil, nil
	}

	var results []models.Datastore
	if err := sqlscan.Select(ctx, p.db, &results, q); err != nil {
		return nil, fmt.Errorf("scanning datastores: %w", err)
	}
	return results, nil
}

func (p *Parser) readHosts(ctx context.Context, queries map[Type]string) ([]models.Host, error) {
	q, ok := queries[Host]
	if !ok {
		return nil, nil
	}

	var results []models.Host
	if err := sqlscan.Select(ctx, p.db, &results, q); err != nil {
		return nil, fmt.Errorf("scanning hosts: %w", err)
	}
	return results, nil
}

func (p *Parser) readNetworks(ctx context.Context, queries map[Type]string) ([]models.Network, error) {
	q, ok := queries[Network]
	if !ok {
		return nil, nil
	}

	var results []models.Network
	if err := sqlscan.Select(ctx, p.db, &results, q); err != nil {
		return nil, fmt.Errorf("scanning networks: %w", err)
	}
	return results, nil
}

func (p *Parser) readOs(ctx context.Context, queries map[Type]string) ([]models.Os, error) {
	q, ok := queries[Os]
	if !ok {
		return nil, nil
	}

	var results []models.Os
	if err := sqlscan.Select(ctx, p.db, &results, q); err != nil {
		return nil, fmt.Errorf("scanning OS: %w", err)
	}
	return results, nil
}

func (p *Parser) readVCenterId(ctx context.Context, queries map[Type]string) (string, error) {
	q, ok := queries[VCenter]
	if !ok {
		return "", nil
	}

	var vcenterId string
	row := p.db.QueryRowContext(ctx, q)
	if err := row.Scan(&vcenterId); err != nil {
		return "", nil // Not an error if no vCenter ID found
	}
	return vcenterId, nil
}

func (p *Parser) readVMs(ctx context.Context, queries map[Type]string) ([]models.VM, error) {
	q, ok := queries[VM]
	if !ok {
		return nil, nil
	}

	rows, err := p.db.QueryContext(ctx, q)
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

func (p *Parser) buildInventory(vcenterId string, datastores []models.Datastore, hosts []models.Host, networks []models.Network, vms []models.VM, osSummary []models.Os) models.Inventory {
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
		VcenterId: vcenterId,
		Clusters:  clusters,
		OsSummary: osSummary,
	}
}
