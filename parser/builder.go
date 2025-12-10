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
func (b *QueryBuilder) CreateSchemaQuery() string {
	return b.buildQuery("create_schema", createSchemaTemplate, nil)
}

// IngestRvtoolsQuery returns a query that inserts data from an RVTools Excel file into schema tables.
func (b *QueryBuilder) IngestRvtoolsQuery(filePath string) string {
	return b.buildQuery("ingest_rvtools", ingestRvtoolsTemplate, ingestParams{FilePath: filePath})
}

// IngestSqliteQuery returns a query that creates RVTools-shaped tables from a forklift SQLite database.
func (b *QueryBuilder) IngestSqliteQuery(filePath string) string {
	return b.buildQuery("ingest_sqlite", ingestSqliteTemplate, ingestParams{FilePath: filePath})
}

// Build generates all SQL queries based on the schema context.
func (b *QueryBuilder) Build() (map[Type]string, error) {
	queries := make(map[Type]string)

	queries[VM] = b.buildVMQuery()
	queries[Os] = b.buildQuery("os_query", osQueryTemplate, nil)
	queries[VCenter] = b.buildQuery("vcenter_query", vcenterQueryTemplate, nil)
	queries[Datastore] = b.buildDatastoreQuery()
	queries[Network] = b.buildNetworkQuery()
	queries[Host] = b.buildQuery("host_query", hostQueryTemplate, nil)

	return queries, nil
}

type vmQueryParams struct {
	NetworkColumns string
}

func (b *QueryBuilder) buildVMQuery() string {
	quoted := make([]string, 0, 25)
	for i := 1; i <= 25; i++ {
		quoted = append(quoted, fmt.Sprintf(`i."Network #%d"`, i))
	}
	networkColumns := strings.Join(quoted, ", ")

	return b.buildQuery("vm_query", vmQueryTemplate, vmQueryParams{NetworkColumns: networkColumns})
}

func (b *QueryBuilder) buildDatastoreQuery() string {
	return b.buildQuery("datastore_query", datastoreQueryTemplate, nil)
}

func (b *QueryBuilder) buildNetworkQuery() string {
	return b.buildQuery("network_query", networkQueryTemplate, nil)
}

func (b *QueryBuilder) buildQuery(name, tmplContent string, params any) string {
	tmpl, err := template.New(name).Parse(tmplContent)
	if err != nil {
		return ""
	}
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, params); err != nil {
		return ""
	}
	return strings.TrimSpace(buf.String())
}
