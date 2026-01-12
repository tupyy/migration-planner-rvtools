package parser_test

import (
	"database/sql"
	_ "embed"
	"os"
	"regexp"
	"strings"

	"github.com/duckdb/duckdb-go/v2"
	_ "github.com/glebarez/go-sqlite"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/tupyy/rvtools/parser"
)

//go:embed testdata/fixtures.sql
var fixturesSQL string

//go:embed testdata/fixtures_incomplete.sql
var fixturesIncompleteSQL string

//go:embed testdata/create_forklift_sqlite.sql
var createForkliftSQL string

const sqliteTestDB = "/tmp/forklift_test.db"

func setupDB(sqlFixtures string) *sql.DB {
	c, err := duckdb.NewConnector("", nil)
	Expect(err).NotTo(HaveOccurred())

	db := sql.OpenDB(c)
	Expect(db).NotTo(BeNil())

	_, err = db.Exec(sqlFixtures)
	Expect(err).NotTo(HaveOccurred())

	return db
}

func tableExists(db *sql.DB, tableName string) bool {
	rows, err := db.Query("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main' AND table_name = ?", tableName)
	if err != nil {
		return false
	}
	defer rows.Close()
	return rows.Next()
}

var _ = Describe("QueryBuilder", func() {
	var (
		db      *sql.DB
		builder *parser.QueryBuilder
	)

	BeforeEach(func() {
		db = setupDB(fixturesSQL)
		builder = parser.NewBuilder()
	})

	AfterEach(func() {
		if db != nil {
			db.Close()
		}
	})

	Describe("Host Query", func() {
		It("returns all hosts with correct fields", func() {
			q, err := builder.HostQuery(parser.Filters{}, parser.Options{})
			Expect(err).NotTo(HaveOccurred())

			rows, err := db.Query(q)
			Expect(err).NotTo(HaveOccurred())
			defer rows.Close()

			var hosts []struct {
				Cluster    string
				CpuCores   int
				CpuSockets int
				ID         string
				MemoryMB   int
				Model      string
				Vendor     string
			}

			for rows.Next() {
				var h struct {
					Cluster    string
					CpuCores   int
					CpuSockets int
					ID         string
					MemoryMB   int
					Model      string
					Vendor     string
				}
				Expect(rows.Scan(&h.Cluster, &h.CpuCores, &h.CpuSockets, &h.ID, &h.MemoryMB, &h.Model, &h.Vendor)).To(Succeed())
				hosts = append(hosts, h)
			}

			Expect(hosts).To(HaveLen(2))
			Expect(hosts[0].Cluster).To(Equal("TestCluster"))
			Expect(hosts[0].CpuCores).To(Equal(16))
			Expect(hosts[0].CpuSockets).To(Equal(2))
			Expect(hosts[0].Model).To(Equal("ProLiant DL380 Gen10"))
		})
	})

	Describe("Network Query", func() {
		It("returns networks with VM counts", func() {
			q, err := builder.NetworkQuery(parser.Filters{}, parser.Options{})
			Expect(err).NotTo(HaveOccurred())

			rows, err := db.Query(q)
			Expect(err).NotTo(HaveOccurred())
			defer rows.Close()

			var networks []struct {
				Cluster  string
				Dvswitch string
				Name     string
				Type     string
				VlanId   string
				VmsCount int
			}

			for rows.Next() {
				var n struct {
					Cluster  string
					Dvswitch string
					Name     string
					Type     string
					VlanId   string
					VmsCount int
				}
				Expect(rows.Scan(&n.Cluster, &n.Dvswitch, &n.Name, &n.Type, &n.VlanId, &n.VmsCount)).To(Succeed())
				networks = append(networks, n)
			}

			Expect(len(networks)).To(BeNumerically(">=", 2))

			var totalVMNetworkCount int
			for _, n := range networks {
				if n.Name == "VM Network" {
					totalVMNetworkCount += n.VmsCount
				}
			}
			Expect(totalVMNetworkCount).To(Equal(2))
		})
	})

	Describe("OS Query", func() {
		It("returns OS distribution summary", func() {
			q, err := builder.OsQuery(parser.Filters{})
			Expect(err).NotTo(HaveOccurred())

			rows, err := db.Query(q)
			Expect(err).NotTo(HaveOccurred())
			defer rows.Close()

			var osSummary []struct {
				Name      string
				Count     int
				Supported bool
			}

			for rows.Next() {
				var os struct {
					Name      string
					Count     int
					Supported bool
				}
				Expect(rows.Scan(&os.Name, &os.Count, &os.Supported)).To(Succeed())
				osSummary = append(osSummary, os)
			}

			Expect(osSummary).To(HaveLen(3))
		})
	})

	Describe("VCenter Query", func() {
		It("returns vCenter UUID", func() {
			q, err := builder.VCenterQuery()
			Expect(err).NotTo(HaveOccurred())

			var vcenterID string
			err = db.QueryRow(q).Scan(&vcenterID)
			Expect(err).NotTo(HaveOccurred())
			Expect(vcenterID).To(Equal("vcenter-uuid-001"))
		})
	})

	Describe("Datastore Query", func() {
		It("returns datastores with capacity info", func() {
			q, err := builder.DatastoreQuery(parser.Filters{}, parser.Options{})
			Expect(err).NotTo(HaveOccurred())

			rows, err := db.Query(q)
			Expect(err).NotTo(HaveOccurred())
			defer rows.Close()

			var datastores []struct {
				Cluster         string
				DiskId          string
				FreeCapacityGB  int
				HWAccelMove     bool
				HostId          string
				Model           string
				ProtocolType    string
				TotalCapacityGB int
				Type            string
				Vendor          string
			}

			for rows.Next() {
				var ds struct {
					Cluster         string
					DiskId          string
					FreeCapacityGB  int
					HWAccelMove     bool
					HostId          string
					Model           string
					ProtocolType    string
					TotalCapacityGB int
					Type            string
					Vendor          string
				}
				Expect(rows.Scan(&ds.Cluster, &ds.DiskId, &ds.FreeCapacityGB, &ds.HWAccelMove, &ds.HostId, &ds.Model, &ds.ProtocolType, &ds.TotalCapacityGB, &ds.Type, &ds.Vendor)).To(Succeed())
				datastores = append(datastores, ds)
			}

			Expect(len(datastores)).To(BeNumerically(">=", 1))
		})
	})
})

var _ = Describe("SQLite ingestion", func() {
	var (
		db      *sql.DB
		builder *parser.QueryBuilder
	)

	BeforeEach(func() {
		// Create SQLite test DB from embedded SQL
		os.Remove(sqliteTestDB)
		sqliteDB, err := sql.Open("sqlite", sqliteTestDB)
		Expect(err).NotTo(HaveOccurred())
		_, err = sqliteDB.Exec(createForkliftSQL)
		Expect(err).NotTo(HaveOccurred())
		sqliteDB.Close()

		c, err := duckdb.NewConnector("", nil)
		Expect(err).NotTo(HaveOccurred())

		db = sql.OpenDB(c)
		Expect(db).NotTo(BeNil())

		builder = parser.NewBuilder()

		// Create schema first
		schemaQuery, err := builder.CreateSchemaQuery()
		Expect(err).NotTo(HaveOccurred())
		_, err = db.Exec(schemaQuery)
		Expect(err).NotTo(HaveOccurred())

		// Execute SQLite ingestion (statement by statement to handle errors)
		query, err := builder.IngestSqliteQuery(sqliteTestDB)
		Expect(err).NotTo(HaveOccurred())
		stmtRegex := regexp.MustCompile(`(?s)(CREATE|INSERT|UPDATE|DROP|WITH|INSTALL|LOAD|ATTACH|DETACH).*?;`)
		stmts := stmtRegex.FindAllString(query, -1)
		for _, stmt := range stmts {
			stmt = strings.TrimSpace(stmt)
			if stmt == "" {
				continue
			}
			_, _ = db.Exec(stmt) // Ignore errors for missing tables
		}
	})

	AfterEach(func() {
		if db != nil {
			db.Close()
		}
	})

	It("creates all RVTools tables", func() {
		Expect(tableExists(db, "vinfo")).To(BeTrue())
		Expect(tableExists(db, "vcpu")).To(BeTrue())
		Expect(tableExists(db, "vmemory")).To(BeTrue())
		Expect(tableExists(db, "vdisk")).To(BeTrue())
		Expect(tableExists(db, "vnetwork")).To(BeTrue())
		Expect(tableExists(db, "vhost")).To(BeTrue())
		Expect(tableExists(db, "vdatastore")).To(BeTrue())
		Expect(tableExists(db, "dvport")).To(BeTrue())
		Expect(tableExists(db, "vhba")).To(BeTrue())
		Expect(tableExists(db, "concerns")).To(BeTrue())
	})

	Describe("Host Query", func() {
		It("returns hosts from SQLite", func() {
			q, err := builder.HostQuery(parser.Filters{}, parser.Options{})
			Expect(err).NotTo(HaveOccurred())

			rows, err := db.Query(q)
			Expect(err).NotTo(HaveOccurred())
			defer rows.Close()

			var hosts []struct {
				Cluster    string
				CpuCores   int
				CpuSockets int
				ID         string
				MemoryMB   int
				Model      string
				Vendor     string
			}

			for rows.Next() {
				var h struct {
					Cluster    string
					CpuCores   int
					CpuSockets int
					ID         string
					MemoryMB   int
					Model      string
					Vendor     string
				}
				Expect(rows.Scan(&h.Cluster, &h.CpuCores, &h.CpuSockets, &h.ID, &h.MemoryMB, &h.Model, &h.Vendor)).To(Succeed())
				hosts = append(hosts, h)
			}

			Expect(hosts).To(HaveLen(2))
			Expect(hosts[0].Cluster).To(Equal("TestCluster"))
			Expect(hosts[0].Model).To(Equal("ProLiant DL380 Gen10"))
			Expect(hosts[0].Vendor).To(Equal("HPE"))
		})
	})

	Describe("Network Query", func() {
		It("returns networks with VLAN from dvport", func() {
			q, err := builder.NetworkQuery(parser.Filters{}, parser.Options{})
			Expect(err).NotTo(HaveOccurred())

			rows, err := db.Query(q)
			Expect(err).NotTo(HaveOccurred())
			defer rows.Close()

			var networks []struct {
				Cluster  string
				Dvswitch string
				Name     string
				Type     string
				VlanId   string
				VmsCount int
			}

			for rows.Next() {
				var n struct {
					Cluster  string
					Dvswitch string
					Name     string
					Type     string
					VlanId   string
					VmsCount int
				}
				Expect(rows.Scan(&n.Cluster, &n.Dvswitch, &n.Name, &n.Type, &n.VlanId, &n.VmsCount)).To(Succeed())
				networks = append(networks, n)
			}

			Expect(len(networks)).To(BeNumerically(">=", 1))
		})
	})

	Describe("VM Query", func() {
		It("returns VMs from SQLite", func() {
			q, err := builder.VMQuery(parser.Filters{}, parser.Options{})
			Expect(err).NotTo(HaveOccurred())

			rows, err := db.Query(q)
			Expect(err).NotTo(HaveOccurred())
			defer rows.Close()

			var count int
			for rows.Next() {
				count++
			}
			Expect(count).To(Equal(3))
		})
	})

	Describe("OS Query", func() {
		It("returns OS distribution from SQLite", func() {
			q, err := builder.OsQuery(parser.Filters{})
			Expect(err).NotTo(HaveOccurred())

			rows, err := db.Query(q)
			Expect(err).NotTo(HaveOccurred())
			defer rows.Close()

			var osSummary []struct {
				Name      string
				Count     int
				Supported bool
			}

			for rows.Next() {
				var os struct {
					Name      string
					Count     int
					Supported bool
				}
				Expect(rows.Scan(&os.Name, &os.Count, &os.Supported)).To(Succeed())
				osSummary = append(osSummary, os)
			}

			Expect(osSummary).To(HaveLen(3))
		})
	})

	Describe("VCenter Query", func() {
		It("returns vCenter UUID from About table", func() {
			q, err := builder.VCenterQuery()
			Expect(err).NotTo(HaveOccurred())

			var vcenterID string
			err = db.QueryRow(q).Scan(&vcenterID)
			Expect(err).NotTo(HaveOccurred())
			Expect(vcenterID).To(Equal("vcenter-uuid-001"))
		})
	})

	Describe("Datastore Query", func() {
		It("returns datastores from SQLite", func() {
			q, err := builder.DatastoreQuery(parser.Filters{}, parser.Options{})
			Expect(err).NotTo(HaveOccurred())

			rows, err := db.Query(q)
			Expect(err).NotTo(HaveOccurred())
			defer rows.Close()

			var count int
			for rows.Next() {
				count++
			}
			Expect(count).To(BeNumerically(">=", 1))
		})
	})
})

var _ = Describe("QueryBuilder with incomplete fixtures", func() {
	var (
		db      *sql.DB
		builder *parser.QueryBuilder
	)

	BeforeEach(func() {
		db = setupDB(fixturesIncompleteSQL)
		builder = parser.NewBuilder()
	})

	AfterEach(func() {
		if db != nil {
			db.Close()
		}
	})

	It("has dvport table (empty)", func() {
		Expect(tableExists(db, "dvport")).To(BeTrue())
	})

	It("has vhba table (empty)", func() {
		Expect(tableExists(db, "vhba")).To(BeTrue())
	})

	Describe("Host Query", func() {
		It("returns hosts without HBA data", func() {
			q, err := builder.HostQuery(parser.Filters{}, parser.Options{})
			Expect(err).NotTo(HaveOccurred())

			rows, err := db.Query(q)
			Expect(err).NotTo(HaveOccurred())
			defer rows.Close()

			var count int
			for rows.Next() {
				count++
			}
			Expect(count).To(Equal(1))
		})
	})

	Describe("Network Query", func() {
		It("returns networks without VLAN data from dvport", func() {
			q, err := builder.NetworkQuery(parser.Filters{}, parser.Options{})
			Expect(err).NotTo(HaveOccurred())

			rows, err := db.Query(q)
			Expect(err).NotTo(HaveOccurred())
			defer rows.Close()

			var networks []struct {
				Cluster  string
				Dvswitch string
				Name     string
				Type     string
				VlanId   string
				VmsCount int
			}

			for rows.Next() {
				var n struct {
					Cluster  string
					Dvswitch string
					Name     string
					Type     string
					VlanId   string
					VmsCount int
				}
				Expect(rows.Scan(&n.Cluster, &n.Dvswitch, &n.Name, &n.Type, &n.VlanId, &n.VmsCount)).To(Succeed())
				networks = append(networks, n)
			}

			Expect(len(networks)).To(BeNumerically(">=", 1))
			// VlanId should be empty since dvport table is missing
			for _, n := range networks {
				Expect(n.VlanId).To(Equal(""))
			}
		})
	})

	Describe("VM Query", func() {
		It("returns VMs", func() {
			q, err := builder.VMQuery(parser.Filters{}, parser.Options{})
			Expect(err).NotTo(HaveOccurred())

			rows, err := db.Query(q)
			Expect(err).NotTo(HaveOccurred())
			defer rows.Close()

			var count int
			for rows.Next() {
				count++
			}
			Expect(count).To(Equal(2))
		})
	})

	Describe("OS Query", func() {
		It("returns OS distribution", func() {
			q, err := builder.OsQuery(parser.Filters{})
			Expect(err).NotTo(HaveOccurred())

			rows, err := db.Query(q)
			Expect(err).NotTo(HaveOccurred())
			defer rows.Close()

			var count int
			for rows.Next() {
				count++
			}
			Expect(count).To(Equal(2))
		})
	})
})

var _ = Describe("New Parser API", func() {
	var (
		db *sql.DB
		p  *parser.Parser
	)

	BeforeEach(func() {
		db = setupDB(fixturesSQL)
		p = parser.New(db, nil)
		Expect(p).NotTo(BeNil())
	})

	AfterEach(func() {
		if db != nil {
			db.Close()
		}
	})

	Describe("VMs", func() {
		It("returns all VMs without filters", func() {
			ctx := GinkgoT().Context()
			vms, err := p.VMs(ctx, parser.Filters{}, parser.Options{})
			Expect(err).NotTo(HaveOccurred())
			Expect(len(vms)).To(Equal(3))
		})

		It("filters VMs by cluster", func() {
			ctx := GinkgoT().Context()
			vms, err := p.VMs(ctx, parser.Filters{Cluster: "TestCluster"}, parser.Options{})
			Expect(err).NotTo(HaveOccurred())
			Expect(len(vms)).To(Equal(3))
			for _, vm := range vms {
				Expect(vm.Cluster).To(Equal("TestCluster"))
			}
		})

		It("filters VMs by OS", func() {
			ctx := GinkgoT().Context()
			vms, err := p.VMs(ctx, parser.Filters{OS: "Windows"}, parser.Options{})
			Expect(err).NotTo(HaveOccurred())
			Expect(len(vms)).To(Equal(1))
			Expect(vms[0].GuestName).To(ContainSubstring("Windows"))
		})

		It("filters VMs by power state", func() {
			ctx := GinkgoT().Context()
			vms, err := p.VMs(ctx, parser.Filters{PowerState: "poweredOn"}, parser.Options{})
			Expect(err).NotTo(HaveOccurred())
			for _, vm := range vms {
				Expect(vm.PowerState).To(Equal("poweredOn"))
			}
		})

		It("applies pagination with limit", func() {
			ctx := GinkgoT().Context()
			vms, err := p.VMs(ctx, parser.Filters{}, parser.Options{Limit: 1})
			Expect(err).NotTo(HaveOccurred())
			Expect(len(vms)).To(Equal(1))
		})

		It("applies pagination with offset", func() {
			ctx := GinkgoT().Context()
			vms, err := p.VMs(ctx, parser.Filters{}, parser.Options{Limit: 1, Offset: 1})
			Expect(err).NotTo(HaveOccurred())
			Expect(len(vms)).To(Equal(1))
		})
	})

	Describe("VMCount", func() {
		It("returns total VM count", func() {
			ctx := GinkgoT().Context()
			count, err := p.VMCount(ctx, parser.Filters{})
			Expect(err).NotTo(HaveOccurred())
			Expect(count).To(Equal(3))
		})

		It("filters by cluster", func() {
			ctx := GinkgoT().Context()
			count, err := p.VMCount(ctx, parser.Filters{Cluster: "TestCluster"})
			Expect(err).NotTo(HaveOccurred())
			Expect(count).To(Equal(3))
		})
	})

	Describe("Hosts", func() {
		It("returns all hosts", func() {
			ctx := GinkgoT().Context()
			hosts, err := p.Hosts(ctx, parser.Filters{}, parser.Options{})
			Expect(err).NotTo(HaveOccurred())
			Expect(len(hosts)).To(BeNumerically(">=", 1))
		})

		It("filters by cluster", func() {
			ctx := GinkgoT().Context()
			hosts, err := p.Hosts(ctx, parser.Filters{Cluster: "TestCluster"}, parser.Options{})
			Expect(err).NotTo(HaveOccurred())
			for _, h := range hosts {
				Expect(h.Cluster).To(Equal("TestCluster"))
			}
		})
	})

	Describe("Networks", func() {
		It("returns all networks", func() {
			ctx := GinkgoT().Context()
			networks, err := p.Networks(ctx, parser.Filters{}, parser.Options{})
			Expect(err).NotTo(HaveOccurred())
			Expect(len(networks)).To(BeNumerically(">=", 1))
		})

		It("filters by cluster", func() {
			ctx := GinkgoT().Context()
			networks, err := p.Networks(ctx, parser.Filters{Cluster: "TestCluster"}, parser.Options{})
			Expect(err).NotTo(HaveOccurred())
			for _, n := range networks {
				Expect(n.Cluster).To(Equal("TestCluster"))
			}
		})
	})

	Describe("Datastores", func() {
		It("returns all datastores", func() {
			ctx := GinkgoT().Context()
			datastores, err := p.Datastores(ctx, parser.Filters{}, parser.Options{})
			Expect(err).NotTo(HaveOccurred())
			Expect(len(datastores)).To(BeNumerically(">=", 1))
		})
	})

	Describe("Clusters", func() {
		It("returns unique cluster names", func() {
			ctx := GinkgoT().Context()
			clusters, err := p.Clusters(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(clusters).To(ContainElement("TestCluster"))
		})
	})

	Describe("OsSummary", func() {
		It("returns OS distribution", func() {
			ctx := GinkgoT().Context()
			osSummary, err := p.OsSummary(ctx, parser.Filters{})
			Expect(err).NotTo(HaveOccurred())
			Expect(len(osSummary)).To(BeNumerically(">=", 1))
		})
	})

	Describe("PowerStateCounts", func() {
		It("returns power state distribution", func() {
			ctx := GinkgoT().Context()
			counts, err := p.PowerStateCounts(ctx, parser.Filters{})
			Expect(err).NotTo(HaveOccurred())
			Expect(counts).NotTo(BeNil())
			// Should have poweredOn and poweredOff (3 VMs total)
			total := 0
			for _, c := range counts {
				total += c
			}
			Expect(total).To(Equal(3))
		})
	})

	Describe("TotalResources", func() {
		It("returns resource totals", func() {
			ctx := GinkgoT().Context()
			resources, err := p.TotalResources(ctx, parser.Filters{})
			Expect(err).NotTo(HaveOccurred())
			Expect(resources.TotalCPUCores).To(BeNumerically(">=", 0))
			Expect(resources.TotalRAMGB).To(BeNumerically(">=", 0))
		})
	})

	Describe("VCenterID", func() {
		It("returns vCenter UUID", func() {
			ctx := GinkgoT().Context()
			vcenterID, err := p.VCenterID(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(vcenterID).NotTo(BeEmpty())
		})
	})

	Describe("Concern-based aggregations", func() {
		It("returns migratable VM count (all VMs when no concerns)", func() {
			ctx := GinkgoT().Context()
			count, err := p.MigratableVMCount(ctx, parser.Filters{})
			Expect(err).NotTo(HaveOccurred())
			// With no concerns, all 3 VMs are migratable
			Expect(count).To(Equal(3))
		})

		It("returns migratable with warnings count (0 when no concerns)", func() {
			ctx := GinkgoT().Context()
			count, err := p.MigratableWithWarningsVMCount(ctx, parser.Filters{})
			Expect(err).NotTo(HaveOccurred())
			Expect(count).To(Equal(0))
		})

		It("returns not migratable count (0 when no concerns)", func() {
			ctx := GinkgoT().Context()
			count, err := p.NotMigratableVMCount(ctx, parser.Filters{})
			Expect(err).NotTo(HaveOccurred())
			Expect(count).To(Equal(0))
		})
	})

	Describe("ResourceBreakdowns", func() {
		It("returns all resources as migratable when no concerns", func() {
			ctx := GinkgoT().Context()
			breakdowns, err := p.ResourceBreakdowns(ctx, parser.Filters{})
			Expect(err).NotTo(HaveOccurred())

			// With no concerns, all resources are migratable
			// Total = Migratable, MigratableWithWarnings = 0, NotMigratable = 0
			Expect(breakdowns.CpuCores.Total).To(Equal(7))
			Expect(breakdowns.CpuCores.TotalForMigratable).To(Equal(7))
			Expect(breakdowns.CpuCores.TotalForMigratableWithWarnings).To(Equal(0))
			Expect(breakdowns.CpuCores.TotalForNotMigratable).To(Equal(0))

			Expect(breakdowns.RamGB.Total).To(Equal(14))
			Expect(breakdowns.RamGB.TotalForMigratable).To(Equal(14))
		})
	})
})

var _ = Describe("Parser API with concerns", func() {
	var (
		db *sql.DB
		p  *parser.Parser
	)

	BeforeEach(func() {
		db = setupDB(fixturesSQL)

		// Insert some concerns for testing
		_, err := db.Exec(`
			INSERT INTO concerns ("VM_ID", "Concern_ID", "Label", "Category", "Assessment") VALUES
			('vm-001', 'concern-1', 'Test Warning', 'Warning', 'Some assessment'),
			('vm-002', 'concern-2', 'Test Critical', 'Critical', 'Critical issue')
		`)
		Expect(err).NotTo(HaveOccurred())

		p = parser.New(db, nil)
		Expect(p).NotTo(BeNil())
	})

	AfterEach(func() {
		if db != nil {
			db.Close()
		}
	})

	Describe("VMs with concerns", func() {
		It("includes concerns in VM results", func() {
			ctx := GinkgoT().Context()
			vms, err := p.VMs(ctx, parser.Filters{}, parser.Options{})
			Expect(err).NotTo(HaveOccurred())

			// Find vm-001 and check concerns
			for _, vm := range vms {
				if vm.ID == "vm-001" {
					Expect(len(vm.Concerns)).To(BeNumerically(">=", 1))
				}
			}
		})
	})

	Describe("MigratableVMCount", func() {
		It("excludes VMs with Critical concerns", func() {
			ctx := GinkgoT().Context()
			count, err := p.MigratableVMCount(ctx, parser.Filters{})
			Expect(err).NotTo(HaveOccurred())
			// vm-002 has Critical, so vm-001 and vm-003 are migratable (2 VMs)
			Expect(count).To(Equal(2))
		})
	})

	Describe("MigratableWithWarningsVMCount", func() {
		It("counts VMs with Warning but no Critical", func() {
			ctx := GinkgoT().Context()
			count, err := p.MigratableWithWarningsVMCount(ctx, parser.Filters{})
			Expect(err).NotTo(HaveOccurred())
			// vm-001 has Warning only
			Expect(count).To(Equal(1))
		})
	})

	Describe("NotMigratableVMCount", func() {
		It("counts VMs with Critical concerns", func() {
			ctx := GinkgoT().Context()
			count, err := p.NotMigratableVMCount(ctx, parser.Filters{})
			Expect(err).NotTo(HaveOccurred())
			// vm-002 has Critical
			Expect(count).To(Equal(1))
		})
	})

	Describe("MigrationIssues", func() {
		It("returns aggregated Warning issues", func() {
			ctx := GinkgoT().Context()
			issues, err := p.MigrationIssues(ctx, parser.Filters{}, "Warning")
			Expect(err).NotTo(HaveOccurred())
			Expect(len(issues)).To(Equal(1))
			Expect(issues[0].Label).To(Equal("Test Warning"))
			Expect(issues[0].Count).To(Equal(1))
		})

		It("returns aggregated Critical issues", func() {
			ctx := GinkgoT().Context()
			issues, err := p.MigrationIssues(ctx, parser.Filters{}, "Critical")
			Expect(err).NotTo(HaveOccurred())
			Expect(len(issues)).To(Equal(1))
			Expect(issues[0].Label).To(Equal("Test Critical"))
		})
	})

	Describe("ResourceBreakdowns", func() {
		It("returns resource breakdowns by migrability", func() {
			ctx := GinkgoT().Context()
			breakdowns, err := p.ResourceBreakdowns(ctx, parser.Filters{})
			Expect(err).NotTo(HaveOccurred())

			// CPU Cores: vm-001 (4 CPUs, Warning), vm-002 (2 CPUs, Critical), vm-003 (1 CPU, no concerns)
			// Total = 7, Migratable (no critical) = 5, MigratableWithWarnings = 4, NotMigratable = 2
			Expect(breakdowns.CpuCores.Total).To(Equal(7))
			Expect(breakdowns.CpuCores.TotalForMigratable).To(Equal(5))             // vm-001 + vm-003
			Expect(breakdowns.CpuCores.TotalForMigratableWithWarnings).To(Equal(4)) // vm-001 only
			Expect(breakdowns.CpuCores.TotalForNotMigratable).To(Equal(2))          // vm-002 only

			// RAM: vm-001 (8GB), vm-002 (4GB), vm-003 (2GB)
			Expect(breakdowns.RamGB.Total).To(Equal(14))
			Expect(breakdowns.RamGB.TotalForMigratable).To(Equal(10))            // vm-001 + vm-003
			Expect(breakdowns.RamGB.TotalForMigratableWithWarnings).To(Equal(8)) // vm-001 only
			Expect(breakdowns.RamGB.TotalForNotMigratable).To(Equal(4))          // vm-002 only

			// Disk count and GB
			Expect(breakdowns.DiskCount.Total).To(BeNumerically(">=", 0))
			Expect(breakdowns.DiskGB.Total).To(BeNumerically(">=", 0))

			// NIC count
			Expect(breakdowns.NicCount.Total).To(BeNumerically(">=", 0))
		})
	})
})
