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
			queries, err := builder.Build()
			Expect(err).NotTo(HaveOccurred())

			q, ok := queries[parser.Host]
			Expect(ok).To(BeTrue())

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
			queries, err := builder.Build()
			Expect(err).NotTo(HaveOccurred())

			q, ok := queries[parser.Network]
			Expect(ok).To(BeTrue())

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
			queries, err := builder.Build()
			Expect(err).NotTo(HaveOccurred())

			q, ok := queries[parser.Os]
			Expect(ok).To(BeTrue())

			rows, err := db.Query(q)
			Expect(err).NotTo(HaveOccurred())
			defer rows.Close()

			var osSummary []struct {
				Name  string
				Count int
			}

			for rows.Next() {
				var os struct {
					Name  string
					Count int
				}
				Expect(rows.Scan(&os.Name, &os.Count)).To(Succeed())
				osSummary = append(osSummary, os)
			}

			Expect(osSummary).To(HaveLen(3))
		})
	})

	Describe("VCenter Query", func() {
		It("returns vCenter UUID", func() {
			queries, err := builder.Build()
			Expect(err).NotTo(HaveOccurred())

			q, ok := queries[parser.VCenter]
			Expect(ok).To(BeTrue())

			var vcenterID string
			err = db.QueryRow(q).Scan(&vcenterID)
			Expect(err).NotTo(HaveOccurred())
			Expect(vcenterID).To(Equal("vcenter-uuid-001"))
		})
	})

	Describe("Datastore Query", func() {
		It("returns datastores with capacity info", func() {
			queries, err := builder.Build()
			Expect(err).NotTo(HaveOccurred())

			q, ok := queries[parser.Datastore]
			Expect(ok).To(BeTrue())

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
		_, err = db.Exec(builder.CreateSchemaQuery())
		Expect(err).NotTo(HaveOccurred())

		// Execute SQLite ingestion (statement by statement to handle errors)
		query, err := builder.IngestSqliteQuery(sqliteTestDB)
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
			queries, err := builder.Build()
			Expect(err).NotTo(HaveOccurred())

			q, ok := queries[parser.Host]
			Expect(ok).To(BeTrue())

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
			queries, err := builder.Build()
			Expect(err).NotTo(HaveOccurred())

			q, ok := queries[parser.Network]
			Expect(ok).To(BeTrue())

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
			queries, err := builder.Build()
			Expect(err).NotTo(HaveOccurred())

			q, ok := queries[parser.VM]
			Expect(ok).To(BeTrue())

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
			queries, err := builder.Build()
			Expect(err).NotTo(HaveOccurred())

			q, ok := queries[parser.Os]
			Expect(ok).To(BeTrue())

			rows, err := db.Query(q)
			Expect(err).NotTo(HaveOccurred())
			defer rows.Close()

			var osSummary []struct {
				Name  string
				Count int
			}

			for rows.Next() {
				var os struct {
					Name  string
					Count int
				}
				Expect(rows.Scan(&os.Name, &os.Count)).To(Succeed())
				osSummary = append(osSummary, os)
			}

			Expect(osSummary).To(HaveLen(3))
		})
	})

	Describe("VCenter Query", func() {
		It("returns vCenter UUID from About table", func() {
			queries, err := builder.Build()
			Expect(err).NotTo(HaveOccurred())

			q, ok := queries[parser.VCenter]
			Expect(ok).To(BeTrue())

			var vcenterID string
			err = db.QueryRow(q).Scan(&vcenterID)
			Expect(err).NotTo(HaveOccurred())
			Expect(vcenterID).To(Equal("vcenter-uuid-001"))
		})
	})

	Describe("Datastore Query", func() {
		It("returns datastores from SQLite", func() {
			queries, err := builder.Build()
			Expect(err).NotTo(HaveOccurred())

			q, ok := queries[parser.Datastore]
			Expect(ok).To(BeTrue())

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
			queries, err := builder.Build()
			Expect(err).NotTo(HaveOccurred())

			q, ok := queries[parser.Host]
			Expect(ok).To(BeTrue())

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
			queries, err := builder.Build()
			Expect(err).NotTo(HaveOccurred())

			q, ok := queries[parser.Network]
			Expect(ok).To(BeTrue())

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
			queries, err := builder.Build()
			Expect(err).NotTo(HaveOccurred())

			q, ok := queries[parser.VM]
			Expect(ok).To(BeTrue())

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
			queries, err := builder.Build()
			Expect(err).NotTo(HaveOccurred())

			q, ok := queries[parser.Os]
			Expect(ok).To(BeTrue())

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
