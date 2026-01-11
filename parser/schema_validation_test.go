package parser_test

import (
	"database/sql"

	"github.com/duckdb/duckdb-go/v2"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/tupyy/rvtools/parser"
)

var _ = Describe("Schema Validation", func() {
	var (
		db      *sql.DB
		p       *parser.Parser
		builder *parser.QueryBuilder
	)

	BeforeEach(func() {
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

		p = parser.New(db, nil)
	})

	AfterEach(func() {
		if db != nil {
			db.Close()
		}
	})

	Describe("ValidateSchema with empty tables", func() {
		It("returns error when vinfo has no VMs", func() {
			ctx := GinkgoT().Context()
			result := p.ValidateSchema(ctx)

			Expect(result.HasErrors()).To(BeTrue())
			Expect(result.IsValid()).To(BeFalse())

			// Should have NO_VMS error
			var foundNoVMs bool
			for _, e := range result.Errors {
				if e.Code == parser.CodeNoVMs {
					foundNoVMs = true
				}
			}
			Expect(foundNoVMs).To(BeTrue(), "Expected NO_VMS error")
		})

		It("returns warnings for empty optional tables", func() {
			ctx := GinkgoT().Context()
			result := p.ValidateSchema(ctx)

			Expect(result.HasWarnings()).To(BeTrue())

			// Should have warnings for empty hosts, datastores, networks
			expectedWarnings := []string{
				parser.CodeEmptyHosts,
				parser.CodeEmptyDatastores,
				parser.CodeEmptyNetworks,
				parser.CodeEmptyCPU,
				parser.CodeEmptyMemory,
				parser.CodeEmptyDisks,
				parser.CodeEmptyNICs,
			}

			for _, expected := range expectedWarnings {
				var found bool
				for _, w := range result.Warnings {
					if w.Code == expected {
						found = true
						break
					}
				}
				Expect(found).To(BeTrue(), "Expected warning: "+expected)
			}
		})
	})

	Describe("ValidateSchema with VMs but no other data", func() {
		BeforeEach(func() {
			// Insert a VM into vinfo
			_, err := db.Exec(`
				INSERT INTO vinfo ("VM ID", "VM", "Powerstate", "CPUs", "Memory", "Cluster")
				VALUES ('vm-001', 'TestVM', 'poweredOn', 4, 8192, 'TestCluster')
			`)
			Expect(err).NotTo(HaveOccurred())
		})

		It("is valid (no errors) when vinfo has VMs", func() {
			ctx := GinkgoT().Context()
			result := p.ValidateSchema(ctx)

			Expect(result.HasErrors()).To(BeFalse())
			Expect(result.IsValid()).To(BeTrue())
			Expect(result.Error()).To(BeNil())
		})

		It("still has warnings for empty optional tables", func() {
			ctx := GinkgoT().Context()
			result := p.ValidateSchema(ctx)

			Expect(result.HasWarnings()).To(BeTrue())

			// Should have warning for empty hosts
			var foundEmptyHosts bool
			for _, w := range result.Warnings {
				if w.Code == parser.CodeEmptyHosts {
					foundEmptyHosts = true
					break
				}
			}
			Expect(foundEmptyHosts).To(BeTrue())
		})
	})

	Describe("ValidateSchema with VMs but missing required columns", func() {
		It("returns error when VM ID is empty for all VMs", func() {
			// Insert VM with empty VM ID
			_, err := db.Exec(`
				INSERT INTO vinfo ("VM ID", "VM", "Powerstate", "CPUs", "Memory")
				VALUES ('', 'TestVM', 'poweredOn', 4, 8192)
			`)
			Expect(err).NotTo(HaveOccurred())

			ctx := GinkgoT().Context()
			result := p.ValidateSchema(ctx)

			Expect(result.HasErrors()).To(BeTrue())

			var foundMissingVMID bool
			for _, e := range result.Errors {
				if e.Code == parser.CodeMissingVMID {
					foundMissingVMID = true
				}
			}
			Expect(foundMissingVMID).To(BeTrue(), "Expected MISSING_VM_ID error")
		})

		It("returns error when VM name is empty for all VMs", func() {
			// Insert VM with empty name
			_, err := db.Exec(`
				INSERT INTO vinfo ("VM ID", "VM", "Powerstate", "CPUs", "Memory")
				VALUES ('vm-001', '', 'poweredOn', 4, 8192)
			`)
			Expect(err).NotTo(HaveOccurred())

			ctx := GinkgoT().Context()
			result := p.ValidateSchema(ctx)

			Expect(result.HasErrors()).To(BeTrue())

			var foundMissingVMName bool
			for _, e := range result.Errors {
				if e.Code == parser.CodeMissingVMName {
					foundMissingVMName = true
				}
			}
			Expect(foundMissingVMName).To(BeTrue(), "Expected MISSING_VM_NAME error")
		})
	})

	Describe("ValidateSchema with complete data", func() {
		BeforeEach(func() {
			// Insert complete test data
			_, err := db.Exec(`
				INSERT INTO vinfo ("VM ID", "VM", "Powerstate", "CPUs", "Memory", "Cluster", "VI SDK UUID")
				VALUES ('vm-001', 'TestVM', 'poweredOn', 4, 8192, 'TestCluster', 'vcenter-uuid');

				INSERT INTO vhost ("Cluster", "# Cores", "# CPU", "Object ID", "# Memory", "Model", "Vendor", "Host")
				VALUES ('TestCluster', 16, 2, 'host-001', 65536, 'ProLiant', 'HPE', '192.168.1.1');

				INSERT INTO vdatastore ("Name", "Capacity MiB", "Free MiB", "Type")
				VALUES ('datastore1', 1024000, 512000, 'VMFS');

				INSERT INTO dvport ("Port", "VLAN")
				VALUES ('VM Network', '100');

				INSERT INTO vcpu ("VM ID", "Sockets", "Cores p/s")
				VALUES ('vm-001', 2, 2);

				INSERT INTO vmemory ("VM ID", "Hot Add")
				VALUES ('vm-001', true);

				INSERT INTO vdisk ("VM ID", "Capacity MiB", "Path")
				VALUES ('vm-001', 102400, '[datastore1] vm-001/vm-001.vmdk');

				INSERT INTO vnetwork ("VM ID", "Network", "Mac Address")
				VALUES ('vm-001', 'VM Network', '00:50:56:00:00:01');
			`)
			Expect(err).NotTo(HaveOccurred())
		})

		It("is valid with no errors and no warnings", func() {
			ctx := GinkgoT().Context()
			result := p.ValidateSchema(ctx)

			Expect(result.HasErrors()).To(BeFalse())
			Expect(result.HasWarnings()).To(BeFalse())
			Expect(result.IsValid()).To(BeTrue())
		})
	})

	Describe("ValidationResult Error method", func() {
		It("returns nil when no errors", func() {
			result := parser.ValidationResult{}
			Expect(result.Error()).To(BeNil())
		})

		It("returns error message with all error codes", func() {
			result := parser.ValidationResult{
				Errors: []parser.ValidationIssue{
					{Code: "ERROR_1", Message: "First error"},
					{Code: "ERROR_2", Message: "Second error"},
				},
			}
			err := result.Error()
			Expect(err).NotTo(BeNil())
			Expect(err.Error()).To(ContainSubstring("ERROR_1"))
			Expect(err.Error()).To(ContainSubstring("ERROR_2"))
			Expect(err.Error()).To(ContainSubstring("First error"))
		})
	})
})
