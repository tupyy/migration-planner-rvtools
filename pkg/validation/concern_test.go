package validation_test

import (
	"context"
	"database/sql"
	"strconv"

	"github.com/duckdb/duckdb-go/v2"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/tupyy/rvtools/models"
	"github.com/tupyy/rvtools/pkg/validation"
)

const createConcernsTable = `
CREATE TABLE concerns (
    "VM_ID" VARCHAR,
    "Concern_ID" VARCHAR,
    "Label" VARCHAR,
    "Category" VARCHAR,
    "Assessment" VARCHAR
);
`

func setupConcernTestDB() *sql.DB {
	c, err := duckdb.NewConnector("", nil)
	Expect(err).NotTo(HaveOccurred())

	db := sql.OpenDB(c)
	Expect(db).NotTo(BeNil())

	_, err = db.Exec(createConcernsTable)
	Expect(err).NotTo(HaveOccurred())

	return db
}

var _ = Describe("Insert concerns", func() {
	var (
		db  *sql.DB
		ctx context.Context
	)

	BeforeEach(func() {
		db = setupConcernTestDB()
		ctx = context.Background()
	})

	AfterEach(func() {
		if db != nil {
			db.Close()
		}
	})

	Describe("Insert", func() {
		It("inserts a new concern using ConcernValuesBuilder", func() {
			concern := models.Concern{
				Id:         "concern-001",
				Label:      "New concern",
				Category:   "Test",
				Assessment: "Info",
			}

			builder := validation.NewConcernValuesBuilder().Append("vm-001", concern)
			err := validation.InsertIntoConcerns(ctx, db, builder)
			Expect(err).NotTo(HaveOccurred())

			var count int
			err = db.QueryRow("SELECT COUNT(*) FROM concerns WHERE VM_ID = 'vm-001'").Scan(&count)
			Expect(err).NotTo(HaveOccurred())
			Expect(count).To(Equal(1))

			var concernId, label string
			err = db.QueryRow("SELECT Concern_ID, Label FROM concerns WHERE VM_ID = 'vm-001'").Scan(&concernId, &label)
			Expect(err).NotTo(HaveOccurred())
			Expect(concernId).To(Equal("concern-001"))
			Expect(label).To(Equal("New concern"))
		})

		It("inserts multiple concerns at once using ConcernValuesBuilder", func() {
			concernA := models.Concern{
				Id:         "concern-005",
				Label:      "Concern A",
				Category:   "Cat A",
				Assessment: "Info",
			}
			concernB := models.Concern{
				Id:         "concern-006",
				Label:      "Concern B",
				Category:   "Cat B",
				Assessment: "Warning",
			}
			var concerns []models.Concern
			for i := 0; i < 5; i++ {
				concerns = append(concerns, models.Concern{
					Id:         "concern-" + strconv.Itoa(i),
					Label:      "Concern " + strconv.Itoa(i),
					Category:   "Cat B",
					Assessment: "Info",
				})
			}

			builder := validation.NewConcernValuesBuilder().
				Append("vm-002", concernA).
				Append("vm-002", concernB).Append("vm-001", concerns...)

			err := validation.InsertIntoConcerns(ctx, db, builder)

			Expect(err).NotTo(HaveOccurred())

			var count int
			err = db.QueryRow("SELECT COUNT(*) FROM concerns WHERE VM_ID = 'vm-002'").Scan(&count)
			Expect(err).NotTo(HaveOccurred())
			Expect(count).To(Equal(2))

			err = db.QueryRow("SELECT COUNT(*) FROM concerns WHERE VM_ID = 'vm-001'").Scan(&count)
			Expect(err).NotTo(HaveOccurred())
			Expect(count).To(Equal(5))
		})

		It("inserts same concern for multiple VMs using ConcernValuesBuilder", func() {
			concern := models.Concern{
				Id:         "concern-007",
				Label:      "Shared concern",
				Category:   "Shared",
				Assessment: "Warning",
			}

			builder := validation.NewConcernValuesBuilder().
				Append("vm-001", concern).
				Append("vm-002", concern).
				Append("vm-003", concern)

			err := validation.InsertIntoConcerns(ctx, db, builder)

			Expect(err).NotTo(HaveOccurred())

			var count int
			err = db.QueryRow("SELECT COUNT(*) FROM concerns WHERE Concern_ID = 'concern-007'").Scan(&count)
			Expect(err).NotTo(HaveOccurred())
			Expect(count).To(Equal(3))
		})

		It("empty ConcernValuesBuilder should return error", func() {
			err := validation.InsertIntoConcerns(ctx, db, validation.NewConcernValuesBuilder())
			Expect(err).To(HaveOccurred())
		})
	})
})

var _ = Describe("ConcernValuesBuilder", func() {
	Describe("Build", func() {
		It("returns empty string when no values are appended", func() {
			builder := validation.NewConcernValuesBuilder()
			result := builder.Build()

			Expect(result).To(BeEmpty())
		})

		It("builds single value correctly", func() {
			concern := models.Concern{
				Id:         "c-001",
				Label:      "Test Label",
				Category:   "Test Category",
				Assessment: "Warning",
			}

			builder := validation.NewConcernValuesBuilder().Append("vm-001", concern)
			result := builder.Build()

			Expect(result).To(Equal("('vm-001', 'c-001', 'Test Label', 'Test Category', 'Warning')"))
		})

		It("builds multiple values correctly", func() {
			concern1 := models.Concern{
				Id:         "c-001",
				Label:      "Label 1",
				Category:   "Category 1",
				Assessment: "Info",
			}
			concern2 := models.Concern{
				Id:         "c-002",
				Label:      "Label 2",
				Category:   "Category 2",
				Assessment: "Critical",
			}

			builder := validation.NewConcernValuesBuilder().
				Append("vm-001", concern1).
				Append("vm-002", concern2)
			result := builder.Build()

			Expect(result).To(Equal("('vm-001', 'c-001', 'Label 1', 'Category 1', 'Info'), ('vm-002', 'c-002', 'Label 2', 'Category 2', 'Critical')"))
		})

		It("supports method chaining", func() {
			concern := models.Concern{
				Id:         "c-001",
				Label:      "Label",
				Category:   "Category",
				Assessment: "Info",
			}

			builder := validation.NewConcernValuesBuilder().
				Append("vm-001", concern).
				Append("vm-002", concern).
				Append("vm-003", concern)

			result := builder.Build()

			Expect(result).To(ContainSubstring("vm-001"))
			Expect(result).To(ContainSubstring("vm-002"))
			Expect(result).To(ContainSubstring("vm-003"))
		})
	})
})
