package validation

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/tupyy/rvtools/models"
)

const (
	VMIDCol          = "VM_ID"
	ConcernIDCol     = "Concern_ID"
	LabelCol         = "Label"
	CategoryCol      = "Category"
	AssessmentCol    = "Assessment"
	insertConcernStm = "INSERT INTO concerns (%s, %s, %s, %s, %s) VALUES %s;"
)

func InsertIntoConcerns(ctx context.Context, db *sql.DB, bu *ConcernValuesBuilder) error {
	valuesStr := bu.Build()
	if valuesStr == "" {
		return fmt.Errorf("no values provided")
	}

	query := fmt.Sprintf(insertConcernStm, VMIDCol, ConcernIDCol, LabelCol, CategoryCol, AssessmentCol, valuesStr)

	_, err := db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("insert concerns failed: %w", err)
	}

	return nil
}

type ConcernValuesBuilder struct {
	values []string
}

func NewConcernValuesBuilder() *ConcernValuesBuilder {
	return &ConcernValuesBuilder{}
}

func (cb *ConcernValuesBuilder) Append(vmId string, concerns ...models.Concern) *ConcernValuesBuilder {
	escape := func(s string) string {
		return strings.ReplaceAll(s, "'", "''")
	}

	for _, c := range concerns {
		value := fmt.Sprintf("('%s', '%s', '%s', '%s', '%s')",
			escape(vmId),
			escape(c.Id),
			escape(c.Label),
			escape(c.Category),
			escape(c.Assessment),
		)
		cb.values = append(cb.values, value)
	}
	return cb
}

func (cb *ConcernValuesBuilder) Build() string {
	if len(cb.values) == 0 {
		return ""
	}

	return strings.Join(cb.values, ", ")
}
