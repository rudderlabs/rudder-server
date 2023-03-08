package warehouse

import (
	"fmt"

	"github.com/rudderlabs/rudder-server/utils/misc"

	"github.com/iancoleman/strcase"
	"github.com/rudderlabs/rudder-server/config"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

var (
	constraintsMap              map[string][]Constraints
	enableConstraintsViolations bool
)

type Constraints interface {
	violates(brEvent *BatchRouterEvent, columnName string) (cv *ConstraintsViolation)
}

type ConstraintsViolation struct {
	IsViolated         bool
	ViolatedIdentifier string
}

type IndexConstraint struct {
	TableName    string
	ColumnName   string
	IndexColumns []string
	Limit        int
}

func Init6() {
	config.RegisterBoolConfigVariable(true, &enableConstraintsViolations, true, "Warehouse.enableConstraintsViolations")
	constraintsMap = map[string][]Constraints{
		warehouseutils.BQ: {
			&IndexConstraint{
				TableName:    "rudder_identity_merge_rules",
				ColumnName:   "merge_property_1_value",
				IndexColumns: []string{"merge_property_1_type", "merge_property_1_value"},
				Limit:        512,
			},
			&IndexConstraint{
				TableName:    "rudder_identity_merge_rules",
				ColumnName:   "merge_property_2_value",
				IndexColumns: []string{"merge_property_2_type", "merge_property_2_value"},
				Limit:        512,
			},
		},
		warehouseutils.SNOWFLAKE: {
			&IndexConstraint{
				TableName:    "RUDDER_IDENTITY_MERGE_RULES",
				ColumnName:   "MERGE_PROPERTY_1_VALUE",
				IndexColumns: []string{"MERGE_PROPERTY_1_TYPE", "MERGE_PROPERTY_1_VALUE"},
				Limit:        512,
			},
			&IndexConstraint{
				TableName:    "RUDDER_IDENTITY_MERGE_RULES",
				ColumnName:   "MERGE_PROPERTY_2_VALUE",
				IndexColumns: []string{"MERGE_PROPERTY_2_TYPE", "MERGE_PROPERTY_2_VALUE"},
				Limit:        512,
			},
		},
	}
}

func ViolatedConstraints(destinationType string, brEvent *BatchRouterEvent, columnName string) (cv *ConstraintsViolation) {
	cv = &ConstraintsViolation{}
	if !enableConstraintsViolations {
		return
	}
	constraints, ok := constraintsMap[destinationType]
	if !ok {
		return
	}
	for _, constraint := range constraints {
		cv = constraint.violates(brEvent, columnName)
		if cv.IsViolated {
			return
		}
	}
	return
}

func (ic *IndexConstraint) violates(brEvent *BatchRouterEvent, columnName string) (cv *ConstraintsViolation) {
	if brEvent.Metadata.Table != ic.TableName || columnName != ic.ColumnName {
		return &ConstraintsViolation{}
	}
	concatenatedLength := 0
	for _, column := range ic.IndexColumns {
		columnInfo, ok := brEvent.GetColumnInfo(column)
		if !ok {
			continue
		}
		if columnInfo.Type == "string" {
			columnVal, ok := columnInfo.Value.(string)
			if !ok {
				continue
			}
			concatenatedLength += len(columnVal)
		}
	}
	return &ConstraintsViolation{
		IsViolated:         concatenatedLength > ic.Limit,
		ViolatedIdentifier: fmt.Sprintf(`%s-%s`, strcase.ToKebab(warehouseutils.DiscardsTable), misc.FastUUID().String()),
	}
}
