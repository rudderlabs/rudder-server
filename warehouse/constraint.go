package warehouse

import (
	"fmt"
	"github.com/gofrs/uuid"
	"github.com/iancoleman/strcase"
	"github.com/rudderlabs/rudder-server/config"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

var (
	constraintsMap              map[string][]ConstraintsI
	enableConstraintsViolations bool
)

type ConstraintsI interface {
	violates(brEvent *BatchRouterEventT, columnName string) (cv *ConstraintsViolationT)
}

type ConstraintsViolationT struct {
	isViolated         bool
	violatedIdentifier string
}

type IndexConstraintT struct {
	TableName    string
	ColumnName   string
	IndexColumns []string
	Limit        int
}

func Init6() {
	config.RegisterBoolConfigVariable(true, &enableConstraintsViolations, true, "Warehouse.enableConstraintsViolations")
	constraintsMap = map[string][]ConstraintsI{
		"BQ": []ConstraintsI{
			&IndexConstraintT{
				TableName:    "rudder_identity_merge_rules",
				ColumnName:   "merge_property_1_value",
				IndexColumns: []string{"merge_property_1_type", "merge_property_1_value"},
				Limit:        512,
			},
			&IndexConstraintT{
				TableName:    "rudder_identity_merge_rules",
				ColumnName:   "merge_property_2_value",
				IndexColumns: []string{"merge_property_2_type", "merge_property_2_value"},
				Limit:        512,
			},
		},
		"SNOWFLAKE": []ConstraintsI{
			&IndexConstraintT{
				TableName:    "RUDDER_IDENTITY_MERGE_RULES",
				ColumnName:   "MERGE_PROPERTY_1_VALUE",
				IndexColumns: []string{"MERGE_PROPERTY_1_TYPE", "MERGE_PROPERTY_1_VALUE"},
				Limit:        512,
			},
			&IndexConstraintT{
				TableName:    "RUDDER_IDENTITY_MERGE_RULES",
				ColumnName:   "MERGE_PROPERTY_2_VALUE",
				IndexColumns: []string{"MERGE_PROPERTY_2_TYPE", "MERGE_PROPERTY_2_VALUE"},
				Limit:        512,
			},
		},
	}
}

func ViolatedConstraints(destinationType string, brEvent *BatchRouterEventT, columnName string) (cv *ConstraintsViolationT) {
	cv = &ConstraintsViolationT{}
	if !enableConstraintsViolations {
		return
	}
	constraints, ok := constraintsMap[destinationType]
	if !ok {
		return
	}
	for _, constraint := range constraints {
		cv = constraint.violates(brEvent, columnName)
		if cv.isViolated {
			return
		}
	}
	return
}

func (ic *IndexConstraintT) violates(brEvent *BatchRouterEventT, columnName string) (cv *ConstraintsViolationT) {
	if brEvent.Metadata.Table != ic.TableName || columnName != ic.ColumnName {
		return &ConstraintsViolationT{}
	}
	var concatenatedLength = 0
	for _, column := range ic.IndexColumns {
		columnInfo, ok := brEvent.GetColumnInfo(column)
		if !ok {
			continue
		}
		if columnInfo.ColumnType == "string" {
			columnVal, ok := columnInfo.ColumnVal.(string)
			if !ok {
				continue
			}
			concatenatedLength += len(columnVal)
		}
	}
	return &ConstraintsViolationT{
		isViolated:         concatenatedLength > ic.Limit,
		violatedIdentifier: fmt.Sprintf(`%s-%s`, strcase.ToKebab(warehouseutils.DiscardsTable), uuid.Must(uuid.NewV4()).String()),
	}
}
