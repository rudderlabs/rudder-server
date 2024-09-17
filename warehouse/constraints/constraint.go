package constraints

import (
	"github.com/rudderlabs/rudder-go-kit/config"

	"github.com/rudderlabs/rudder-server/warehouse/utils/types"

	"github.com/rudderlabs/rudder-server/utils/misc"

	"github.com/iancoleman/strcase"

	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type constraints interface {
	violates(brEvent *types.BatchRouterEvent, columnName string) (cv *Violation)
}

type Violation struct {
	IsViolated         bool
	ViolatedIdentifier string
	Reason             string
}

type indexConstraint struct {
	tableName    string
	columnName   string
	indexColumns []string
	limit        int
	reason       string
}

type Manager struct {
	constraintsMap              map[string][]constraints
	enableConstraintsViolations config.ValueLoader[bool]
}

func New(conf *config.Config) *Manager {
	cm := &Manager{}

	cm.constraintsMap = map[string][]constraints{
		warehouseutils.BQ: {
			&indexConstraint{
				tableName:    "rudder_identity_merge_rules",
				columnName:   "merge_property_1_value",
				indexColumns: []string{"merge_property_1_type", "merge_property_1_value"},
				limit:        512,
				reason:       "The maximum length of the concatenated columns " + "merge_property_1_type" + " and " + "merge_property_1_value" + " should be less than 512",
			},
			&indexConstraint{
				tableName:    "rudder_identity_merge_rules",
				columnName:   "merge_property_2_value",
				indexColumns: []string{"merge_property_2_type", "merge_property_2_value"},
				limit:        512,
				reason:       "The maximum length of the concatenated columns " + "merge_property_2_type" + " and " + "merge_property_2_value" + " should be less than 512",
			},
		},
		warehouseutils.SNOWFLAKE: {
			&indexConstraint{
				tableName:    "RUDDER_IDENTITY_MERGE_RULES",
				columnName:   "MERGE_PROPERTY_1_VALUE",
				indexColumns: []string{"MERGE_PROPERTY_1_TYPE", "MERGE_PROPERTY_1_VALUE"},
				limit:        512,
				reason:       "The maximum length of the concatenated columns MERGE_PROPERTY_1_TYPE and MERGE_PROPERTY_1_VALUE should be less than 512",
			},
			&indexConstraint{
				tableName:    "RUDDER_IDENTITY_MERGE_RULES",
				columnName:   "MERGE_PROPERTY_2_VALUE",
				indexColumns: []string{"MERGE_PROPERTY_2_TYPE", "MERGE_PROPERTY_2_VALUE"},
				limit:        512,
				reason:       "The maximum length of the concatenated columns MERGE_PROPERTY_2_TYPE and MERGE_PROPERTY_2_VALUE should be less than 512",
			},
		},
	}
	cm.enableConstraintsViolations = conf.GetReloadableBoolVar(true, "Warehouse.enableConstraintsViolations")

	return cm
}

func (cm *Manager) ViolatedConstraints(destinationType string, brEvent *types.BatchRouterEvent, columnName string) (cv *Violation) {
	cv = &Violation{}

	if !cm.enableConstraintsViolations.Load() {
		return
	}

	constraints, ok := cm.constraintsMap[destinationType]
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

func (ic *indexConstraint) violates(brEvent *types.BatchRouterEvent, columnName string) *Violation {
	if brEvent.Metadata.Table != ic.tableName || columnName != ic.columnName {
		return &Violation{}
	}

	concatenatedLength := 0

	for _, column := range ic.indexColumns {
		columnInfo, ok := brEvent.GetColumnInfo(column)
		if !ok {
			continue
		}
		if columnInfo.Type != "string" {
			continue
		}

		columnVal, ok := columnInfo.Value.(string)
		if !ok {
			continue
		}
		concatenatedLength += len(columnVal)
	}
	return &Violation{
		IsViolated:         concatenatedLength > ic.limit,
		ViolatedIdentifier: strcase.ToKebab(warehouseutils.DiscardsTable) + "-" + misc.FastUUID().String(),
		Reason:             ic.reason,
	}
}
