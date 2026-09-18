package bigquery

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
)

func TestDeduplicationQueryQuotesIdentifiers(t *testing.T) {
	bq := &BigQuery{projectID: "project-id", namespace: "data`set", conf: config.New()}
	query, err := bq.deduplicationQuery("table`x", model.TableSchema{
		"id":        "string",
		"loaded_at": "datetime",
	})
	require.NoError(t, err)
	require.Contains(t, query, "FROM `project-id.data\\`set.table\\`x`")
	require.Contains(t, query, "PARTITION BY `id` ORDER BY `loaded_at` DESC")
	require.NotContains(t, query, "data`set.table`x")

	partitionQuery, err := bq.deduplicationQuery("partitioned", model.TableSchema{"id": "string"})
	require.NoError(t, err)
	require.True(t, strings.Contains(partitionQuery, "_PARTITIONTIME BETWEEN"), partitionQuery)
}

func TestUsersMergeQueryQuotesIdentifiers(t *testing.T) {
	bq := &BigQuery{namespace: "data`set"}
	query := bq.usersMergeQuery(model.TableSchema{
		"id":                     "string",
		"email":                  "string",
		"evil`) AS x FROM y; --": "string",
		`trailing\`:              "string",
	}, "SELECT * FROM dedup", "rudder_staging_users")

	require.Equal(t, `SELECT DISTINCT * FROM (
			SELECT `+"`id`"+`, FIRST_VALUE(`+"`email`"+` IGNORE NULLS) OVER (PARTITION BY `+"`id`"+` ORDER BY `+"`received_at`"+` DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS `+"`email`"+`,FIRST_VALUE(`+"`evil\\`) AS x FROM y; --`"+` IGNORE NULLS) OVER (PARTITION BY `+"`id`"+` ORDER BY `+"`received_at`"+` DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS `+"`evil\\`) AS x FROM y; --`"+`,FIRST_VALUE(`+"`trailing\\\\`"+` IGNORE NULLS) OVER (PARTITION BY `+"`id`"+` ORDER BY `+"`received_at`"+` DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS `+"`trailing\\\\`"+` FROM (
				(
					SELECT `+"`id`"+`, `+"`email`,`evil\\`) AS x FROM y; --`,`trailing\\\\`"+` FROM (SELECT * FROM dedup) WHERE (
						`+"`id`"+` in (SELECT `+"`id`"+` FROM `+"`data\\`set.rudder_staging_users`"+`)
					)
				) UNION ALL (
					SELECT `+"`id`"+`, `+"`email`,`evil\\`) AS x FROM y; --`,`trailing\\\\`"+` FROM `+"`data\\`set.rudder_staging_users`"+`
				)
			)
		)`, query)
}
