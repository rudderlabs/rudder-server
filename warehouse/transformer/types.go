package transformer

import (
	"sync"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	ptrans "github.com/rudderlabs/rudder-server/processor/transformer"
)

type (
	Transformer struct {
		now func() time.Time

		logger       logger.Logger
		statsFactory stats.Stats

		stats struct {
			comparisionTime  stats.Timer
			mismatchedEvents stats.Histogram
		}
		config struct {
			enableIDResolution           config.ValueLoader[bool]
			populateSrcDestInfoInContext config.ValueLoader[bool]
			maxColumnsInEvent            config.ValueLoader[int]
			maxLoggedEvents              config.ValueLoader[int]
		}

		loggedEvents   int64
		loggedEventsMu sync.Mutex

		loggedFileName string
	}

	cache struct {
		safeTableNameCache       sync.Map
		safeColumnNameCache      sync.Map
		transformTableNameCache  sync.Map
		transformColumnNameCache sync.Map
	}

	safeTableNameCacheKey struct {
		destType, tableName          string
		skipReservedKeywordsEscaping bool
	}
	safeColumnNameCacheKey struct {
		destType, columnName         string
		skipReservedKeywordsEscaping bool
	}
	transformTableNameCacheKey struct {
		underscoreDivideNumbers bool
		useBlendoCasing         bool
		tableName               string
	}
	transformColumnNameCacheKey struct {
		destType, columnName    string
		underscoreDivideNumbers bool
		useBlendoCasing         bool
	}

	transformEventContext struct {
		event         *ptrans.TransformerEvent
		intrOpts      *intrOptions
		destOpts      *destOptions
		jsonPathsInfo *jsonPathInfo
		cache         *cache
	}

	intrOptions struct {
		// skipReservedKeywordsEscaping when set to true, will skip the escaping of reserved keywords
		skipReservedKeywordsEscaping bool
		// skipTracksTable when set to true, will skip the tracks event
		skipTracksTable bool
		// skipUsersTable when set to true, will skip the users event
		skipUsersTable bool
		// useBlendoCasing when set to true, will use the casing as per Blendo's requirement
		useBlendoCasing bool
		// jsonPaths is a list of json paths that should be extracted from the event and stored as raw instead of normalizing them
		jsonPaths []string
	}
	destOptions struct {
		// skipTracksTable when set to true, will skip the tracks event
		skipTracksTable bool
		// skipUsersTable when set to true, will skip the users event
		skipUsersTable bool
		// storeFullEvent when set to true, will store the full event as rudder_event (JSON)
		storeFullEvent bool
		// jsonPaths is a list of json paths that should be extracted from the event and stored as raw instead of normalizing them
		jsonPaths []string
		// underscoreDivideNumbers when set to false, if a column has a format like "_v_3_", it will be formatted to "_v3_"
		// underscoreDivideNumbers when set to true, if a column has a format like "_v_3_", we keep it like that
		// For older destinations, it will come as true and for new destinations this config will not be present which means we will treat it as false.
		underscoreDivideNumbers bool
		// allowUsersContextTraits when set to true, if context.traits.* is present, it will be added as context_traits_* and *,
		// e.g., for context.traits.name, context_traits_name and name will be added to the user's table.
		// allowUsersContextTraits when set to false, if context.traits.* is present, it will be added only as context_traits_*
		// e.g., for context.traits.name, only context_traits_name will be added to the user's table.
		// For older destinations, it will come as true, and for new destinations this config will not be present, which means we will treat it as false.
		allowUsersContextTraits bool
	}

	mergeRule struct {
		Type, Value any
	}
	mergeRulesColumns struct {
		Prop1Type, Prop1Value, Prop2Type, Prop2Value string
	}

	prefixInfo struct {
		completePrefix string
		completeLevel  int
		prefix         string
		level          int
	}

	jsonPathInfo struct {
		keysMap       map[string]int
		legacyKeysMap map[string]int
	}
)
