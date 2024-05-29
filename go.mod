module github.com/rudderlabs/rudder-server

go 1.22.3

// Addressing snyk vulnerabilities in indirect dependencies
// When upgrading a dependency, please make sure that
// the same version is used both here and in the require section
replace (
	github.com/containerd/containerd => github.com/containerd/containerd v1.6.32
	github.com/cyphar/filepath-securejoin => github.com/cyphar/filepath-securejoin v0.2.5
	github.com/docker/docker => github.com/docker/docker v26.1.3+incompatible
	github.com/gin-gonic/gin => github.com/gin-gonic/gin v1.10.0
	github.com/go-jose/go-jose/v3 => github.com/go-jose/go-jose/v3 v3.0.3
	github.com/opencontainers/runc => github.com/opencontainers/runc v1.1.12
	github.com/satori/go.uuid => github.com/satori/go.uuid v1.2.0
	github.com/xitongsys/parquet-go => github.com/rudderlabs/parquet-go v0.0.2
	golang.org/x/crypto => golang.org/x/crypto v0.23.0
	golang.org/x/image => golang.org/x/image v0.16.0
	golang.org/x/net => golang.org/x/net v0.25.0
	golang.org/x/text => golang.org/x/text v0.15.0
	gopkg.in/yaml.v2 => gopkg.in/yaml.v2 v2.4.0
	gopkg.in/yaml.v3 => gopkg.in/yaml.v3 v3.0.1
	k8s.io/kubernetes => k8s.io/kubernetes v1.30.1
)

require (
	cloud.google.com/go/bigquery v1.61.0
	cloud.google.com/go/pubsub v1.38.0
	cloud.google.com/go/storage v1.41.0
	github.com/Azure/azure-storage-blob-go v0.15.0
	github.com/ClickHouse/clickhouse-go v1.5.4
	github.com/DATA-DOG/go-sqlmock v1.5.2
	github.com/alexeyco/simpletable v1.0.0
	github.com/allisson/go-pglock/v2 v2.0.1
	github.com/apache/pulsar-client-go v0.12.1
	github.com/araddon/dateparse v0.0.0-20210429162001-6b43995a97de
	github.com/aws/aws-sdk-go v1.53.10
	github.com/bugsnag/bugsnag-go/v2 v2.4.0
	github.com/cenkalti/backoff v2.2.1+incompatible
	github.com/cenkalti/backoff/v4 v4.3.0
	github.com/confluentinc/confluent-kafka-go/v2 v2.4.0
	github.com/databricks/databricks-sql-go v1.5.5
	github.com/denisenkom/go-mssqldb v0.12.3
	github.com/dgraph-io/badger/v4 v4.2.0
	github.com/docker/docker v26.1.3+incompatible
	github.com/go-chi/chi/v5 v5.0.12
	github.com/go-redis/redis v6.15.9+incompatible
	github.com/go-redis/redis/v8 v8.11.5
	github.com/golang-migrate/migrate/v4 v4.17.1
	github.com/golang/mock v1.6.0
	github.com/gomodule/redigo v1.9.2
	github.com/google/uuid v1.6.0
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.20.0
	github.com/hashicorp/go-retryablehttp v0.7.6
	github.com/hashicorp/golang-lru/v2 v2.0.7
	github.com/hashicorp/yamux v0.1.1
	github.com/iancoleman/strcase v0.3.0
	github.com/jeremywohl/flatten v1.0.1
	github.com/joho/godotenv v1.5.1
	github.com/json-iterator/go v1.1.12
	github.com/lensesio/tableprinter v0.0.0-20201125135848-89e81fc956e7
	github.com/lib/pq v1.10.9
	github.com/linkedin/goavro/v2 v2.13.0
	github.com/manifoldco/promptui v0.9.0
	github.com/marcboeker/go-duckdb v1.6.5
	github.com/minio/minio-go/v7 v7.0.70
	github.com/mitchellh/mapstructure v1.5.0
	github.com/olekukonko/tablewriter v0.0.5
	github.com/onsi/ginkgo/v2 v2.19.0
	github.com/onsi/gomega v1.33.1
	github.com/ory/dockertest/v3 v3.10.0
	github.com/oschwald/maxminddb-golang v1.12.0
	github.com/phayes/freeport v0.0.0-20220201140144-74d24b5ae9f5
	github.com/prometheus/client_model v0.6.1
	github.com/redis/go-redis/v9 v9.5.1
	github.com/rs/cors v1.11.0
	github.com/rudderlabs/analytics-go v3.3.3+incompatible
	github.com/rudderlabs/bing-ads-go-sdk v0.2.1
	github.com/rudderlabs/compose-test v0.1.3
	github.com/rudderlabs/rudder-go-kit v0.33.0
	github.com/rudderlabs/rudder-observability-kit v0.0.3
	github.com/rudderlabs/rudder-schemas v0.4.0
	github.com/rudderlabs/sql-tunnels v0.1.6
	github.com/samber/lo v1.39.0
	github.com/segmentio/kafka-go v0.4.47
	github.com/segmentio/ksuid v1.0.4
	github.com/snowflakedb/gosnowflake v1.10.0
	github.com/sony/gobreaker v1.0.0
	github.com/spaolacci/murmur3 v1.1.0
	github.com/spf13/cast v1.6.0
	github.com/spf13/viper v1.18.2
	github.com/stretchr/testify v1.9.0
	github.com/tidwall/gjson v1.17.1
	github.com/tidwall/sjson v1.2.5
	github.com/trinodb/trino-go-client v0.315.0
	github.com/urfave/cli/v2 v2.27.2
	github.com/valyala/fasthttp v1.54.0
	github.com/viney-shih/go-lock v1.1.2
	github.com/xitongsys/parquet-go v1.5.1
	github.com/xitongsys/parquet-go-source v0.0.0-20240122235623-d6294584ab18
	go.etcd.io/etcd/api/v3 v3.5.13
	go.etcd.io/etcd/client/v3 v3.5.13
	go.uber.org/atomic v1.11.0
	go.uber.org/automaxprocs v1.5.3
	go.uber.org/goleak v1.3.0
	golang.org/x/exp v0.0.0-20240222234643-814bf88cf225
	golang.org/x/oauth2 v0.20.0
	golang.org/x/sync v0.7.0
	google.golang.org/api v0.182.0
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240521202816-d264139d666e
	google.golang.org/grpc v1.64.0
	google.golang.org/protobuf v1.34.1
)

require (
	cloud.google.com/go v0.114.0 // indirect
	cloud.google.com/go/auth v0.4.2 // indirect
	cloud.google.com/go/auth/oauth2adapt v0.2.2 // indirect
	cloud.google.com/go/compute/metadata v0.3.0 // indirect
	cloud.google.com/go/iam v1.1.8 // indirect
	dario.cat/mergo v1.0.0 // indirect
	github.com/99designs/go-keychain v0.0.0-20191008050251-8e49817e8af4 // indirect
	github.com/99designs/keyring v1.2.2 // indirect
	github.com/AthenZ/athenz v1.10.39 // indirect
	github.com/Azure/azure-pipeline-go v0.2.3 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/azcore v1.6.0 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/internal v1.3.0 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/storage/azblob v1.1.0 // indirect
	github.com/Azure/go-ansiterm v0.0.0-20230124172434-306776ec8161 // indirect
	github.com/DataDog/zstd v1.5.0 // indirect
	github.com/JohnCGriffin/overflow v0.0.0-20211019200055-46fa312c352c // indirect
	github.com/Microsoft/go-winio v0.6.1 // indirect
	github.com/Nvveen/Gotty v0.0.0-20120604004816-cd527374f1e5 // indirect
	github.com/actgardner/gogen-avro/v10 v10.2.1 // indirect
	github.com/andybalholm/brotli v1.1.0 // indirect
	github.com/apache/arrow/go/arrow v0.0.0-20200730104253-651201b0f516 // indirect
	github.com/apache/arrow/go/v14 v14.0.2 // indirect
	github.com/apache/arrow/go/v15 v15.0.2 // indirect
	github.com/apache/arrow/go/v16 v16.0.0 // indirect
	github.com/apache/thrift v0.19.0 // indirect
	github.com/ardielle/ardielle-go v1.5.2 // indirect
	github.com/aws/aws-sdk-go-v2 v1.23.0 // indirect
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.5.1 // indirect
	github.com/aws/aws-sdk-go-v2/credentials v1.16.2 // indirect
	github.com/aws/aws-sdk-go-v2/feature/s3/manager v1.14.0 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.2.3 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.5.3 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.2.3 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.10.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.2.3 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.10.3 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.16.3 // indirect
	github.com/aws/aws-sdk-go-v2/service/s3 v1.43.0 // indirect
	github.com/aws/smithy-go v1.17.0 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bits-and-blooms/bitset v1.4.0 // indirect
	github.com/bmizerany/assert v0.0.0-20160611221934-b7ed37b82869 // indirect
	github.com/bugsnag/panicwrap v1.3.4 // indirect
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/chzyer/readline v1.5.1 // indirect
	github.com/cloudflare/golz4 v0.0.0-20150217214814-ef862a3cdc58 // indirect
	github.com/containerd/containerd v1.7.12 // indirect
	github.com/containerd/continuity v0.4.3 // indirect
	github.com/containerd/log v0.1.0 // indirect
	github.com/coreos/go-oidc/v3 v3.5.0 // indirect
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/coreos/go-systemd/v22 v22.3.3-0.20220203105225-a9a7ef127534 // indirect
	github.com/cpuguy83/go-md2man/v2 v2.0.4 // indirect
	github.com/danieljoos/wincred v1.1.2 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/dgraph-io/ristretto v0.1.1 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/dnephin/pflag v1.0.7 // indirect
	github.com/docker/cli v26.0.1+incompatible // indirect
	github.com/docker/go-connections v0.5.0 // indirect
	github.com/docker/go-units v0.5.0 // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/dvsekhvalnov/jose2go v1.6.0 // indirect
	github.com/fatih/color v1.16.0 // indirect
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/fsnotify/fsnotify v1.7.0 // indirect
	github.com/gabriel-vasile/mimetype v1.4.3 // indirect
	github.com/go-jose/go-jose/v3 v3.0.3 // indirect
	github.com/go-logr/logr v1.4.1 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-ole/go-ole v1.2.6 // indirect
	github.com/go-playground/locales v0.14.1 // indirect
	github.com/go-playground/universal-translator v0.18.1 // indirect
	github.com/go-playground/validator/v10 v10.20.0 // indirect
	github.com/go-task/slim-sprig/v3 v3.0.0 // indirect
	github.com/goccy/go-json v0.10.2 // indirect
	github.com/goccy/go-reflect v1.2.0 // indirect
	github.com/godbus/dbus v0.0.0-20190726142602-4481cbc300e2 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang-jwt/jwt v3.2.1+incompatible // indirect
	github.com/golang-jwt/jwt/v5 v5.2.1 // indirect
	github.com/golang-sql/civil v0.0.0-20190719163853-cb61b32ac6fe // indirect
	github.com/golang-sql/sqlexp v0.1.0 // indirect
	github.com/golang/glog v1.2.0 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/flatbuffers v24.3.25+incompatible // indirect
	github.com/google/go-cmp v0.6.0 // indirect
	github.com/google/pprof v0.0.0-20240424215950-a892ee059fd6 // indirect
	github.com/google/s2a-go v0.1.7 // indirect
	github.com/google/shlex v0.0.0-20191202100458-e7afc7fbc510 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.3.2 // indirect
	github.com/googleapis/gax-go/v2 v2.12.4 // indirect
	github.com/gsterjov/go-libsecret v0.0.0-20161001094733-a6f4afe4910c // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-cleanhttp v0.5.2 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/hashicorp/go-uuid v1.0.3 // indirect
	github.com/hashicorp/hcl v1.0.0 // indirect
	github.com/heetch/avro v0.4.4 // indirect
	github.com/jcmturner/gofork v1.7.6 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/kardianos/osext v0.0.0-20190222173326-2bc1f35cddc0 // indirect
	github.com/kataras/tablewriter v0.0.0-20180708051242-e063d29b7c23 // indirect
	github.com/klauspost/compress v1.17.7 // indirect
	github.com/klauspost/cpuid/v2 v2.2.7 // indirect
	github.com/kr/fs v0.1.0 // indirect
	github.com/leodido/go-urn v1.4.0 // indirect
	github.com/lufia/plan9stats v0.0.0-20211012122336-39d0f177ccd0 // indirect
	github.com/magiconair/properties v1.8.7 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-ieproxy v0.0.1 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/mattn/go-runewidth v0.0.15 // indirect
	github.com/minio/md5-simd v1.1.2 // indirect
	github.com/moby/docker-image-spec v1.3.1 // indirect
	github.com/moby/term v0.5.0 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/mtibben/percent v0.2.1 // indirect
	github.com/opencontainers/go-digest v1.0.0 // indirect
	github.com/opencontainers/image-spec v1.1.0 // indirect
	github.com/opencontainers/runc v1.1.12 // indirect
	github.com/openzipkin/zipkin-go v0.4.2 // indirect
	github.com/pelletier/go-toml/v2 v2.2.2 // indirect
	github.com/pierrec/lz4 v2.0.5+incompatible // indirect
	github.com/pierrec/lz4/v4 v4.1.21 // indirect
	github.com/pkg/browser v0.0.0-20210911075715-681adbf594b8 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pkg/sftp v1.13.6 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/power-devops/perfstat v0.0.0-20210106213030-5aafc221ea8c // indirect
	github.com/prometheus/client_golang v1.19.1 // indirect
	github.com/prometheus/common v0.53.0 // indirect
	github.com/prometheus/procfs v0.12.0 // indirect
	github.com/rivo/uniseg v0.2.0 // indirect
	github.com/rs/xid v1.5.0 // indirect
	github.com/rs/zerolog v1.28.0 // indirect
	github.com/russross/blackfriday/v2 v2.1.0 // indirect
	github.com/sagikazarmark/locafero v0.4.0 // indirect
	github.com/sagikazarmark/slog-shim v0.1.0 // indirect
	github.com/segmentio/backo-go v1.0.1 // indirect
	github.com/shirou/gopsutil/v3 v3.24.4 // indirect
	github.com/sirupsen/logrus v1.9.3 // indirect
	github.com/sourcegraph/conc v0.3.0 // indirect
	github.com/spf13/afero v1.11.0 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/subosito/gotenv v1.6.0 // indirect
	github.com/throttled/throttled/v2 v2.12.0 // indirect
	github.com/tidwall/match v1.1.1 // indirect
	github.com/tidwall/pretty v1.2.0 // indirect
	github.com/tklauser/go-sysconf v0.3.12 // indirect
	github.com/tklauser/numcpus v0.6.1 // indirect
	github.com/valyala/bytebufferpool v1.0.0 // indirect
	github.com/xdg-go/pbkdf2 v1.0.0 // indirect
	github.com/xdg-go/scram v1.1.2 // indirect
	github.com/xdg-go/stringprep v1.0.4 // indirect
	github.com/xeipuuv/gojsonpointer v0.0.0-20190905194746-02993c407bfb // indirect
	github.com/xeipuuv/gojsonreference v0.0.0-20180127040603-bd5ef7bd5415 // indirect
	github.com/xeipuuv/gojsonschema v1.2.0 // indirect
	github.com/xrash/smetrics v0.0.0-20240312152122-5f08fbb34913 // indirect
	github.com/xtgo/uuid v0.0.0-20140804021211-a0b114877d4c // indirect
	github.com/yusufpapurcu/wmi v1.2.4 // indirect
	github.com/zeebo/xxh3 v1.0.2 // indirect
	go.etcd.io/etcd/client/pkg/v3 v3.5.13 // indirect
	go.opencensus.io v0.24.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.49.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.49.0 // indirect
	go.opentelemetry.io/otel v1.27.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc v1.26.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.26.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.26.0 // indirect
	go.opentelemetry.io/otel/exporters/stdout/stdouttrace v1.26.0 // indirect
	go.opentelemetry.io/otel/exporters/zipkin v1.26.0 // indirect
	go.opentelemetry.io/otel/metric v1.27.0 // indirect
	go.opentelemetry.io/otel/sdk v1.27.0 // indirect
	go.opentelemetry.io/otel/sdk/metric v1.27.0 // indirect
	go.opentelemetry.io/otel/trace v1.27.0 // indirect
	go.opentelemetry.io/proto/otlp v1.2.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.27.0 // indirect
	golang.org/x/crypto v0.23.0 // indirect
	golang.org/x/mod v0.17.0 // indirect
	golang.org/x/net v0.25.0 // indirect
	golang.org/x/sys v0.20.0 // indirect
	golang.org/x/term v0.20.0 // indirect
	golang.org/x/text v0.15.0 // indirect
	golang.org/x/time v0.5.0 // indirect
	golang.org/x/tools v0.21.0 // indirect
	golang.org/x/xerrors v0.0.0-20231012003039-104605ab7028 // indirect
	google.golang.org/genproto v0.0.0-20240401170217-c3f982113cda // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20240513163218-0867130af1f8 // indirect
	gopkg.in/alexcesaro/statsd.v2 v2.0.0 // indirect
	gopkg.in/ini.v1 v1.67.0 // indirect
	gopkg.in/jcmturner/aescts.v1 v1.0.1 // indirect
	gopkg.in/jcmturner/dnsutils.v1 v1.0.1 // indirect
	gopkg.in/jcmturner/gokrb5.v6 v6.1.1 // indirect
	gopkg.in/jcmturner/rpc.v1 v1.1.0 // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.2.1 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	gotest.tools/gotestsum v1.8.2 // indirect
)
