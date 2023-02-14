module github.com/rudderlabs/rudder-server

go 1.19

replace (
	// FIXME: this is a hacky way to address vulnerabilities in indirect dependencies
	// 	We should frequently review this section to remove or update the replace directives
	github.com/aws/aws-sdk-go => github.com/aws/aws-sdk-go v1.44.123 // xitongsys/parquet-go-source uses a vulnerable version
	github.com/dhui/dktest => github.com/dhui/dktest v0.3.13 // many dependencies require this for testing
	github.com/prometheus/client_golang => github.com/prometheus/client_golang v1.13.0 // many dependencies a vulnerable version of this package
	golang.org/x/crypto => golang.org/x/crypto v0.0.0-20221010152910-d6f0a8c073c2 // many dependencies a vulnerable version of this package
	golang.org/x/net => golang.org/x/net v0.0.0-20221004154528-8021a29435af // many dependencies a vulnerable version of this package
	golang.org/x/text => golang.org/x/text v0.3.8 // many dependencies a vulnerable version of this package
	gopkg.in/yaml.v2 => gopkg.in/yaml.v2 v2.4.0 // github.com/spf13/viper uses a vulnerable version
	gopkg.in/yaml.v3 => gopkg.in/yaml.v3 v3.0.1 // github.com/spf13/viper uses a vulnerable version
)

require (
	cloud.google.com/go/bigquery v1.43.0
	cloud.google.com/go/pubsub v1.27.0
	cloud.google.com/go/storage v1.27.0
	github.com/Azure/azure-storage-blob-go v0.14.0
	github.com/ClickHouse/clickhouse-go v1.5.4
	github.com/EagleChen/restrictor v0.0.0-20180420073700-9b81bbf8df1d
	github.com/alexeyco/simpletable v1.0.0
	github.com/allisson/go-pglock/v2 v2.0.1
	github.com/araddon/dateparse v0.0.0-20190622164848-0fb0a474d195
	github.com/aws/aws-sdk-go v1.44.123
	github.com/bugsnag/bugsnag-go/v2 v2.1.2
	github.com/cenkalti/backoff v2.2.1+incompatible
	github.com/cenkalti/backoff/v4 v4.1.3
	github.com/denisenkom/go-mssqldb v0.12.0
	github.com/dgraph-io/badger/v2 v2.2007.4
	github.com/dgraph-io/badger/v3 v3.2103.3
	github.com/foxcpp/go-mockdns v1.0.1-0.20220408113050-3599dc5d2c7d
	github.com/fsnotify/fsnotify v1.6.0
	github.com/go-redis/redis v6.15.8+incompatible
	github.com/go-redis/redis/v8 v8.11.5
	github.com/gofrs/uuid v4.4.0+incompatible
	github.com/golang-migrate/migrate/v4 v4.15.2
	github.com/golang/mock v1.6.0
	github.com/gomodule/redigo v1.8.5
	github.com/google/uuid v1.3.0
	github.com/gorilla/mux v1.8.0
	github.com/hashicorp/go-retryablehttp v0.7.0
	github.com/hashicorp/yamux v0.0.0-20200609203250-aecfd211c9ce
	github.com/iancoleman/strcase v0.2.0
	github.com/jeremywohl/flatten v1.0.1
	github.com/joho/godotenv v1.3.0
	github.com/json-iterator/go v1.1.12
	github.com/lib/pq v1.10.7
	github.com/minio/minio-go v6.0.14+incompatible
	github.com/minio/minio-go/v6 v6.0.57
	github.com/minio/minio-go/v7 v7.0.34
	github.com/mkmik/multierror v0.3.0
	github.com/onsi/ginkgo/v2 v2.1.6
	github.com/onsi/gomega v1.20.2
	github.com/phayes/freeport v0.0.0-20220201140144-74d24b5ae9f5
	github.com/rs/cors v1.7.0
	github.com/rudderlabs/analytics-go v3.3.1+incompatible
	github.com/rudderlabs/sql-tunnels v0.1.2
	github.com/samber/lo v1.37.0
	github.com/segmentio/kafka-go v0.4.35
	github.com/shirou/gopsutil/v3 v3.23.1
	github.com/snowflakedb/gosnowflake v1.6.13
	github.com/sony/gobreaker v0.5.0
	github.com/spaolacci/murmur3 v1.1.0
	github.com/spf13/viper v1.13.0
	github.com/stretchr/testify v1.8.1
	github.com/thoas/go-funk v0.9.1
	github.com/throttled/throttled/v2 v2.9.1
	github.com/tidwall/gjson v1.14.3
	github.com/tidwall/sjson v1.2.5
	github.com/viney-shih/go-lock v1.1.2
	github.com/xitongsys/parquet-go v1.6.2
	github.com/zenizh/go-capturer v0.0.0-20211219060012-52ea6c8fed04
	go.etcd.io/etcd/api/v3 v3.5.5
	go.etcd.io/etcd/client/v3 v3.5.5
	go.uber.org/automaxprocs v1.4.0
	go.uber.org/goleak v1.2.0
	go.uber.org/zap v1.23.0
	golang.org/x/net v0.5.0
	golang.org/x/oauth2 v0.0.0-20221014153046-6fdb5e3db783
	golang.org/x/sync v0.1.0
	google.golang.org/api v0.103.0
	google.golang.org/grpc v1.51.0
	google.golang.org/protobuf v1.28.1
	gopkg.in/alexcesaro/statsd.v2 v2.0.0
	gopkg.in/natefinch/lumberjack.v2 v2.0.0
)

require (
	cloud.google.com/go v0.105.0 // indirect
	cloud.google.com/go/compute v1.12.1 // indirect
	cloud.google.com/go/compute/metadata v0.2.1 // indirect
	cloud.google.com/go/iam v0.7.0 // indirect
	github.com/Azure/azure-pipeline-go v0.2.3 // indirect
	github.com/Azure/go-ansiterm v0.0.0-20210617225240-d185dfc1b5a1 // indirect
	github.com/Azure/go-autorest/autorest/adal v0.9.20 // indirect
	github.com/EagleChen/mapmutex v0.0.0-20180418073615-e1a5ae258d8d // indirect
	github.com/Microsoft/go-winio v0.5.2 // indirect
	github.com/Nvveen/Gotty v0.0.0-20120604004816-cd527374f1e5 // indirect
	github.com/apache/arrow/go/arrow v0.0.0-20211112161151-bc219186db40 // indirect
	github.com/apache/thrift v0.14.2 // indirect
	github.com/aws/aws-sdk-go-v2 v1.16.2 // indirect
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.4.1 // indirect
	github.com/aws/aws-sdk-go-v2/credentials v1.11.2 // indirect
	github.com/aws/aws-sdk-go-v2/feature/s3/manager v1.11.3 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.1.9 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.4.3 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.9.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.1.3 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.9.3 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.13.3 // indirect
	github.com/aws/aws-sdk-go-v2/service/s3 v1.26.3 // indirect
	github.com/aws/smithy-go v1.11.2
	github.com/bugsnag/panicwrap v1.3.4 // indirect
	github.com/cespare/xxhash v1.1.0 // indirect
	github.com/cespare/xxhash/v2 v2.1.2 // indirect
	github.com/cloudflare/golz4 v0.0.0-20150217214814-ef862a3cdc58 // indirect
	github.com/containerd/continuity v0.3.0 // indirect
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/coreos/go-systemd/v22 v22.3.2 // indirect
	github.com/cpuguy83/go-md2man/v2 v2.0.2 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dgraph-io/ristretto v0.1.1 // indirect
	github.com/dgryski/go-farm v0.0.0-20200201041132-a6ae2369ad13 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/docker/cli v20.10.14+incompatible // indirect
	github.com/docker/docker v20.10.21+incompatible // indirect
	github.com/docker/go-connections v0.4.0 // indirect
	github.com/docker/go-units v0.5.0 // indirect
	github.com/dustin/go-humanize v1.0.0 // indirect
	github.com/form3tech-oss/jwt-go v3.2.5+incompatible // indirect
	github.com/gabriel-vasile/mimetype v1.4.0 // indirect
	github.com/garyburd/redigo v1.6.0 // indirect
	github.com/go-ini/ini v1.63.2 // indirect
	github.com/go-ole/go-ole v1.2.6 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang-sql/civil v0.0.0-20190719163853-cb61b32ac6fe // indirect
	github.com/golang-sql/sqlexp v0.0.0-20170517235910-f1bb20e5a188 // indirect
	github.com/golang/glog v0.0.0-20160126235308-23def4e6c14b // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/flatbuffers v2.0.0+incompatible // indirect
	github.com/google/go-cmp v0.5.9 // indirect
	github.com/google/shlex v0.0.0-20191202100458-e7afc7fbc510 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.2.0 // indirect
	github.com/googleapis/gax-go/v2 v2.7.0 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-cleanhttp v0.5.2 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/hashicorp/golang-lru v0.5.4 // indirect
	github.com/hashicorp/hcl v1.0.0 // indirect
	github.com/imdario/mergo v0.3.12 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/kardianos/osext v0.0.0-20190222173326-2bc1f35cddc0 // indirect
	github.com/klauspost/compress v1.15.12 // indirect
	github.com/klauspost/cpuid/v2 v2.1.0 // indirect
	github.com/linkedin/goavro v2.1.0+incompatible
	github.com/lufia/plan9stats v0.0.0-20211012122336-39d0f177ccd0 // indirect
	github.com/magiconair/properties v1.8.6 // indirect
	github.com/mattn/go-ieproxy v0.0.1 // indirect
	github.com/mattn/go-runewidth v0.0.12 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.2-0.20181231171920-c182affec369 // indirect
	github.com/miekg/dns v1.1.25 // indirect
	github.com/minio/md5-simd v1.1.2 // indirect
	github.com/minio/sha256-simd v1.0.0 // indirect
	github.com/mitchellh/go-homedir v1.1.0 // indirect
	github.com/mitchellh/mapstructure v1.5.0
	github.com/moby/term v0.0.0-20220808134915-39b0c02b01ae // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/opencontainers/go-digest v1.0.0 // indirect
	github.com/opencontainers/image-spec v1.1.0-rc2 // indirect
	github.com/opencontainers/runc v1.1.4 // indirect
	github.com/ory/dockertest/v3 v3.9.1
	github.com/patrickmn/go-cache v2.1.0+incompatible // indirect
	github.com/pelletier/go-toml v1.9.5 // indirect
	github.com/pelletier/go-toml/v2 v2.0.5 // indirect
	github.com/pierrec/lz4 v2.6.1+incompatible // indirect
	github.com/pierrec/lz4/v4 v4.1.17 // indirect
	github.com/pkg/browser v0.0.0-20210911075715-681adbf594b8 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/power-devops/perfstat v0.0.0-20210106213030-5aafc221ea8c // indirect
	github.com/prometheus/client_model v0.3.0
	github.com/prometheus/common v0.37.0
	github.com/rivo/uniseg v0.1.0 // indirect
	github.com/rogpeppe/go-internal v1.9.0 // indirect
	github.com/rs/xid v1.4.0 // indirect
	github.com/russross/blackfriday/v2 v2.1.0 // indirect
	github.com/segmentio/backo-go v0.0.0-20160424052352-204274ad699c // indirect
	github.com/sirupsen/logrus v1.9.0 // indirect
	github.com/spf13/afero v1.9.2 // indirect
	github.com/spf13/cast v1.5.0
	github.com/spf13/jwalterweatherman v1.1.0 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/subosito/gotenv v1.4.1 // indirect
	github.com/tidwall/match v1.1.1 // indirect
	github.com/tidwall/pretty v1.2.0 // indirect
	github.com/tklauser/go-sysconf v0.3.11 // indirect
	github.com/tklauser/numcpus v0.6.0 // indirect
	github.com/urfave/cli/v2 v2.20.3
	github.com/xdg/scram v1.0.5 // indirect
	github.com/xdg/stringprep v1.0.3 // indirect
	github.com/xeipuuv/gojsonpointer v0.0.0-20180127040702-4e3ac2762d5f // indirect
	github.com/xeipuuv/gojsonreference v0.0.0-20180127040603-bd5ef7bd5415 // indirect
	github.com/xeipuuv/gojsonschema v1.2.0 // indirect
	github.com/xitongsys/parquet-go-source v0.0.0-20220803203939-583c0659c569
	github.com/xrash/smetrics v0.0.0-20201216005158-039620a65673 // indirect
	github.com/xtgo/uuid v0.0.0-20140804021211-a0b114877d4c // indirect
	github.com/yusufpapurcu/wmi v1.2.2 // indirect
	go.etcd.io/etcd/client/pkg/v3 v3.5.5 // indirect
	go.opencensus.io v0.24.0 // indirect
	go.uber.org/atomic v1.10.0 // indirect
	go.uber.org/multierr v1.8.0 // indirect
	golang.org/x/crypto v0.4.0 // indirect
	golang.org/x/exp v0.0.0-20221109205753-fc8884afc316
	golang.org/x/sys v0.4.0 // indirect
	golang.org/x/text v0.6.0 // indirect
	golang.org/x/tools v0.5.0 // indirect
	golang.org/x/xerrors v0.0.0-20220907171357-04be3eba64a2 // indirect
	google.golang.org/appengine v1.6.7 // indirect
	google.golang.org/genproto v0.0.0-20221118155620-16455021b5e6
	gopkg.in/ini.v1 v1.67.0 // indirect
	gopkg.in/linkedin/goavro.v1 v1.0.5 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
