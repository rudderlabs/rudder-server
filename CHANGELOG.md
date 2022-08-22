# Changelog

## [1.0.0](https://github.com/rudderlabs/rudder-server/compare/v0.2.0...v1.0.0) (2022-08-19)


### Features

* backend config namespace support ([#2232](https://github.com/rudderlabs/rudder-server/issues/2232)) ([30da9ec](https://github.com/rudderlabs/rudder-server/commit/30da9eca83d8ece123d35467ece02ffb5cefe134))
* **core:** secure customer s3 bucket access for backups using cross-account role ([#2250](https://github.com/rudderlabs/rudder-server/issues/2250)) ([964c2d9](https://github.com/rudderlabs/rudder-server/commit/964c2d92a64875566f4cb6e71bc515ab71771ff9))
* **destination:** onboarding Lambda destination ([#2229](https://github.com/rudderlabs/rudder-server/issues/2229)) ([581aef4](https://github.com/rudderlabs/rudder-server/commit/581aef4bc02c06d99a7f1f637d150391bb294988))
* introduce retention for datasets ([#2214](https://github.com/rudderlabs/rudder-server/issues/2214)) ([beb7be8](https://github.com/rudderlabs/rudder-server/commit/beb7be8d56ad388de1dbd7954cd1f94bdf6acba8))
* minor changes for namespace config endpoint ([#2307](https://github.com/rudderlabs/rudder-server/issues/2307)) ([8c5c2bc](https://github.com/rudderlabs/rudder-server/commit/8c5c2bcbc090c7b3560d8ea1a7258d3844416af3))
* **processor:** filter unsupported messages (type & event fields) ([#2211](https://github.com/rudderlabs/rudder-server/issues/2211)) ([e701950](https://github.com/rudderlabs/rudder-server/commit/e70195060db44e06408a463fd9edb095690799fb))
* update namespace endpoint according to the latest changes ([#2269](https://github.com/rudderlabs/rudder-server/issues/2269)) ([eb67dca](https://github.com/rudderlabs/rudder-server/commit/eb67dca0e808d4932b4aee8bce60edb90865363c))
* use different http client timeouts for different components ([#2300](https://github.com/rudderlabs/rudder-server/issues/2300)) ([9dc3d48](https://github.com/rudderlabs/rudder-server/commit/9dc3d48f5994418a527f632a182ff00237440004))
* **warehouse:** handle bucket config change ([#2084](https://github.com/rudderlabs/rudder-server/issues/2084)) ([7f83f0b](https://github.com/rudderlabs/rudder-server/commit/7f83f0b12c479fe17be97457c35afa5a6d0dfef5))


### Bug Fixes

* awsutils to support secretAccessKey ([#2306](https://github.com/rudderlabs/rudder-server/issues/2306)) ([801355e](https://github.com/rudderlabs/rudder-server/commit/801355ee5c8c5355a2cab241ff209387d0829a3c))
* backend config auth ([#2259](https://github.com/rudderlabs/rudder-server/issues/2259)) ([4c014a0](https://github.com/rudderlabs/rudder-server/commit/4c014a092cd9a4b5dfd26fa6d79c70a6c86a943a))
* backend config refactoring ([#2200](https://github.com/rudderlabs/rudder-server/issues/2200)) ([e242c7d](https://github.com/rudderlabs/rudder-server/commit/e242c7d5d94ecc903f2565639a1b43b4a3d31ffa))
* **core:** configSubscriber lock, router_status_events count ([#2181](https://github.com/rudderlabs/rudder-server/issues/2181)) ([3c89b96](https://github.com/rudderlabs/rudder-server/commit/3c89b96523fe1e4af701a20b2fdca9ab73adced7))
* **core:** order inserts(updates) to avoid deadlocks ([#2167](https://github.com/rudderlabs/rudder-server/issues/2167)) ([28c8872](https://github.com/rudderlabs/rudder-server/commit/28c8872c4d4ac04397470379ba1e64aa0396329f))
* **destination:** oauth expired secret type change to json.RawMessage from string ([#2236](https://github.com/rudderlabs/rudder-server/issues/2236)) ([b001747](https://github.com/rudderlabs/rudder-server/commit/b00174723355eeb8c352f66f46ef6121876c661e))
* **destination:** transformer proxy respond with timeout for slow or unresponsive dests. ([#2258](https://github.com/rudderlabs/rudder-server/issues/2258)) ([c592462](https://github.com/rudderlabs/rudder-server/commit/c592462d48b949c7d2c813ec4d77d7c7fb843a8c))
* dockerfile issues related to scripts ([#2298](https://github.com/rudderlabs/rudder-server/issues/2298)) ([7348cb2](https://github.com/rudderlabs/rudder-server/commit/7348cb210f9c0b2eb60464da52ba0ba466937f03))
* fetch filemanagers timeout from config instead of registering ([#2213](https://github.com/rudderlabs/rudder-server/issues/2213)) ([b764798](https://github.com/rudderlabs/rudder-server/commit/b764798caa531dc07a9d7ecebaf15adfeb78a8cf))
* **gateway:** make gateway user logic synonymous to proxy logic ([#2205](https://github.com/rudderlabs/rudder-server/issues/2205)) ([8c889ae](https://github.com/rudderlabs/rudder-server/commit/8c889aefc8123b3b85016f0e6051c0463f24933a))
* group errors swallowed + honouring ctx ([#2309](https://github.com/rudderlabs/rudder-server/issues/2309)) ([ae53e6e](https://github.com/rudderlabs/rudder-server/commit/ae53e6e642ff9780ed7fc795d35984eaba55ed41))
* **jobsdb:** fix ANALYSE for Negative Indices during internal migration ([#2192](https://github.com/rudderlabs/rudder-server/issues/2192)) ([69ae857](https://github.com/rudderlabs/rudder-server/commit/69ae857639e31e7faf540a3c77fa203a15f9f041))
* multi_tenant_test.go improvements ([#2247](https://github.com/rudderlabs/rudder-server/issues/2247)) ([b644468](https://github.com/rudderlabs/rudder-server/commit/b644468a7418e8e51814338a9e776cd578272404))
* only tag images with `:latest` if they are part of a release ([#2174](https://github.com/rudderlabs/rudder-server/issues/2174)) ([b8c5353](https://github.com/rudderlabs/rudder-server/commit/b8c5353e60be9c7d9cad4b9d32de5ecf74150fe0))
* parse JSON error field as a string ([#2246](https://github.com/rudderlabs/rudder-server/issues/2246)) ([fbb9b62](https://github.com/rudderlabs/rudder-server/commit/fbb9b6291cb6449480f1c5a927fbf73bb101106c))
* reinstate bugsnag notifications ([#2265](https://github.com/rudderlabs/rudder-server/issues/2265)) ([781862c](https://github.com/rudderlabs/rudder-server/commit/781862cca5e8fe7d3a549e3aed62eec2fd03b79b))
* restore / endpoint ([#2237](https://github.com/rudderlabs/rudder-server/issues/2237)) ([2316604](https://github.com/rudderlabs/rudder-server/commit/23166042b7f20fbdde0a5a414673f57aca6f3608))
* **router:** handle transformer in/out inconsistencies ([#2276](https://github.com/rudderlabs/rudder-server/issues/2276)) ([ecca8ec](https://github.com/rudderlabs/rudder-server/commit/ecca8ec91038aa48f4fce0e9cc603ac792a2e258))
* stream manager logging format string ([#2263](https://github.com/rudderlabs/rudder-server/issues/2263)) ([5aea319](https://github.com/rudderlabs/rudder-server/commit/5aea3192c32cfc3689ab1c7757ab4a2669be93b3))
* **warehouse:** marshall pg_notifier_queue payload during the time of inserting the data ([#2243](https://github.com/rudderlabs/rudder-server/issues/2243)) ([a3d1632](https://github.com/rudderlabs/rudder-server/commit/a3d1632a92d8bc456d9c74fc607d7bcd62e784cc))
* **warehouse:** reset priority if earlier upload is already in progress ([b9e2674](https://github.com/rudderlabs/rudder-server/commit/b9e26741d2262eebfcae7a33071f4c1a2ec15d5a))
* **warehouse:** support skipping escaping of reserved keywords ([#2115](https://github.com/rudderlabs/rudder-server/issues/2115)) ([2b4425a](https://github.com/rudderlabs/rudder-server/commit/2b4425ae53fc8ea4ef69f5e0921ccd4fa0c46a72))
* **warehouse:** use correct config for destination name ([#2221](https://github.com/rudderlabs/rudder-server/issues/2221)) ([666654e](https://github.com/rudderlabs/rudder-server/commit/666654e35b64af625111c7c5d809a5e4199efa0c))
* **warehouse:** use global config for parquet patallel writers ([#2184](https://github.com/rudderlabs/rudder-server/issues/2184)) ([54091ed](https://github.com/rudderlabs/rudder-server/commit/54091ed987135a7febf384fb6382bccdd856fafe))


### Miscellaneous Chores

* prepare v1 release ([040b734](https://github.com/rudderlabs/rudder-server/commit/040b734aa178eedb26f7ed9f21d324efac69e2ab))

## [0.2.0](https://github.com/rudderlabs/rudder-server/compare/v0.1.11...v0.2.0) (2022-07-06)


### Features

* **core:** add support for managing failed records in rsources.JobService ([#2087](https://github.com/rudderlabs/rudder-server/issues/2087)) ([c22541c](https://github.com/rudderlabs/rudder-server/commit/c22541cbe248efe76c184bb426ad18c78f007588))
* **core:** introduce new failed-records endpoint ([#2120](https://github.com/rudderlabs/rudder-server/issues/2120)) ([70cbdcd](https://github.com/rudderlabs/rudder-server/commit/70cbdcdbd6ea10bd4c0269f5fa86d471d03c1cb3))
* **destination:** putting support for AVRO in Kafka. ([#2042](https://github.com/rudderlabs/rudder-server/issues/2042)) ([b9677a8](https://github.com/rudderlabs/rudder-server/commit/b9677a8858046eec2856c0365520a21778c8913f))
* expose robots.txt to disable crawling ([#2090](https://github.com/rudderlabs/rudder-server/issues/2090)) ([506e9aa](https://github.com/rudderlabs/rudder-server/commit/506e9aaf251fec44c723806a012ef3b8faec7b69))
* **warehouse:** enable postgres query execution ([#2106](https://github.com/rudderlabs/rudder-server/issues/2106)) ([66a8846](https://github.com/rudderlabs/rudder-server/commit/66a8846cf67d9ace6671ba6ca7cbf07674073de5))


### Bug Fixes

* calculate consistent tagStr cache keys in stats ([#2108](https://github.com/rudderlabs/rudder-server/issues/2108)) ([fea8e30](https://github.com/rudderlabs/rudder-server/commit/fea8e307db1b2091808ca578686bc3c7f9ca36c8))
* kafka multiple hosts ([#2103](https://github.com/rudderlabs/rudder-server/issues/2103)) ([30ee189](https://github.com/rudderlabs/rudder-server/commit/30ee189c8d3f89c0a86f475e085b6110ad3e348e))
* make sure jobsdb is started when using the replay feature ([#2071](https://github.com/rudderlabs/rudder-server/issues/2071)) ([b64a37f](https://github.com/rudderlabs/rudder-server/commit/b64a37fd7209317b3928df0dba9da7092bc8af06))
* **multi-tenant:** removed unused mock calls ([#2073](https://github.com/rudderlabs/rudder-server/issues/2073)) ([7e33ede](https://github.com/rudderlabs/rudder-server/commit/7e33edeff55a26bcd4eb489bf71df9453995bd9c))
* **processor:** include test for sources fields in transform event metadata ([#2143](https://github.com/rudderlabs/rudder-server/issues/2143)) ([5a0f854](https://github.com/rudderlabs/rudder-server/commit/5a0f854d9e4ccbe12a5ec2faf093d30695362e0b))
* properly start a new badgerdb transaction on ErrTxnTooBig ([#2061](https://github.com/rudderlabs/rudder-server/issues/2061)) ([ea839f4](https://github.com/rudderlabs/rudder-server/commit/ea839f446856890d3c61c9b127a0e5046a95276d))
* removed the output of goRoutine count from the HTTP GET / of the server ([#2079](https://github.com/rudderlabs/rudder-server/issues/2079)) ([ca6450b](https://github.com/rudderlabs/rudder-server/commit/ca6450be226369bce188911f3b172d105ad86e8d))
* update failedJobIDMap after draining a job in router ([#2065](https://github.com/rudderlabs/rudder-server/issues/2065)) ([19d1e3d](https://github.com/rudderlabs/rudder-server/commit/19d1e3d0ef80c9255dd1ce928e43aa259d2354e3))
* use a writer jobsdb for writing to gw tables during replay ([#2086](https://github.com/rudderlabs/rudder-server/issues/2086)) ([bad7dea](https://github.com/rudderlabs/rudder-server/commit/bad7dea0e0121cb85a57590b3fdce16be5d687c3))
* **warehouse:** close connections after validation steps ([#2122](https://github.com/rudderlabs/rudder-server/issues/2122)) ([6491d65](https://github.com/rudderlabs/rudder-server/commit/6491d65aa64a761dbff26eded370e3a7026dc06c))


### Miscellaneous Chores

* change release to 0.2.0 ([3cd36f4](https://github.com/rudderlabs/rudder-server/commit/3cd36f46ae5de7b3c6a71d3e259fcb5a47fbb1ba))

## [0.1.11](https://github.com/rudderlabs/rudder-server/compare/v0.1.10...v0.1.11) (2022-06-16)


### Features

* **core:** pub-sub for sources jobs stats in a multi-tenant scenario ([#2022](https://github.com/rudderlabs/rudder-server/issues/2022)) ([81d01ce](https://github.com/rudderlabs/rudder-server/commit/81d01ced855cc6f939dcb3f8ef6c930793e10078))
* integration tags ([#2053](https://github.com/rudderlabs/rudder-server/issues/2053)) ([3ad96b4](https://github.com/rudderlabs/rudder-server/commit/3ad96b47404e897ca641929a22b8dfbb529b3f26))


### Bug Fixes

* **jobsdb:** use transaction instead of previous prepared statement for executing ANALYZE ([#2054](https://github.com/rudderlabs/rudder-server/issues/2054)) ([ef01b16](https://github.com/rudderlabs/rudder-server/commit/ef01b16b5e79ee0705c8b4ced57770ab712837dc))

## 0.1.10 (2022-06-14)


### Features

* calculate aggregated pending_events_count metrics for all workspaces ([#1858](https://github.com/rudderlabs/rudder-server/issues/1858)) ([f16a239](https://github.com/rudderlabs/rudder-server/commit/f16a2399720a0209982742b5430dd50a3fcf04fa))
* capture rudder-sources job statistics ([#1949](https://github.com/rudderlabs/rudder-server/issues/1949)) ([d14deba](https://github.com/rudderlabs/rudder-server/commit/d14debaa1112fe71db63ba2490c406661bfa29b8))
* Checks checksum written to the destination ssl folder, rewrite only if checksum changes ([214e774](https://github.com/rudderlabs/rudder-server/commit/214e774b0b3c8f76ace375a06c88ba8e479f827e))
* Checks checksum written to the destination ssl folder, rewrite only if checksum changes ([57f73e8](https://github.com/rudderlabs/rudder-server/commit/57f73e805843dad196f9e1fac3973f4904baf275))
* **core:** add support for transient sources ([#1869](https://github.com/rudderlabs/rudder-server/issues/1869)) ([049b015](https://github.com/rudderlabs/rudder-server/commit/049b01594f17c9be98ab792944c0d72bcaa6ac03))
* **core:** default rsources.JobService implementation ([#1938](https://github.com/rudderlabs/rudder-server/issues/1938)) ([b08bdee](https://github.com/rudderlabs/rudder-server/commit/b08bdee277e0a55c4f4e56228f4ae656a1ec8006))
* **core:** updating enterprise commit with changes related reporting service ([#2035](https://github.com/rudderlabs/rudder-server/issues/2035)) ([e94d313](https://github.com/rudderlabs/rudder-server/commit/e94d3139d9571d19ab520917ad978a33d42de17d))
* enable timeouts for google destinations ([#1857](https://github.com/rudderlabs/rudder-server/issues/1857)) ([31b64d7](https://github.com/rudderlabs/rudder-server/commit/31b64d7205ef9faa5b9457e8847a567e43c23a6c))
* gateway back pressure mechanism implementation ([#1847](https://github.com/rudderlabs/rudder-server/issues/1847)) ([c9b6f02](https://github.com/rudderlabs/rudder-server/commit/c9b6f02a42bc69aae54d44151892a990fa210fec))
* **gateway:** introduce /job-status endpoint ([#1917](https://github.com/rudderlabs/rudder-server/issues/1917)) ([688cf1e](https://github.com/rudderlabs/rudder-server/commit/688cf1ed1cf2cd6710522fe8a870b3132cfd1376))
* google sheets batching support ([#1764](https://github.com/rudderlabs/rudder-server/issues/1764)) ([497b1d9](https://github.com/rudderlabs/rudder-server/commit/497b1d9a5eeee46ee41e19b37e4ce799e41bc5dc))
* **jobsdb:** made backup related config hot-reloadable ([#1844](https://github.com/rudderlabs/rudder-server/issues/1844)) ([5f69442](https://github.com/rudderlabs/rudder-server/commit/5f69442a09342826edc8260c193ecf72c00d2716))
* **jobsdb:** support limiting jobs' pickup based on total payload size ([#1884](https://github.com/rudderlabs/rudder-server/issues/1884)) ([699727f](https://github.com/rudderlabs/rudder-server/commit/699727f3c1a66899b84afd84f8b71389d92480be))
* kafka client ([#1875](https://github.com/rudderlabs/rudder-server/issues/1875)) ([f900211](https://github.com/rudderlabs/rudder-server/commit/f900211d56cb578152940922e18f088269b40cfd))
* kafka publishTime stat ([#2008](https://github.com/rudderlabs/rudder-server/issues/2008)) ([01454ad](https://github.com/rudderlabs/rudder-server/commit/01454ad3f10722e2b16e2a17d2cc503a6f7a61a8))
* more kafka stats ([#2013](https://github.com/rudderlabs/rudder-server/issues/2013)) ([c2379c1](https://github.com/rudderlabs/rudder-server/commit/c2379c144881a8e8dea03eb981c2ed248fb58772))
* **multi-tenant:** add mode provider in docker-compose file ([#1931](https://github.com/rudderlabs/rudder-server/issues/1931)) ([a81bb0a](https://github.com/rudderlabs/rudder-server/commit/a81bb0aaff4a1e339168f7bd36dd9fc70c36cad4))
* **multi-tenant:** cluster manager gateway int ([#1943](https://github.com/rudderlabs/rudder-server/issues/1943)) ([1f2ca4d](https://github.com/rudderlabs/rudder-server/commit/1f2ca4d23d0ec58d4124b29f21f8059f6169a251))
* **multi-tenant:** etcd provider and config backend updates ([#1839](https://github.com/rudderlabs/rudder-server/issues/1839)) ([6985721](https://github.com/rudderlabs/rudder-server/commit/69857216e8b736d2924a761a8eef578edce8d40e))
* **multi-tenant:** introduce deployment types, enabling multitenant with etcd ([#1882](https://github.com/rudderlabs/rudder-server/issues/1882)) ([6c04f99](https://github.com/rudderlabs/rudder-server/commit/6c04f9913e19d68ff5cd3fbb720ee1e7568faf76))
* **multi-tenant:** multitenant support for embedded app ([#2018](https://github.com/rudderlabs/rudder-server/issues/2018)) ([8ea4ff6](https://github.com/rudderlabs/rudder-server/commit/8ea4ff6ea1546326ccf1ff41b17e37ed6df1a47e))
* **multi-tenant:** use cached backend config ([#1996](https://github.com/rudderlabs/rudder-server/issues/1996)) ([8f1cc8b](https://github.com/rudderlabs/rudder-server/commit/8f1cc8b2e3986f8f82609bd2fc51d1946db8289d))
* **router:** enable batching config at destType level. ([#1902](https://github.com/rudderlabs/rudder-server/issues/1902)) ([d69d489](https://github.com/rudderlabs/rudder-server/commit/d69d4892ac26397c612d269fc67495dafc336dc5))
* **router:** use a circuit breaker while producing custom destination clients ([#2016](https://github.com/rudderlabs/rudder-server/issues/2016)) ([235f3c2](https://github.com/rudderlabs/rudder-server/commit/235f3c2333c431956c65ea64d0dc622929792714))
* stats for skipped messages on kafka destinations ([#1934](https://github.com/rudderlabs/rudder-server/issues/1934)) ([b58c7e9](https://github.com/rudderlabs/rudder-server/commit/b58c7e94f6f0ace053a385e46d5aade4b750422f))
* use new client for kafkamanager to support timeouts ([#1920](https://github.com/rudderlabs/rudder-server/issues/1920)) ([82db596](https://github.com/rudderlabs/rudder-server/commit/82db5965a1051065c2ce8e3f1b1512e98d2ad0e4))
* **warehouse:** added metrics for capturing stats in warehouse scheduling ([d2868c7](https://github.com/rudderlabs/rudder-server/commit/d2868c732b198b44f8827a6aa2c1def0f430ab9e))
* **warehouse:** added support for deltalake partition and external location support. ([#1914](https://github.com/rudderlabs/rudder-server/issues/1914)) ([7d85221](https://github.com/rudderlabs/rudder-server/commit/7d85221f71065e4d2654f9d15160d19307986779))
* **warehouse:** added support for multi retry admin api ([#1889](https://github.com/rudderlabs/rudder-server/issues/1889)) ([230473b](https://github.com/rudderlabs/rudder-server/commit/230473b45ba02b878581eef760eef097a384470d))
* **warehouse:** configurable timewindow layout for gcs datalake ([#2015](https://github.com/rudderlabs/rudder-server/issues/2015)) ([931c5fc](https://github.com/rudderlabs/rudder-server/commit/931c5fc6c2b4d20cef645810f8a47de76f65c756))
* **warehouse:** json column support for warehouse destinations bq,postgres,snowlake,rs ([#1897](https://github.com/rudderlabs/rudder-server/issues/1897)) ([2f877f7](https://github.com/rudderlabs/rudder-server/commit/2f877f7815539a79305f4a3fa8ecd1fc4fcb9236))


### Bug Fixes

* `pre_drop` table pileup issue ([#1799](https://github.com/rudderlabs/rudder-server/issues/1799)) ([e301b56](https://github.com/rudderlabs/rudder-server/commit/e301b56a80d1e7643e45cb92db1df48a34905d65))
* added http client timeout ([#1873](https://github.com/rudderlabs/rudder-server/issues/1873)) ([83f8041](https://github.com/rudderlabs/rudder-server/commit/83f8041b2c35ce1aa0035038972bda5366459638))
* added http client timeout in processor transformer ([#1887](https://github.com/rudderlabs/rudder-server/issues/1887)) ([7028f15](https://github.com/rudderlabs/rudder-server/commit/7028f15dbbdd84ca2b76f59c9cf0e2f931840793))
* admin status handlers data race ([#1838](https://github.com/rudderlabs/rudder-server/issues/1838)) ([d4bdb5c](https://github.com/rudderlabs/rudder-server/commit/d4bdb5c85bc2ffac22da876e53f099cb2850bb2b))
* disable bq dedup by default ([#1855](https://github.com/rudderlabs/rudder-server/issues/1855)) ([8c05b37](https://github.com/rudderlabs/rudder-server/commit/8c05b3789e6538c6ec0ed464de7be4b2fe08ee90))
* disable creation of views when dedup is enabled in BQ ([#1885](https://github.com/rudderlabs/rudder-server/issues/1885)) ([414551d](https://github.com/rudderlabs/rudder-server/commit/414551d8511b6305881ac260d5acdea675269548))
* disable stash backup if master Backup is disabled. ([#1874](https://github.com/rudderlabs/rudder-server/issues/1874)) ([ea9921d](https://github.com/rudderlabs/rudder-server/commit/ea9921dc2fe4036d23eb4f38e8444179b3313cdb))
* disable support for Google Developers Console client_credentials.json file ([#2001](https://github.com/rudderlabs/rudder-server/issues/2001)) ([ef83509](https://github.com/rudderlabs/rudder-server/commit/ef83509e33dbdf9a0112c262781e351186dd73cb))
* do not skip stash aborting jobs if backup is disabled ([#2003](https://github.com/rudderlabs/rudder-server/issues/2003)) ([498bd93](https://github.com/rudderlabs/rudder-server/commit/498bd9352f4fd074b6ba701880a982fb5258ac45))
* enabling earlier usage of stats in other components ([#1937](https://github.com/rudderlabs/rudder-server/issues/1937)) ([b71c93d](https://github.com/rudderlabs/rudder-server/commit/b71c93ddf16650571913bf55848128af0bb35bd9))
* EventBus memory issue with slow subscribers ([#1802](https://github.com/rudderlabs/rudder-server/issues/1802)) ([d6b85c1](https://github.com/rudderlabs/rudder-server/commit/d6b85c1f980050558e586f46d6b7d7f256551315))
* execute ANALYZE after completing batch jobsdb operations ([#1800](https://github.com/rudderlabs/rudder-server/issues/1800)) ([db951aa](https://github.com/rudderlabs/rudder-server/commit/db951aaedeb37d4103b47395bafc0b17ec9b1339))
* fix repetitive bugsnag alert for a single crash ([#1928](https://github.com/rudderlabs/rudder-server/issues/1928)) ([0653956](https://github.com/rudderlabs/rudder-server/commit/06539564714f288653e4623594c64913b026afe0))
* Force set lower version during pg notifier sql migration ([#1907](https://github.com/rudderlabs/rudder-server/issues/1907)) ([151e308](https://github.com/rudderlabs/rudder-server/commit/151e308f88b1d50e2468c9702029bc1b461ffdcc))
* Force set lower version during pg notifier sql migration ([#1907](https://github.com/rudderlabs/rudder-server/issues/1907)) ([#1908](https://github.com/rudderlabs/rudder-server/issues/1908)) ([a54bc45](https://github.com/rudderlabs/rudder-server/commit/a54bc452f0f6434fd49a03cfe69ab9af33b3a568))
* **gateway:** getUsersPayload optimizations ([#2040](https://github.com/rudderlabs/rudder-server/issues/2040)) ([f48693e](https://github.com/rudderlabs/rudder-server/commit/f48693ed4cfbe61d612402906d2769a2f1958dae))
* install ginkgo V2 cli ([ee904a4](https://github.com/rudderlabs/rudder-server/commit/ee904a42e49dbf39680010c6d20725b4b54a04b9))
* Jobsdb migration bug ([#1785](https://github.com/rudderlabs/rudder-server/issues/1785)) ([b091116](https://github.com/rudderlabs/rudder-server/commit/b09111653daae69825678d6fc4bd631a1cf2be5d))
* **jobsdb:** mark MIGRATE_COPY done and POST_MIGRATE_DS_OP start atomically ([#1999](https://github.com/rudderlabs/rudder-server/issues/1999)) ([0ef882f](https://github.com/rudderlabs/rudder-server/commit/0ef882f10c61e40c658b830635ed872e3bc786e8))
* **jobsdb:** sanitize jsonb values before inserting them to the database ([#1963](https://github.com/rudderlabs/rudder-server/issues/1963)) ([c74df14](https://github.com/rudderlabs/rudder-server/commit/c74df1401ef1c49525f5d2d53dc31f2742c49ba1))
* lock by value ([#1842](https://github.com/rudderlabs/rudder-server/issues/1842)) ([777541c](https://github.com/rudderlabs/rudder-server/commit/777541cb6f8b9e1d164bfc0108f84192143e3621))
* **Makefile:** Keep separate cover profiles ([823ab85](https://github.com/rudderlabs/rudder-server/commit/823ab85666543d1fdcf9672f79a4f5327b2710bc))
* move initialisation to setup ([23445a4](https://github.com/rudderlabs/rudder-server/commit/23445a41c6a3bfd9deb1dbad365432dae0ee3f93))
* **multi-tenant:** etcd Key case fix ([#1954](https://github.com/rudderlabs/rudder-server/issues/1954)) ([304a0b7](https://github.com/rudderlabs/rudder-server/commit/304a0b7b73ff1eea3cc72738fb8c69d09ce0e73a))
* **multi-tenant:** multitenanat backend config ([#1930](https://github.com/rudderlabs/rudder-server/issues/1930)) ([1ed73fa](https://github.com/rudderlabs/rudder-server/commit/1ed73fac4ea400c8d987a07c39868033268fe577))
* ovverride arm64 check ([#1936](https://github.com/rudderlabs/rudder-server/issues/1936)) ([e887e23](https://github.com/rudderlabs/rudder-server/commit/e887e235479f94abb30c8d710d0c6ed7abd4c4b3))
* **rateLimit:** Moving Rate Limit Block post Validations ([4720e6b](https://github.com/rudderlabs/rudder-server/commit/4720e6b185ea21efc1388bace878bc9c388d35f5))
* **readme:** update developer machine setup link ([#1843](https://github.com/rudderlabs/rudder-server/issues/1843)) ([cd32fb7](https://github.com/rudderlabs/rudder-server/commit/cd32fb7b05b59c92ceea47f810debf0a8df1f32c))
* remove ginkgo version ([87c3d89](https://github.com/rudderlabs/rudder-server/commit/87c3d89a46a2ecb7ac4664989532caf833ac681e))
* Remove unused Setup and Finish methods ([0149a18](https://github.com/rudderlabs/rudder-server/commit/0149a184c41e1b0a80ab84031476d20986dfe6b1))
* reporting the drained events to the reports table ([#1982](https://github.com/rudderlabs/rudder-server/issues/1982)) ([b40dabe](https://github.com/rudderlabs/rudder-server/commit/b40dabee51ab96062f0cccbf089293b814753bde))
* **reporting:** gateway events are not reported when source is connected to no destination(or device mode destination) ([#1822](https://github.com/rudderlabs/rudder-server/issues/1822)) ([2962bd6](https://github.com/rudderlabs/rudder-server/commit/2962bd690381c151ed8d0fbcee2990a6cd1063d5))
* **router:** Added/Modified Stats for assistance in debugging for transformerProxy ([#1979](https://github.com/rudderlabs/rudder-server/issues/1979)) ([b79d9d9](https://github.com/rudderlabs/rudder-server/commit/b79d9d94ad37530cc9e0697babd4b8d79c214a02))
* **router:** panic sources grouped together ([#1846](https://github.com/rudderlabs/rudder-server/issues/1846)) ([69344c6](https://github.com/rudderlabs/rudder-server/commit/69344c691fccbd77398d88db3db1bcd2c72c6022))
* safe concurrent access of metric registry's index values ([#1805](https://github.com/rudderlabs/rudder-server/issues/1805)) ([7e672e6](https://github.com/rudderlabs/rudder-server/commit/7e672e6caa4121bbb85fd9e487a734a07fe23e20))
* Save responses from the destinations with odd content-type header ([#1749](https://github.com/rudderlabs/rudder-server/issues/1749)) ([67c1f59](https://github.com/rudderlabs/rudder-server/commit/67c1f59f45dd1e84fe2756e2aa2de1c957be1dc3))
* support multiple configvars registration ([#1878](https://github.com/rudderlabs/rudder-server/issues/1878)) ([e1ece33](https://github.com/rudderlabs/rudder-server/commit/e1ece3385da9744e902cd98056002d2d1f16d116))
* take care of nullable columns when retrieving dangling connections ([#1801](https://github.com/rudderlabs/rudder-server/issues/1801)) ([099b3c0](https://github.com/rudderlabs/rudder-server/commit/099b3c0e1a0d10086f6e8f906532219d22f0f155))
* **tests.yaml:** remove go get ginkgo ([5ad45bc](https://github.com/rudderlabs/rudder-server/commit/5ad45bc44ffa2511a484071669903c0fb0feabd4))
* **tests.yaml:** remove go get ginkgo open source ([bac703c](https://github.com/rudderlabs/rudder-server/commit/bac703ce20b5a6190b1531bddb670bf39422a869))
* **tooling:** add prometheus endpoints for OSS users ([867c9c3](https://github.com/rudderlabs/rudder-server/commit/867c9c3c4515d1713f3e50cb6039a40306542e7a))
* update enterprise commit ([ad3dc56](https://github.com/rudderlabs/rudder-server/commit/ad3dc56afc0e31b3048224697deba28ef5c3743e))
* Update enterprise commit ([55a0f83](https://github.com/rudderlabs/rudder-server/commit/55a0f8396d84c05c1382a8b13ae15e6f803f7342))
* use the correct measurement name and tags for aggregated pending events metrics ([#1891](https://github.com/rudderlabs/rudder-server/issues/1891)) ([5f45c1f](https://github.com/rudderlabs/rudder-server/commit/5f45c1f402093b6237485b026e8b64abec3caa5b))
* **warehouse:** Add explicit migration for pg notifier priority column ([#1898](https://github.com/rudderlabs/rudder-server/issues/1898)) ([224038d](https://github.com/rudderlabs/rudder-server/commit/224038d928d3df7baa39caa4c6fa0c4d0e41a8b8))
* **warehouse:** add support for multiline in deltalake. ([#1872](https://github.com/rudderlabs/rudder-server/issues/1872)) ([ecace44](https://github.com/rudderlabs/rudder-server/commit/ecace44de08c979cd27fc2d2bbe1abf361a3751e))
* **warehouse:** added ability to test destination credentials once the upload gets aborted ([#1890](https://github.com/rudderlabs/rudder-server/issues/1890)) ([fbc862d](https://github.com/rudderlabs/rudder-server/commit/fbc862d454d8a4759865b3e2bfc006e9ac1dea1e))
* **warehouse:** closing file while creating load file for warehouse configuration test ([#1919](https://github.com/rudderlabs/rudder-server/issues/1919)) ([8d71648](https://github.com/rudderlabs/rudder-server/commit/8d716483ade7e2c155b2dc791ccb1c365ec332fd))
* **warehouse:** columns need to be provided during copy command for deltalake ([#2033](https://github.com/rudderlabs/rudder-server/issues/2033)) ([75d15c6](https://github.com/rudderlabs/rudder-server/commit/75d15c638b929e93d43b32fa3e7e44ece4eb57de))
* **warehouse:** deltalake users table getting populated corruptly. ([#1976](https://github.com/rudderlabs/rudder-server/issues/1976)) ([#1977](https://github.com/rudderlabs/rudder-server/issues/1977)) ([8ba4182](https://github.com/rudderlabs/rudder-server/commit/8ba4182196565f134277fa7f43c6690bb0e7af1e))
* **warehouse:** dsiable reporting setup for slave ([#2037](https://github.com/rudderlabs/rudder-server/issues/2037)) ([983338a](https://github.com/rudderlabs/rudder-server/commit/983338ad9f62deb4213e783462e77a1c4114c3a8))
* **warehouse:** fix s3 manager for aws glue region ([#1951](https://github.com/rudderlabs/rudder-server/issues/1951)) ([539c0c9](https://github.com/rudderlabs/rudder-server/commit/539c0c9e7c6a3a9b74dec70a842a433ef6f02994))
* **warehouse:** fixing timeout issue for snowflake ([#1945](https://github.com/rudderlabs/rudder-server/issues/1945)) ([c82428a](https://github.com/rudderlabs/rudder-server/commit/c82428a85b2f256d40bbd9f6dc4198b82e67809d))
* **warehouse:** skip backendConfig check for slave ([#2034](https://github.com/rudderlabs/rudder-server/issues/2034)) ([e513501](https://github.com/rudderlabs/rudder-server/commit/e513501f931b4013456e95ab16bebc2b6118e6ca))
* **warehouse:** skipping bigquery integration test for now ([4bae57b](https://github.com/rudderlabs/rudder-server/commit/4bae57b5671a00e50a5c47454e1999d8183fadda))
* **warehouse:** updated reserved keywords for warehouse mssql and azure destination ([#1932](https://github.com/rudderlabs/rudder-server/issues/1932)) ([928d3d4](https://github.com/rudderlabs/rudder-server/commit/928d3d4193b5ddbe563c05f60371ef5879b2506e))
* **warehouse:** use correct locks for dateformat in processor ([#2020](https://github.com/rudderlabs/rudder-server/issues/2020)) ([972577a](https://github.com/rudderlabs/rudder-server/commit/972577a8c474cb8bdfbf228f9e3df621aa13ac99))
* **warehouse:** warehouse schema int to float schema fix ([#1974](https://github.com/rudderlabs/rudder-server/issues/1974)) ([bf46c85](https://github.com/rudderlabs/rudder-server/commit/bf46c850a5469acbb680f486f20d686b82fd2e6f))
* wrong routing of warehouse crash alerts ([#1926](https://github.com/rudderlabs/rudder-server/issues/1926)) ([e77dc83](https://github.com/rudderlabs/rudder-server/commit/e77dc830d222601832a4e3fca3afc56e2365f2b8))


### Miscellaneous Chores

* new release 0.1.10 ([e8fc750](https://github.com/rudderlabs/rudder-server/commit/e8fc750eff79aa558e26a2e5800f9841f4cbb268))
