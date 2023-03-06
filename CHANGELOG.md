# Changelog

## [1.6.3](https://github.com/rudderlabs/rudder-server/compare/v1.6.2...v1.6.3) (2023-03-01)


### Bug Fixes

* limiter deadlock while trying to notify a dynamic priority item ([#3056](https://github.com/rudderlabs/rudder-server/issues/3056)) ([5f967dc](https://github.com/rudderlabs/rudder-server/commit/5f967dc77a2e14338e8a7a79e60dd705cb2bc213))

## [1.6.2](https://github.com/rudderlabs/rudder-server/compare/v1.6.1...v1.6.2) (2023-02-28)


### Bug Fixes

* payload limiter gauge reports invalid value type ([#3048](https://github.com/rudderlabs/rudder-server/issues/3048)) ([5605abc](https://github.com/rudderlabs/rudder-server/commit/5605abcc1bbfe43c0a9baaae9ebf66d616664897))
* user/anonymous id read at gateway ([#3051](https://github.com/rudderlabs/rudder-server/issues/3051)) ([b87cc25](https://github.com/rudderlabs/rudder-server/commit/b87cc25ad79c7b70d3a102f949b99f71c9f58c37))

## [1.6.1](https://github.com/rudderlabs/rudder-server/compare/v1.6.0...v1.6.1) (2023-02-28)


### Miscellaneous

* debugger's badgerdb cache optimisations ([#3042](https://github.com/rudderlabs/rudder-server/issues/3042)) ([34602c3](https://github.com/rudderlabs/rudder-server/commit/34602c34924f2f007874009f6fdc69bbfb1fae44))

## [1.6.0](https://github.com/rudderlabs/rudder-server/compare/v1.5.0...v1.6.0) (2023-02-23)


### Features

* adaptive payload size limits ([#2949](https://github.com/rudderlabs/rudder-server/issues/2949)) ([fbfd845](https://github.com/rudderlabs/rudder-server/commit/fbfd845c68f8a131fd031e7bead17b40bdeeec00))
* add webhook source error logging ([#2958](https://github.com/rudderlabs/rudder-server/issues/2958)) ([042d9b3](https://github.com/rudderlabs/rudder-server/commit/042d9b3285edfc97ed3bc1da02ceb01aff22b2c3))
* filter events based on destination consent management rules ([#2973](https://github.com/rudderlabs/rudder-server/issues/2973)) ([47a12bd](https://github.com/rudderlabs/rudder-server/commit/47a12bd3a270d76da8e2509bd59746ddd078a04d))
* introduce upload id to associate staging files to uploads ([#2908](https://github.com/rudderlabs/rudder-server/issues/2908)) ([d127a82](https://github.com/rudderlabs/rudder-server/commit/d127a82b04452777a070c5bc8b93c834c0ae99b6))
* **processor:** add support for workspace-level isolation ([#2891](https://github.com/rudderlabs/rudder-server/issues/2891)) ([e8dfff3](https://github.com/rudderlabs/rudder-server/commit/e8dfff308c80e763c405b221c55890770a8e485f))
* **warehouse:** added use rudder storage option to deltalake ([#2929](https://github.com/rudderlabs/rudder-server/issues/2929)) ([6885ba1](https://github.com/rudderlabs/rudder-server/commit/6885ba1ec22e8a40b0161f42c51d8291ab0768ec))
* **warehouse:** clickhouse s3 table engine for load tables ([#2739](https://github.com/rudderlabs/rudder-server/issues/2739)) ([074e789](https://github.com/rudderlabs/rudder-server/commit/074e7897fbc0f6a43e8f5b1cddb14f2bd544ea32))
* **warehouse:** databricks unity catalog ([#2878](https://github.com/rudderlabs/rudder-server/issues/2878)) ([074e789](https://github.com/rudderlabs/rudder-server/commit/074e7897fbc0f6a43e8f5b1cddb14f2bd544ea32))
* **warehouse:** error tagging ([#2956](https://github.com/rudderlabs/rudder-server/issues/2956)) ([3131b96](https://github.com/rudderlabs/rudder-server/commit/3131b96c284840939bc9fe9ed7e0e48f5df0a9fa))
* **warehouse:** glue partitions ([#2899](https://github.com/rudderlabs/rudder-server/issues/2899)) ([9a928d4](https://github.com/rudderlabs/rudder-server/commit/9a928d46009c86d2390d5415981edfbdf37ef19f))


### Bug Fixes

* adding throttling stats in router ([#2923](https://github.com/rudderlabs/rudder-server/issues/2923)) ([61109e2](https://github.com/rudderlabs/rudder-server/commit/61109e2b99ae43e066b8c1a1041ec7f04fc007d2))
* adding throttling stats in router ([#2923](https://github.com/rudderlabs/rudder-server/issues/2923)) ([5a36692](https://github.com/rudderlabs/rudder-server/commit/5a366926ecbcad2a02e9a86c04ddd2497f3e7e41))
* create index concurrently cannot run inside a transaction block ([#3008](https://github.com/rudderlabs/rudder-server/issues/3008)) ([a9f0192](https://github.com/rudderlabs/rudder-server/commit/a9f01922cf33962c5d9e040be67fbf0fe846ca14))
* handle NULL on upload timings ([#2967](https://github.com/rudderlabs/rudder-server/issues/2967)) ([58381db](https://github.com/rudderlabs/rudder-server/commit/58381dbc49eacdfde7b35fcb1a49d26200a2a94e))
* job ordering inconsistencies with router destination isolation ([#3030](https://github.com/rudderlabs/rudder-server/issues/3030)) ([a77c382](https://github.com/rudderlabs/rudder-server/commit/a77c382c953a3dfa74e83d8939be65cb68919db0))
* limit entire transformationStatus struct when caching them ([#2928](https://github.com/rudderlabs/rudder-server/issues/2928)) ([2283aef](https://github.com/rudderlabs/rudder-server/commit/2283aefadcbb3ac7596d2e027ee67075f44ff04e))
* move recovery from scheduler to upload ([#2959](https://github.com/rudderlabs/rudder-server/issues/2959)) ([df7a491](https://github.com/rudderlabs/rudder-server/commit/df7a4918fa9ee6e221a6f8b9a557ca124ac9d4c8))
* processor read of dest consent categories ([#3028](https://github.com/rudderlabs/rudder-server/issues/3028)) ([c83582c](https://github.com/rudderlabs/rudder-server/commit/c83582ccf9cd19724e092270281b9743a8d17f19))
* **router:** wrong job order keys used in batching job order logic ([#3019](https://github.com/rudderlabs/rudder-server/issues/3019)) ([e9314cf](https://github.com/rudderlabs/rudder-server/commit/e9314cfeb9d49375a7dbb3076905cff7914f6753))
* suppression syncer using wrong credentials in multi-tenant mode ([#2936](https://github.com/rudderlabs/rudder-server/issues/2936)) ([0eb3abb](https://github.com/rudderlabs/rudder-server/commit/0eb3abbc6abf2e0ae4abfec489a84395a01a2581))
* upload missing metric ([#2947](https://github.com/rudderlabs/rudder-server/issues/2947)) ([b1fc5ee](https://github.com/rudderlabs/rudder-server/commit/b1fc5eec439f70b7f53a74220cc37782b2bb0dae))
* **warehouse:** added check for nil warehouse manager during error mapping ([#2981](https://github.com/rudderlabs/rudder-server/issues/2981)) ([a258f74](https://github.com/rudderlabs/rudder-server/commit/a258f7412f589b3010265ac3fa45857112954ece))
* **warehouse:** consolidaiton for merged schema to give preference for text datatype ([#2983](https://github.com/rudderlabs/rudder-server/issues/2983)) ([b262f1d](https://github.com/rudderlabs/rudder-server/commit/b262f1d2356b94ceee4eae64401aa022931d411a))
* **warehouse:** deltalake client crashing when failing on connectionstep ([#2961](https://github.com/rudderlabs/rudder-server/issues/2961)) ([3373144](https://github.com/rudderlabs/rudder-server/commit/337314489329ae5fd1d82d7c2cb40ddd7f12208b))
* **warehouse:** fix db migration to add upload_id to wh_staging_files… ([#2948](https://github.com/rudderlabs/rudder-server/issues/2948)) ([7fa1c9e](https://github.com/rudderlabs/rudder-server/commit/7fa1c9ef9168f17322d85b92b105bf7cd15efd0b))
* **warehouse:** fix upload error and add additional logs ([#2972](https://github.com/rudderlabs/rudder-server/issues/2972)) ([d1d2338](https://github.com/rudderlabs/rudder-server/commit/d1d2338811eec9ce2af7ad871e5a08f61215068b))
* **warehouse:** redshift dedup window ([#3013](https://github.com/rudderlabs/rudder-server/issues/3013)) ([1dbe505](https://github.com/rudderlabs/rudder-server/commit/1dbe5055aff174d865760aa917dae8aa4b8b7b50))
* **warehouse:** skipping deprecated columns ([#3000](https://github.com/rudderlabs/rudder-server/issues/3000)) ([82a35d6](https://github.com/rudderlabs/rudder-server/commit/82a35d654f0396385b601e6f99205b92638b0de3))
* **warehouse:** use single protocol source category while doing dedup for new record ([#2937](https://github.com/rudderlabs/rudder-server/issues/2937)) ([c46ba75](https://github.com/rudderlabs/rudder-server/commit/c46ba75385d1d8f4a1e00a477a670b4c14da549c))
* **warehouse:** warehouse successful upload exists ([#2931](https://github.com/rudderlabs/rudder-server/issues/2931)) ([c46ba75](https://github.com/rudderlabs/rudder-server/commit/c46ba75385d1d8f4a1e00a477a670b4c14da549c))
* **warehouse:** warehouse validations fixed to lookup secrets using sshKeyId ([#2950](https://github.com/rudderlabs/rudder-server/issues/2950)) ([017d8ba](https://github.com/rudderlabs/rudder-server/commit/017d8ba028bbfc108bf65e9e612a95160aeffc7c))


### Miscellaneous

* add workspace id in wh_async_jobs table ([#2848](https://github.com/rudderlabs/rudder-server/issues/2848)) ([074e789](https://github.com/rudderlabs/rudder-server/commit/074e7897fbc0f6a43e8f5b1cddb14f2bd544ea32))
* capture cgroup-aware memory stats ([#2945](https://github.com/rudderlabs/rudder-server/issues/2945)) ([4e6cf3b](https://github.com/rudderlabs/rudder-server/commit/4e6cf3b9683a1fa643a9304a4c8c344b33e40690))
* decouple loadfile logic and repo ([#2753](https://github.com/rudderlabs/rudder-server/issues/2753)) ([074e789](https://github.com/rudderlabs/rudder-server/commit/074e7897fbc0f6a43e8f5b1cddb14f2bd544ea32))
* dedup destination metadata in router based on job id ([#2982](https://github.com/rudderlabs/rudder-server/issues/2982)) ([dfc2273](https://github.com/rudderlabs/rudder-server/commit/dfc227302b23bf8ceb3cdc72601f1bbd94435178))
* **deps:** bump github.com/gofrs/uuid from 4.2.0+incompatible to 4.4.0+incompatible ([#2906](https://github.com/rudderlabs/rudder-server/issues/2906)) ([e8ecf32](https://github.com/rudderlabs/rudder-server/commit/e8ecf325f6d7a91e32c166d8fb4a92a0c4179350))
* **deps:** bump github.com/samber/lo from 1.35.0 to 1.37.0 ([#2913](https://github.com/rudderlabs/rudder-server/issues/2913)) ([6718048](https://github.com/rudderlabs/rudder-server/commit/6718048682a080e6249fdbb2ad416370ad615deb))
* drop column only if exists ([#2979](https://github.com/rudderlabs/rudder-server/issues/2979)) ([03e7390](https://github.com/rudderlabs/rudder-server/commit/03e73906991d7ee7fe15cfaf7858415a4a89811c))
* enhance data sent to reporting ([#2914](https://github.com/rudderlabs/rudder-server/issues/2914)) ([c58100c](https://github.com/rudderlabs/rudder-server/commit/c58100c8b3f1abd7cf78f6397e130c4a4b99697e))
* introduce stat for skip upload syncs ([#2938](https://github.com/rudderlabs/rudder-server/issues/2938)) ([b1fc5ee](https://github.com/rudderlabs/rudder-server/commit/b1fc5eec439f70b7f53a74220cc37782b2bb0dae))
* **jobsdb:** support query filtering by workspaceID ([#2911](https://github.com/rudderlabs/rudder-server/issues/2911)) ([d049f2b](https://github.com/rudderlabs/rudder-server/commit/d049f2b20806f56bdda747e6fdeb6bc04f7eb339))
* make GC discard ratio configurable ([#3023](https://github.com/rudderlabs/rudder-server/issues/3023)) ([0801818](https://github.com/rudderlabs/rudder-server/commit/0801818c810745a7886ec20fa3d5b2be7db21c2e))
* **processor:** stop using global variables ([#2881](https://github.com/rudderlabs/rudder-server/issues/2881)) ([074e789](https://github.com/rudderlabs/rudder-server/commit/074e7897fbc0f6a43e8f5b1cddb14f2bd544ea32))
* protect measurement & tag names from empty strings ([#2946](https://github.com/rudderlabs/rudder-server/issues/2946)) ([f20a6f3](https://github.com/rudderlabs/rudder-server/commit/f20a6f3bf758de6226f16e230318a27fd2ee7b86))
* refactored gateway marshalling ([#2915](https://github.com/rudderlabs/rudder-server/issues/2915)) ([2625f1a](https://github.com/rudderlabs/rudder-server/commit/2625f1ad2d1e036f6fd1d159810fdf1c84e4b799))
* remove gorate throttling algorithm ([#2918](https://github.com/rudderlabs/rudder-server/issues/2918)) ([772187a](https://github.com/rudderlabs/rudder-server/commit/772187ad336d99a517540a33e51770a10f3b9f1c))
* remove reports down migrations ([#2920](https://github.com/rudderlabs/rudder-server/issues/2920)) ([2fa72d6](https://github.com/rudderlabs/rudder-server/commit/2fa72d62995b2ba92cf683fba4388afd707fcb4a))
* replace readme v1 announcement header ([#2893](https://github.com/rudderlabs/rudder-server/issues/2893)) ([074e789](https://github.com/rudderlabs/rudder-server/commit/074e7897fbc0f6a43e8f5b1cddb14f2bd544ea32))
* staging files to uploads ([#2863](https://github.com/rudderlabs/rudder-server/issues/2863)) ([692f55c](https://github.com/rudderlabs/rudder-server/commit/692f55c41e6ce84edbdbc64ac078eb36d15b49ab))
* tunable badgerDB config params ([#3027](https://github.com/rudderlabs/rudder-server/issues/3027)) ([0742fd7](https://github.com/rudderlabs/rudder-server/commit/0742fd753d401c8c73337bd11643340cce9b5ea1))
* use a partition lock when updating badger live events cache ([#2902](https://github.com/rudderlabs/rudder-server/issues/2902)) ([a4d1a71](https://github.com/rudderlabs/rudder-server/commit/a4d1a714db24dfabeb35948e95d1e2aba9bf8de5))
* use badgerdb for live events cache ([#2833](https://github.com/rudderlabs/rudder-server/issues/2833)) ([074e789](https://github.com/rudderlabs/rudder-server/commit/074e7897fbc0f6a43e8f5b1cddb14f2bd544ea32))
* use gauge for capturing pending events ([#2960](https://github.com/rudderlabs/rudder-server/issues/2960)) ([169ab96](https://github.com/rudderlabs/rudder-server/commit/169ab96cebe7656c2d8f1f0585deacf1007cd48b))
* **warehouse:** alter handling for redshift ([#2888](https://github.com/rudderlabs/rudder-server/issues/2888)) ([e1918c1](https://github.com/rudderlabs/rudder-server/commit/e1918c103fe3b41353054684d94ae5e568c0bc07))
* **warehouse:** filemanager nil fix for validations ([#2986](https://github.com/rudderlabs/rudder-server/issues/2986)) ([306d55b](https://github.com/rudderlabs/rudder-server/commit/306d55befcde12bba07b6b2ab7755b127713575a))
* **warehouse:** log improvements for total count in warehouse integrations ([#2976](https://github.com/rudderlabs/rudder-server/issues/2976)) ([994c89b](https://github.com/rudderlabs/rudder-server/commit/994c89b71623fd17140f60797655c40900e3d80b))
* **warehouse:** move warehouse destinations to integrations package ([#2885](https://github.com/rudderlabs/rudder-server/issues/2885)) ([074e789](https://github.com/rudderlabs/rudder-server/commit/074e7897fbc0f6a43e8f5b1cddb14f2bd544ea32))
* **warehouse:** remove unused stats from deltalake integration ([#2977](https://github.com/rudderlabs/rudder-server/issues/2977)) ([d8639ee](https://github.com/rudderlabs/rudder-server/commit/d8639eede967b91d4a34497f3a83349b088d49b8))
* **warehouse:** use warn while getting total count in warehouse ([#2944](https://github.com/rudderlabs/rudder-server/issues/2944)) ([4f225f5](https://github.com/rudderlabs/rudder-server/commit/4f225f5bae61e8d556f39a104045b6d5872753f5))
* warmup gcra algorithms in throttling test ([#2909](https://github.com/rudderlabs/rudder-server/issues/2909)) ([2754027](https://github.com/rudderlabs/rudder-server/commit/27540272b5d5cee6a569dcb091cb755d91bc2594))

## [1.5.5](https://github.com/rudderlabs/rudder-server/compare/v1.5.4...v1.5.5) (2023-02-07)


### Bug Fixes

* upload missing metric ([#2947](https://github.com/rudderlabs/rudder-server/issues/2947)) ([cbb4b1a](https://github.com/rudderlabs/rudder-server/commit/cbb4b1a046a1a9d68ca8131ac1e56bbd65c6b726))

## [1.5.4](https://github.com/rudderlabs/rudder-server/compare/v1.5.3...v1.5.4) (2023-02-06)


### Miscellaneous

* introduce stat for skip upload syncs ([#2938](https://github.com/rudderlabs/rudder-server/issues/2938)) ([9ac5006](https://github.com/rudderlabs/rudder-server/commit/9ac5006831f8711a3d5d6af7a4c0f881d3940324))

## [1.5.3](https://github.com/rudderlabs/rudder-server/compare/v1.5.2...v1.5.3) (2023-02-03)


### Bug Fixes

* **warehouse:** use single protocol source category while doing dedup for new record ([#2937](https://github.com/rudderlabs/rudder-server/issues/2937)) ([8087e46](https://github.com/rudderlabs/rudder-server/commit/8087e46c5dd430075e878ca55df065ff04e6daea))
* **warehouse:** warehouse successful upload exists ([#2931](https://github.com/rudderlabs/rudder-server/issues/2931)) ([bcb2bb2](https://github.com/rudderlabs/rudder-server/commit/bcb2bb2a76cc479110dae22d28a1acef6977a1b6))

## [1.5.2](https://github.com/rudderlabs/rudder-server/compare/v1.5.1...v1.5.2) (2023-02-02)


### Bug Fixes

* runtime error: slice bounds out of range [:-1] ([#2932](https://github.com/rudderlabs/rudder-server/issues/2932)) ([07dd59e](https://github.com/rudderlabs/rudder-server/commit/07dd59e1769578eb6fc2cc8f72015819b3f3e705))

## [1.5.1](https://github.com/rudderlabs/rudder-server/compare/v1.5.0...v1.5.1) (2023-02-01)


### Bug Fixes

* adding throttling stats in router ([#2923](https://github.com/rudderlabs/rudder-server/issues/2923)) ([6de8862](https://github.com/rudderlabs/rudder-server/commit/6de88627060376dd7544f027a8f46c98fcfe8ef3))

## [1.5.0](https://github.com/rudderlabs/rudder-server/compare/v1.4.5...v1.5.0) (2023-01-24)


### Features

* add `total_bytes` field in staging file model ([#2853](https://github.com/rudderlabs/rudder-server/issues/2853)) ([cc74fcb](https://github.com/rudderlabs/rudder-server/commit/cc74fcbc1182101453aebddb0d58210053574160))
* added MT support for regulation worker ([#2831](https://github.com/rudderlabs/rudder-server/issues/2831)) ([f1d3d03](https://github.com/rudderlabs/rudder-server/commit/f1d3d033a00f65533528167601ddc3e6cf59b7c6))
* changes to make rudder-scheduler work with HA gateway ([#2823](https://github.com/rudderlabs/rudder-server/issues/2823)) ([04a8559](https://github.com/rudderlabs/rudder-server/commit/04a8559124e1d9cdaa145eddb3a70c1dbabd9122))
* controlplane client support for destination history ([#2747](https://github.com/rudderlabs/rudder-server/issues/2747)) ([736a70b](https://github.com/rudderlabs/rudder-server/commit/736a70bfc27eb79fe5f9375c7c25edbec24ad5ae))
* distributed throttling ([#2606](https://github.com/rudderlabs/rudder-server/issues/2606)) ([736a70b](https://github.com/rudderlabs/rudder-server/commit/736a70bfc27eb79fe5f9375c7c25edbec24ad5ae))
* **warehouse:** added support for workspaceID's configuration ([#2760](https://github.com/rudderlabs/rudder-server/issues/2760)) ([736a70b](https://github.com/rudderlabs/rudder-server/commit/736a70bfc27eb79fe5f9375c7c25edbec24ad5ae))
* **warehouse:** implement ssh tunnelling ([#2804](https://github.com/rudderlabs/rudder-server/issues/2804)) ([1cbb1e7](https://github.com/rudderlabs/rudder-server/commit/1cbb1e75297737bcf4ce3409892e75b8c8958f12))
* **warehouse:** upload job processing metrics ([#2780](https://github.com/rudderlabs/rudder-server/issues/2780)) ([736a70b](https://github.com/rudderlabs/rudder-server/commit/736a70bfc27eb79fe5f9375c7c25edbec24ad5ae))


### Bug Fixes

* degraded workspace ids should be excluded by upload job allocator ([#2773](https://github.com/rudderlabs/rudder-server/issues/2773)) ([736a70b](https://github.com/rudderlabs/rudder-server/commit/736a70bfc27eb79fe5f9375c7c25edbec24ad5ae))
* **destination:** add proper stats for rt/batch transformation & proxy ([#2855](https://github.com/rudderlabs/rudder-server/issues/2855)) ([b7aa800](https://github.com/rudderlabs/rudder-server/commit/b7aa80003e237ddda370dff125898d4ac52ea820))
* failing executing jobs(except gateway) instead of deleting them ([#2736](https://github.com/rudderlabs/rudder-server/issues/2736)) ([736a70b](https://github.com/rudderlabs/rudder-server/commit/736a70bfc27eb79fe5f9375c7c25edbec24ad5ae))
* **processor:** wrong event filter in count stat captured ([#2889](https://github.com/rudderlabs/rudder-server/issues/2889)) ([a8a92c6](https://github.com/rudderlabs/rudder-server/commit/a8a92c6d00d2240dad6d6bd307352aee2fea29b4))
* regulation-worker panics during startup ([#2877](https://github.com/rudderlabs/rudder-server/issues/2877)) ([9ad84be](https://github.com/rudderlabs/rudder-server/commit/9ad84bedfad0d5af5b1ba4f78afd435798fb5501))
* shallow copy config in processor ([#2856](https://github.com/rudderlabs/rudder-server/issues/2856)) ([b7aa800](https://github.com/rudderlabs/rudder-server/commit/b7aa80003e237ddda370dff125898d4ac52ea820))
* stash reporting back less error jobs after store ([#2882](https://github.com/rudderlabs/rudder-server/issues/2882)) ([5b8f5c0](https://github.com/rudderlabs/rudder-server/commit/5b8f5c07f98e4a6f51220c95324ff126f5d8bff5))
* stats.Timer#End panics with nil pointer ([#2866](https://github.com/rudderlabs/rudder-server/issues/2866)) ([17c078b](https://github.com/rudderlabs/rudder-server/commit/17c078b2da4dace01cdd52650ab1636ff5d67ff2))
* unnecessary router pending event counts ([#2849](https://github.com/rudderlabs/rudder-server/issues/2849)) ([6c5d928](https://github.com/rudderlabs/rudder-server/commit/6c5d9285bf464c52db79efd616de1694030f7ec2))
* **warehouse:** added support for testing ssh tunnelling using postgres ([#2836](https://github.com/rudderlabs/rudder-server/issues/2836)) ([333310d](https://github.com/rudderlabs/rudder-server/commit/333310df51bb7e8784d3e19d11ba20ad27a24c80))
* **warehouse:** control plane client needs to be initialized for master mode ([#2826](https://github.com/rudderlabs/rudder-server/issues/2826)) ([5ad7ee7](https://github.com/rudderlabs/rudder-server/commit/5ad7ee7e4e3f8964092be68d6818e12712b6c9ea))
* **warehouse:** grcp cp-router logs ([#2766](https://github.com/rudderlabs/rudder-server/issues/2766)) ([736a70b](https://github.com/rudderlabs/rudder-server/commit/736a70bfc27eb79fe5f9375c7c25edbec24ad5ae))
* **warehouse:** include exclusion window in status tracker ([#2812](https://github.com/rudderlabs/rudder-server/issues/2812)) ([0459aff](https://github.com/rudderlabs/rudder-server/commit/0459aff6727ff39a33831074ed310ebd87002f4f))
* **warehouse:** processing available workers to be gauge stats ([#2822](https://github.com/rudderlabs/rudder-server/issues/2822)) ([dfc4b23](https://github.com/rudderlabs/rudder-server/commit/dfc4b2348347df2bb4b45a625509b319583ef166))


### Miscellaneous

* abort jobs in regulation worker for unsupported destinations ([#2821](https://github.com/rudderlabs/rudder-server/issues/2821)) ([791c235](https://github.com/rudderlabs/rudder-server/commit/791c2353c998a4807bb0dc9bcbb7464c2021d2c8))
* add logs when backup is skipped due to preferences ([#2867](https://github.com/rudderlabs/rudder-server/issues/2867)) ([1d9320d](https://github.com/rudderlabs/rudder-server/commit/1d9320d0bc2d40bea78eea8d485de90f4ea39609))
* add tags(sourceType, sdkVersion) to gateway stats ([#2896](https://github.com/rudderlabs/rudder-server/issues/2896)) ([078ee91](https://github.com/rudderlabs/rudder-server/commit/078ee91931ac5e96109a45f4ef36cac58e155b3b))
* added prefix to regulation worker stats ([#2879](https://github.com/rudderlabs/rudder-server/issues/2879)) ([bab5a85](https://github.com/rudderlabs/rudder-server/commit/bab5a859bda6850a90fa5c525df625406534e7e0))
* applying 1.4.3 and 1.4.4 hotfixes to main branch ([#2794](https://github.com/rudderlabs/rudder-server/issues/2794)) ([736a70b](https://github.com/rudderlabs/rudder-server/commit/736a70bfc27eb79fe5f9375c7c25edbec24ad5ae))
* close http responses in a way to allow the Transport to re-use the TCP connection ([#2718](https://github.com/rudderlabs/rudder-server/issues/2718)) ([736a70b](https://github.com/rudderlabs/rudder-server/commit/736a70bfc27eb79fe5f9375c7c25edbec24ad5ae))
* completely replace gofrs with google uuid  ([#2733](https://github.com/rudderlabs/rudder-server/issues/2733)) ([736a70b](https://github.com/rudderlabs/rudder-server/commit/736a70bfc27eb79fe5f9375c7c25edbec24ad5ae))
* **deps:** bump cloud.google.com/go/pubsub from 1.19.0 to 1.27.0 ([#2755](https://github.com/rudderlabs/rudder-server/issues/2755)) ([736a70b](https://github.com/rudderlabs/rudder-server/commit/736a70bfc27eb79fe5f9375c7c25edbec24ad5ae))
* disable batch destinations by default in regulation worker ([#2886](https://github.com/rudderlabs/rudder-server/issues/2886)) ([2767dfb](https://github.com/rudderlabs/rudder-server/commit/2767dfbe02f9f1038381260cea9676db9941e1ae))
* docs update - unlimited event copy ([#2813](https://github.com/rudderlabs/rudder-server/issues/2813)) ([7f6c834](https://github.com/rudderlabs/rudder-server/commit/7f6c8344919bd041d9e9d50cac0d441064ce9dc2))
* drop support for v1 pending events ([#2820](https://github.com/rudderlabs/rudder-server/issues/2820)) ([eadf1da](https://github.com/rudderlabs/rudder-server/commit/eadf1dae410e89d05abc806ed77f83ce6d25e1c1))
* gateway stats ([#2758](https://github.com/rudderlabs/rudder-server/issues/2758)) ([65774e2](https://github.com/rudderlabs/rudder-server/commit/65774e21c3d823935cc73f79ac16950e3ef5396c))
* handling wrapped temporary errors for Kafka destinations ([#2898](https://github.com/rudderlabs/rudder-server/issues/2898)) ([0efa116](https://github.com/rudderlabs/rudder-server/commit/0efa116859959345f588e6043a1cce74263b3670))
* introduce region in reporting url ([#2832](https://github.com/rudderlabs/rudder-server/issues/2832)) ([87a967e](https://github.com/rudderlabs/rudder-server/commit/87a967e1a734789402db70d01b9e626699d40917))
* limit live events stored in memory ([#2803](https://github.com/rudderlabs/rudder-server/issues/2803)) ([3257aac](https://github.com/rudderlabs/rudder-server/commit/3257aac27db9e1c5e35afd4d5b19def660a8258f))
* prohibit deletion of incomplete job-status stats ([#2850](https://github.com/rudderlabs/rudder-server/issues/2850)) ([ea4829a](https://github.com/rudderlabs/rudder-server/commit/ea4829ae358bb930a8898b0f3c5c1ebc4b033ac7))
* remove deepsource badge ([#2765](https://github.com/rudderlabs/rudder-server/issues/2765)) ([736a70b](https://github.com/rudderlabs/rudder-server/commit/736a70bfc27eb79fe5f9375c7c25edbec24ad5ae))
* timerStat.RecordDuration, Since instead of timerStat.Start, End ([#2870](https://github.com/rudderlabs/rudder-server/issues/2870)) ([134bb0a](https://github.com/rudderlabs/rudder-server/commit/134bb0a3e139eb9301c633333ffcd0340baf8d97))
* upgrade postgres image ([#2764](https://github.com/rudderlabs/rudder-server/issues/2764)) ([736a70b](https://github.com/rudderlabs/rudder-server/commit/736a70bfc27eb79fe5f9375c7c25edbec24ad5ae))
* use destination config from config-be than from message ([#2830](https://github.com/rudderlabs/rudder-server/issues/2830)) ([28f8750](https://github.com/rudderlabs/rudder-server/commit/28f8750e1d8a345be75fb7ef7c1eb0cd12379a2c))
* using an exponential backoff ([#2865](https://github.com/rudderlabs/rudder-server/issues/2865)) ([56f84df](https://github.com/rudderlabs/rudder-server/commit/56f84dfcb6d102c4effc59da0aae2786edfcae86))
* **warehouse:** integration test improvements ([#2685](https://github.com/rudderlabs/rudder-server/issues/2685)) ([736a70b](https://github.com/rudderlabs/rudder-server/commit/736a70bfc27eb79fe5f9375c7c25edbec24ad5ae))
* **warehouse:** use strings.builder for warehouse add columns ([#2801](https://github.com/rudderlabs/rudder-server/issues/2801)) ([736a70b](https://github.com/rudderlabs/rudder-server/commit/736a70bfc27eb79fe5f9375c7c25edbec24ad5ae))

## [1.4.6](https://github.com/rudderlabs/rudder-server/compare/v1.4.5...v1.4.6) (2023-01-10)


### Bug Fixes

* **destination:** add proper stats for rt/batch transformation & proxy ([#2855](https://github.com/rudderlabs/rudder-server/issues/2855)) ([6127684](https://github.com/rudderlabs/rudder-server/commit/6127684ff211b64cc256e33efc1d9084204e5d22))
* shallow copy config in processor ([#2856](https://github.com/rudderlabs/rudder-server/issues/2856)) ([ea3bfdc](https://github.com/rudderlabs/rudder-server/commit/ea3bfdc2e9b82ada80bae48ab32ce0c65403592a))

## [1.4.5](https://github.com/rudderlabs/rudder-server/compare/v1.4.4...v1.4.5) (2022-12-15)


### Bug Fixes

* aws session creation is failing for s3 manager when roles are used ([#2799](https://github.com/rudderlabs/rudder-server/issues/2799)) ([1534d64](https://github.com/rudderlabs/rudder-server/commit/1534d64b3d52f7d715dfc7fed113cb91ecc50708))
* **destination:** alert flag setting for router-aborted-count alert definition ([#2808](https://github.com/rudderlabs/rudder-server/issues/2808)) ([f2e9001](https://github.com/rudderlabs/rudder-server/commit/f2e900190412a1cbd6d2f19d5fca8f0411793110))
* pending events stats incremented twice during startup due to tenantstats being initialized twice ([#2798](https://github.com/rudderlabs/rudder-server/issues/2798)) ([49aa37f](https://github.com/rudderlabs/rudder-server/commit/49aa37f33b895483a88b4caee19fc71fb1a76865))


### Miscellaneous

* enable failed keys by default ([#2805](https://github.com/rudderlabs/rudder-server/issues/2805)) ([ba9832c](https://github.com/rudderlabs/rudder-server/commit/ba9832c4eb4e90f93da860209cd0ce919bc88608))
* **gateway:** add webhook failure stats ([#2811](https://github.com/rudderlabs/rudder-server/issues/2811)) ([60fc5f7](https://github.com/rudderlabs/rudder-server/commit/60fc5f7a6fe2bf00e09f76144080e48a4e6f3061))
* **gateway:** enabling source transformation alerts ([#2809](https://github.com/rudderlabs/rudder-server/issues/2809)) ([cc14e6c](https://github.com/rudderlabs/rudder-server/commit/cc14e6c634f63d30353e701aaf29a40f2929c41e))
* quote table identifier ([#2810](https://github.com/rudderlabs/rudder-server/issues/2810)) ([58f2e4c](https://github.com/rudderlabs/rudder-server/commit/58f2e4cdbf8a28c1d192bc461ab4a92515eccdeb))

## [1.4.4](https://github.com/rudderlabs/rudder-server/compare/v1.4.3...v1.4.4) (2022-12-09)


### Bug Fixes

* using a wrong datatype for pg_class.reltuples causes internal migration to fail ([#2792](https://github.com/rudderlabs/rudder-server/issues/2792)) ([006a203](https://github.com/rudderlabs/rudder-server/commit/006a203c7137471a2219ff8690e4e2dbaa11d32b))

## [1.4.3](https://github.com/rudderlabs/rudder-server/compare/v1.4.2...v1.4.3) (2022-12-08)


### Bug Fixes

* remove schema from stage file model ([#2790](https://github.com/rudderlabs/rudder-server/issues/2790)) ([2bc1f1d](https://github.com/rudderlabs/rudder-server/commit/2bc1f1d32bba10d74309a589e552b512093274f3))

## [1.4.2](https://github.com/rudderlabs/rudder-server/compare/v1.4.1...v1.4.2) (2022-12-07)


### Miscellaneous

* remove shopify autoreload ([#2784](https://github.com/rudderlabs/rudder-server/issues/2784)) ([85ba13a](https://github.com/rudderlabs/rudder-server/commit/85ba13a44ebbd318cafbc3fa4428b6150e3c6081))

## [1.4.1](https://github.com/rudderlabs/rudder-server/compare/v1.4.0...v1.4.1) (2022-12-07)


### Miscellaneous

* fail transformer timeouts ([#2779](https://github.com/rudderlabs/rudder-server/issues/2779)) ([05d8264](https://github.com/rudderlabs/rudder-server/commit/05d8264b977f66c604b67875f105624826a756f1))
* remove hacky commit hash ([cf0cfad](https://github.com/rudderlabs/rudder-server/commit/cf0cfade1aacecae5675d61c8768d2c2ed527d54))

## [1.4.0](https://github.com/rudderlabs/rudder-server/compare/v1.2.0...v1.4.0) (2022-12-05)


### Features

* add support for request payloads using gzip content-encoding ([#2741](https://github.com/rudderlabs/rudder-server/issues/2741)) ([d9b9084](https://github.com/rudderlabs/rudder-server/commit/d9b9084b77090db00097b479afeffc2247a04161))
* regulation api support for Universal Analytics ([#2632](https://github.com/rudderlabs/rudder-server/issues/2632)) ([87a5d02](https://github.com/rudderlabs/rudder-server/commit/87a5d024f3521e3720974b71c95af6bf9c403753))
* support using badgerDB as a repository for data regulation suppressions ([#2619](https://github.com/rudderlabs/rudder-server/issues/2619)) ([712b6a5](https://github.com/rudderlabs/rudder-server/commit/712b6a5ff5503a44b5129edf179056d57be53ad0))
* **warehouse:** added support for bigquery custom partition for workspaceIDs ([#2679](https://github.com/rudderlabs/rudder-server/issues/2679)) ([3bb21c9](https://github.com/rudderlabs/rudder-server/commit/3bb21c950ea9cdd4422086a449d9a796a21bd233))
* **warehouse:** added support for warehouse column count limit ([#2723](https://github.com/rudderlabs/rudder-server/issues/2723)) ([bed58c5](https://github.com/rudderlabs/rudder-server/commit/bed58c5d8927e99123b33c9832af06060dd09b34))
* **warehouse:** degraded workspace id ([#2627](https://github.com/rudderlabs/rudder-server/issues/2627)) ([3bb21c9](https://github.com/rudderlabs/rudder-server/commit/3bb21c950ea9cdd4422086a449d9a796a21bd233))


### Bug Fixes

* aws role session name ([#2712](https://github.com/rudderlabs/rudder-server/issues/2712)) ([87f57f8](https://github.com/rudderlabs/rudder-server/commit/87f57f881a6b4d8c4557c13cbde957820845eb53))
* batch router event delivery time ([#2711](https://github.com/rudderlabs/rudder-server/issues/2711)) ([3376dc9](https://github.com/rudderlabs/rudder-server/commit/3376dc906aef23b622e4db00d49945107d7432c8))
* capture router's iterator stats after iteration is complete ([#2698](https://github.com/rudderlabs/rudder-server/issues/2698)) ([5d466d3](https://github.com/rudderlabs/rudder-server/commit/5d466d394722aa6303cb7c304a0f16fc05bb528c))
* **destination:** delete users support add test-cases and better error strings ([#2744](https://github.com/rudderlabs/rudder-server/issues/2744)) ([b69c4bb](https://github.com/rudderlabs/rudder-server/commit/b69c4bbc4286904b14167cdc8b27d315fe9c6a96))
* goroutine leak in lock.AsyncLockWithCtx ([#2722](https://github.com/rudderlabs/rudder-server/issues/2722)) ([b29da14](https://github.com/rudderlabs/rudder-server/commit/b29da1474b94ce1360b5085baf9361704ca9468c))
* incorrect stats captured at gateway ([#2710](https://github.com/rudderlabs/rudder-server/issues/2710)) ([a6c1a16](https://github.com/rudderlabs/rudder-server/commit/a6c1a167d41d5c6c313c9802077c972f77c6dd3f))
* init jobsdb logger at the right place ([#2729](https://github.com/rudderlabs/rudder-server/issues/2729)) ([ef64bba](https://github.com/rudderlabs/rudder-server/commit/ef64bba7bdaa5d57e25bb8a2530b344736e2f816))
* jobsdb schema migration not performed against all datasets ([#2737](https://github.com/rudderlabs/rudder-server/issues/2737)) ([4d7fd5a](https://github.com/rudderlabs/rudder-server/commit/4d7fd5afe3b3cc0b71cfe77a40d368593e84b360))
* metadata in event_schema table has TotalCount which exceeds 32bit integer limits ([#2738](https://github.com/rudderlabs/rudder-server/issues/2738)) ([0cf5fd6](https://github.com/rudderlabs/rudder-server/commit/0cf5fd631137793d2c3d3d870ab32bd70d1b1931))
* no timezone while parsing ([#2671](https://github.com/rudderlabs/rudder-server/issues/2671)) ([9f006e5](https://github.com/rudderlabs/rudder-server/commit/9f006e5a3c5c08855cce15c07f569ca822459390))
* regulation-worker changes for oauth destinations ([#2730](https://github.com/rudderlabs/rudder-server/issues/2730)) ([0ed5a82](https://github.com/rudderlabs/rudder-server/commit/0ed5a82c9c4d965feb36db789d05cbb4cf6dfec2))
* removed quote Identifier on sourceDestId ([#2763](https://github.com/rudderlabs/rudder-server/issues/2763)) ([2cc87f4](https://github.com/rudderlabs/rudder-server/commit/2cc87f4b41e32f346bdc4a745f7d67b0e88f97a3))
* use local variable for workspaceId in stash backup ([#2706](https://github.com/rudderlabs/rudder-server/issues/2706)) ([b3a5fc1](https://github.com/rudderlabs/rudder-server/commit/b3a5fc1dcfdd5ca61d300bf1e476a88be9c14e8d))
* warehouse sync job queries for snowflake and bigquery ([#2740](https://github.com/rudderlabs/rudder-server/issues/2740)) ([d33d8c3](https://github.com/rudderlabs/rudder-server/commit/d33d8c3f4448b3a9fbdd20363b6c57238fefb71b))
* **warehouse:** deepsource fix for use of empty error string in errors.New ([#2731](https://github.com/rudderlabs/rudder-server/issues/2731)) ([867ce5b](https://github.com/rudderlabs/rudder-server/commit/867ce5b4d7eadbd46a6174cd1129bed7eda7fb37))
* **warehouse:** increase async job timeout ([#2721](https://github.com/rudderlabs/rudder-server/issues/2721)) ([88f1ec1](https://github.com/rudderlabs/rudder-server/commit/88f1ec1d31dd65f8cc4093fdb1754972ef016482))
* **warehouse:** syncs issues ([#2732](https://github.com/rudderlabs/rudder-server/issues/2732)) ([0941cc0](https://github.com/rudderlabs/rudder-server/commit/0941cc067e348d8d1c5f96a1271f52c92ec65615))
* **warehouse:** use correct config for columns batch size ([#2720](https://github.com/rudderlabs/rudder-server/issues/2720)) ([0e26b30](https://github.com/rudderlabs/rudder-server/commit/0e26b30f3fd38063b4180d73298a56254468c6d0))
* **warehouse:** warehouse archiver initialize ([#2697](https://github.com/rudderlabs/rudder-server/issues/2697)) ([c7af5d9](https://github.com/rudderlabs/rudder-server/commit/c7af5d9402efd299aa3cacdee63a8275490b5bb6))


### Documentation

* create SECURITY.md ([#2656](https://github.com/rudderlabs/rudder-server/issues/2656)) ([3bb21c9](https://github.com/rudderlabs/rudder-server/commit/3bb21c950ea9cdd4422086a449d9a796a21bd233))


### Miscellaneous

* add workspaceid label to router aborted events ([#2724](https://github.com/rudderlabs/rudder-server/issues/2724)) ([8a04871](https://github.com/rudderlabs/rudder-server/commit/8a0487152b844069cc301d37e8786edf599340b6))
* advertise server features in version endpoint ([#2751](https://github.com/rudderlabs/rudder-server/issues/2751)) ([73fc7ea](https://github.com/rudderlabs/rudder-server/commit/73fc7eaf72d28da519bc85feba43953a748edb74))
* by default enable max concurrent gw request limit. ([#2648](https://github.com/rudderlabs/rudder-server/issues/2648)) ([3bb21c9](https://github.com/rudderlabs/rudder-server/commit/3bb21c950ea9cdd4422086a449d9a796a21bd233))
* compact status tables before proceeding with migration ([#2688](https://github.com/rudderlabs/rudder-server/issues/2688)) ([27299f2](https://github.com/rudderlabs/rudder-server/commit/27299f2092e07c85739d16371f2d8733be9af565))
* data residency adaptations ([#2703](https://github.com/rudderlabs/rudder-server/issues/2703)) ([52d9392](https://github.com/rudderlabs/rudder-server/commit/52d939208ab57e6eb9030f22b132f635cd05d4c6))
* **deps:** bump github.com/samber/lo from 1.33.0 to 1.35.0 ([#2707](https://github.com/rudderlabs/rudder-server/issues/2707)) ([a01b515](https://github.com/rudderlabs/rudder-server/commit/a01b515f9085be24a418b7e54846a64a1d4c152d))
* **deps:** bump google.golang.org/grpc from 1.50.0 to 1.51.0 ([#2717](https://github.com/rudderlabs/rudder-server/issues/2717)) ([29f2de4](https://github.com/rudderlabs/rudder-server/commit/29f2de49608f3d38fbfb758a23c07ecd4c4a4876))
* don't use release-please in main branch ([bc0d1c7](https://github.com/rudderlabs/rudder-server/commit/bc0d1c78d0f4969056f33b286d7655d436a39124))
* enhance transformer error with more metadata ([#2742](https://github.com/rudderlabs/rudder-server/issues/2742)) ([e9b75e9](https://github.com/rudderlabs/rudder-server/commit/e9b75e92ebcd7a45ff982d7558b6bb4d52d32cde))
* ensure internal migration of large job-status datasets ([#2748](https://github.com/rudderlabs/rudder-server/issues/2748)) ([a0fc4f5](https://github.com/rudderlabs/rudder-server/commit/a0fc4f58079675e2dca5c2316a36970eb9f2bf18))
* fixing a flaky rsources test ([#2680](https://github.com/rudderlabs/rudder-server/issues/2680)) ([ad2626f](https://github.com/rudderlabs/rudder-server/commit/ad2626f697f7c548438fcaef74162e1d64e0b187))
* include missed changelogs due to rebase ([3bb21c9](https://github.com/rudderlabs/rudder-server/commit/3bb21c950ea9cdd4422086a449d9a796a21bd233))
* introduce api & repo components ([#2691](https://github.com/rudderlabs/rudder-server/issues/2691)) ([c26844e](https://github.com/rudderlabs/rudder-server/commit/c26844e3a70916556e1b917d509de3e6bd76b9df))
* introduce in-memory stats for testing ([#2735](https://github.com/rudderlabs/rudder-server/issues/2735)) ([6ac8c31](https://github.com/rudderlabs/rudder-server/commit/6ac8c3143b815284d7e91fc641c855d2d6c3ee23))
* **jobsdb:** add support for schema migration changesets to run always ([#2746](https://github.com/rudderlabs/rudder-server/issues/2746)) ([463533e](https://github.com/rudderlabs/rudder-server/commit/463533e38d43f8650a23c328f3a10eb79419e4eb))
* **jobsdb:** latest job status query optimization ([#2693](https://github.com/rudderlabs/rudder-server/issues/2693)) ([4e0008c](https://github.com/rudderlabs/rudder-server/commit/4e0008cb58c21caad118f3fc5b66638275095ac8))
* regression while starting a standalone warehouse slave ([#2743](https://github.com/rudderlabs/rudder-server/issues/2743)) ([4e8df6f](https://github.com/rudderlabs/rudder-server/commit/4e8df6f066bc5a7d25d714efe0754e8137b0b03f))
* regulation worker avoid panic in case of timeout ([#2657](https://github.com/rudderlabs/rudder-server/issues/2657)) ([3bb21c9](https://github.com/rudderlabs/rudder-server/commit/3bb21c950ea9cdd4422086a449d9a796a21bd233))
* release 1.3.1 ([#2708](https://github.com/rudderlabs/rudder-server/issues/2708)) ([c96169d](https://github.com/rudderlabs/rudder-server/commit/c96169dc2ae0ad75a92793d264ea4f3279894905))
* release 1.3.2 ([#2713](https://github.com/rudderlabs/rudder-server/issues/2713)) ([bca84b7](https://github.com/rudderlabs/rudder-server/commit/bca84b732dd48edb2c3df6f32a317b1bb1893b8d))
* release 1.3.3 ([#2726](https://github.com/rudderlabs/rudder-server/issues/2726)) ([bc6afca](https://github.com/rudderlabs/rudder-server/commit/bc6afcaf5c6d5af7f2e7a8e4de6c15d43aaf831b))
* remove redundant/duplicate add_ds distributed lock ([#2769](https://github.com/rudderlabs/rudder-server/issues/2769)) ([808f02d](https://github.com/rudderlabs/rudder-server/commit/808f02d16cb94e0ad5173d9c7bb68e1445f44088))
* safe database initialisation and schema migration during startup ([#2734](https://github.com/rudderlabs/rudder-server/issues/2734)) ([cf80d67](https://github.com/rudderlabs/rudder-server/commit/cf80d67dd0116fe589d7ede06ca7781aa9fc2ec5))
* set max connections to readonly jobsdbs ([#2745](https://github.com/rudderlabs/rudder-server/issues/2745)) ([5e8f73d](https://github.com/rudderlabs/rudder-server/commit/5e8f73dd021206deff10629f4cae6e0c1965d80f))
* update config backend url domain name ([#2699](https://github.com/rudderlabs/rudder-server/issues/2699)) ([9e06351](https://github.com/rudderlabs/rudder-server/commit/9e06351b921fb9e6d40878002b8b49efbada1e48))
* use azure-sql image with m1 compatible healthcheck ([#2702](https://github.com/rudderlabs/rudder-server/issues/2702)) ([4f0d189](https://github.com/rudderlabs/rudder-server/commit/4f0d189f5e1412d96656e7fa4c4749aa27f650cc))
* use bugsnag in router and processor goroutines ([#2686](https://github.com/rudderlabs/rudder-server/issues/2686)) ([3bb21c9](https://github.com/rudderlabs/rudder-server/commit/3bb21c950ea9cdd4422086a449d9a796a21bd233))
* using arm64 compatible images if necessary ([#2670](https://github.com/rudderlabs/rudder-server/issues/2670)) ([3bb21c9](https://github.com/rudderlabs/rudder-server/commit/3bb21c950ea9cdd4422086a449d9a796a21bd233))
* **warehouse:** handle schema change ([#2654](https://github.com/rudderlabs/rudder-server/issues/2654)) ([d01f98b](https://github.com/rudderlabs/rudder-server/commit/d01f98b705c46235abfd4883b58229aa1fba371f))
* **warehouse:** use fastUUID with google UUID generation ([#2598](https://github.com/rudderlabs/rudder-server/issues/2598)) ([07093b1](https://github.com/rudderlabs/rudder-server/commit/07093b17a454b25b8ce873c9129f953d37ffa91b))

## [1.3.3](https://github.com/rudderlabs/rudder-server/compare/v1.3.2...v1.3.3) (2022-11-23)


### Bug Fixes

* goroutine leak in lock.AsyncLockWithCtx ([#2722](https://github.com/rudderlabs/rudder-server/issues/2722)) ([7597ecf](https://github.com/rudderlabs/rudder-server/commit/7597ecfe41b10b4eaaa91077971c5354fad89ad2))

## [1.3.2](https://github.com/rudderlabs/rudder-server/compare/v1.3.1...v1.3.2) (2022-11-17)


### Bug Fixes

* batch router event delivery time ([#2711](https://github.com/rudderlabs/rudder-server/issues/2711)) ([3782597](https://github.com/rudderlabs/rudder-server/commit/3782597f469cfda6dbe8d30689b4483becf63fec))
* incorrect stats captured at gateway ([#2710](https://github.com/rudderlabs/rudder-server/issues/2710)) ([52e8fa0](https://github.com/rudderlabs/rudder-server/commit/52e8fa0e6a90cac218c24a7491f8d3a9fbce33d6))

## [1.3.1](https://github.com/rudderlabs/rudder-server/compare/v1.3.0...v1.3.1) (2022-11-16)


### Bug Fixes

* use local variable for workspaceId in stash backup ([#2706](https://github.com/rudderlabs/rudder-server/issues/2706)) ([f0d3612](https://github.com/rudderlabs/rudder-server/commit/f0d36125a7cf8ef0e8ccf2c1d2134d40a62fd732))

## [1.3.0](https://github.com/rudderlabs/rudder-server/compare/v1.2.0...v1.3.0) (2022-11-10)


### Features

* adding metadata in the proxy request ([#2622](https://github.com/rudderlabs/rudder-server/issues/2622)) ([f6f91ea](https://github.com/rudderlabs/rudder-server/commit/f6f91eaeffce0726781e821a3d54cf0c249262fa))
* **destination:** bqstream - add support for batch of properties ([#2367](https://github.com/rudderlabs/rudder-server/issues/2367)) ([73baf76](https://github.com/rudderlabs/rudder-server/commit/73baf766915ad14247666b8233383738c5e0e057))
* **router:** avoid worker starvation during job pickup ([fee04b7](https://github.com/rudderlabs/rudder-server/commit/fee04b75c078f7b19554ea8f671ac021d89b7222))
* **router:** avoid worker starvation during job pickup ([#2379](https://github.com/rudderlabs/rudder-server/issues/2379)) ([0ec74d1](https://github.com/rudderlabs/rudder-server/commit/0ec74d1a4d34743ac362615530b5dbaa14cb03cc))
* support for config to custom destinations ([#2625](https://github.com/rudderlabs/rudder-server/issues/2625)) ([cb230b0](https://github.com/rudderlabs/rudder-server/commit/cb230b0595dd4f598d0aa248dac294135e09480f))
* support initialisation of producer without topic for kafka, azure event hub and confluent cloud ([#2569](https://github.com/rudderlabs/rudder-server/issues/2569)) ([0312c55](https://github.com/rudderlabs/rudder-server/commit/0312c5523538de3e67d9093727227da323f00961))
* support reporting pii filter based on config backend ([#2655](https://github.com/rudderlabs/rudder-server/issues/2655)) ([ef95eba](https://github.com/rudderlabs/rudder-server/commit/ef95ebacacf69f46253d5032e2a28a4fd6793421))
* user suppressions adaptations for namespaces ([#2604](https://github.com/rudderlabs/rudder-server/issues/2604)) ([5c26d1b](https://github.com/rudderlabs/rudder-server/commit/5c26d1bd8262098adf1cb4bddb155f189ee71cc9))
* **warehouse:** added base support for s3 datalake deletion as part of regulation API. ([#2515](https://github.com/rudderlabs/rudder-server/issues/2515)) ([4369abf](https://github.com/rudderlabs/rudder-server/commit/4369abfc0e67677553c269d89cc6cc087d75a7ab))
* **warehouse:** batching of alter add statements  ([#2484](https://github.com/rudderlabs/rudder-server/issues/2484)) ([37d32f1](https://github.com/rudderlabs/rudder-server/commit/37d32f1e68e4ac9ac05c95ea62e099d7dd8afe88))
* **warehouse:** unrecognized schema in warehouse ([#2638](https://github.com/rudderlabs/rudder-server/issues/2638)) ([366c1f5](https://github.com/rudderlabs/rudder-server/commit/366c1f51cf71ed97105ee91699e437e2829ac31f))


### Bug Fixes

* always use a sql safe table name in failed events manager ([#2664](https://github.com/rudderlabs/rudder-server/issues/2664)) ([0d061ff](https://github.com/rudderlabs/rudder-server/commit/0d061ff2d8c16845179d215bf8012afceba12a30))
* **batchrouter:** concurrent modification of job parameters causes panic ([#2631](https://github.com/rudderlabs/rudder-server/issues/2631)) ([79e3e34](https://github.com/rudderlabs/rudder-server/commit/79e3e34199f18b48575cc8c4cf08091f8044084f))
* changed query to accept user input in prepared sql statement ([#2652](https://github.com/rudderlabs/rudder-server/issues/2652)) ([2f956b7](https://github.com/rudderlabs/rudder-server/commit/2f956b7eb3d5eb2de3e79d7df2c87405af25071e))
* close init after assigning storage settings ([#2678](https://github.com/rudderlabs/rudder-server/issues/2678)) ([4986595](https://github.com/rudderlabs/rudder-server/commit/4986595e3a05d832c32c4befa13e1d29f198b499))
* **destination:** empty oauth account check ([#2675](https://github.com/rudderlabs/rudder-server/issues/2675)) ([1584352](https://github.com/rudderlabs/rudder-server/commit/1584352ccba4a293f928cc79987a65e70d0fa2db))
* don't ignore stats middleware template path calculation ([#2594](https://github.com/rudderlabs/rudder-server/issues/2594)) ([f589f5f](https://github.com/rudderlabs/rudder-server/commit/f589f5f25137b72d3d55eabe47774f5b5fec05c9))
* gcs manager cancels context ([#2662](https://github.com/rudderlabs/rudder-server/issues/2662)) ([0964e83](https://github.com/rudderlabs/rudder-server/commit/0964e839671515645f8a3235779456cc3e6e398f))
* golangci-lint issues ([#2641](https://github.com/rudderlabs/rudder-server/issues/2641)) ([c9bd5d4](https://github.com/rudderlabs/rudder-server/commit/c9bd5d4dd12d7e33704044588686ac0965e67ce4))
* jobsDB backup query time ([#2521](https://github.com/rudderlabs/rudder-server/issues/2521)) ([46b5b3f](https://github.com/rudderlabs/rudder-server/commit/46b5b3f3f47a48e55507bd26d65dd32b3e669941))
* **jobsdb:** update cache after transaction completes ([#2567](https://github.com/rudderlabs/rudder-server/issues/2567)) ([2d70da7](https://github.com/rudderlabs/rudder-server/commit/2d70da79f60b9f90cb0fde7791acb71cc1ca94f0))
* only close response body in oauth handler in case of no error ([#2593](https://github.com/rudderlabs/rudder-server/issues/2593)) ([83ace48](https://github.com/rudderlabs/rudder-server/commit/83ace48bc5d77d0ae15f6a12b3e87e77d970de7c))
* page size fixed irrespectiveof suppressAPIToken ([#2611](https://github.com/rudderlabs/rudder-server/issues/2611)) ([71d8c99](https://github.com/rudderlabs/rudder-server/commit/71d8c999f05c09a35ed7270750464a2913e23992))
* preferences should be false incase of invalid bucket ([#2661](https://github.com/rudderlabs/rudder-server/issues/2661)) ([725e9e7](https://github.com/rudderlabs/rudder-server/commit/725e9e75860df8ea872b0802481dc32116160738))
* properly escape table name when querying for failed events ([#2663](https://github.com/rudderlabs/rudder-server/issues/2663)) ([9c009d9](https://github.com/rudderlabs/rudder-server/commit/9c009d9775abc99e72fc470f4c4c8e8f1775e82a))
* remove gateway db write panic ([#2644](https://github.com/rudderlabs/rudder-server/issues/2644)) ([60bc174](https://github.com/rudderlabs/rudder-server/commit/60bc174cf8cc317aca24224ed63202ae0632da77))
* removing the backup bucket env check to determine if backup is enabled ([#2668](https://github.com/rudderlabs/rudder-server/issues/2668)) ([ea8cf4c](https://github.com/rudderlabs/rudder-server/commit/ea8cf4cb7da4f6ea0dff66b78d87f18451f4e288))
* reset job status ids during internal migration ([#2684](https://github.com/rudderlabs/rudder-server/issues/2684)) ([3cffe02](https://github.com/rudderlabs/rudder-server/commit/3cffe02fd164fc5a2d0ecd79ea10330e8c431c06))
* router destinationsMap access ([#2582](https://github.com/rudderlabs/rudder-server/issues/2582)) ([3770720](https://github.com/rudderlabs/rudder-server/commit/3770720c400880c529842cca986184ff1e8cfb62))
* send router transform failures to live events ([#2637](https://github.com/rudderlabs/rudder-server/issues/2637)) ([6931b17](https://github.com/rudderlabs/rudder-server/commit/6931b170881f07bbe3fea1a82c28fa30dc980b91))
* take workpaceid from config backend key ([#2683](https://github.com/rudderlabs/rudder-server/issues/2683)) ([931cb5c](https://github.com/rudderlabs/rudder-server/commit/931cb5c02374d3c2de96b90d3afc641ea0b41005))
* use enterprise token from env as fallback ([#2667](https://github.com/rudderlabs/rudder-server/issues/2667)) ([56f0f23](https://github.com/rudderlabs/rudder-server/commit/56f0f23f2aced8069d057ee98911c009e3541008))
* use proper status code to handle warehouse process ([#2659](https://github.com/rudderlabs/rudder-server/issues/2659)) ([a53657d](https://github.com/rudderlabs/rudder-server/commit/a53657d958e7da9d06c7786800ec7a3fc8ce388a))
* use sum64 to avoid checkptr race bug ([#2645](https://github.com/rudderlabs/rudder-server/issues/2645)) ([126cbdf](https://github.com/rudderlabs/rudder-server/commit/126cbdf1f42446683cff383f496ed7ba919d438a))
* **warehouse:** id resolution index issue ([#2676](https://github.com/rudderlabs/rudder-server/issues/2676)) ([8ddfb8e](https://github.com/rudderlabs/rudder-server/commit/8ddfb8edf38bb3702b34ddaeb231117703c1ec48))
* **warehouse:** parquet oom and ftr changes ([#2599](https://github.com/rudderlabs/rudder-server/issues/2599)) ([b820a55](https://github.com/rudderlabs/rudder-server/commit/b820a5551cb72a368db712da9160d56af101f588))
* **warehouse:** read unlock in historic identities ([#2592](https://github.com/rudderlabs/rudder-server/issues/2592)) ([09dba21](https://github.com/rudderlabs/rudder-server/commit/09dba218a2f7cf3ff8fdd706f6a0842e21aea975))


### Miscellaneous

* add github action for prerelease workflow ([#2660](https://github.com/rudderlabs/rudder-server/issues/2660)) ([43dde0c](https://github.com/rudderlabs/rudder-server/commit/43dde0c6f8abf72c85c3225e349b48384345d5af))
* add source to event_delivery metric ([#2642](https://github.com/rudderlabs/rudder-server/issues/2642)) ([fcf9e5e](https://github.com/rudderlabs/rudder-server/commit/fcf9e5e204ed9cce34cdb8a5bbcb941a3d86d73b))
* added status table cleanup logic before backup with code refactor ([#2542](https://github.com/rudderlabs/rudder-server/issues/2542)) ([9196f8f](https://github.com/rudderlabs/rudder-server/commit/9196f8f6986a325bdf7b5247c472cd238000e409))
* adding log entry to multi tenant test ([#2565](https://github.com/rudderlabs/rudder-server/issues/2565)) ([8444433](https://github.com/rudderlabs/rudder-server/commit/844443360a89879ee9ac15489e6d99d2cc5b7c34))
* address snyk vulnerabilities ([#2579](https://github.com/rudderlabs/rudder-server/issues/2579)) ([d1b1988](https://github.com/rudderlabs/rudder-server/commit/d1b1988dbff51694c75014b1d1f25a80bf572af2))
* applying 1.2.2 hotfixes to main branch ([#2585](https://github.com/rudderlabs/rudder-server/issues/2585)) ([71c8a15](https://github.com/rudderlabs/rudder-server/commit/71c8a15fe7b961fa19adbfd2edd7f12c9e6b3d46))
* applying 1.2.4 hotfixes to main branch ([#2597](https://github.com/rudderlabs/rudder-server/issues/2597)) ([0e1da7e](https://github.com/rudderlabs/rudder-server/commit/0e1da7ef944528bc86be358c9777415f00cae337))
* backup support for new multitenant system ([#2549](https://github.com/rudderlabs/rudder-server/issues/2549)) ([4741989](https://github.com/rudderlabs/rudder-server/commit/4741989750b8a1ef12c7acf2f10d45ee4453ef5f))
* clean up unused variables ([#2647](https://github.com/rudderlabs/rudder-server/issues/2647)) ([5541e7e](https://github.com/rudderlabs/rudder-server/commit/5541e7e3d47110ccd145cd71bec1f3645c9da428))
* cleanup unused code ([#2561](https://github.com/rudderlabs/rudder-server/issues/2561)) ([4de6105](https://github.com/rudderlabs/rudder-server/commit/4de6105b37722319d9e67fa9ef560987b5fe52d9))
* cleanup unused/obsolete code ([#2612](https://github.com/rudderlabs/rudder-server/issues/2612)) ([f13ea33](https://github.com/rudderlabs/rudder-server/commit/f13ea3397bea593208f033991e8dd8850ed43bcf))
* fix defect with router destinations map access event order ([#2589](https://github.com/rudderlabs/rudder-server/issues/2589)) ([a9d515e](https://github.com/rudderlabs/rudder-server/commit/a9d515e83927171ac852d864e15a2b272220b4cf))
* fix replay bugs ([#2653](https://github.com/rudderlabs/rudder-server/issues/2653)) ([8ac05c2](https://github.com/rudderlabs/rudder-server/commit/8ac05c2fa6a0b26f3cfba4b62600bed8d23314a5))
* improve grpc tooling and upgrade setup-go to v3 ([#2553](https://github.com/rudderlabs/rudder-server/issues/2553)) ([ab20ad6](https://github.com/rudderlabs/rudder-server/commit/ab20ad6344e388738d19c3651bc17d8deff12917))
* internal migration in postgres ([#2559](https://github.com/rudderlabs/rudder-server/issues/2559)) ([3cb2ec6](https://github.com/rudderlabs/rudder-server/commit/3cb2ec6c60d9d46cb3af7739e63cd7fb1808a998))
* **jobsdb:** use a different advisory lock for different table prefixes ([#2575](https://github.com/rudderlabs/rudder-server/issues/2575)) ([26b218c](https://github.com/rudderlabs/rudder-server/commit/26b218c7413e04fc24ea6b8f549810664a6465f2))
* modified statsMiddleware to be generic ([#2626](https://github.com/rudderlabs/rudder-server/issues/2626)) ([b6ce9b9](https://github.com/rudderlabs/rudder-server/commit/b6ce9b98859b1667660f2b313cbb40287cce61ba))
* release 1.2.1 ([#2573](https://github.com/rudderlabs/rudder-server/issues/2573)) ([eb705df](https://github.com/rudderlabs/rudder-server/commit/eb705dff53a788a517a9f4e43c5fc328dab71a39))
* release 1.2.5 ([#2602](https://github.com/rudderlabs/rudder-server/issues/2602)) ([854e359](https://github.com/rudderlabs/rudder-server/commit/854e359db8c5a94d445408604e200a3100b4cd75))
* remove leftover flag.Parse() ([#2643](https://github.com/rudderlabs/rudder-server/issues/2643)) ([3659e12](https://github.com/rudderlabs/rudder-server/commit/3659e12e432e4be61aab9c836164c355f720284a))
* rsources flaky test - start services before all ([#2650](https://github.com/rudderlabs/rudder-server/issues/2650)) ([1c49e96](https://github.com/rudderlabs/rudder-server/commit/1c49e960aa6ee0a76f9a1e54408bd54d3fb1a2be))
* upgrade GO version to 1.19 ([#2635](https://github.com/rudderlabs/rudder-server/issues/2635)) ([768be08](https://github.com/rudderlabs/rudder-server/commit/768be08ff100bcdb04e37e49e7dc7c134adbe970))
* use gotestsum for test execution ([#2586](https://github.com/rudderlabs/rudder-server/issues/2586)) ([a194b0c](https://github.com/rudderlabs/rudder-server/commit/a194b0c4d840b54c54991008d0d1412b5d952b99))
* use WriteTimeout instead of Gateway.pendingEventsQueryTimeout ([#2649](https://github.com/rudderlabs/rudder-server/issues/2649)) ([e9aff77](https://github.com/rudderlabs/rudder-server/commit/e9aff77c4a8c20360e1bebf5d0f06713e8f30b0d))
* using error from router http client ([#2628](https://github.com/rudderlabs/rudder-server/issues/2628)) ([a6e253b](https://github.com/rudderlabs/rudder-server/commit/a6e253be09ec2b9b466570ca23a34b1c4584e302))
* **warehouse:** add user transformer url env ([#2651](https://github.com/rudderlabs/rudder-server/issues/2651)) ([00ba231](https://github.com/rudderlabs/rudder-server/commit/00ba23162e3b05cfad753cd38cbe75faab5b8efc))
* **warehouse:** fix log level of async jobs ([#2617](https://github.com/rudderlabs/rudder-server/issues/2617)) ([078bf76](https://github.com/rudderlabs/rudder-server/commit/078bf76d2c3f7f776ca501d8c31e3358cabb8cf7))
* **warehouse:** verify workspace ID in stats ([#2615](https://github.com/rudderlabs/rudder-server/issues/2615)) ([dceaf29](https://github.com/rudderlabs/rudder-server/commit/dceaf29eb63e9313d24ccc5757ccd3c0967c4223))
* **warehouse:** warehouse formatting changes ([#2568](https://github.com/rudderlabs/rudder-server/issues/2568)) ([888f6f8](https://github.com/rudderlabs/rudder-server/commit/888f6f8ffc22f0f87fc4da36b7c34a7ca5669b44))

## [1.2.5](https://github.com/rudderlabs/rudder-server/compare/v1.2.4...v1.2.5) (2022-10-20)


### Bug Fixes

* **warehouse:** parquet oom and ftr changes ([#2599](https://github.com/rudderlabs/rudder-server/issues/2599)) ([ff07f37](https://github.com/rudderlabs/rudder-server/commit/ff07f37b299fc320bcfeac697de9a42ace157e6b))

## [1.2.4](https://github.com/rudderlabs/rudder-server/compare/v1.2.3...v1.2.4) (2022-10-19)


### Bug Fixes

* **warehouse:** default job type ([#2595](https://github.com/rudderlabs/rudder-server/issues/2595)) ([fd2652a](https://github.com/rudderlabs/rudder-server/commit/fd2652a0849c390879461baf520dc2cdb90dbb9e))

## [1.2.3](https://github.com/rudderlabs/rudder-server/compare/v1.2.2...v1.2.3) (2022-10-19)


### Bug Fixes

* **warehouse:** remove bad unlock ([#2590](https://github.com/rudderlabs/rudder-server/issues/2590)) ([aba7893](https://github.com/rudderlabs/rudder-server/commit/aba7893850dbb5ed224bc82bc19f1924b65874d2))

## [1.2.2](https://github.com/rudderlabs/rudder-server/compare/v1.2.1...v1.2.2) (2022-10-19)


### Bug Fixes

* **jobsdb:** race - repeat job count calculation after acquiring migration lock ([#2583](https://github.com/rudderlabs/rudder-server/issues/2583)) ([87f9768](https://github.com/rudderlabs/rudder-server/commit/87f9768b2edac095cba6fcc21ea320ef2d56cde4))

## [1.2.1](https://github.com/rudderlabs/rudder-server/compare/v1.2.0...v1.2.1) (2022-10-14)


### Bug Fixes

* **jobsdb:** update cache after transaction completes ([#2567](https://github.com/rudderlabs/rudder-server/issues/2567)) ([6c0db53](https://github.com/rudderlabs/rudder-server/commit/6c0db5300848b234c994b3495c88caa1dc979ab1))

## [1.2.0](https://github.com/rudderlabs/rudder-server/compare/v1.1.0...v1.2.0) (2022-10-12)


### Features

* add support for role based auth flag ([#2519](https://github.com/rudderlabs/rudder-server/issues/2519)) ([810bb5d](https://github.com/rudderlabs/rudder-server/commit/810bb5d183c9116d667c975fe9bf9d3de50af57e))
* add support iam role support for warehouse destinations ([#2496](https://github.com/rudderlabs/rudder-server/issues/2496)) ([867123a](https://github.com/rudderlabs/rudder-server/commit/867123af01850d3221a2821481b5362b3d480682))
* addition of Azure SAS Tokens for Azure Object storage destinations  ([#2358](https://github.com/rudderlabs/rudder-server/issues/2358)) ([cf5afe1](https://github.com/rudderlabs/rudder-server/commit/cf5afe175bde39e4e27f5946e4fd029b1b46afdf))
* changes to support variadic keys for deletion from API dest. ([#2457](https://github.com/rudderlabs/rudder-server/issues/2457)) ([1950713](https://github.com/rudderlabs/rudder-server/commit/19507133f98cab63b4133e3a5e623b7dcb408aae))
* **core:** logical replication monitoring query routine ([#2436](https://github.com/rudderlabs/rudder-server/issues/2436)) ([8271aab](https://github.com/rudderlabs/rudder-server/commit/8271aabffd687cf5e4ea9fea2a833d93268e2207))
* **destination:** add new tags to router-aborted-count alert definition ([#2514](https://github.com/rudderlabs/rudder-server/issues/2514)) ([60bc1be](https://github.com/rudderlabs/rudder-server/commit/60bc1be9755de0898aa6c126a86ae51a6a039b4f))
* introduce features/settings client ([#2377](https://github.com/rudderlabs/rudder-server/issues/2377)) ([711f266](https://github.com/rudderlabs/rudder-server/commit/711f26672fdf477c4c9a3f94a370a103534e30b7))
* updating supportedDestinations in api.go ([#2468](https://github.com/rudderlabs/rudder-server/issues/2468)) ([3a2e75c](https://github.com/rudderlabs/rudder-server/commit/3a2e75c4bf5887f4a6848f966041602feaca5627))
* warehouse_jobs support for async warehouse jobs ([#2319](https://github.com/rudderlabs/rudder-server/issues/2319)) ([73cc48a](https://github.com/rudderlabs/rudder-server/commit/73cc48ade5ea52eaf64c7232a79d79a5ce241107))
* **warehouse:** added warehouse features ([#2545](https://github.com/rudderlabs/rudder-server/issues/2545)) ([8ea8be5](https://github.com/rudderlabs/rudder-server/commit/8ea8be571129560d985273fe6805afdf1716b1ae))
* **warehouse:** introduce workspace id in warehouse  ([#2523](https://github.com/rudderlabs/rudder-server/issues/2523)) ([2c5fa3d](https://github.com/rudderlabs/rudder-server/commit/2c5fa3de3cb0689b0d1d89f41875a7caab948c1f))
* **warehouse:** skipping scanning the entire table for duplicates in redshift ([#2518](https://github.com/rudderlabs/rudder-server/issues/2518)) ([104410a](https://github.com/rudderlabs/rudder-server/commit/104410a2d03f671b9b7abfa07ca260d0dbb9e09c))
* **warehouse:** timeout during warehouse table count ([#2543](https://github.com/rudderlabs/rudder-server/issues/2543)) ([b2b1c08](https://github.com/rudderlabs/rudder-server/commit/b2b1c0822e92575ae64de3a45ca22a35d552cfdd))


### Bug Fixes

* **core:** cache backend config and block processor ([#2225](https://github.com/rudderlabs/rudder-server/issues/2225)) ([40fe19d](https://github.com/rudderlabs/rudder-server/commit/40fe19d4cbd42ed89e0b1cdbb958320605c35f99))
* json out of bounds in batchrouter ([#2465](https://github.com/rudderlabs/rudder-server/issues/2465)) ([2bf0f6a](https://github.com/rudderlabs/rudder-server/commit/2bf0f6a3ed3a3e92ba33d3f36371c30934c99c13))
* only report features on master / standalone warehouse ([#2483](https://github.com/rudderlabs/rudder-server/issues/2483)) ([33ba5ef](https://github.com/rudderlabs/rudder-server/commit/33ba5ef6fe72905ee0269b1057f694d3e6cf7090))
* remove custom credentials chain to use default ([#2485](https://github.com/rudderlabs/rudder-server/issues/2485)) ([de0fe36](https://github.com/rudderlabs/rudder-server/commit/de0fe363606e8e013df147b88d8aaefd8afc9222))
* **router:** preserve event order while draining a previously failed job ([#2546](https://github.com/rudderlabs/rudder-server/issues/2546)) ([f0654b0](https://github.com/rudderlabs/rudder-server/commit/f0654b01e9dcc127b2da04bd46a63af7223bf116))
* silent backup ds drop ([#2550](https://github.com/rudderlabs/rudder-server/issues/2550)) ([4fe3038](https://github.com/rudderlabs/rudder-server/commit/4fe30388a12a2fba034ba4134d8c34f9a5297806))
* to prevent migration of tables every loop after maxdsretention is passed ([#2554](https://github.com/rudderlabs/rudder-server/issues/2554)) ([af27add](https://github.com/rudderlabs/rudder-server/commit/af27addda3bf01d41367621d789d902611c8f39c))
* use workspace id as aws iam role external id ([#2386](https://github.com/rudderlabs/rudder-server/issues/2386)) ([bced8d5](https://github.com/rudderlabs/rudder-server/commit/bced8d58bf8d2a880c41f9f26e965d64c1efdd76))
* **warehouse:** omit view tables while fetching schema ([#2482](https://github.com/rudderlabs/rudder-server/issues/2482)) ([74f268c](https://github.com/rudderlabs/rudder-server/commit/74f268ca1d9d63f739e48de36f86a10abe38c868))
* **warehouse:** typo with fetch schema for bigquery ([#2556](https://github.com/rudderlabs/rudder-server/issues/2556)) ([ce69ae3](https://github.com/rudderlabs/rudder-server/commit/ce69ae3bb4a6a56c7e39555a76bee22c4b7ea4c0))


### Documentation

* docker fails to parse quotes in env value ([#2345](https://github.com/rudderlabs/rudder-server/issues/2345)) ([67584f7](https://github.com/rudderlabs/rudder-server/commit/67584f7fde97afdf696ae60dc2a801fb2a1eef06))
* remove unnecessary bold style in README ([#2509](https://github.com/rudderlabs/rudder-server/issues/2509)) ([1392de1](https://github.com/rudderlabs/rudder-server/commit/1392de1f3dd0bf044214002166766bf8ee08d901))
* v1.0 announcement and event link ([#2400](https://github.com/rudderlabs/rudder-server/issues/2400)) ([44f3786](https://github.com/rudderlabs/rudder-server/commit/44f3786231f52a095d6c7f655b0ccda7e93ef2b8))


### Miscellaneous

* add object storage support to replay ([#2277](https://github.com/rudderlabs/rudder-server/issues/2277)) ([e9e15b9](https://github.com/rudderlabs/rudder-server/commit/e9e15b9adbe24d326e2222520bfc52601be5ad72))
* add reason tag in failed requests and failed events stats ([#2430](https://github.com/rudderlabs/rudder-server/issues/2430)) ([a6137a9](https://github.com/rudderlabs/rudder-server/commit/a6137a965c65b7558e64fa1bb08f973474585c12))
* add replay support for all object storage ([e9e15b9](https://github.com/rudderlabs/rudder-server/commit/e9e15b9adbe24d326e2222520bfc52601be5ad72))
* add resource tags in metrics ([#2462](https://github.com/rudderlabs/rudder-server/issues/2462)) ([2f73704](https://github.com/rudderlabs/rudder-server/commit/2f737046dd8f29d0b249dff8b06fbd6c9dcc0da0))
* added pre_drop_table count metrics ([#2441](https://github.com/rudderlabs/rudder-server/issues/2441)) ([869d694](https://github.com/rudderlabs/rudder-server/commit/869d694c0a1cc7d352208e66176cc0c3d530ed00))
* added rudder-server config metrics with version and other details as tag ([#2445](https://github.com/rudderlabs/rudder-server/issues/2445)) ([da55a86](https://github.com/rudderlabs/rudder-server/commit/da55a8689048b959d75be3958e65f59d34c537d7))
* applying 1.1.1 hotfixes to main branch ([#2449](https://github.com/rudderlabs/rudder-server/issues/2449)) ([ac9afbd](https://github.com/rudderlabs/rudder-server/commit/ac9afbd893d1975a60bb3316ef68c1f55d2c9fa8))
* applying 1.1.2 hotfixes to main branch  ([#2474](https://github.com/rudderlabs/rudder-server/issues/2474)) ([575c9e4](https://github.com/rudderlabs/rudder-server/commit/575c9e40479316e05ce993f70cb8a4bdd694c824))
* change base image to alpine for build and prod ([#2480](https://github.com/rudderlabs/rudder-server/issues/2480)) ([4593f65](https://github.com/rudderlabs/rudder-server/commit/4593f659a9846651ff051404ddbeae4b41690d96))
* clean up pathfinder which is related to old migrator code. ([#2479](https://github.com/rudderlabs/rudder-server/issues/2479)) ([3230fc6](https://github.com/rudderlabs/rudder-server/commit/3230fc62a6cdbfe1d13f0642eb38fa5c711b5b07))
* config package ([#2439](https://github.com/rudderlabs/rudder-server/issues/2439)) ([2ce9210](https://github.com/rudderlabs/rudder-server/commit/2ce92105e3f2e896422419d998989cfc065996dc))
* dataset index calculation ([#2512](https://github.com/rudderlabs/rudder-server/issues/2512)) ([bfba9cf](https://github.com/rudderlabs/rudder-server/commit/bfba9cf230e00a1ac567779514f0cb475a0a0f40))
* **deps:** bump github.com/mitchellh/mapstructure from 1.4.1 to 1.5.0 ([#2442](https://github.com/rudderlabs/rudder-server/issues/2442)) ([79c63e5](https://github.com/rudderlabs/rudder-server/commit/79c63e5e81894f99976d26071d2f5d9ed793485e))
* **deps:** bump github.com/segmentio/kafka-go from 0.4.32 to 0.4.35 ([#2455](https://github.com/rudderlabs/rudder-server/issues/2455)) ([3441862](https://github.com/rudderlabs/rudder-server/commit/34418627770d94ce74fecbb51ad53194305d9d9d))
* **deps:** bump github.com/snowflakedb/gosnowflake from 1.6.3 to 1.6.13 ([#2432](https://github.com/rudderlabs/rudder-server/issues/2432)) ([7ae6421](https://github.com/rudderlabs/rudder-server/commit/7ae6421664c0214493af021fd891c5719ff21638))
* **deps:** bump go.etcd.io/etcd/client/v3 from 3.5.2 to 3.5.5 ([#2499](https://github.com/rudderlabs/rudder-server/issues/2499)) ([6ece30e](https://github.com/rudderlabs/rudder-server/commit/6ece30ea7c23a30fc38c0c7fe54016c2b14e3d8a))
* disable deepsource transformer ([0466ec6](https://github.com/rudderlabs/rudder-server/commit/0466ec69c1ba440c2b629c3fd6bafa7375036be8))
* disable semantic PR single commit validation ([#2487](https://github.com/rudderlabs/rudder-server/issues/2487)) ([261345e](https://github.com/rudderlabs/rudder-server/commit/261345e5d349fb13d84c6b0966ed2afd45c813af))
* drop old migrator ([#2417](https://github.com/rudderlabs/rudder-server/issues/2417)) ([870bccf](https://github.com/rudderlabs/rudder-server/commit/870bccf35498e845538edca100d7599c2ce34dc0))
* excluding destId tag for free customers ([#2555](https://github.com/rudderlabs/rudder-server/issues/2555)) ([18b7f7b](https://github.com/rudderlabs/rudder-server/commit/18b7f7b253c300200a72194395a0288c6d27f64d))
* fix defect and use noCache if database setup fails during cache.Start ([#2537](https://github.com/rudderlabs/rudder-server/issues/2537)) ([89afba3](https://github.com/rudderlabs/rudder-server/commit/89afba3de0342549a73923678587f87afde48f0b))
* fix error log defect in processor's transformer ([#2547](https://github.com/rudderlabs/rudder-server/issues/2547)) ([f49df7f](https://github.com/rudderlabs/rudder-server/commit/f49df7f0bae554e430c7bf2047837b59471a18b3))
* fix flaky tests ([#2424](https://github.com/rudderlabs/rudder-server/issues/2424)) ([dadb4df](https://github.com/rudderlabs/rudder-server/commit/dadb4df8482e40a5211c290f0053e4cc90420330))
* fix race defect after stats refactoring ([#2511](https://github.com/rudderlabs/rudder-server/issues/2511)) ([13e28a0](https://github.com/rudderlabs/rudder-server/commit/13e28a02ead0458d57e47f00fa42cda3d7cdfa60))
* fix unused method receiver ([#2498](https://github.com/rudderlabs/rudder-server/issues/2498)) ([d17ece5](https://github.com/rudderlabs/rudder-server/commit/d17ece5ce7ddf0f9086c0a31d64dce9bd89aa37c))
* gateway middleware refactoring ([#2416](https://github.com/rudderlabs/rudder-server/issues/2416)) ([1ea77a0](https://github.com/rudderlabs/rudder-server/commit/1ea77a0cd4c169e2fa4542c731a374ccfaa3a058))
* include matched template path in gateway's request metrics ([#2531](https://github.com/rudderlabs/rudder-server/issues/2531)) ([ded763c](https://github.com/rudderlabs/rudder-server/commit/ded763c43443cf475177803f26e6b61bfbc87a56))
* include status code in pipeline_processed_events metric ([#2450](https://github.com/rudderlabs/rudder-server/issues/2450)) ([45936bb](https://github.com/rudderlabs/rudder-server/commit/45936bb72ce31b7ea749e2500341c024e78a5387))
* introduce tooling for local development ([#2413](https://github.com/rudderlabs/rudder-server/issues/2413)) ([85b0342](https://github.com/rudderlabs/rudder-server/commit/85b034229adf2c5a2a2969d2ee4a21c2773abef2))
* introduced log & stats for retry of  jobsDB execute & query methods ([#2434](https://github.com/rudderlabs/rudder-server/issues/2434)) ([c1b90f1](https://github.com/rudderlabs/rudder-server/commit/c1b90f1d453b32c287326914e1134df887848c06))
* **jobsdb:** introduce context aware mutex locks,  transactional migrations & cancelable maintenance operations ([#2505](https://github.com/rudderlabs/rudder-server/issues/2505)) ([096db9c](https://github.com/rudderlabs/rudder-server/commit/096db9c91367341763a5f88cabec91dd08e22fa4))
* kafka cloud platform CI integration ([#2504](https://github.com/rudderlabs/rudder-server/issues/2504)) ([1f1c989](https://github.com/rudderlabs/rudder-server/commit/1f1c98911368e3355a4ea04d61db04fee5038816))
* limit queries to ds ([#2446](https://github.com/rudderlabs/rudder-server/issues/2446)) ([082b9f5](https://github.com/rudderlabs/rudder-server/commit/082b9f53a0394d014e7862963f92dcc275b8b685))
* linter fix unused method receiver ([#2486](https://github.com/rudderlabs/rudder-server/issues/2486)) ([7ae57b3](https://github.com/rudderlabs/rudder-server/commit/7ae57b3af6cf45930f0c97ea041cc0210bb6d038))
* linter fix: unused parameter should be replaced by underscore ([#2490](https://github.com/rudderlabs/rudder-server/issues/2490)) ([f1dfdab](https://github.com/rudderlabs/rudder-server/commit/f1dfdab48751fdcf8e1bcd1f105cacb8c126dec8))
* logger package ([#2477](https://github.com/rudderlabs/rudder-server/issues/2477)) ([19c08e2](https://github.com/rudderlabs/rudder-server/commit/19c08e2bf4ce294c54b22252d5d06a8071b73579))
* minor error message change ([#2557](https://github.com/rudderlabs/rudder-server/issues/2557)) ([a305aca](https://github.com/rudderlabs/rudder-server/commit/a305acaa0979bcce84ff60da7a80732a277d8e70))
* reduce the warehouse slave payload ([#2533](https://github.com/rudderlabs/rudder-server/issues/2533)) ([1013a93](https://github.com/rudderlabs/rudder-server/commit/1013a9312634e4673d6bc1f34a6a78e817a61c75))
* remove unused suite test files ([#2429](https://github.com/rudderlabs/rudder-server/issues/2429)) ([be670e1](https://github.com/rudderlabs/rudder-server/commit/be670e12ccd771ce89461765bcf19cb883475062))
* reverse proxy warehouse pending-events endpoint ([#2438](https://github.com/rudderlabs/rudder-server/issues/2438)) ([8199002](https://github.com/rudderlabs/rudder-server/commit/81990023948fcd90db425cebdd47b39af8e49284))
* **router:** include runtime information during event ordering errors ([#2421](https://github.com/rudderlabs/rudder-server/issues/2421)) ([12d3e59](https://github.com/rudderlabs/rudder-server/commit/12d3e59ab45f81108209d449581608aaf2c2e9ea))
* simplify migrations using go's embed fs ([#2535](https://github.com/rudderlabs/rudder-server/issues/2535)) ([41ea78e](https://github.com/rudderlabs/rudder-server/commit/41ea78e5151bc7d1846f027c87c702fb1e07b96e))
* skip json truncating chars ([#2491](https://github.com/rudderlabs/rudder-server/issues/2491)) ([e4fb710](https://github.com/rudderlabs/rudder-server/commit/e4fb7102ac7b7a8cb1fae63617651830ec194812))
* stats package ([#2489](https://github.com/rudderlabs/rudder-server/issues/2489)) ([c36b303](https://github.com/rudderlabs/rudder-server/commit/c36b303c43c46ff160af3e9fd5fcaa2cf9f1ece1))
* upgrade go dependencies with ([#2522](https://github.com/rudderlabs/rudder-server/issues/2522)) ([3147333](https://github.com/rudderlabs/rudder-server/commit/3147333f5e78351e916db6a25c2f05a5b43ec342))
* warehouse lint ([#2493](https://github.com/rudderlabs/rudder-server/issues/2493)) ([bb4f3d6](https://github.com/rudderlabs/rudder-server/commit/bb4f3d633bd5a9780db60415f8cd1ea59e28fcc5))
* **warehouse:** add coverage for warehouse configuration validations ([#2538](https://github.com/rudderlabs/rudder-server/issues/2538)) ([07dbe71](https://github.com/rudderlabs/rudder-server/commit/07dbe7137e2597b8b38006481925992b568d8f86))
* **warehouse:** added integration test for snowflake case-sensitive database ([#2501](https://github.com/rudderlabs/rudder-server/issues/2501)) ([cf661fd](https://github.com/rudderlabs/rudder-server/commit/cf661fddd30cc6615be3fe781c1944bf8b38da00))
* **warehouse:** bigquery handler restructuring ([#2551](https://github.com/rudderlabs/rudder-server/issues/2551)) ([cb6fc27](https://github.com/rudderlabs/rudder-server/commit/cb6fc27ab91117d836937fb8989e2967bcbdb6f3))
* **warehouse:** format warehouse queries ([#2540](https://github.com/rudderlabs/rudder-server/issues/2540)) ([9b944c9](https://github.com/rudderlabs/rudder-server/commit/9b944c91a5493171698f4f5af26ab760672497db))
* **warehouse:** integration test changes v3 ([#2426](https://github.com/rudderlabs/rudder-server/issues/2426)) ([4e98109](https://github.com/rudderlabs/rudder-server/commit/4e981091d07bf263240fd821d8beb1bdf9d8c011))
* **warehouse:** pump more test for warehouse module ([#2460](https://github.com/rudderlabs/rudder-server/issues/2460)) ([58671c1](https://github.com/rudderlabs/rudder-server/commit/58671c1637fd9e345e79208834b0bcc78924412e))
* **warehouse:** utility for staging table name and some refactoring ([#2529](https://github.com/rudderlabs/rudder-server/issues/2529)) ([079449c](https://github.com/rudderlabs/rudder-server/commit/079449cbff8b3831eeb98ff808d38b28b25e5223))
* **warehouse:** validate object storage credentials ([#2440](https://github.com/rudderlabs/rudder-server/issues/2440)) ([3d2d87e](https://github.com/rudderlabs/rudder-server/commit/3d2d87e705ce6f36cf90c0eddc1360b29f040b0f))
* **warehouse:** warehouse api to capture task run ID when calculating the pending uploads. ([#2435](https://github.com/rudderlabs/rudder-server/issues/2435)) ([d43705e](https://github.com/rudderlabs/rudder-server/commit/d43705e2cd0b0dc62a1613c70b36416eb566ff75))
* **warehouse:** warehouse retry uploads count ([#2275](https://github.com/rudderlabs/rudder-server/issues/2275)) ([212db1e](https://github.com/rudderlabs/rudder-server/commit/212db1e75cdd8c6137dd484f63d76be736f42e1a))

## [1.1.2](https://github.com/rudderlabs/rudder-server/compare/v1.1.1...v1.1.2) (2022-09-23)


### Bug Fixes

* snowflake identifier issue ([#2471](https://github.com/rudderlabs/rudder-server/issues/2471)) ([d4b2583](https://github.com/rudderlabs/rudder-server/commit/d4b2583a81104eb35b226b3606e178506897fab0))

## [1.1.1](https://github.com/rudderlabs/rudder-server/compare/v1.1.0...v1.1.1) (2022-09-15)


### Bug Fixes

* close http response body only if there is no error ([#2447](https://github.com/rudderlabs/rudder-server/issues/2447)) ([ef72aee](https://github.com/rudderlabs/rudder-server/commit/ef72aeea30ca478477339d4a7564d50bb2250ac7))

## [1.1.0](https://github.com/rudderlabs/rudder-server/compare/v1.0.2...v1.1.0) (2022-09-12)


### Features

* add support for sending custom payload to webhook source  ([#2315](https://github.com/rudderlabs/rudder-server/issues/2315)) ([8c0d811](https://github.com/rudderlabs/rudder-server/commit/8c0d811bc662fa9a422158988d3b3f0e1a6563c3))
* cpRouter Multitenant Support ([#2233](https://github.com/rudderlabs/rudder-server/issues/2233)) ([3c86dab](https://github.com/rudderlabs/rudder-server/commit/3c86dab822d9c71791577761db04b9b254588954))
* honour anonId only from header ([#2323](https://github.com/rudderlabs/rudder-server/issues/2323)) ([cbe74b6](https://github.com/rudderlabs/rudder-server/commit/cbe74b66a9b14d53d934d98d551908795b175848))
* improvements in rudder-sources integration ([#2366](https://github.com/rudderlabs/rudder-server/issues/2366)) ([54133d1](https://github.com/rudderlabs/rudder-server/commit/54133d1b67aa5e6f1493292d0b8820856527c5d1))
* introduce context, timeout and retries in jobsDB queries ([#2217](https://github.com/rudderlabs/rudder-server/issues/2217)) ([32b2f40](https://github.com/rudderlabs/rudder-server/commit/32b2f407c7309350fe9b8703741bba4f48875401))
* isolate destID in router ([#2369](https://github.com/rudderlabs/rudder-server/issues/2369)) ([0708615](https://github.com/rudderlabs/rudder-server/commit/0708615155cc388ef97071f06bdcbb7fee77c05b))
* **jobsdb:** thread safe addNewDSLoop ([#2356](https://github.com/rudderlabs/rudder-server/issues/2356)) ([8ef6c39](https://github.com/rudderlabs/rudder-server/commit/8ef6c39974aa7443017547e7e5c9de85ef9f7a78))
* perform ADD_DS operation in a single transaction ([#2324](https://github.com/rudderlabs/rudder-server/issues/2324)) ([7ee2d11](https://github.com/rudderlabs/rudder-server/commit/7ee2d1140f377f31f25a9a441e3964998eddbd5c))
* thread safe Job Storage ([#2403](https://github.com/rudderlabs/rudder-server/issues/2403)) ([a123b10](https://github.com/rudderlabs/rudder-server/commit/a123b10925425a813d35cacfa6985789603a6771))


### Bug Fixes

* add trailing slash for licensing link ([ca00c32](https://github.com/rudderlabs/rudder-server/commit/ca00c32d773bec0f979aa7373662c814347612f3))
* add www and trailing slash to RS links ([7574509](https://github.com/rudderlabs/rudder-server/commit/757450976830fd371adb4bcd15b24f4052b660b9))
* safe webhook concurrent map access ([#2389](https://github.com/rudderlabs/rudder-server/issues/2389)) ([b72f6be](https://github.com/rudderlabs/rudder-server/commit/b72f6bef74c9175c64873a9d9ecdb25bef9e815d))
* use jsoniter when unmarshaling to avoid perfromance issues ([#2381](https://github.com/rudderlabs/rudder-server/issues/2381)) ([35ad8e4](https://github.com/rudderlabs/rudder-server/commit/35ad8e40078f353335a6612d32cdb9fe2d61f07f))


### Miscellaneous

* add token type for success message ([#2408](https://github.com/rudderlabs/rudder-server/issues/2408)) ([92600fd](https://github.com/rudderlabs/rudder-server/commit/92600fda0892074e962b01c40061f1df00670bf8))
* added metrics to track failed DS backup ([#2370](https://github.com/rudderlabs/rudder-server/issues/2370)) ([1d3cd42](https://github.com/rudderlabs/rudder-server/commit/1d3cd428d8579355a8a719e6746fb23816741338))
* change proto message names ([#2394](https://github.com/rudderlabs/rudder-server/issues/2394)) ([689edd6](https://github.com/rudderlabs/rudder-server/commit/689edd6a61c34acb771940960803cb786ab6af17))
* disable parallel test execution and refactor jobsdb tests ([#2410](https://github.com/rudderlabs/rudder-server/issues/2410)) ([3bd27ee](https://github.com/rudderlabs/rudder-server/commit/3bd27ee0ab1352a55d9d9e4f521927d313bd862f))
* fix cache bug ([#2412](https://github.com/rudderlabs/rudder-server/issues/2412)) ([4f83ba7](https://github.com/rudderlabs/rudder-server/commit/4f83ba7c9ec17b67871471e8d2e0948145cecd89))
* fix for TestNewForDeployment test ([#2425](https://github.com/rudderlabs/rudder-server/issues/2425)) ([1c4a36d](https://github.com/rudderlabs/rudder-server/commit/1c4a36d1a6034119b5f89f77c33c5ae0b98d86bb))
* log improvements ([#2365](https://github.com/rudderlabs/rudder-server/issues/2365)) ([95a24ca](https://github.com/rudderlabs/rudder-server/commit/95a24ca9fa841edd98faba4cc6f877e89e73d4f0))
* migrate small tables ([#2327](https://github.com/rudderlabs/rudder-server/issues/2327)) ([1ca1faa](https://github.com/rudderlabs/rudder-server/commit/1ca1faa4b70852460b33fcd7b2a1770e956f44ab))
* modified regulation-worker response code handling from 404 to 204, when no job is available ([#2422](https://github.com/rudderlabs/rudder-server/issues/2422)) ([44e8553](https://github.com/rudderlabs/rudder-server/commit/44e85538cb64fbbc3d0fd4ad98ce9572b998793a))
* **router:** include original client creation error when circuit breaker is open ([#2373](https://github.com/rudderlabs/rudder-server/issues/2373)) ([4d8a9bc](https://github.com/rudderlabs/rudder-server/commit/4d8a9bcfa78179753fd837644e8680579fe3b596))
* **router:** new event ordering algorithm with proper aborted job limiting ([#2344](https://github.com/rudderlabs/rudder-server/issues/2344)) ([097c61b](https://github.com/rudderlabs/rudder-server/commit/097c61bafd041a87060da4ec27ecb39c8efa94ec))
* skip double mutex unlock ([#2415](https://github.com/rudderlabs/rudder-server/issues/2415)) ([366e1b9](https://github.com/rudderlabs/rudder-server/commit/366e1b9dfed6041f2e00d764dd225fb16f5cecee))
* upgrade golangci and linting fixes ([#2347](https://github.com/rudderlabs/rudder-server/issues/2347)) ([6348406](https://github.com/rudderlabs/rudder-server/commit/6348406e4432322021139fc97eeadb106810703c))
* use generics for contains function ([#2396](https://github.com/rudderlabs/rudder-server/issues/2396)) ([2134c54](https://github.com/rudderlabs/rudder-server/commit/2134c54d1b10fe40a15abeac153f15982b0509ad))
* use right id to allocate worker ([#2399](https://github.com/rudderlabs/rudder-server/issues/2399)) ([312a301](https://github.com/rudderlabs/rudder-server/commit/312a3015b150e4bfb54ece2356232299990e2b79))
* using go test ([#2411](https://github.com/rudderlabs/rudder-server/issues/2411)) ([7eb62d9](https://github.com/rudderlabs/rudder-server/commit/7eb62d99718e0c7ce326a4f765022ea6cf87c0ec))
* **warehouse:** remove tokens from logs for redshift and snowflake ([#2359](https://github.com/rudderlabs/rudder-server/issues/2359)) ([7409c56](https://github.com/rudderlabs/rudder-server/commit/7409c56695719dbe71097f8aa891910305698e68))
* **warehouse:** skip warehouse snowflake test ([#2404](https://github.com/rudderlabs/rudder-server/issues/2404)) ([09bc408](https://github.com/rudderlabs/rudder-server/commit/09bc4088fb24a1a87aae6b02f1893b29af115750))
* **warehouse:** unnecessary join in fetch schema for postgres and snowflake ([#2360](https://github.com/rudderlabs/rudder-server/issues/2360)) ([2de0d68](https://github.com/rudderlabs/rudder-server/commit/2de0d684b9cef139c435c40762991d44bc64241e))

## [1.0.2](https://github.com/rudderlabs/rudder-server/compare/v1.0.0...v1.0.2) (2022-08-25)


### Bug Fixes

* **jobsdb:** report correct table count metrics from gateway writer ([#2333](https://github.com/rudderlabs/rudder-server/issues/2333)) ([bb445a5](https://github.com/rudderlabs/rudder-server/commit/bb445a56e086bf95e0a6c3188b19b0b93332b06b))
* release-please behaviour ([33d1dec](https://github.com/rudderlabs/rudder-server/commit/33d1dec23b325c5f51f0daea4bdc440d31e2a2fb))
* remove anti pattern issues ([#2335](https://github.com/rudderlabs/rudder-server/issues/2335)) ([7468709](https://github.com/rudderlabs/rudder-server/commit/74687090ce0fbdcad2e529b4dd65bc126810dfe2))
* **router:** event ordering algorithm with proper aborted job limiting support ([#2329](https://github.com/rudderlabs/rudder-server/issues/2329)) ([c6f8173](https://github.com/rudderlabs/rudder-server/commit/c6f8173a54ae165b321e99af47b2028761593e0e))
* **router:** trying to stop the router before the generatorLoop is started causes router to hang ([#2321](https://github.com/rudderlabs/rudder-server/issues/2321)) ([cbaef78](https://github.com/rudderlabs/rudder-server/commit/cbaef78b1335a6702b9fcf231fc27eda60d45f80))
* **warehouse:** added initialization for datalake ([#2332](https://github.com/rudderlabs/rudder-server/issues/2332)) ([8fe1bf9](https://github.com/rudderlabs/rudder-server/commit/8fe1bf99404760b51810901202a7156e9df8d2d9))
* **warehouse:** upload validations should happen when validator is set ([#2348](https://github.com/rudderlabs/rudder-server/issues/2348)) ([f0a6416](https://github.com/rudderlabs/rudder-server/commit/f0a6416f7c4bcad071af82f8f574e2b5d7061f7a))


### Miscellaneous

* fix lint errors in test ([#2352](https://github.com/rudderlabs/rudder-server/issues/2352)) ([d67ef3a](https://github.com/rudderlabs/rudder-server/commit/d67ef3a7e57438810ba0989a29b62ddbfb864d33))
* fix misspell ([c5d5060](https://github.com/rudderlabs/rudder-server/commit/c5d5060d31e9a3701b18a24187875aae88c3ab2f))
* include all supported pr types in changelog ([#2339](https://github.com/rudderlabs/rudder-server/issues/2339)) ([c074298](https://github.com/rudderlabs/rudder-server/commit/c074298dc4bd6ed118bfb39d74ffca95baecb73d))
* introduce go report card ([#2320](https://github.com/rudderlabs/rudder-server/issues/2320)) ([578de21](https://github.com/rudderlabs/rudder-server/commit/578de21fbb7383f39a819a37c59934ce931232e5))
* remove build tag from test ([#2350](https://github.com/rudderlabs/rudder-server/issues/2350)) ([cf6d629](https://github.com/rudderlabs/rudder-server/commit/cf6d629da762ccbee8b75bdc2a067bc8a901d463))
* remove build tags from tests ([#2346](https://github.com/rudderlabs/rudder-server/issues/2346)) ([16cc643](https://github.com/rudderlabs/rudder-server/commit/16cc6433d20e0966d5f3c303ee3f58ad1649af92))
* skip v1.0.1 release ([d65658d](https://github.com/rudderlabs/rudder-server/commit/d65658d9f35c8ce57f885cf5691e1b7e3a1e14f9))
* upgrade actions runner to ubuntu 20.04 ([#2326](https://github.com/rudderlabs/rudder-server/issues/2326)) ([8c970f6](https://github.com/rudderlabs/rudder-server/commit/8c970f6dce64490cdb329c53fc4505309e36b6bf))
* upgrade GO version to 1.18 ([#2328](https://github.com/rudderlabs/rudder-server/issues/2328)) ([edde758](https://github.com/rudderlabs/rudder-server/commit/edde7588af2880cf85c010e7848f4cffe56999da))
* **warehouse:** update errorMap for deltalake partition query ([#2334](https://github.com/rudderlabs/rudder-server/issues/2334)) ([e011726](https://github.com/rudderlabs/rudder-server/commit/e0117268aa350d05b7a8fbadce444340ae79cebf))

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
