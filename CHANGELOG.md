# Changelog

## [1.15.0-test](https://github.com/rudderlabs/rudder-server/compare/v1.14.0...v1.15.0-test) (2023-10-10)


### Features

* add gzip bodyFormat support ([#3904](https://github.com/rudderlabs/rudder-server/issues/3904)) ([b050450](https://github.com/rudderlabs/rudder-server/commit/b050450cd02202f7d27c58d267f9c53b3bb64657))
* added flags for event audit ([#3859](https://github.com/rudderlabs/rudder-server/issues/3859)) ([890ca68](https://github.com/rudderlabs/rudder-server/commit/890ca68ea983e436e08a9f11afc1bd89ce7d428e))
* filter events support ([#3882](https://github.com/rudderlabs/rudder-server/issues/3882)) ([7ead8a9](https://github.com/rudderlabs/rudder-server/commit/7ead8a9e488033dab863554a9e0f8dedfe53ed86))
* health dashboard ([#3906](https://github.com/rudderlabs/rudder-server/issues/3906)) ([dbd4ea6](https://github.com/rudderlabs/rudder-server/commit/dbd4ea6d1928fb4c9786ad6caef14620a7907562))


### Bug Fixes

* access_denied error handling for OAuth destinations ([#3853](https://github.com/rudderlabs/rudder-server/issues/3853)) ([0d30d3b](https://github.com/rudderlabs/rudder-server/commit/0d30d3b37fa1bef9a7cd8d11fd2deb9f481c3a51))
* change jobsdb pathPrefix config ([a747653](https://github.com/rudderlabs/rudder-server/commit/a7476537817a636da828cb02ce6e13356ab0c06d))
* event schema versions without a sample event ([#3913](https://github.com/rudderlabs/rudder-server/issues/3913)) ([ea92a2e](https://github.com/rudderlabs/rudder-server/commit/ea92a2e604abb1978f9c2b0ed96ac4b55855d4af))
* googlecloudfunction destination implementation and refactor code ([#3907](https://github.com/rudderlabs/rudder-server/issues/3907)) ([44f5c23](https://github.com/rudderlabs/rudder-server/commit/44f5c238aea21902beb2a23bae17455bf23bf1a4))
* increasing default marketo bulk upload timeout ([#3940](https://github.com/rudderlabs/rudder-server/issues/3940)) ([bd9166b](https://github.com/rudderlabs/rudder-server/commit/bd9166b55a6310249c2a1c78a3040f05e284992d))
* increasing default marketo upload timeout ([bd9166b](https://github.com/rudderlabs/rudder-server/commit/bd9166b55a6310249c2a1c78a3040f05e284992d))
* invalid JobsDB.backup.pathPrefix configuration ([#3921](https://github.com/rudderlabs/rudder-server/issues/3921)) ([a747653](https://github.com/rudderlabs/rudder-server/commit/a7476537817a636da828cb02ce6e13356ab0c06d))
* replay request fails with 400 - request neither has anonymousId nor userId ([#3911](https://github.com/rudderlabs/rudder-server/issues/3911)) ([85adbbf](https://github.com/rudderlabs/rudder-server/commit/85adbbf586ef4aefb617ba9b428e304fe1d4458b))
* router transformer client fails with error connect: cannot assign requested address ([#3898](https://github.com/rudderlabs/rudder-server/issues/3898)) ([f6c51b7](https://github.com/rudderlabs/rudder-server/commit/f6c51b7995d34368075f239ae0603a98e7842b19))
* rsources dropped jobs at processor ([#3905](https://github.com/rudderlabs/rudder-server/issues/3905)) ([1c4fc5e](https://github.com/rudderlabs/rudder-server/commit/1c4fc5e9ab96f14785d5cb36f70c7f6a5f45250f))
* skip snakecasing for namespace if skipNamespaceSnakeCasing is set to true ([#3932](https://github.com/rudderlabs/rudder-server/issues/3932)) ([6da163b](https://github.com/rudderlabs/rudder-server/commit/6da163be3d0d777f07c18154085a6bd9cc386af4))
* test ssh ([#3915](https://github.com/rudderlabs/rudder-server/issues/3915)) ([36cdb44](https://github.com/rudderlabs/rudder-server/commit/36cdb44a64d72087f868ede95bcb4718e5e1c3da))
* use dedup on new records for deltalake ([#3927](https://github.com/rudderlabs/rudder-server/issues/3927)) ([9192975](https://github.com/rudderlabs/rudder-server/commit/91929753bec137f5b234f322e7dbce3cfea96c79))
* wh query types ([#3951](https://github.com/rudderlabs/rudder-server/issues/3951)) ([1e415e0](https://github.com/rudderlabs/rudder-server/commit/1e415e004e5c2b63560e9dc7b58290ae54076f14))


### Miscellaneous

* add rsources stats for dropped events at processor ([#3852](https://github.com/rudderlabs/rudder-server/issues/3852)) ([f5b8e7b](https://github.com/rudderlabs/rudder-server/commit/f5b8e7b4976945207ec0c011ed6d7fe30e1eb323))
* adding missing query types ([#3917](https://github.com/rudderlabs/rudder-server/issues/3917)) ([be24be3](https://github.com/rudderlabs/rudder-server/commit/be24be3251970d863f798fc1fcceb39abff44d24))
* app for warehouse ([#3862](https://github.com/rudderlabs/rudder-server/issues/3862)) ([5c9789f](https://github.com/rudderlabs/rudder-server/commit/5c9789fd5c5afb8e0a149cb96a90c8f351abb31f))
* applying 1.14.3 hotfixes to main branch ([#3926](https://github.com/rudderlabs/rudder-server/issues/3926)) ([e08decb](https://github.com/rudderlabs/rudder-server/commit/e08decba7cb101f10b94df2517f58c00e4c9fee3))
* backmerge 1.14.5 to master ([#3935](https://github.com/rudderlabs/rudder-server/issues/3935)) ([c7a485f](https://github.com/rudderlabs/rudder-server/commit/c7a485f0ef672919a20699d3d6c5bd7fe8de24d9))
* cleanup JobsDB.useSingleGetJobsQuery config option ([#3893](https://github.com/rudderlabs/rudder-server/issues/3893)) ([be0a62e](https://github.com/rudderlabs/rudder-server/commit/be0a62e40b9a09bc922b22fb65c01fb52cac4555))
* cleanup notifier ([#3867](https://github.com/rudderlabs/rudder-server/issues/3867)) ([98498ee](https://github.com/rudderlabs/rudder-server/commit/98498eed972a79016c9e531fc29abb9fa654ba6f))
* different router retry limits for sources jobs ([#3944](https://github.com/rudderlabs/rudder-server/issues/3944)) ([f13dbc3](https://github.com/rudderlabs/rudder-server/commit/f13dbc367225d89e249430dd7afa2054fecaecc2))
* empty batch payload ([#3947](https://github.com/rudderlabs/rudder-server/issues/3947)) ([1e20655](https://github.com/rudderlabs/rudder-server/commit/1e2065512ffe591cb9a22975319bb3e3b3da9c28))
* enable user deletion for supported destinations ([#3946](https://github.com/rudderlabs/rudder-server/issues/3946)) ([6ceb4e5](https://github.com/rudderlabs/rudder-server/commit/6ceb4e500914d29fbf7cca093fb711bb02ed4c1e))
* fix archiver test - inconsistent number of files ([#3902](https://github.com/rudderlabs/rudder-server/issues/3902)) ([01a0737](https://github.com/rudderlabs/rudder-server/commit/01a07370e23f28416eb0463a8d5870416f47b3ae))
* introduce load table total rows ([#3851](https://github.com/rudderlabs/rudder-server/issues/3851)) ([1fcabdd](https://github.com/rudderlabs/rudder-server/commit/1fcabddf9624fc12c5e3f1426320e0703856cc61))
* moving uploadSchema into UploadJob ([#3888](https://github.com/rudderlabs/rudder-server/issues/3888)) ([2a5547f](https://github.com/rudderlabs/rudder-server/commit/2a5547f8045ca4d4a0efcaeafaddda976cfe7bde))
* omit failedMessages from reporting json object ([#3936](https://github.com/rudderlabs/rudder-server/issues/3936)) ([0969b31](https://github.com/rudderlabs/rudder-server/commit/0969b31f75477969aa002267925ef98a75534850))
* populate error category ([#3949](https://github.com/rudderlabs/rudder-server/issues/3949)) ([37bbbbe](https://github.com/rudderlabs/rudder-server/commit/37bbbbea1faaba059269910d94ed912c65a4241c))
* reporting feature ([#3912](https://github.com/rudderlabs/rudder-server/issues/3912)) ([37b9cbc](https://github.com/rudderlabs/rudder-server/commit/37b9cbc13fe5ccc8dc4ba80edcc0302ef72ec2ff))
* rsources tests - postgres resources use different creds ([#3919](https://github.com/rudderlabs/rudder-server/issues/3919)) ([7ca721c](https://github.com/rudderlabs/rudder-server/commit/7ca721c1041d366471d97a9c357d200c776aea3a))
* sampling errors if no message in the batch succeeds ([#3918](https://github.com/rudderlabs/rudder-server/issues/3918)) ([69be777](https://github.com/rudderlabs/rudder-server/commit/69be7771069cb57e212a9b309ea20a7676c0574c))
* split warehouse package ([#3937](https://github.com/rudderlabs/rudder-server/issues/3937)) ([c94c953](https://github.com/rudderlabs/rudder-server/commit/c94c953ff672222248c07a4523f8eb0e90cf9676))
* support replay with new file format ([#3834](https://github.com/rudderlabs/rudder-server/issues/3834)) ([5c76185](https://github.com/rudderlabs/rudder-server/commit/5c7618595f03243e14fb2748926661d8d433e954))
* upgrade go mod replace versions ([#3880](https://github.com/rudderlabs/rudder-server/issues/3880)) ([3c3e407](https://github.com/rudderlabs/rudder-server/commit/3c3e407dba4234f528273662cc2e2e6007a47f8e))
* upgrade go mod replace versions ([#3880](https://github.com/rudderlabs/rudder-server/issues/3880)) ([5c3aefb](https://github.com/rudderlabs/rudder-server/commit/5c3aefbc9956f30bb62f5e79ca55072abf63475a))
* use common PAT instead of rudder-server-bot ([#3952](https://github.com/rudderlabs/rudder-server/issues/3952)) ([08d036e](https://github.com/rudderlabs/rudder-server/commit/08d036eed9f21dbae64169b555d6d7e4d350d2f4))
* use memoized payloadFunc for dedup ([#3780](https://github.com/rudderlabs/rudder-server/issues/3780)) ([74f5f11](https://github.com/rudderlabs/rudder-server/commit/74f5f11db024c9cdd40f09d78fd783924c54be55))
* use new reloadable config api for warehouse ([#3920](https://github.com/rudderlabs/rudder-server/issues/3920)) ([6f2b7b9](https://github.com/rudderlabs/rudder-server/commit/6f2b7b9a18225bce7c0837a137c9819302eda1fe))
* use publicly available transformer docker image ([#3916](https://github.com/rudderlabs/rudder-server/issues/3916)) ([5f3820f](https://github.com/rudderlabs/rudder-server/commit/5f3820f98058410ad3d62166a263863863746e95))
* use the new reloadable config api in reporting ([#3909](https://github.com/rudderlabs/rudder-server/issues/3909)) ([741fd74](https://github.com/rudderlabs/rudder-server/commit/741fd74b57ee104b9b0f7c314e034a965bd19684))
* warehouse schema race and cleanup ([#3900](https://github.com/rudderlabs/rudder-server/issues/3900)) ([61883be](https://github.com/rudderlabs/rudder-server/commit/61883bed00204c543153e75fbfda553142100651))

## [1.14.6](https://github.com/rudderlabs/rudder-server/compare/v1.14.5...v1.14.6) (2023-10-04)


### Bug Fixes

* skip snakecasing for namespace if skipNamespaceSnakeCasing is set to true ([#3932](https://github.com/rudderlabs/rudder-server/issues/3932)) ([8b3cb32](https://github.com/rudderlabs/rudder-server/commit/8b3cb3282b002908da15b998ab6cc01ed44e6241))

## [1.14.5](https://github.com/rudderlabs/rudder-server/compare/v1.14.4...v1.14.5) (2023-10-03)


### Miscellaneous

* fix backup prefix ([#3933](https://github.com/rudderlabs/rudder-server/issues/3933)) ([206e20e](https://github.com/rudderlabs/rudder-server/commit/206e20ee679c51dee7fca87445ee55b3ea04b424))

## [1.14.4](https://github.com/rudderlabs/rudder-server/compare/v1.14.3...v1.14.4) (2023-09-29)


### Bug Fixes

* use dedup on new records for deltalake ([#3927](https://github.com/rudderlabs/rudder-server/issues/3927)) ([5656ab3](https://github.com/rudderlabs/rudder-server/commit/5656ab3f68dd58e332b68d45f21d1965594ef479))

## [1.14.3](https://github.com/rudderlabs/rudder-server/compare/v1.14.2...v1.14.3) (2023-09-28)


### Miscellaneous

* added flags for event audit ([#3924](https://github.com/rudderlabs/rudder-server/issues/3924)) ([6908b0a](https://github.com/rudderlabs/rudder-server/commit/6908b0a03df8c59fd1c22c312a825cb62abdab3d))

## [1.14.2](https://github.com/rudderlabs/rudder-server/compare/v1.14.1...v1.14.2) (2023-09-25)


### Bug Fixes

* googlecloudfunction destination implementation and refactor code ([#3907](https://github.com/rudderlabs/rudder-server/issues/3907)) ([0676b0a](https://github.com/rudderlabs/rudder-server/commit/0676b0a70e26d70e1fcb4aeb7fc34c3a258b23ef))

## [1.14.1](https://github.com/rudderlabs/rudder-server/compare/v1.14.0...v1.14.1) (2023-09-21)


### Bug Fixes

* router transformer client fails with error connect: cannot assign requested address ([#3898](https://github.com/rudderlabs/rudder-server/issues/3898)) ([1cb5d5d](https://github.com/rudderlabs/rudder-server/commit/1cb5d5d725c8c8b84a66129e6d05a02ace9ff2c6))

## [1.14.0](https://github.com/rudderlabs/rudder-server/compare/v1.13.0...v1.14.0) (2023-09-20)


### Features

* onboard bulk upload destination eloqua   ([#3779](https://github.com/rudderlabs/rudder-server/issues/3779)) ([0a9954d](https://github.com/rudderlabs/rudder-server/commit/0a9954dee1163eadbea206a08ff80623217a5e4a))
* onboard destination google cloud function ([#3794](https://github.com/rudderlabs/rudder-server/issues/3794)) ([3b616a5](https://github.com/rudderlabs/rudder-server/commit/3b616a5f36478dd98d284de2072a6205cf2550da))


### Bug Fixes

* detected illegal job sequence during barrier enter/wait ([#3881](https://github.com/rudderlabs/rudder-server/issues/3881)) ([7891da3](https://github.com/rudderlabs/rudder-server/commit/7891da36bb62284ecbe5080ee798940b8f064f69))
* error message for deltalake during test connection ([#3883](https://github.com/rudderlabs/rudder-server/issues/3883)) ([1fa2f45](https://github.com/rudderlabs/rudder-server/commit/1fa2f45bf26981bff5552cb9c775f81e5ada7629))
* error while cleaning up old jobs: sql: no rows in result set ([#3850](https://github.com/rudderlabs/rudder-server/issues/3850)) ([1f87a26](https://github.com/rudderlabs/rudder-server/commit/1f87a261bd616a1961c8ed95f172be3d38f18eac))
* jobsdb ds max table size ([#3887](https://github.com/rudderlabs/rudder-server/issues/3887)) ([38e604a](https://github.com/rudderlabs/rudder-server/commit/38e604a01429551edee4dcb19ca34edafa059414))
* redis cluster mode ([#3831](https://github.com/rudderlabs/rudder-server/issues/3831)) ([3b90266](https://github.com/rudderlabs/rudder-server/commit/3b90266778dc99430823e7ef4c8fcf692c90f8ba))
* remove table name for load file ([#3837](https://github.com/rudderlabs/rudder-server/issues/3837)) ([3b90266](https://github.com/rudderlabs/rudder-server/commit/3b90266778dc99430823e7ef4c8fcf692c90f8ba))
* sample duplicate messagesids for snowflake ([#3884](https://github.com/rudderlabs/rudder-server/issues/3884)) ([b06dc36](https://github.com/rudderlabs/rudder-server/commit/b06dc3627b9749a8cb81845a1ca411a82bb2adf5))
* send reportedAt in milliseconds for error reporting ([#3857](https://github.com/rudderlabs/rudder-server/issues/3857)) ([7ec96c9](https://github.com/rudderlabs/rudder-server/commit/7ec96c96fa06d7a974f6b13786f65a6f4683a364))
* update error processing of eloqua ([#3877](https://github.com/rudderlabs/rudder-server/issues/3877)) ([c22f22e](https://github.com/rudderlabs/rudder-server/commit/c22f22e3a762a8f8cce15d92773fae3968d64a63))


### Miscellaneous

* add new logo ([#3865](https://github.com/rudderlabs/rudder-server/issues/3865)) ([7f290f4](https://github.com/rudderlabs/rudder-server/commit/7f290f41050efab19cb74ada29af264830b76155))
* add rudder-cli source and rm binaries ([#3872](https://github.com/rudderlabs/rudder-server/issues/3872)) ([c7ab148](https://github.com/rudderlabs/rudder-server/commit/c7ab148dfc20654d3ae751d0e4a7dec00abaea72))
* add stats for archiver ([#3836](https://github.com/rudderlabs/rudder-server/issues/3836)) ([3b90266](https://github.com/rudderlabs/rudder-server/commit/3b90266778dc99430823e7ef4c8fcf692c90f8ba))
* alerting on reports pileup  ([#3755](https://github.com/rudderlabs/rudder-server/issues/3755)) ([4bf9ce7](https://github.com/rudderlabs/rudder-server/commit/4bf9ce792a4bebde447be8772469a315420d9448))
* **batchrouter:** decouple upload frequency from main loop frequency ([#3889](https://github.com/rudderlabs/rudder-server/issues/3889)) ([e188b94](https://github.com/rudderlabs/rudder-server/commit/e188b9497a515fe133d51f10a7308d44fa9621c4))
* **batchrouter:** honour upload frequency when limitsReached if destination is failing ([#3874](https://github.com/rudderlabs/rudder-server/issues/3874)) ([ae989cd](https://github.com/rudderlabs/rudder-server/commit/ae989cd3db3c39b08197a9238611aaf6382123ba))
* better log message ([#3878](https://github.com/rudderlabs/rudder-server/issues/3878)) ([fcd6676](https://github.com/rudderlabs/rudder-server/commit/fcd667606dafc10e9b8c1fb40f2a7f2ee1600035))
* bump rudderlabs/rudder-go-kit from 0.15.7 to 0.15.8 ([#3863](https://github.com/rudderlabs/rudder-server/issues/3863)) ([785577d](https://github.com/rudderlabs/rudder-server/commit/785577dac7b6897e97c5aeff60d9a18eb13b4a33))
* check list permissions for replay ([#3873](https://github.com/rudderlabs/rudder-server/issues/3873)) ([fddbc0d](https://github.com/rudderlabs/rudder-server/commit/fddbc0dbd59c5005bfc968ccf883d2c1420988a0))
* cleanup grpc ([#3814](https://github.com/rudderlabs/rudder-server/issues/3814)) ([594828e](https://github.com/rudderlabs/rudder-server/commit/594828e8618df0cedff982ffaae36435afa50b9d))
* **deps:** bump github.com/aws/aws-sdk-go from 1.45.1 to 1.45.3 ([#3842](https://github.com/rudderlabs/rudder-server/issues/3842)) ([3b31818](https://github.com/rudderlabs/rudder-server/commit/3b31818b9d01ecfaab13b18cda41678f66455a1f))
* **deps:** bump github.com/rs/cors from 1.9.0 to 1.10.0 ([#3840](https://github.com/rudderlabs/rudder-server/issues/3840)) ([c1969e1](https://github.com/rudderlabs/rudder-server/commit/c1969e1c409a7a04071dc99f1167e81a46fac206))
* **deps:** bump github.com/rudderlabs/sql-tunnels from 0.1.4 to 0.1.5 ([#3841](https://github.com/rudderlabs/rudder-server/issues/3841)) ([a7136c5](https://github.com/rudderlabs/rudder-server/commit/a7136c535d25e0a7a0e31d5fe4986534c9dc5f86))
* **deps:** bump golang.org/x/oauth2 from 0.11.0 to 0.12.0 ([#3839](https://github.com/rudderlabs/rudder-server/issues/3839)) ([c4c49fe](https://github.com/rudderlabs/rudder-server/commit/c4c49fef80ee4016aab0f6e3d99684d822846b16))
* don't create custom destination clients for disabled destination ([#3885](https://github.com/rudderlabs/rudder-server/issues/3885)) ([aa07262](https://github.com/rudderlabs/rudder-server/commit/aa072627798bbc24bed1194b9de6d0c788821e8f))
* go-kit v0.15.9 ([#3864](https://github.com/rudderlabs/rudder-server/issues/3864)) ([60749af](https://github.com/rudderlabs/rudder-server/commit/60749afc88d4db59674f84d194de45d3eb8e8467))
* merge release 1.13.0 in main branch ([#3833](https://github.com/rudderlabs/rudder-server/issues/3833)) ([0e5a477](https://github.com/rudderlabs/rudder-server/commit/0e5a4770576b7631e2bc4b79056ce1850019b47a))
* new reloadable config in jobsdb ([#3868](https://github.com/rudderlabs/rudder-server/issues/3868)) ([2c655f9](https://github.com/rudderlabs/rudder-server/commit/2c655f9c1d6928da00cabaa5eebc36225be8dfb2))
* update joborderlog function ([#3848](https://github.com/rudderlabs/rudder-server/issues/3848)) ([3bfe354](https://github.com/rudderlabs/rudder-server/commit/3bfe354ae970680cfd78a5e4f8be9ca914156ffc))
* upgrade compose test ([#3847](https://github.com/rudderlabs/rudder-server/issues/3847)) ([3a34e06](https://github.com/rudderlabs/rudder-server/commit/3a34e066c2b48505f193ef78f411f48186d110a5))
* upgrade go version 1.21 ([#3838](https://github.com/rudderlabs/rudder-server/issues/3838)) ([a1075da](https://github.com/rudderlabs/rudder-server/commit/a1075da9f3db51a47fc6962b9249fff3300f3f88))
* uploader can append ([#3805](https://github.com/rudderlabs/rudder-server/issues/3805)) ([56ab090](https://github.com/rudderlabs/rudder-server/commit/56ab090aecca5d4b54e8d0490fea343932b633df))
* use new reloadable config api in processor and gateway ([#3875](https://github.com/rudderlabs/rudder-server/issues/3875)) ([b0576d6](https://github.com/rudderlabs/rudder-server/commit/b0576d66ebadf4fdb0dd9db9450e26eb59d1ee45))
* use new reloadable config api in router and batchrouter ([#3871](https://github.com/rudderlabs/rudder-server/issues/3871)) ([12f4d83](https://github.com/rudderlabs/rudder-server/commit/12f4d832f1d09d5ced9495b9057a702facff9681))
* use single query for getting jobs ([#3820](https://github.com/rudderlabs/rudder-server/issues/3820)) ([2aff3b2](https://github.com/rudderlabs/rudder-server/commit/2aff3b238a8f82e19a005b2f54056736637686e5))
* use structured logging in jobsdb backup ([#3786](https://github.com/rudderlabs/rudder-server/issues/3786)) ([d254bc2](https://github.com/rudderlabs/rudder-server/commit/d254bc2228df7258147349cde22dda2d6a9fb269))

## [1.13.2](https://github.com/rudderlabs/rudder-server/compare/v1.13.1...v1.13.2) (2023-09-12)


### Bug Fixes

* send reportedAt for error-reporting in milliseconds ([29ea342](https://github.com/rudderlabs/rudder-server/commit/29ea342939951cd39e14c5e921f7b4e23601b99c))
* send reportedAt in milliseconds for error reporting ([#3857](https://github.com/rudderlabs/rudder-server/issues/3857)) ([29ea342](https://github.com/rudderlabs/rudder-server/commit/29ea342939951cd39e14c5e921f7b4e23601b99c))


### Miscellaneous

* fix reporting tests ([b241a79](https://github.com/rudderlabs/rudder-server/commit/b241a79c487c3c83bd9c5d4038536f6418dda7f4))
* upgrade compose test ([#3847](https://github.com/rudderlabs/rudder-server/issues/3847)) ([8333c35](https://github.com/rudderlabs/rudder-server/commit/8333c353153f1fc9e378653abb41a6451f016a75))

## [1.13.1](https://github.com/rudderlabs/rudder-server/compare/v1.13.0...v1.13.1) (2023-09-06)


### Bug Fixes

* redis cluster mode ([#3831](https://github.com/rudderlabs/rudder-server/issues/3831)) ([3a5e974](https://github.com/rudderlabs/rudder-server/commit/3a5e97469f26ddec72eada47b0a460ff35d1c384))
* remove table name for load file ([#3837](https://github.com/rudderlabs/rudder-server/issues/3837)) ([54bf914](https://github.com/rudderlabs/rudder-server/commit/54bf9143053a2b10240bced430b9fd29510e8fb5))


### Miscellaneous

* add stats for archiver ([#3836](https://github.com/rudderlabs/rudder-server/issues/3836)) ([7b559a0](https://github.com/rudderlabs/rudder-server/commit/7b559a0c92924a95c2c7ebe16848655b3ea746fe))

## [1.13.0](https://github.com/rudderlabs/rudder-server/compare/v1.12.1...v1.13.0) (2023-09-05)


### Features

* archive jobs to object storage ([#3721](https://github.com/rudderlabs/rudder-server/issues/3721)) ([c65ded3](https://github.com/rudderlabs/rudder-server/commit/c65ded3fb0475ad1ad25d0b804bc784f8382ef69))
* snowflake appendmode ([#3745](https://github.com/rudderlabs/rudder-server/issues/3745)) ([2fc1389](https://github.com/rudderlabs/rudder-server/commit/2fc1389ee7ac32bcb028e720dee6abd17765f256))
* support `HSET` in redis ([#3768](https://github.com/rudderlabs/rudder-server/issues/3768)) ([fd2b341](https://github.com/rudderlabs/rudder-server/commit/fd2b3412295e2fbbbeef46e1d0bfcf66b13f5c32))


### Bug Fixes

* adding the poll failure response details to db ([#3826](https://github.com/rudderlabs/rudder-server/issues/3826)) ([f3d9d11](https://github.com/rudderlabs/rudder-server/commit/f3d9d11d4da3b7aad3137e091b27d76fc46abe4b))
* correct jobsdb table count stat ([#3808](https://github.com/rudderlabs/rudder-server/issues/3808)) ([ebc918e](https://github.com/rudderlabs/rudder-server/commit/ebc918efadb7e209790cc2973d416f416f101b53))
* do not delete staging files when opted for rudderstorge during archival ([#3801](https://github.com/rudderlabs/rudder-server/issues/3801)) ([3dff32d](https://github.com/rudderlabs/rudder-server/commit/3dff32d270a0ef5de53cdcf6a8b198390e760278))
* don't query db if archival is disabled, set parameters in jobStatus ([#3810](https://github.com/rudderlabs/rudder-server/issues/3810)) ([fdfb4f6](https://github.com/rudderlabs/rudder-server/commit/fdfb4f6547afbd4b62e2358914c7032681c9b65a))
* extend sql middleware for mssql, azure_synapse and clickhouse ([#3771](https://github.com/rudderlabs/rudder-server/issues/3771)) ([3bfc7e1](https://github.com/rudderlabs/rudder-server/commit/3bfc7e12889638aad6fc87a747303d98173aa88a))
* increased postgres cpu usage after switching to a single get jobs query ([#3812](https://github.com/rudderlabs/rudder-server/issues/3812)) ([e4a65f3](https://github.com/rudderlabs/rudder-server/commit/e4a65f38503675834175eafb78fb0836c65f49dd))
* marketo bulk upload edits ([#3778](https://github.com/rudderlabs/rudder-server/issues/3778)) ([94df125](https://github.com/rudderlabs/rudder-server/commit/94df12573175374c0795f70e860cae0ebcbd89a8))


### Miscellaneous

* add gw failure stats ([#3809](https://github.com/rudderlabs/rudder-server/issues/3809)) ([437b8d5](https://github.com/rudderlabs/rudder-server/commit/437b8d51ee0532c1c61647f1cf538acd8f4254b3))
* add support for logging duplicate messageIDs ([#3759](https://github.com/rudderlabs/rudder-server/issues/3759)) ([890ccb6](https://github.com/rudderlabs/rudder-server/commit/890ccb6224af0f4abc0bfbef639a4c671f0eaccf))
* change some worker pool logs to debug ([#3744](https://github.com/rudderlabs/rudder-server/issues/3744)) ([6c3fcca](https://github.com/rudderlabs/rudder-server/commit/6c3fcca04412895bc34f894ff6299501405524b2))
* cleanup http handlers ([#3767](https://github.com/rudderlabs/rudder-server/issues/3767)) ([5dacdcf](https://github.com/rudderlabs/rudder-server/commit/5dacdcfbad533a310f33bf47fb8abd494f1b16fe))
* cloud extract should always merge ([#3800](https://github.com/rudderlabs/rudder-server/issues/3800)) ([7d2446b](https://github.com/rudderlabs/rudder-server/commit/7d2446b921c08ef7ef044c40baaf2481c0441609))
* deltalake error message length ([#3766](https://github.com/rudderlabs/rudder-server/issues/3766)) ([3bfc7e1](https://github.com/rudderlabs/rudder-server/commit/3bfc7e12889638aad6fc87a747303d98173aa88a))
* **deps:** bump cloud.google.com/go/bigquery from 1.53.0 to 1.54.0 ([#3763](https://github.com/rudderlabs/rudder-server/issues/3763)) ([0612cc1](https://github.com/rudderlabs/rudder-server/commit/0612cc1a3466fd61ded52663019539c178dfff6e))
* **deps:** bump cloud.google.com/go/storage from 1.31.0 to 1.32.0 ([#3754](https://github.com/rudderlabs/rudder-server/issues/3754)) ([8ab2ac2](https://github.com/rudderlabs/rudder-server/commit/8ab2ac272b02b2deeb39e60a72303479e6db6628))
* **deps:** bump github.com/aws/aws-sdk-go from 1.44.319 to 1.44.323 ([#3748](https://github.com/rudderlabs/rudder-server/issues/3748)) ([7d10800](https://github.com/rudderlabs/rudder-server/commit/7d10800e9be8108fdcd2b96064a2b2e63d75de69))
* **deps:** bump github.com/aws/aws-sdk-go from 1.44.323 to 1.44.324 ([#3753](https://github.com/rudderlabs/rudder-server/issues/3753)) ([a3f28c7](https://github.com/rudderlabs/rudder-server/commit/a3f28c7a1187f045150ee675cc730583991dc76e))
* **deps:** bump github.com/aws/aws-sdk-go from 1.44.324 to 1.44.326 ([#3761](https://github.com/rudderlabs/rudder-server/issues/3761)) ([9fcead7](https://github.com/rudderlabs/rudder-server/commit/9fcead7c16394911b8f5fcfeb62edbfed8e47cad))
* **deps:** bump github.com/aws/aws-sdk-go from 1.44.326 to 1.44.327 ([#3769](https://github.com/rudderlabs/rudder-server/issues/3769)) ([930c914](https://github.com/rudderlabs/rudder-server/commit/930c91466bbbcdeedf9ba94a954343682cf598e4))
* **deps:** bump github.com/aws/aws-sdk-go from 1.44.327 to 1.44.328 ([#3774](https://github.com/rudderlabs/rudder-server/issues/3774)) ([901c69e](https://github.com/rudderlabs/rudder-server/commit/901c69e19e4a1383163a4155d01706181fc9d6fc))
* **deps:** bump github.com/aws/aws-sdk-go from 1.44.328 to 1.44.329 ([#3777](https://github.com/rudderlabs/rudder-server/issues/3777)) ([f2184ad](https://github.com/rudderlabs/rudder-server/commit/f2184adf3975d618c4ca48cefadbf05a401226f5))
* **deps:** bump github.com/aws/aws-sdk-go from 1.44.329 to 1.44.330 ([#3781](https://github.com/rudderlabs/rudder-server/issues/3781)) ([7da6d0a](https://github.com/rudderlabs/rudder-server/commit/7da6d0a08b3a34ac85e3d0a8f64bb4e0543ddb47))
* **deps:** bump github.com/aws/aws-sdk-go from 1.44.330 to 1.44.331 ([#3785](https://github.com/rudderlabs/rudder-server/issues/3785)) ([a7d7c74](https://github.com/rudderlabs/rudder-server/commit/a7d7c7474cc5bb0d16e07125a525de7c962f54cd))
* **deps:** bump github.com/aws/aws-sdk-go from 1.44.331 to 1.44.332 ([#3798](https://github.com/rudderlabs/rudder-server/issues/3798)) ([6abd76e](https://github.com/rudderlabs/rudder-server/commit/6abd76eba7be403cfdcd9cba365ae50250a0473e))
* **deps:** bump github.com/aws/aws-sdk-go from 1.44.332 to 1.45.1 ([#3818](https://github.com/rudderlabs/rudder-server/issues/3818)) ([96588bb](https://github.com/rudderlabs/rudder-server/commit/96588bb30bd9fd51ecb059dfd92feb6529d52c80))
* **deps:** bump github.com/google/uuid from 1.3.0 to 1.3.1 ([#3775](https://github.com/rudderlabs/rudder-server/issues/3775)) ([91889ad](https://github.com/rudderlabs/rudder-server/commit/91889adb790bb3de7be41cec1dd2659124875a03))
* **deps:** bump github.com/hashicorp/golang-lru/v2 from 2.0.5 to 2.0.6 ([#3789](https://github.com/rudderlabs/rudder-server/issues/3789)) ([4e5b6a7](https://github.com/rudderlabs/rudder-server/commit/4e5b6a7423695fa2d45cbfb0182392a76e78c5cc))
* **deps:** bump github.com/minio/minio-go/v7 from 7.0.61 to 7.0.62 ([#3762](https://github.com/rudderlabs/rudder-server/issues/3762)) ([417c981](https://github.com/rudderlabs/rudder-server/commit/417c981fa42fe082e47489dbd24f6b32227bd895))
* **deps:** bump github.com/minio/minio-go/v7 from 7.0.62 to 7.0.63 ([#3802](https://github.com/rudderlabs/rudder-server/issues/3802)) ([ae66f4f](https://github.com/rudderlabs/rudder-server/commit/ae66f4f795bf5987f4453bceba92226e70e503b7))
* **deps:** bump github.com/onsi/ginkgo/v2 from 2.11.0 to 2.12.0 ([#3783](https://github.com/rudderlabs/rudder-server/issues/3783)) ([0e382b4](https://github.com/rudderlabs/rudder-server/commit/0e382b4f9507ed1549c09e3ec87396953ef9b312))
* **deps:** bump github.com/rudderlabs/rudder-go-kit from 0.15.5 to 0.15.6 ([#3782](https://github.com/rudderlabs/rudder-server/issues/3782)) ([f5a7e6e](https://github.com/rudderlabs/rudder-server/commit/f5a7e6e741cc38822e6c33afcad7dc010edc5f46))
* **deps:** bump github.com/rudderlabs/sql-tunnels from 0.1.3 to 0.1.4 ([#3797](https://github.com/rudderlabs/rudder-server/issues/3797)) ([1083cfe](https://github.com/rudderlabs/rudder-server/commit/1083cfe8dabeae6422397b580bec07f467447649))
* **deps:** bump github.com/snowflakedb/gosnowflake from 1.6.23 to 1.6.24 ([#3776](https://github.com/rudderlabs/rudder-server/issues/3776)) ([ca74e38](https://github.com/rudderlabs/rudder-server/commit/ca74e3848fb48a02111220f21e97213a7291f93d))
* **deps:** bump github.com/spf13/cast from 1.5.0 to 1.5.1 ([#3770](https://github.com/rudderlabs/rudder-server/issues/3770)) ([fd30711](https://github.com/rudderlabs/rudder-server/commit/fd307118f75c7fe5b90621bca8ddc74051135295))
* **deps:** bump google.golang.org/api from 0.136.0 to 0.137.0 ([#3749](https://github.com/rudderlabs/rudder-server/issues/3749)) ([3aff568](https://github.com/rudderlabs/rudder-server/commit/3aff5687685c463c78f10f402d04684596d958fd))
* **deps:** bump google.golang.org/api from 0.137.0 to 0.138.0 ([#3760](https://github.com/rudderlabs/rudder-server/issues/3760)) ([862a0b9](https://github.com/rudderlabs/rudder-server/commit/862a0b92f67175f84f941094a9e3e9fb6a80b0a0))
* **gateway:** authentication middlewares and context information ([#3736](https://github.com/rudderlabs/rudder-server/issues/3736)) ([264c52a](https://github.com/rudderlabs/rudder-server/commit/264c52ae80d033c6e36345861d899175e18b3def))
* introduce encoding factory ([#3740](https://github.com/rudderlabs/rudder-server/issues/3740)) ([188b95c](https://github.com/rudderlabs/rudder-server/commit/188b95cd5aa199f9bd29085dabacd9ef48d87fe5))
* **jobsdb:** unify get processed and unprocessed queries ([#3742](https://github.com/rudderlabs/rudder-server/issues/3742)) ([66c7e2e](https://github.com/rudderlabs/rudder-server/commit/66c7e2efcba61ff285788c64edbb4a5405c2cd73))
* License update ([#3821](https://github.com/rudderlabs/rudder-server/issues/3821)) ([dd4dd3d](https://github.com/rudderlabs/rudder-server/commit/dd4dd3d3256d80b106f946b8e91588472e1213c9))
* option to pass *config.Config to jobsdb ([#3764](https://github.com/rudderlabs/rudder-server/issues/3764)) ([a04142e](https://github.com/rudderlabs/rudder-server/commit/a04142e65ca357c59a920d964b82c2ef7a1929ee))
* populate context with validation errors when context is nil ([#3815](https://github.com/rudderlabs/rudder-server/issues/3815)) ([dcb6a15](https://github.com/rudderlabs/rudder-server/commit/dcb6a15f1848390f1eb6cdca7f1c8a8a4f11e9fc))
* readme updates ([#3822](https://github.com/rudderlabs/rudder-server/issues/3822)) ([4c258a3](https://github.com/rudderlabs/rudder-server/commit/4c258a32d18756f5941ca9221a43c13163b9396c))
* remove archival for jobsdb journal tables ([#3758](https://github.com/rudderlabs/rudder-server/issues/3758)) ([ff80a1d](https://github.com/rudderlabs/rudder-server/commit/ff80a1d3bcfdce7cdb9508b936d6a07892e0e9f9))
* remove Init and exported struct variables from Archiver ([#3735](https://github.com/rudderlabs/rudder-server/issues/3735)) ([6c3fcca](https://github.com/rudderlabs/rudder-server/commit/6c3fcca04412895bc34f894ff6299501405524b2))
* replay internal endpoint ([#3746](https://github.com/rudderlabs/rudder-server/issues/3746)) ([cd7557f](https://github.com/rudderlabs/rudder-server/commit/cd7557f3e5f1001734a602754a136926fec29bb4))
* revert warehouse tests race detection ([#3825](https://github.com/rudderlabs/rudder-server/issues/3825)) ([d08198e](https://github.com/rudderlabs/rudder-server/commit/d08198e69a46cce09fa55b7f37d0cd2ed0ad4e27))
* router_response_counts stat now shows if it's a retry attempt ([#3799](https://github.com/rudderlabs/rudder-server/issues/3799)) ([ec07777](https://github.com/rudderlabs/rudder-server/commit/ec077773e15d2b97e43af7b7859a4f0849ac748f))
* **router:** provide more debug info when detecting an illegal job sequence ([#3787](https://github.com/rudderlabs/rudder-server/issues/3787)) ([9f1c5fb](https://github.com/rudderlabs/rudder-server/commit/9f1c5fb0b1bde26c4d7e242006794fbe096c8757))
* slave cleanup and introduce tests ([#3728](https://github.com/rudderlabs/rudder-server/issues/3728)) ([6c3fcca](https://github.com/rudderlabs/rudder-server/commit/6c3fcca04412895bc34f894ff6299501405524b2))
* update license to ELv2 ([#3823](https://github.com/rudderlabs/rudder-server/issues/3823)) ([4648ea3](https://github.com/rudderlabs/rudder-server/commit/4648ea315962a925bd017d43c5e4a779dd6a87ab))
* uploads cleanup ([#3743](https://github.com/rudderlabs/rudder-server/issues/3743)) ([ca52403](https://github.com/rudderlabs/rudder-server/commit/ca52403c0fd952d6256e2a6cf624ae11a3a05ebb))
* warehouse tests race detection ([#3773](https://github.com/rudderlabs/rudder-server/issues/3773)) ([ce2ed33](https://github.com/rudderlabs/rudder-server/commit/ce2ed332a45b3deafc7284e2629fb21a3f756aed))

## [1.12.2](https://github.com/rudderlabs/rudder-server/compare/v1.12.1...v1.12.2) (2023-08-22)


### Bug Fixes

* extend sql middleware for mssql, azure_synapse and clickhouse ([#3771](https://github.com/rudderlabs/rudder-server/issues/3771)) ([e8809bb](https://github.com/rudderlabs/rudder-server/commit/e8809bbbf123940548a3112529a7dbc0ca0125d7))


### Miscellaneous

* deltalake error message length ([#3766](https://github.com/rudderlabs/rudder-server/issues/3766)) ([fa09173](https://github.com/rudderlabs/rudder-server/commit/fa091738b2460c58fc8e575d381e74093ee46ba6))

## [1.12.1](https://github.com/rudderlabs/rudder-server/compare/v1.12.0...v1.12.1) (2023-08-16)


### Miscellaneous

* bingads safetypecast ([#3750](https://github.com/rudderlabs/rudder-server/issues/3750)) ([6204c3e](https://github.com/rudderlabs/rudder-server/commit/6204c3e3e8796a38e9c21906bcc088a1dd2e454f))

## [1.12.0](https://github.com/rudderlabs/rudder-server/compare/v1.11.2...v1.12.0) (2023-08-14)


### Features

* bing ads bulk upload ([#3371](https://github.com/rudderlabs/rudder-server/issues/3371)) ([574d0a5](https://github.com/rudderlabs/rudder-server/commit/574d0a52de66e71bc5756e435a5d1e9c9014ac28))
* parse replay configuration from backend config ([#3703](https://github.com/rudderlabs/rudder-server/issues/3703)) ([35f55e0](https://github.com/rudderlabs/rudder-server/commit/35f55e0537bd664756330906c19f07c16206a762))


### Bug Fixes

* add index on reported_at column of reports table ([#3606](https://github.com/rudderlabs/rudder-server/issues/3606)) ([4171517](https://github.com/rudderlabs/rudder-server/commit/4171517b8c896e033eb2267e631560a11e353ad4))
* cleanup for warehouse integration tests ([#3596](https://github.com/rudderlabs/rudder-server/issues/3596)) ([4171517](https://github.com/rudderlabs/rudder-server/commit/4171517b8c896e033eb2267e631560a11e353ad4))
* ignore workspace if no backup config is found ([#3685](https://github.com/rudderlabs/rudder-server/issues/3685)) ([dd3f42e](https://github.com/rudderlabs/rudder-server/commit/dd3f42e347813727e6a3f164d2a50bdfe0ca5595))
* illegal job sequence during barrier enter in router pickup ([#3737](https://github.com/rudderlabs/rudder-server/issues/3737)) ([5017146](https://github.com/rudderlabs/rudder-server/commit/5017146b3dd3f0bacd20db8e5e96c869ccb55b03))
* **jobsdb:** when running in embedded mode migration fails with column workspace_id does not exist ([#3714](https://github.com/rudderlabs/rudder-server/issues/3714)) ([817e97f](https://github.com/rudderlabs/rudder-server/commit/817e97ff18c3fe4b5eef06debaeaaab816b200ec))
* minio manager race ([#3672](https://github.com/rudderlabs/rudder-server/issues/3672)) ([54e3055](https://github.com/rudderlabs/rudder-server/commit/54e305591d0d64fb39a4499f3a1d9ddd3e603d16))
* redshift dedup pick latest entry ([#3603](https://github.com/rudderlabs/rudder-server/issues/3603)) ([4171517](https://github.com/rudderlabs/rudder-server/commit/4171517b8c896e033eb2267e631560a11e353ad4))
* send last error response for aborted jobs to reporting ([#3692](https://github.com/rudderlabs/rudder-server/issues/3692)) ([cfbeee9](https://github.com/rudderlabs/rudder-server/commit/cfbeee972c9fa7a78890c01b69fd585f0894d212))
* user transformations timeout ([#3583](https://github.com/rudderlabs/rudder-server/issues/3583)) ([4171517](https://github.com/rudderlabs/rudder-server/commit/4171517b8c896e033eb2267e631560a11e353ad4))
* warehouse stringmap configs are lower case w.r.t viper ([#3592](https://github.com/rudderlabs/rudder-server/issues/3592)) ([4171517](https://github.com/rudderlabs/rudder-server/commit/4171517b8c896e033eb2267e631560a11e353ad4))


### Miscellaneous

* add custom buckets to gateway.user_suppression_age ([#3679](https://github.com/rudderlabs/rudder-server/issues/3679)) ([bef1280](https://github.com/rudderlabs/rudder-server/commit/bef12808b5b32905157becd619cc9c83d40d5032))
* add max connections to warehouse and pgnotifier ([#3597](https://github.com/rudderlabs/rudder-server/issues/3597)) ([4171517](https://github.com/rudderlabs/rudder-server/commit/4171517b8c896e033eb2267e631560a11e353ad4))
* add provision to disable tracking event names from a source for reporting ([#3632](https://github.com/rudderlabs/rudder-server/issues/3632)) ([4171517](https://github.com/rudderlabs/rudder-server/commit/4171517b8c896e033eb2267e631560a11e353ad4))
* added trino and spark test ([#3525](https://github.com/rudderlabs/rudder-server/issues/3525)) ([4171517](https://github.com/rudderlabs/rudder-server/commit/4171517b8c896e033eb2267e631560a11e353ad4))
* addressing review coments from [#3602](https://github.com/rudderlabs/rudder-server/issues/3602) ([#3713](https://github.com/rudderlabs/rudder-server/issues/3713)) ([141d109](https://github.com/rudderlabs/rudder-server/commit/141d109a5b4e8f4eda51c20b2c3a867ba9348c7f))
* archiver cleanup ([#3726](https://github.com/rudderlabs/rudder-server/issues/3726)) ([e9b6a69](https://github.com/rudderlabs/rudder-server/commit/e9b6a69bc4513d7917c841a6c93b0401ab10be94))
* cleanup integrations package ([#3635](https://github.com/rudderlabs/rudder-server/issues/3635)) ([4171517](https://github.com/rudderlabs/rudder-server/commit/4171517b8c896e033eb2267e631560a11e353ad4))
* **deps:** bump cloud.google.com/go/bigquery from 1.52.0 to 1.53.0 ([#3671](https://github.com/rudderlabs/rudder-server/issues/3671)) ([8be90cb](https://github.com/rudderlabs/rudder-server/commit/8be90cb062be687044d73eae562309fc3a4fe77d))
* **deps:** bump cloud.google.com/go/pubsub from 1.32.0 to 1.33.0 ([#3668](https://github.com/rudderlabs/rudder-server/issues/3668)) ([c35d127](https://github.com/rudderlabs/rudder-server/commit/c35d127f5c071c2f95537d81a52c0dd15c3540a9))
* **deps:** bump cloud.google.com/go/storage from 1.30.1 to 1.31.0 ([#3625](https://github.com/rudderlabs/rudder-server/issues/3625)) ([4171517](https://github.com/rudderlabs/rudder-server/commit/4171517b8c896e033eb2267e631560a11e353ad4))
* **deps:** bump github.com/apache/pulsar-client-go from 0.10.0 to 0.11.0 ([#3622](https://github.com/rudderlabs/rudder-server/issues/3622)) ([4171517](https://github.com/rudderlabs/rudder-server/commit/4171517b8c896e033eb2267e631560a11e353ad4))
* **deps:** bump github.com/aws/aws-sdk-go from 1.44.295 to 1.44.299 ([#3615](https://github.com/rudderlabs/rudder-server/issues/3615)) ([4171517](https://github.com/rudderlabs/rudder-server/commit/4171517b8c896e033eb2267e631560a11e353ad4))
* **deps:** bump github.com/aws/aws-sdk-go from 1.44.299 to 1.44.300 ([#3636](https://github.com/rudderlabs/rudder-server/issues/3636)) ([4171517](https://github.com/rudderlabs/rudder-server/commit/4171517b8c896e033eb2267e631560a11e353ad4))
* **deps:** bump github.com/aws/aws-sdk-go from 1.44.300 to 1.44.301 ([#3640](https://github.com/rudderlabs/rudder-server/issues/3640)) ([4171517](https://github.com/rudderlabs/rudder-server/commit/4171517b8c896e033eb2267e631560a11e353ad4))
* **deps:** bump github.com/aws/aws-sdk-go from 1.44.301 to 1.44.302 ([#3646](https://github.com/rudderlabs/rudder-server/issues/3646)) ([4171517](https://github.com/rudderlabs/rudder-server/commit/4171517b8c896e033eb2267e631560a11e353ad4))
* **deps:** bump github.com/aws/aws-sdk-go from 1.44.301 to 1.44.302 ([#3646](https://github.com/rudderlabs/rudder-server/issues/3646)) ([b05d2d6](https://github.com/rudderlabs/rudder-server/commit/b05d2d64f7cc733b4d4af77d5ea33a018216d03d))
* **deps:** bump github.com/aws/aws-sdk-go from 1.44.302 to 1.44.304 ([#3650](https://github.com/rudderlabs/rudder-server/issues/3650)) ([4171517](https://github.com/rudderlabs/rudder-server/commit/4171517b8c896e033eb2267e631560a11e353ad4))
* **deps:** bump github.com/aws/aws-sdk-go from 1.44.302 to 1.44.304 ([#3650](https://github.com/rudderlabs/rudder-server/issues/3650)) ([edc9396](https://github.com/rudderlabs/rudder-server/commit/edc9396dd8ca65ae7b0ae1a7ac273e4e5dfe716b))
* **deps:** bump github.com/aws/aws-sdk-go from 1.44.304 to 1.44.305 ([#3654](https://github.com/rudderlabs/rudder-server/issues/3654)) ([4171517](https://github.com/rudderlabs/rudder-server/commit/4171517b8c896e033eb2267e631560a11e353ad4))
* **deps:** bump github.com/aws/aws-sdk-go from 1.44.304 to 1.44.305 ([#3654](https://github.com/rudderlabs/rudder-server/issues/3654)) ([366393e](https://github.com/rudderlabs/rudder-server/commit/366393e5bb42ae3437d88fa652ca31356748c590))
* **deps:** bump github.com/aws/aws-sdk-go from 1.44.305 to 1.44.306 ([#3663](https://github.com/rudderlabs/rudder-server/issues/3663)) ([f3cd10c](https://github.com/rudderlabs/rudder-server/commit/f3cd10cfc18c41704a04667df7c92a1b272d01ec))
* **deps:** bump github.com/aws/aws-sdk-go from 1.44.306 to 1.44.307 ([#3669](https://github.com/rudderlabs/rudder-server/issues/3669)) ([e179725](https://github.com/rudderlabs/rudder-server/commit/e179725f40c867e049b43c971cfd772f29036d29))
* **deps:** bump github.com/aws/aws-sdk-go from 1.44.307 to 1.44.312 ([#3686](https://github.com/rudderlabs/rudder-server/issues/3686)) ([a4edf6c](https://github.com/rudderlabs/rudder-server/commit/a4edf6ce548bf4308d44527c73ecd644577eb5fd))
* **deps:** bump github.com/aws/aws-sdk-go from 1.44.312 to 1.44.314 ([#3693](https://github.com/rudderlabs/rudder-server/issues/3693)) ([2bc503d](https://github.com/rudderlabs/rudder-server/commit/2bc503d0d0fc24afc9689d558411887adf5f5064))
* **deps:** bump github.com/aws/aws-sdk-go from 1.44.314 to 1.44.315 ([#3699](https://github.com/rudderlabs/rudder-server/issues/3699)) ([2467eab](https://github.com/rudderlabs/rudder-server/commit/2467eab3ace3410827a53785a0bdd34a503de37a))
* **deps:** bump github.com/aws/aws-sdk-go from 1.44.315 to 1.44.317 ([#3712](https://github.com/rudderlabs/rudder-server/issues/3712)) ([de33fc3](https://github.com/rudderlabs/rudder-server/commit/de33fc3dfc8fc7b79b5d4911442689e87985eba6))
* **deps:** bump github.com/aws/aws-sdk-go from 1.44.317 to 1.44.318 ([#3718](https://github.com/rudderlabs/rudder-server/issues/3718)) ([7719719](https://github.com/rudderlabs/rudder-server/commit/7719719125002d80f57fe2f1a2cafce6df97402e))
* **deps:** bump github.com/aws/aws-sdk-go from 1.44.318 to 1.44.319 ([#3722](https://github.com/rudderlabs/rudder-server/issues/3722)) ([06477fc](https://github.com/rudderlabs/rudder-server/commit/06477fcd508b10971be976faf4d44b2db30cfdcc))
* **deps:** bump github.com/confluentinc/confluent-kafka-go/v2 from 2.1.1 to 2.2.0 ([#3628](https://github.com/rudderlabs/rudder-server/issues/3628)) ([4171517](https://github.com/rudderlabs/rudder-server/commit/4171517b8c896e033eb2267e631560a11e353ad4))
* **deps:** bump github.com/databricks/databricks-sql-go from 1.3.1 to 1.4.0 ([#3734](https://github.com/rudderlabs/rudder-server/issues/3734)) ([c948122](https://github.com/rudderlabs/rudder-server/commit/c9481229b3b6fbf6dec6954c3367cadd1ea94dbb))
* **deps:** bump github.com/dgraph-io/badger/v4 from 4.1.0 to 4.2.0 ([#3711](https://github.com/rudderlabs/rudder-server/issues/3711)) ([6a1c49b](https://github.com/rudderlabs/rudder-server/commit/6a1c49bc9f6346cd9677ed265658bf1db462f914))
* **deps:** bump github.com/go-chi/chi/v5 from 5.0.8 to 5.0.10 ([#3637](https://github.com/rudderlabs/rudder-server/issues/3637)) ([4171517](https://github.com/rudderlabs/rudder-server/commit/4171517b8c896e033eb2267e631560a11e353ad4))
* **deps:** bump github.com/hashicorp/golang-lru/v2 from 2.0.4 to 2.0.5 ([#3725](https://github.com/rudderlabs/rudder-server/issues/3725)) ([5a36137](https://github.com/rudderlabs/rudder-server/commit/5a3613763b289b3dff17173579e4b35bf88c9a8b))
* **deps:** bump github.com/iancoleman/strcase from 0.2.0 to 0.3.0 ([#3626](https://github.com/rudderlabs/rudder-server/issues/3626)) ([4171517](https://github.com/rudderlabs/rudder-server/commit/4171517b8c896e033eb2267e631560a11e353ad4))
* **deps:** bump github.com/minio/minio-go/v7 from 7.0.59 to 7.0.60 ([#3621](https://github.com/rudderlabs/rudder-server/issues/3621)) ([4171517](https://github.com/rudderlabs/rudder-server/commit/4171517b8c896e033eb2267e631560a11e353ad4))
* **deps:** bump github.com/minio/minio-go/v7 from 7.0.60 to 7.0.61 ([#3639](https://github.com/rudderlabs/rudder-server/issues/3639)) ([4171517](https://github.com/rudderlabs/rudder-server/commit/4171517b8c896e033eb2267e631560a11e353ad4))
* **deps:** bump github.com/onsi/gomega from 1.27.8 to 1.27.9 ([#3664](https://github.com/rudderlabs/rudder-server/issues/3664)) ([197ee3f](https://github.com/rudderlabs/rudder-server/commit/197ee3f0ff493944e305ce1c39237be301405780))
* **deps:** bump github.com/onsi/gomega from 1.27.9 to 1.27.10 ([#3667](https://github.com/rudderlabs/rudder-server/issues/3667)) ([1c3cd4f](https://github.com/rudderlabs/rudder-server/commit/1c3cd4f4534e9ca6f4c4695722b96af4d48047a2))
* **deps:** bump github.com/rudderlabs/rudder-go-kit from 0.15.4 to 0.15.5 ([#3724](https://github.com/rudderlabs/rudder-server/issues/3724)) ([b199d1d](https://github.com/rudderlabs/rudder-server/commit/b199d1dcaffec04159dbcb0248b9e3bed9c6228f))
* **deps:** bump github.com/segmentio/kafka-go from 0.4.40 to 0.4.42 ([#3620](https://github.com/rudderlabs/rudder-server/issues/3620)) ([4171517](https://github.com/rudderlabs/rudder-server/commit/4171517b8c896e033eb2267e631560a11e353ad4))
* **deps:** bump github.com/snowflakedb/gosnowflake from 1.6.22 to 1.6.23 ([#3673](https://github.com/rudderlabs/rudder-server/issues/3673)) ([a22ea8c](https://github.com/rudderlabs/rudder-server/commit/a22ea8c2785f953b3e7372fd38e8152836b3d630))
* **deps:** bump github.com/tidwall/gjson from 1.14.4 to 1.15.0 ([#3681](https://github.com/rudderlabs/rudder-server/issues/3681)) ([b25173e](https://github.com/rudderlabs/rudder-server/commit/b25173e601386fa0b433b0e169f4ba5813102673))
* **deps:** bump github.com/tidwall/gjson from 1.15.0 to 1.16.0 ([#3732](https://github.com/rudderlabs/rudder-server/issues/3732)) ([48f1717](https://github.com/rudderlabs/rudder-server/commit/48f171702f2bb07cda818f40bb6e64ca03671b06))
* **deps:** bump go.uber.org/atomic from 1.10.0 to 1.11.0 ([#3587](https://github.com/rudderlabs/rudder-server/issues/3587)) ([4171517](https://github.com/rudderlabs/rudder-server/commit/4171517b8c896e033eb2267e631560a11e353ad4))
* **deps:** bump go.uber.org/automaxprocs from 1.5.2 to 1.5.3 ([#3651](https://github.com/rudderlabs/rudder-server/issues/3651)) ([4171517](https://github.com/rudderlabs/rudder-server/commit/4171517b8c896e033eb2267e631560a11e353ad4))
* **deps:** bump go.uber.org/automaxprocs from 1.5.2 to 1.5.3 ([#3651](https://github.com/rudderlabs/rudder-server/issues/3651)) ([f9a19f3](https://github.com/rudderlabs/rudder-server/commit/f9a19f31558eed57dcd031f7c675f0ead52a1c98))
* **deps:** bump golang.org/x/oauth2 from 0.10.0 to 0.11.0 ([#3710](https://github.com/rudderlabs/rudder-server/issues/3710)) ([92e1bf2](https://github.com/rudderlabs/rudder-server/commit/92e1bf25f7a4fc385eb3f9eed88b67701502c5c7))
* **deps:** bump golang.org/x/oauth2 from 0.9.0 to 0.10.0 ([#3585](https://github.com/rudderlabs/rudder-server/issues/3585)) ([4171517](https://github.com/rudderlabs/rudder-server/commit/4171517b8c896e033eb2267e631560a11e353ad4))
* **deps:** bump google.golang.org/api from 0.129.0 to 0.130.0 ([#3588](https://github.com/rudderlabs/rudder-server/issues/3588)) ([4171517](https://github.com/rudderlabs/rudder-server/commit/4171517b8c896e033eb2267e631560a11e353ad4))
* **deps:** bump google.golang.org/api from 0.130.0 to 0.131.0 ([#3627](https://github.com/rudderlabs/rudder-server/issues/3627)) ([4171517](https://github.com/rudderlabs/rudder-server/commit/4171517b8c896e033eb2267e631560a11e353ad4))
* **deps:** bump google.golang.org/api from 0.131.0 to 0.132.0 ([#3647](https://github.com/rudderlabs/rudder-server/issues/3647)) ([4171517](https://github.com/rudderlabs/rudder-server/commit/4171517b8c896e033eb2267e631560a11e353ad4))
* **deps:** bump google.golang.org/api from 0.131.0 to 0.132.0 ([#3647](https://github.com/rudderlabs/rudder-server/issues/3647)) ([ffb993e](https://github.com/rudderlabs/rudder-server/commit/ffb993e79e4de6aafe4ddc0a4c597545e0f856ad))
* **deps:** bump google.golang.org/api from 0.132.0 to 0.133.0 ([#3670](https://github.com/rudderlabs/rudder-server/issues/3670)) ([5c3a0d0](https://github.com/rudderlabs/rudder-server/commit/5c3a0d035278ad10314400f14970949f70af3730))
* **deps:** bump google.golang.org/api from 0.133.0 to 0.134.0 ([#3677](https://github.com/rudderlabs/rudder-server/issues/3677)) ([06b1e7b](https://github.com/rudderlabs/rudder-server/commit/06b1e7b8fd33078b7406dcd0e86b43d65d101241))
* **deps:** bump google.golang.org/api from 0.134.0 to 0.135.0 ([#3717](https://github.com/rudderlabs/rudder-server/issues/3717)) ([bd329f8](https://github.com/rudderlabs/rudder-server/commit/bd329f868628ba218ea09452628609797faaa9ea))
* **deps:** bump google.golang.org/api from 0.135.0 to 0.136.0 ([#3723](https://github.com/rudderlabs/rudder-server/issues/3723)) ([e045ef3](https://github.com/rudderlabs/rudder-server/commit/e045ef38b7d38d5ee9af1590a4fcc72d1a759418))
* **deps:** bump google.golang.org/grpc from 1.56.1 to 1.56.2 ([#3599](https://github.com/rudderlabs/rudder-server/issues/3599)) ([4171517](https://github.com/rudderlabs/rudder-server/commit/4171517b8c896e033eb2267e631560a11e353ad4))
* **deps:** bump google.golang.org/grpc from 1.56.2 to 1.57.0 ([#3675](https://github.com/rudderlabs/rudder-server/issues/3675)) ([277767b](https://github.com/rudderlabs/rudder-server/commit/277767b1bb91cf2d9fab8d323f577a44b60df33b))
* drop failing backups after few retries ([#3695](https://github.com/rudderlabs/rudder-server/issues/3695)) ([c81c66f](https://github.com/rudderlabs/rudder-server/commit/c81c66f724c6fa360ea06fe349ce60200f11e6f1))
* gateway ([#3708](https://github.com/rudderlabs/rudder-server/issues/3708)) ([3cc17f3](https://github.com/rudderlabs/rudder-server/commit/3cc17f390d038d6c3f57ce2ec6a329fa1505c196))
* gateway backend config initialisation improvements ([#3688](https://github.com/rudderlabs/rudder-server/issues/3688)) ([3cca234](https://github.com/rudderlabs/rudder-server/commit/3cca234a9c9141aec4fef77fa156095e027c652c))
* increase transformer request timeouts ([#3701](https://github.com/rudderlabs/rudder-server/issues/3701)) ([998ce44](https://github.com/rudderlabs/rudder-server/commit/998ce44b37ab77729c774fcc6ece87118dea6c5d))
* introduce source category for deduplication ([#3730](https://github.com/rudderlabs/rudder-server/issues/3730)) ([00f994b](https://github.com/rudderlabs/rudder-server/commit/00f994b6a47399ba6ec4a0196de481b82577c56d))
* **jobsdb:** increase maintenance operation timeouts and make them hot-reloadable ([#3601](https://github.com/rudderlabs/rudder-server/issues/3601)) ([1c94c53](https://github.com/rudderlabs/rudder-server/commit/1c94c53975c29caef0492f016634aac52ff02919))
* move warehouse handle to router ([#3687](https://github.com/rudderlabs/rudder-server/issues/3687)) ([e227046](https://github.com/rudderlabs/rudder-server/commit/e227046b1dfae9d88e98a51da7db77b7d7b7a662))
* remove google cloud storage dependency for gcs datalake test using fake-gcs-server ([#3576](https://github.com/rudderlabs/rudder-server/issues/3576)) ([4171517](https://github.com/rudderlabs/rudder-server/commit/4171517b8c896e033eb2267e631560a11e353ad4))
* remove notion artefacts ([#3694](https://github.com/rudderlabs/rudder-server/issues/3694)) ([fbd99c7](https://github.com/rudderlabs/rudder-server/commit/fbd99c75528c69758fc3b2d5de64b0a80b508a1e))
* remove unused ginkgo suites ([#3662](https://github.com/rudderlabs/rudder-server/issues/3662)) ([66a863a](https://github.com/rudderlabs/rudder-server/commit/66a863acc9e94329e18e5a4da1dbf4371f76bdc2))
* standardize warehouse timeout config name ([#3553](https://github.com/rudderlabs/rudder-server/issues/3553)) ([1cdc6d1](https://github.com/rudderlabs/rudder-server/commit/1cdc6d12d8311c62bf41c3e5b4246ac87eb39844))
* warehouse backend config refactoring ([#3602](https://github.com/rudderlabs/rudder-server/issues/3602)) ([e48c98e](https://github.com/rudderlabs/rudder-server/commit/e48c98ea939150be4ea46e214d4703a183eed6fa))
* warehouse sql stats ([#3638](https://github.com/rudderlabs/rudder-server/issues/3638)) ([4171517](https://github.com/rudderlabs/rudder-server/commit/4171517b8c896e033eb2267e631560a11e353ad4))
* warehouse sql stats ([#3638](https://github.com/rudderlabs/rudder-server/issues/3638)) ([e20976d](https://github.com/rudderlabs/rudder-server/commit/e20976df8243a60ca64b02ea10f436986ef6064e))

## [1.11.4](https://github.com/rudderlabs/rudder-server/compare/v1.11.3...v1.11.4) (2023-08-08)


### Bug Fixes

* **jobsdb:** when running in embedded mode migration fails with column workspace_id does not exist ([#3714](https://github.com/rudderlabs/rudder-server/issues/3714)) ([744cfd6](https://github.com/rudderlabs/rudder-server/commit/744cfd6f7dc6f3ebf1d28a71fd2582b5b0ca1355))

## [1.11.3](https://github.com/rudderlabs/rudder-server/compare/v1.11.2...v1.11.3) (2023-07-28)


### Miscellaneous

* add custom buckets to gateway.user_suppression_age ([#3679](https://github.com/rudderlabs/rudder-server/issues/3679)) ([d1bc95c](https://github.com/rudderlabs/rudder-server/commit/d1bc95c519832e392ea5d87ee13261264c4c7ebf))

## [1.11.2](https://github.com/rudderlabs/rudder-server/compare/v1.11.1...v1.11.2) (2023-07-19)


### Miscellaneous

* fix stat table prefix ([#3644](https://github.com/rudderlabs/rudder-server/issues/3644)) ([204a577](https://github.com/rudderlabs/rudder-server/commit/204a577b343ac4bd95a5dd462893e759495ee482))

## [1.11.1](https://github.com/rudderlabs/rudder-server/compare/v1.11.0...v1.11.1) (2023-07-19)


### Bug Fixes

* **destination:** fix: add max-open connections & close deleted rows for reporting modules ([#3643](https://github.com/rudderlabs/rudder-server/issues/3643)) ([8e5a8c5](https://github.com/rudderlabs/rudder-server/commit/8e5a8c525b7d832d434117c8fbb6637727a90f06))


### Miscellaneous

* fix negative pending event count ([#3641](https://github.com/rudderlabs/rudder-server/issues/3641)) ([74d98fd](https://github.com/rudderlabs/rudder-server/commit/74d98fd6a1eccceb7f5ddcec095a311565d2f76b))

## [1.11.0](https://github.com/rudderlabs/rudder-server/compare/v1.10.0...v1.11.0) (2023-07-13)


### Features

* save webhook (/source transformation) failures to proc errors ([#3522](https://github.com/rudderlabs/rudder-server/issues/3522)) ([45a1802](https://github.com/rudderlabs/rudder-server/commit/45a18028c094906377928d8b58de74b4c741609f))
* send stats for the time taken for suppression ([#3542](https://github.com/rudderlabs/rudder-server/issues/3542)) ([45955e2](https://github.com/rudderlabs/rudder-server/commit/45955e2461881e5a1169af1fdc69a06a4e5de499))


### Bug Fixes

* jobsdb panics during recovery after backup failure(s) ([#3580](https://github.com/rudderlabs/rudder-server/issues/3580)) ([abd9c8c](https://github.com/rudderlabs/rudder-server/commit/abd9c8c4a3507cdf52051b8a10108a70cc271a3a))
* transformation does not retry indefinitely when control plane is down ([#3581](https://github.com/rudderlabs/rudder-server/issues/3581)) ([1c6fc24](https://github.com/rudderlabs/rudder-server/commit/1c6fc244a23616aa9d0b3c91937434186ad87a5c))


### Miscellaneous

* **deps:** bump cloud.google.com/go/bigquery from 1.51.2 to 1.52.0 ([#3556](https://github.com/rudderlabs/rudder-server/issues/3556)) ([2936c04](https://github.com/rudderlabs/rudder-server/commit/2936c048c2bb29207b6236678d90db8d9c2c91b4))
* **deps:** bump cloud.google.com/go/pubsub from 1.31.0 to 1.32.0 ([#3563](https://github.com/rudderlabs/rudder-server/issues/3563)) ([496aca7](https://github.com/rudderlabs/rudder-server/commit/496aca763cee6a37ca7f64bc03646b65c5c881a6))
* **deps:** bump github.com/aws/aws-sdk-go from 1.44.282 to 1.44.283 ([#3509](https://github.com/rudderlabs/rudder-server/issues/3509)) ([a69c422](https://github.com/rudderlabs/rudder-server/commit/a69c4223f733198ba0ca70795a8d1296465a8f0f))
* **deps:** bump github.com/aws/aws-sdk-go from 1.44.283 to 1.44.284 ([#3515](https://github.com/rudderlabs/rudder-server/issues/3515)) ([a69c422](https://github.com/rudderlabs/rudder-server/commit/a69c4223f733198ba0ca70795a8d1296465a8f0f))
* **deps:** bump github.com/aws/aws-sdk-go from 1.44.284 to 1.44.285 ([#3520](https://github.com/rudderlabs/rudder-server/issues/3520)) ([a69c422](https://github.com/rudderlabs/rudder-server/commit/a69c4223f733198ba0ca70795a8d1296465a8f0f))
* **deps:** bump github.com/aws/aws-sdk-go from 1.44.285 to 1.44.287 ([#3536](https://github.com/rudderlabs/rudder-server/issues/3536)) ([a69c422](https://github.com/rudderlabs/rudder-server/commit/a69c4223f733198ba0ca70795a8d1296465a8f0f))
* **deps:** bump github.com/aws/aws-sdk-go from 1.44.287 to 1.44.288 ([#3545](https://github.com/rudderlabs/rudder-server/issues/3545)) ([486a7e5](https://github.com/rudderlabs/rudder-server/commit/486a7e5a52d496997c0880440192048f16884ab5))
* **deps:** bump github.com/aws/aws-sdk-go from 1.44.288 to 1.44.289 ([#3550](https://github.com/rudderlabs/rudder-server/issues/3550)) ([4d1e3e6](https://github.com/rudderlabs/rudder-server/commit/4d1e3e6accf8ac9e3c239438fb60ef7a7ddf716c))
* **deps:** bump github.com/aws/aws-sdk-go from 1.44.289 to 1.44.290 ([#3555](https://github.com/rudderlabs/rudder-server/issues/3555)) ([0dbb328](https://github.com/rudderlabs/rudder-server/commit/0dbb328d186ef2f7d52a5af8010b491d5bc1379a))
* **deps:** bump github.com/aws/aws-sdk-go from 1.44.290 to 1.44.294 ([#3575](https://github.com/rudderlabs/rudder-server/issues/3575)) ([6cb7bf1](https://github.com/rudderlabs/rudder-server/commit/6cb7bf1ce609b8c291696648bac09b3ad33de95d))
* **deps:** bump github.com/aws/aws-sdk-go from 1.44.294 to 1.44.295 ([#3579](https://github.com/rudderlabs/rudder-server/issues/3579)) ([5c442c0](https://github.com/rudderlabs/rudder-server/commit/5c442c0ec0c6c1ad4701b1b9b5259294c492dfef))
* **deps:** bump github.com/databricks/databricks-sql-go from 1.3.0 to 1.3.1 ([#3558](https://github.com/rudderlabs/rudder-server/issues/3558)) ([4e661dd](https://github.com/rudderlabs/rudder-server/commit/4e661ddd39273b7aea13a272b6b07158f282ec1a))
* **deps:** bump github.com/hashicorp/golang-lru/v2 from 2.0.3 to 2.0.4 ([#3538](https://github.com/rudderlabs/rudder-server/issues/3538)) ([a69c422](https://github.com/rudderlabs/rudder-server/commit/a69c4223f733198ba0ca70795a8d1296465a8f0f))
* **deps:** bump github.com/minio/minio-go/v7 from 7.0.56 to 7.0.57 ([#3511](https://github.com/rudderlabs/rudder-server/issues/3511)) ([a69c422](https://github.com/rudderlabs/rudder-server/commit/a69c4223f733198ba0ca70795a8d1296465a8f0f))
* **deps:** bump github.com/minio/minio-go/v7 from 7.0.57 to 7.0.58 ([#3546](https://github.com/rudderlabs/rudder-server/issues/3546)) ([9b15bb9](https://github.com/rudderlabs/rudder-server/commit/9b15bb96f15f9058d73347e274bc329ac6e84f2b))
* **deps:** bump github.com/minio/minio-go/v7 from 7.0.58 to 7.0.59 ([#3569](https://github.com/rudderlabs/rudder-server/issues/3569)) ([3bdd305](https://github.com/rudderlabs/rudder-server/commit/3bdd305a07332a202e9f1edee1fc0f46ef4b8dd5))
* **deps:** bump github.com/onsi/ginkgo/v2 from 2.10.0 to 2.11.0 ([#3516](https://github.com/rudderlabs/rudder-server/issues/3516)) ([a69c422](https://github.com/rudderlabs/rudder-server/commit/a69c4223f733198ba0ca70795a8d1296465a8f0f))
* **deps:** bump github.com/rudderlabs/compose-test from 0.1.1 to 0.1.2 ([#3547](https://github.com/rudderlabs/rudder-server/issues/3547)) ([55bde71](https://github.com/rudderlabs/rudder-server/commit/55bde716dfddd17a572c875ea5d656b0a49f1184))
* **deps:** bump github.com/rudderlabs/rudder-go-kit from 0.15.0 to 0.15.1 ([#3530](https://github.com/rudderlabs/rudder-server/issues/3530)) ([a69c422](https://github.com/rudderlabs/rudder-server/commit/a69c4223f733198ba0ca70795a8d1296465a8f0f))
* **deps:** bump github.com/urfave/cli/v2 from 2.25.6 to 2.25.7 ([#3514](https://github.com/rudderlabs/rudder-server/issues/3514)) ([a69c422](https://github.com/rudderlabs/rudder-server/commit/a69c4223f733198ba0ca70795a8d1296465a8f0f))
* **deps:** bump google.golang.org/api from 0.127.0 to 0.128.0 ([#3508](https://github.com/rudderlabs/rudder-server/issues/3508)) ([a69c422](https://github.com/rudderlabs/rudder-server/commit/a69c4223f733198ba0ca70795a8d1296465a8f0f))
* **deps:** bump google.golang.org/api from 0.128.0 to 0.129.0 ([#3564](https://github.com/rudderlabs/rudder-server/issues/3564)) ([6cda94d](https://github.com/rudderlabs/rudder-server/commit/6cda94d45a38b60e416f39a0b40ed19caa7b5d24))
* **deps:** bump google.golang.org/grpc from 1.55.0 to 1.56.0 ([#3510](https://github.com/rudderlabs/rudder-server/issues/3510)) ([a69c422](https://github.com/rudderlabs/rudder-server/commit/a69c4223f733198ba0ca70795a8d1296465a8f0f))
* **deps:** bump google.golang.org/grpc from 1.56.0 to 1.56.1 ([#3537](https://github.com/rudderlabs/rudder-server/issues/3537)) ([a69c422](https://github.com/rudderlabs/rudder-server/commit/a69c4223f733198ba0ca70795a8d1296465a8f0f))
* **deps:** bump google.golang.org/protobuf from 1.30.0 to 1.31.0 ([#3557](https://github.com/rudderlabs/rudder-server/issues/3557)) ([6f42c97](https://github.com/rudderlabs/rudder-server/commit/6f42c976d7fcec660cf077a61f4eee8c29579b5a))
* drop attempt_number tag from metrics ([#3551](https://github.com/rudderlabs/rudder-server/issues/3551)) ([af3fec1](https://github.com/rudderlabs/rudder-server/commit/af3fec183909a8e9be6e20d4c8501376619157a0))
* drop multitenant ([573d6ff](https://github.com/rudderlabs/rudder-server/commit/573d6ff43124603ea0d32412c893467e850ee8e6))
* drop union query, fair pickup and multitenant handle ([#3521](https://github.com/rudderlabs/rudder-server/issues/3521)) ([573d6ff](https://github.com/rudderlabs/rudder-server/commit/573d6ff43124603ea0d32412c893467e850ee8e6))
* exclude attempt from metrics ([#3549](https://github.com/rudderlabs/rudder-server/issues/3549)) ([75d08dc](https://github.com/rudderlabs/rudder-server/commit/75d08dc7d5d8ee2401d43160c252bb31804d8307))
* gateway_response_time buckets ([#3554](https://github.com/rudderlabs/rudder-server/issues/3554)) ([bed100d](https://github.com/rudderlabs/rudder-server/commit/bed100da4db87fa6f01c78b921120d12daf8b289))
* **gateway:** move warehouse fetch tables under internal ([#3540](https://github.com/rudderlabs/rudder-server/issues/3540)) ([a69c422](https://github.com/rudderlabs/rudder-server/commit/a69c4223f733198ba0ca70795a8d1296465a8f0f))
* increase golangci lint execution timeout ([#3539](https://github.com/rudderlabs/rudder-server/issues/3539)) ([a69c422](https://github.com/rudderlabs/rudder-server/commit/a69c4223f733198ba0ca70795a8d1296465a8f0f))
* introduce goimports ([#3507](https://github.com/rudderlabs/rudder-server/issues/3507)) ([a69c422](https://github.com/rudderlabs/rudder-server/commit/a69c4223f733198ba0ca70795a8d1296465a8f0f))
* **jobsdb:** dsList lock performance improvements ([#3559](https://github.com/rudderlabs/rudder-server/issues/3559)) ([702ce95](https://github.com/rudderlabs/rudder-server/commit/702ce95c2ee63ec24662c2856d6e8031ab286409))
* **jobsdb:** tuning and improvements ([#3584](https://github.com/rudderlabs/rudder-server/issues/3584)) ([cfa6132](https://github.com/rudderlabs/rudder-server/commit/cfa613220d8d7dc47ebe90e372624b4151549c8e))
* load file upload ([#3552](https://github.com/rudderlabs/rudder-server/issues/3552)) ([a8abd1d](https://github.com/rudderlabs/rudder-server/commit/a8abd1db5b6a9b434dd3363b3f959302ecdfc60b))
* minor processor refactor for error handling and readability  ([#3534](https://github.com/rudderlabs/rudder-server/issues/3534)) ([8f4635d](https://github.com/rudderlabs/rudder-server/commit/8f4635d9b650ddedb6d3cfede2bdd56d18eb1767))
* minor refactor for readability ([8f4635d](https://github.com/rudderlabs/rudder-server/commit/8f4635d9b650ddedb6d3cfede2bdd56d18eb1767))
* move filemanager to rudder-go-kit ([#3517](https://github.com/rudderlabs/rudder-server/issues/3517)) ([9b82187](https://github.com/rudderlabs/rudder-server/commit/9b8218758c93164ed68bfd01013f3da5b5012f6e))
* remove unused code from warehouse ([#3567](https://github.com/rudderlabs/rudder-server/issues/3567)) ([a07cf9e](https://github.com/rudderlabs/rudder-server/commit/a07cf9e295f79aac073f2bdad22fd07b557c7869))
* retry on 408 response events ([#3617](https://github.com/rudderlabs/rudder-server/issues/3617)) ([6c44758](https://github.com/rudderlabs/rudder-server/commit/6c44758b1aadc2cb90d21efdb10acd85b9582c2e))
* **router:** penalise continuous non-productive pickup loops by sleeping ([#3571](https://github.com/rudderlabs/rudder-server/issues/3571)) ([b3050a7](https://github.com/rudderlabs/rudder-server/commit/b3050a7a425455e1b2b33fb508451d3fd1acdd8f))
* **router:** stop pickup iteration conditions ([#3582](https://github.com/rudderlabs/rudder-server/issues/3582)) ([9e7f117](https://github.com/rudderlabs/rudder-server/commit/9e7f117776bfdb488b1349c49c68ddb1ce7de303))
* support replay with iam role ([#3560](https://github.com/rudderlabs/rudder-server/issues/3560)) ([f471fb0](https://github.com/rudderlabs/rudder-server/commit/f471fb0e8c2b66282968f79cc191f9f219018d95))
* typo for workspaceId in stats ([#3566](https://github.com/rudderlabs/rudder-server/issues/3566)) ([8975dcc](https://github.com/rudderlabs/rudder-server/commit/8975dcc375691ff2a9f272c252adbeaab3c7261e))

## [1.10.0](https://github.com/rudderlabs/rudder-server/compare/v1.9.0...v1.10.0) (2023-06-23)


### Features

* add config param for batch router for name customize ([#3461](https://github.com/rudderlabs/rudder-server/issues/3461)) ([c16e692](https://github.com/rudderlabs/rudder-server/commit/c16e692da2e13d6b9f141bb06c70b84bce0a069c))
* error detail reporting ([#3265](https://github.com/rudderlabs/rudder-server/issues/3265)) ([34f4c0d](https://github.com/rudderlabs/rudder-server/commit/34f4c0d2a9fd14d2d436962020c50fc36e37c472))
* **router:** support for isolation modes using limiters ([#3379](https://github.com/rudderlabs/rudder-server/issues/3379)) ([fbe109f](https://github.com/rudderlabs/rudder-server/commit/fbe109f682fb0eac8f2797236bd1497caaddcca0))
* **warehouse:** staging file schema consolidation ([#3088](https://github.com/rudderlabs/rudder-server/issues/3088)) ([b245915](https://github.com/rudderlabs/rudder-server/commit/b245915a1c922856e37483ea751dc2c17705caf5))


### Bug Fixes

* **batchrouter:** batchrouter stops processing events for destinations where a destType-specific config option is set ([#3421](https://github.com/rudderlabs/rudder-server/issues/3421)) ([ee87636](https://github.com/rudderlabs/rudder-server/commit/ee87636c7ddf6672fad51c430d7bdd6b203bbfc1))
* clickhouse flaky test ([#3392](https://github.com/rudderlabs/rudder-server/issues/3392)) ([a89ef27](https://github.com/rudderlabs/rudder-server/commit/a89ef2712e1e0272ee02b76b31f8e6b5c2c406db))
* error extraction for errors in destination transformation ([#3499](https://github.com/rudderlabs/rudder-server/issues/3499)) ([5c43457](https://github.com/rudderlabs/rudder-server/commit/5c43457d8c2ef0776fd6677b3b8f28c87861b03e))
* flag for warehouse altering ([#3460](https://github.com/rudderlabs/rudder-server/issues/3460)) ([c23ffb2](https://github.com/rudderlabs/rudder-server/commit/c23ffb2a8ed7dd2f1cfc82339db2d881ae16f064))
* gateway flaky test ([#3356](https://github.com/rudderlabs/rudder-server/issues/3356)) ([b245915](https://github.com/rudderlabs/rudder-server/commit/b245915a1c922856e37483ea751dc2c17705caf5))
* **gateway:** use get method for /fetch-tables ([#3528](https://github.com/rudderlabs/rudder-server/issues/3528)) ([08cd99c](https://github.com/rudderlabs/rudder-server/commit/08cd99c11fddb257e0123f9fbd3fb857e927ca43))
* gw transient errors crash  ([#3397](https://github.com/rudderlabs/rudder-server/issues/3397)) ([6ad51e8](https://github.com/rudderlabs/rudder-server/commit/6ad51e8e7411584d61a546b8645e4bc1b88c1fbb))
* jobsforwarder panics with slice bounds out of range ([#3414](https://github.com/rudderlabs/rudder-server/issues/3414)) ([0bda13f](https://github.com/rudderlabs/rudder-server/commit/0bda13f3fb2ca7a7dd1efb3a029454118079e750))
* kafka flaky batching test ([#3447](https://github.com/rudderlabs/rudder-server/issues/3447)) ([fcd49e4](https://github.com/rudderlabs/rudder-server/commit/fcd49e4d09edf612a2e3b1f1c84033d4fcfadb76))
* migration fails with error pq: VACUUM cannot run inside a transaction block ([#3464](https://github.com/rudderlabs/rudder-server/issues/3464)) ([9e32802](https://github.com/rudderlabs/rudder-server/commit/9e328022f9adb6a8e31cccfa14cada97fad65f9e))
* regulation-worker flaky test ([#3374](https://github.com/rudderlabs/rudder-server/issues/3374)) ([b245915](https://github.com/rudderlabs/rudder-server/commit/b245915a1c922856e37483ea751dc2c17705caf5))
* respect user schema modification by user in warehouse ([#3419](https://github.com/rudderlabs/rudder-server/issues/3419)) ([fda4baf](https://github.com/rudderlabs/rudder-server/commit/fda4baf400f1e401516e01ac9d6144a21c07d713))
* router panics with limit for rt_pickup must be greater than 0 ([#3467](https://github.com/rudderlabs/rudder-server/issues/3467)) ([cc56b56](https://github.com/rudderlabs/rudder-server/commit/cc56b566b2f6180b5c73ab193be7653d2ad7139e))
* **router:** using wrong partition tag in limiter stats ([#3518](https://github.com/rudderlabs/rudder-server/issues/3518)) ([893504f](https://github.com/rudderlabs/rudder-server/commit/893504f90439e956d92a53ddf49ee5a29961772b))
* stats duplicated labels ([#3411](https://github.com/rudderlabs/rudder-server/issues/3411)) ([0bda13f](https://github.com/rudderlabs/rudder-server/commit/0bda13f3fb2ca7a7dd1efb3a029454118079e750))
* warehouse flaky test ([#3402](https://github.com/rudderlabs/rudder-server/issues/3402)) ([3f88f50](https://github.com/rudderlabs/rudder-server/commit/3f88f50ca017238fd4878da594d8b9662785b31c))
* warehouse proxy endpoints ([#3476](https://github.com/rudderlabs/rudder-server/issues/3476)) ([c23ffb2](https://github.com/rudderlabs/rudder-server/commit/c23ffb2a8ed7dd2f1cfc82339db2d881ae16f064))


### Miscellaneous

* add missing index warehouse load files table ([#3519](https://github.com/rudderlabs/rudder-server/issues/3519)) ([9dcea7b](https://github.com/rudderlabs/rudder-server/commit/9dcea7bd8db4a20ae1ee11f76ccc7e35c4324f0a))
* add resp body status check ([#3446](https://github.com/rudderlabs/rudder-server/issues/3446)) ([e0d7b6d](https://github.com/rudderlabs/rudder-server/commit/e0d7b6d32a16b3f58c3d4fce0ac32297941a713e))
* add support for detecting bot events in gateway ([#3504](https://github.com/rudderlabs/rudder-server/issues/3504)) ([51a4c45](https://github.com/rudderlabs/rudder-server/commit/51a4c459ab3fd7be5531ae1ae3acd75bf6559200))
* applying 1.9.3 hotfixes to main branch ([#3423](https://github.com/rudderlabs/rudder-server/issues/3423)) ([ee1630d](https://github.com/rudderlabs/rudder-server/commit/ee1630de55d45e29ba81d6239bc0ff6d0d7f64af))
* avoid querying a dataset in case AfterJobID falls after said dataset ([#3478](https://github.com/rudderlabs/rudder-server/issues/3478)) ([f612aae](https://github.com/rudderlabs/rudder-server/commit/f612aae091e65f1ccd1ae84dd0a3058685f6f26d))
* batch event schema messages for faster processing ([#3406](https://github.com/rudderlabs/rudder-server/issues/3406)) ([0bda13f](https://github.com/rudderlabs/rudder-server/commit/0bda13f3fb2ca7a7dd1efb3a029454118079e750))
* clean up warehouse indexes and queries ([#3448](https://github.com/rudderlabs/rudder-server/issues/3448)) ([7a3589a](https://github.com/rudderlabs/rudder-server/commit/7a3589afd411b73cf82fd0d46abcfe825a983298))
* cleanup for integration tests for warehouse ([#3412](https://github.com/rudderlabs/rudder-server/issues/3412)) ([3323427](https://github.com/rudderlabs/rudder-server/commit/332342744b2f7e77ac78ac961628c5a065d2ca74))
* **deps:** bump cloud.google.com/go/pubsub from 1.30.1 to 1.31.0 ([#3399](https://github.com/rudderlabs/rudder-server/issues/3399)) ([ef8e86c](https://github.com/rudderlabs/rudder-server/commit/ef8e86c784be5a31fdfe01684f53b64a79588e53))
* **deps:** bump github.com/aws/aws-sdk-go from 1.44.264 to 1.44.265 ([#3361](https://github.com/rudderlabs/rudder-server/issues/3361)) ([b245915](https://github.com/rudderlabs/rudder-server/commit/b245915a1c922856e37483ea751dc2c17705caf5))
* **deps:** bump github.com/aws/aws-sdk-go from 1.44.265 to 1.44.266 ([#3368](https://github.com/rudderlabs/rudder-server/issues/3368)) ([b245915](https://github.com/rudderlabs/rudder-server/commit/b245915a1c922856e37483ea751dc2c17705caf5))
* **deps:** bump github.com/aws/aws-sdk-go from 1.44.266 to 1.44.271 ([#3409](https://github.com/rudderlabs/rudder-server/issues/3409)) ([fc48d93](https://github.com/rudderlabs/rudder-server/commit/fc48d93a2b3643cde4633a069b8b5b2f038ae418))
* **deps:** bump github.com/aws/aws-sdk-go from 1.44.271 to 1.44.275 ([#3442](https://github.com/rudderlabs/rudder-server/issues/3442)) ([8b179b6](https://github.com/rudderlabs/rudder-server/commit/8b179b68d3444bb22ae97ad3f7d93a5fdafc0f57))
* **deps:** bump github.com/aws/aws-sdk-go from 1.44.275 to 1.44.280 ([#3481](https://github.com/rudderlabs/rudder-server/issues/3481)) ([9c0a4e7](https://github.com/rudderlabs/rudder-server/commit/9c0a4e78d06f154933762b98cb1a07458326a016))
* **deps:** bump github.com/aws/aws-sdk-go from 1.44.280 to 1.44.281 ([#3488](https://github.com/rudderlabs/rudder-server/issues/3488)) ([af72e90](https://github.com/rudderlabs/rudder-server/commit/af72e90dd4a3481bc755fc04659eb439245ec296))
* **deps:** bump github.com/aws/aws-sdk-go from 1.44.281 to 1.44.282 ([#3494](https://github.com/rudderlabs/rudder-server/issues/3494)) ([aa6e194](https://github.com/rudderlabs/rudder-server/commit/aa6e194b0950bb14d72714a1e39d5459ab8589f0))
* **deps:** bump github.com/databricks/databricks-sql-go from 1.2.0 to 1.3.0 ([#3466](https://github.com/rudderlabs/rudder-server/issues/3466)) ([3f187d9](https://github.com/rudderlabs/rudder-server/commit/3f187d9bc75b1dfbbfb96336e2f5e481e654762e))
* **deps:** bump github.com/golang-migrate/migrate/v4 from 4.15.2 to 4.16.1 ([#3441](https://github.com/rudderlabs/rudder-server/issues/3441)) ([ef16663](https://github.com/rudderlabs/rudder-server/commit/ef166634df0109a0386a29e2f06d259adbaad563))
* **deps:** bump github.com/golang-migrate/migrate/v4 from 4.16.1 to 4.16.2 ([#3480](https://github.com/rudderlabs/rudder-server/issues/3480)) ([7465dd4](https://github.com/rudderlabs/rudder-server/commit/7465dd4daa88104c1aef07b95b05cddd0fca639c))
* **deps:** bump github.com/hashicorp/go-retryablehttp from 0.7.2 to 0.7.4 ([#3457](https://github.com/rudderlabs/rudder-server/issues/3457)) ([b3cd0f6](https://github.com/rudderlabs/rudder-server/commit/b3cd0f635a0c273a3050204c5b9538a46b0a3f18))
* **deps:** bump github.com/hashicorp/golang-lru/v2 from 2.0.2 to 2.0.3 ([#3475](https://github.com/rudderlabs/rudder-server/issues/3475)) ([74b4163](https://github.com/rudderlabs/rudder-server/commit/74b4163e29999abd7d14802cc784fe387232b62d))
* **deps:** bump github.com/minio/minio-go/v7 from 7.0.52 to 7.0.53 ([#3370](https://github.com/rudderlabs/rudder-server/issues/3370)) ([b245915](https://github.com/rudderlabs/rudder-server/commit/b245915a1c922856e37483ea751dc2c17705caf5))
* **deps:** bump github.com/minio/minio-go/v7 from 7.0.53 to 7.0.56 ([#3437](https://github.com/rudderlabs/rudder-server/issues/3437)) ([dbd9bf1](https://github.com/rudderlabs/rudder-server/commit/dbd9bf19e75bf7ee5b6614dd26c3ee037262f341))
* **deps:** bump github.com/onsi/ginkgo/v2 from 2.9.5 to 2.9.7 ([#3449](https://github.com/rudderlabs/rudder-server/issues/3449)) ([1af6b90](https://github.com/rudderlabs/rudder-server/commit/1af6b90a01d1090dc061f0e34ebf3a017d789798))
* **deps:** bump github.com/onsi/ginkgo/v2 from 2.9.7 to 2.10.0 ([#3458](https://github.com/rudderlabs/rudder-server/issues/3458)) ([dacfdc8](https://github.com/rudderlabs/rudder-server/commit/dacfdc845c9a8e3f5740558655c84afc7e1df955))
* **deps:** bump github.com/onsi/gomega from 1.27.6 to 1.27.7 ([#3360](https://github.com/rudderlabs/rudder-server/issues/3360)) ([b245915](https://github.com/rudderlabs/rudder-server/commit/b245915a1c922856e37483ea751dc2c17705caf5))
* **deps:** bump github.com/onsi/gomega from 1.27.7 to 1.27.8 ([#3459](https://github.com/rudderlabs/rudder-server/issues/3459)) ([d64c405](https://github.com/rudderlabs/rudder-server/commit/d64c405a52e5d970fb8c8ba93d511e96f08859d9))
* **deps:** bump github.com/rudderlabs/rudder-go-kit from 0.14.3 to 0.15.0 ([#3473](https://github.com/rudderlabs/rudder-server/issues/3473)) ([6eea667](https://github.com/rudderlabs/rudder-server/commit/6eea667edcb1f291c4bb892ddcd88c2f672391d8))
* **deps:** bump github.com/snowflakedb/gosnowflake from 1.6.20 to 1.6.21 ([#3385](https://github.com/rudderlabs/rudder-server/issues/3385)) ([4a06f44](https://github.com/rudderlabs/rudder-server/commit/4a06f445275b755eeb90f8bb857a1f9baaa25f6f))
* **deps:** bump github.com/snowflakedb/gosnowflake from 1.6.21 to 1.6.22 ([#3503](https://github.com/rudderlabs/rudder-server/issues/3503)) ([0f9c816](https://github.com/rudderlabs/rudder-server/commit/0f9c816333eec0d386bf5dad0480db02c0b81c9b))
* **deps:** bump github.com/stretchr/testify from 1.8.2 to 1.8.3 ([#3359](https://github.com/rudderlabs/rudder-server/issues/3359)) ([b245915](https://github.com/rudderlabs/rudder-server/commit/b245915a1c922856e37483ea751dc2c17705caf5))
* **deps:** bump github.com/stretchr/testify from 1.8.3 to 1.8.4 ([#3450](https://github.com/rudderlabs/rudder-server/issues/3450)) ([f5367ed](https://github.com/rudderlabs/rudder-server/commit/f5367ed3aff0c8bcba985ce97390712df4be4068))
* **deps:** bump github.com/urfave/cli/v2 from 2.25.3 to 2.25.5 ([#3418](https://github.com/rudderlabs/rudder-server/issues/3418)) ([63a24eb](https://github.com/rudderlabs/rudder-server/commit/63a24ebbd78a9fd7f492acd85f06b838c9090ff2))
* **deps:** bump github.com/urfave/cli/v2 from 2.25.5 to 2.25.6 ([#3489](https://github.com/rudderlabs/rudder-server/issues/3489)) ([3a00ee6](https://github.com/rudderlabs/rudder-server/commit/3a00ee6b6372b7f43f5a9eb23170265c4fe6ec82))
* **deps:** bump golang.org/x/oauth2 from 0.8.0 to 0.9.0 ([#3495](https://github.com/rudderlabs/rudder-server/issues/3495)) ([5021989](https://github.com/rudderlabs/rudder-server/commit/5021989ad115e948c437c5e32000eb6afc177ed2))
* **deps:** bump golang.org/x/sync from 0.2.0 to 0.3.0 ([#3502](https://github.com/rudderlabs/rudder-server/issues/3502)) ([f3391d7](https://github.com/rudderlabs/rudder-server/commit/f3391d705ae9ab0c1e9c6254e4d669e4ae2b51b7))
* **deps:** bump google.golang.org/api from 0.122.0 to 0.123.0 ([#3362](https://github.com/rudderlabs/rudder-server/issues/3362)) ([b245915](https://github.com/rudderlabs/rudder-server/commit/b245915a1c922856e37483ea751dc2c17705caf5))
* **deps:** bump google.golang.org/api from 0.123.0 to 0.124.0 ([#3386](https://github.com/rudderlabs/rudder-server/issues/3386)) ([da6215c](https://github.com/rudderlabs/rudder-server/commit/da6215c495611444615ab18a8b7fa53adcdc3141))
* **deps:** bump google.golang.org/api from 0.124.0 to 0.125.0 ([#3427](https://github.com/rudderlabs/rudder-server/issues/3427)) ([a81b684](https://github.com/rudderlabs/rudder-server/commit/a81b684587a5085bc09e5dbc0abc48b661767db7))
* **deps:** bump google.golang.org/api from 0.125.0 to 0.126.0 ([#3472](https://github.com/rudderlabs/rudder-server/issues/3472)) ([0f573e4](https://github.com/rudderlabs/rudder-server/commit/0f573e49492b9bbee992826bacc80956bbefd640))
* **deps:** bump google.golang.org/api from 0.126.0 to 0.127.0 ([#3487](https://github.com/rudderlabs/rudder-server/issues/3487)) ([b525949](https://github.com/rudderlabs/rudder-server/commit/b52594963e536b247cb4fe634e299c1fa4244e1f))
* drop merged schema column ([#3493](https://github.com/rudderlabs/rudder-server/issues/3493)) ([ba797a4](https://github.com/rudderlabs/rudder-server/commit/ba797a42d3f837640ecd9795010be8cecc2d2ee1))
* fix flaky dedup test ([#3417](https://github.com/rudderlabs/rudder-server/issues/3417)) ([2f0a684](https://github.com/rudderlabs/rudder-server/commit/2f0a68424d779a5d860fdfd8ebf9a2c26bdb71ba))
* fix idle query ([#3430](https://github.com/rudderlabs/rudder-server/issues/3430)) ([3cf342d](https://github.com/rudderlabs/rudder-server/commit/3cf342d7360a5c1a0aa8a38653dff5903497e505))
* gateway health endpoint should return 503 if db is down ([#3351](https://github.com/rudderlabs/rudder-server/issues/3351)) ([e22c790](https://github.com/rudderlabs/rudder-server/commit/e22c7901a530e1a401c7096c856c7e8ad7ee2a1b))
* gateway stores singular event batches ([#3256](https://github.com/rudderlabs/rudder-server/issues/3256)) ([1ccec6e](https://github.com/rudderlabs/rudder-server/commit/1ccec6e6647213ecc0a98533931156080b6db9c6))
* getUploadsToProcess error handling ([#3380](https://github.com/rudderlabs/rudder-server/issues/3380)) ([b245915](https://github.com/rudderlabs/rudder-server/commit/b245915a1c922856e37483ea751dc2c17705caf5))
* jobsdb max age for jobs, cleanup routine ([#3420](https://github.com/rudderlabs/rudder-server/issues/3420)) ([5025a72](https://github.com/rudderlabs/rudder-server/commit/5025a72704bc469ed06023bbb27d351690f329a1))
* kafka manager errors ([#3465](https://github.com/rudderlabs/rudder-server/issues/3465)) ([21487c2](https://github.com/rudderlabs/rudder-server/commit/21487c2a35f81ba72810199a286ca938985102fb))
* make tests required for passing ([#3347](https://github.com/rudderlabs/rudder-server/issues/3347)) ([b245915](https://github.com/rudderlabs/rudder-server/commit/b245915a1c922856e37483ea751dc2c17705caf5))
* move suppression-backup-service from rudderlabs to rudderstack directory in dockerhub ([#3505](https://github.com/rudderlabs/rudder-server/issues/3505)) ([6c9b25b](https://github.com/rudderlabs/rudder-server/commit/6c9b25bc724c48e650cf3c2fb3a0ad8b367e3e2a))
* nil check for health handler ([#3500](https://github.com/rudderlabs/rudder-server/issues/3500)) ([195e2a0](https://github.com/rudderlabs/rudder-server/commit/195e2a029a5432e73713ce0f24757eb609c9f45f))
* pass context ([#3326](https://github.com/rudderlabs/rudder-server/issues/3326)) ([990a405](https://github.com/rudderlabs/rudder-server/commit/990a40510c1ecf88467ccb858d981a1643d3e590))
* periodically push zero output measurement for detecting stuck processing pipelines ([#3453](https://github.com/rudderlabs/rudder-server/issues/3453)) ([4116f37](https://github.com/rudderlabs/rudder-server/commit/4116f37881328fa019f398716503cd72b60896c8))
* **processor:** support multiple jobsdb writers when source isolation is enabled ([#3428](https://github.com/rudderlabs/rudder-server/issues/3428)) ([b25003d](https://github.com/rudderlabs/rudder-server/commit/b25003dd1b8715ff6dd6d3547bc0300a6d621c5e))
* remove deprecated merged schema field ([#3482](https://github.com/rudderlabs/rudder-server/issues/3482)) ([39a0915](https://github.com/rudderlabs/rudder-server/commit/39a091528a23b108aeb7402e8b15a58355b10e54))
* remove namespace tag from measurements ([#3468](https://github.com/rudderlabs/rudder-server/issues/3468)) ([a6ac7bd](https://github.com/rudderlabs/rudder-server/commit/a6ac7bd0c443d4e17d59faa24fc16c70e911bd5e))
* replace announcement header with data learning centre link ([#3358](https://github.com/rudderlabs/rudder-server/issues/3358)) ([b245915](https://github.com/rudderlabs/rudder-server/commit/b245915a1c922856e37483ea751dc2c17705caf5))
* revert drop merged schema column ([#3501](https://github.com/rudderlabs/rudder-server/issues/3501)) ([c8861ee](https://github.com/rudderlabs/rudder-server/commit/c8861eeecfb523436bbcf752d2bd654d10c5ae6e))
* **router:** observability on job iterator discards & stop iteration if throttled with destination isolation enabled [#3491](https://github.com/rudderlabs/rudder-server/issues/3491) ([66e32ad](https://github.com/rudderlabs/rudder-server/commit/66e32ad68fdb501a9b7eb42fa3145643ab0306e6))
* **router:** periodic flush during pickup ([#3497](https://github.com/rudderlabs/rudder-server/issues/3497)) ([1193acc](https://github.com/rudderlabs/rudder-server/commit/1193acc57cb3804351cbf76be2a352b6548461ec))
* set limits for event schema messages and discard messages above these limits ([#3435](https://github.com/rudderlabs/rudder-server/issues/3435)) ([b70c075](https://github.com/rudderlabs/rudder-server/commit/b70c0759bad104de8ecbf44f6d9dfbfb365b8d6c))
* source transformation failures stat ([#3524](https://github.com/rudderlabs/rudder-server/issues/3524)) ([1146217](https://github.com/rudderlabs/rudder-server/commit/1146217407938018655da13b14e06e7e5a5a5959))
* source transformation failures stat tag correction ([#3541](https://github.com/rudderlabs/rudder-server/issues/3541)) ([b17dec9](https://github.com/rudderlabs/rudder-server/commit/b17dec9dc6b4169331fe5f4f9eb7ba97f6b41ce6))
* split unit tests ([#3416](https://github.com/rudderlabs/rudder-server/issues/3416)) ([471a562](https://github.com/rudderlabs/rudder-server/commit/471a56230f8fe6ba8f8032c401d18f279dd4cd01))
* split unit tests ([#3492](https://github.com/rudderlabs/rudder-server/issues/3492)) ([1d0c720](https://github.com/rudderlabs/rudder-server/commit/1d0c720bff3d7fcb6205eb1f5554dff3dbadfc5b))
* tests coverage ([#3349](https://github.com/rudderlabs/rudder-server/issues/3349)) ([b245915](https://github.com/rudderlabs/rudder-server/commit/b245915a1c922856e37483ea751dc2c17705caf5))
* timeout for warehouse sql queries ([#3433](https://github.com/rudderlabs/rudder-server/issues/3433)) ([ca512a0](https://github.com/rudderlabs/rudder-server/commit/ca512a06c3a66b0307932132609786ba589ec787))
* upgrade golangci version and lint fixes ([#3443](https://github.com/rudderlabs/rudder-server/issues/3443)) ([3d03653](https://github.com/rudderlabs/rudder-server/commit/3d03653242b2a71b5309b5e33b02184a5a43fd7d))
* upgrade to badgerV4 ([#3340](https://github.com/rudderlabs/rudder-server/issues/3340)) ([3ecea32](https://github.com/rudderlabs/rudder-server/commit/3ecea32bbd88e105c380d3753558db05bd3e5507))
* using parquet-rudderlabs-go ([#3490](https://github.com/rudderlabs/rudder-server/issues/3490)) ([fbbf101](https://github.com/rudderlabs/rudder-server/commit/fbbf101faa2c1598aba643a4f9931af7a58e5caa))
* vaccum status tables if they cross threshold ([#3434](https://github.com/rudderlabs/rudder-server/issues/3434)) ([8d35882](https://github.com/rudderlabs/rudder-server/commit/8d35882d13f68bcc9ca3bc641fea96870c473096))

## [1.9.6](https://github.com/rudderlabs/rudder-server/compare/v1.9.5...v1.9.6) (2023-06-09)


### Bug Fixes

* warehouse proxy endpoints ([#3476](https://github.com/rudderlabs/rudder-server/issues/3476)) ([fda977f](https://github.com/rudderlabs/rudder-server/commit/fda977f9e440aaa21337ff620c8fb8fe68385b2a))

## [1.9.5](https://github.com/rudderlabs/rudder-server/compare/v1.9.4...v1.9.5) (2023-06-07)


### Bug Fixes

* flag for warehouse altering ([#3460](https://github.com/rudderlabs/rudder-server/issues/3460)) ([472d310](https://github.com/rudderlabs/rudder-server/commit/472d3102fd04e91da2e832737dbb42ba2671dc3c))

## [1.9.4](https://github.com/rudderlabs/rudder-server/compare/v1.9.3...v1.9.4) (2023-06-06)


### Miscellaneous

* add resp body status check ([#3446](https://github.com/rudderlabs/rudder-server/issues/3446)) ([b251b21](https://github.com/rudderlabs/rudder-server/commit/b251b217787b0b2c919378a40c950b8ea719d7be))

## [1.9.3](https://github.com/rudderlabs/rudder-server/compare/v1.9.2...v1.9.3) (2023-05-31)


### Bug Fixes

* **batchrouter:** batchrouter stops processing events for destinations where a destType-specific config option is set [#3421](https://github.com/rudderlabs/rudder-server/issues/3421) ([f2dc53c](https://github.com/rudderlabs/rudder-server/commit/f2dc53cf6a976c09c6c9f45b0b3d846fbcd55745))

## [1.9.2](https://github.com/rudderlabs/rudder-server/compare/v1.9.1...v1.9.2) (2023-05-29)


### Bug Fixes

* jobsforwarder panics with slice bounds out of range ([#3414](https://github.com/rudderlabs/rudder-server/issues/3414)) ([99b7e62](https://github.com/rudderlabs/rudder-server/commit/99b7e62ab9763236bddb7c58918cb67eda45156d))

## [1.9.1](https://github.com/rudderlabs/rudder-server/compare/v1.9.0...v1.9.1) (2023-05-29)


### Bug Fixes

* stats duplicated labels ([#3411](https://github.com/rudderlabs/rudder-server/issues/3411)) ([0beb455](https://github.com/rudderlabs/rudder-server/commit/0beb455f4065e006dcaa5ad5c94029c52872e45e))


### Miscellaneous

* batch event schema messages for faster processing ([#3406](https://github.com/rudderlabs/rudder-server/issues/3406)) ([e9f2174](https://github.com/rudderlabs/rudder-server/commit/e9f2174f83935c39f098731705be27ba803f03e2))

## [1.9.0](https://github.com/rudderlabs/rudder-server/compare/v1.8.0...v1.9.0) (2023-05-25)


### Features

* **batchrouter:** introduce isolation levels and concurrency limiters ([#3248](https://github.com/rudderlabs/rudder-server/issues/3248)) ([d90ea68](https://github.com/rudderlabs/rudder-server/commit/d90ea68178d8ad6175d0145568fd7e2d651ed760))
* support for incremental updates while fetching backend config ([#3175](https://github.com/rudderlabs/rudder-server/issues/3175)) ([9de904d](https://github.com/rudderlabs/rudder-server/commit/9de904d68ee122f4c2df4f1bf9335f9a851470fa))
* wh Endpoint To Fetch Tables Per Connection ([#3279](https://github.com/rudderlabs/rudder-server/issues/3279)) ([ea7d5ce](https://github.com/rudderlabs/rudder-server/commit/ea7d5ce8b8fe0b5e8f3f3e32b665d9b94232c58c))


### Bug Fixes

* batchrouter acquiring a read lock twice ([#3363](https://github.com/rudderlabs/rudder-server/issues/3363)) ([6fd8552](https://github.com/rudderlabs/rudder-server/commit/6fd855258daf1a4fbbd7130072d66e4e2e3d3d55))
* deltalake parquet datatype support ([#3393](https://github.com/rudderlabs/rudder-server/issues/3393)) ([0a6c85e](https://github.com/rudderlabs/rudder-server/commit/0a6c85ec913158169e341957c2bdbc2ffba3ba64))
* external table support for databricks validations ([#3378](https://github.com/rudderlabs/rudder-server/issues/3378)) ([acf0c47](https://github.com/rudderlabs/rudder-server/commit/acf0c47ece7195a445dab52029e773f41845e3f9))
* include source definition type when destination has UT ([#3338](https://github.com/rudderlabs/rudder-server/issues/3338))  ([1ab2f55](https://github.com/rudderlabs/rudder-server/commit/1ab2f553dec3b34f341fdcc707540d5758d69b09))
* kafka secret comes first ([#3307](https://github.com/rudderlabs/rudder-server/issues/3307)) ([19ceebb](https://github.com/rudderlabs/rudder-server/commit/19ceebb6f20c26630a407d33490ed13b72ccf083))
* limiter not respecting WithLimiterTags option ([#3367](https://github.com/rudderlabs/rudder-server/issues/3367)) ([9a3e6fc](https://github.com/rudderlabs/rudder-server/commit/9a3e6fc9432b5ab8d1d26b8ac4ec7e6b1bcdde10))
* max connections for warehouse slaves ([#3341](https://github.com/rudderlabs/rudder-server/issues/3341)) ([1ab2f55](https://github.com/rudderlabs/rudder-server/commit/1ab2f553dec3b34f341fdcc707540d5758d69b09))
* processor panicking during shutdown ([#3396](https://github.com/rudderlabs/rudder-server/issues/3396)) ([4e3981f](https://github.com/rudderlabs/rudder-server/commit/4e3981f31f669af127d88c5f5a78173db000d81d))
* schema forwarder records invalid json in statuses ([#3350](https://github.com/rudderlabs/rudder-server/issues/3350)) ([91b1902](https://github.com/rudderlabs/rudder-server/commit/91b1902973bd22be0994a7b3a3518b1778d79877))
* staging files status when insert ([#3332](https://github.com/rudderlabs/rudder-server/issues/3332)) ([fb7277f](https://github.com/rudderlabs/rudder-server/commit/fb7277f00ded6ccfeb4cd61becc6cdc9aef905aa))
* stash sleep ([#3312](https://github.com/rudderlabs/rudder-server/issues/3312)) ([19ceebb](https://github.com/rudderlabs/rudder-server/commit/19ceebb6f20c26630a407d33490ed13b72ccf083))


### Miscellaneous

* adaptive processor worker sleep time ([#3334](https://github.com/rudderlabs/rudder-server/issues/3334)) ([4a4f293](https://github.com/rudderlabs/rudder-server/commit/4a4f2931ccdd9266597845c5ee7b713424c34bc4))
* add toggle for backendconfig db caching ([#3320](https://github.com/rudderlabs/rudder-server/issues/3320)) ([0d198b8](https://github.com/rudderlabs/rudder-server/commit/0d198b81db518cc5a57ee35669560856b136a952))
* added logs to help debug suppression backup service issue ([#3249](https://github.com/rudderlabs/rudder-server/issues/3249)) ([a28984a](https://github.com/rudderlabs/rudder-server/commit/a28984a96b5550f3a3e79550f09d813f262a4a8b))
* cleanup fetch schema ([#3242](https://github.com/rudderlabs/rudder-server/issues/3242)) ([a28984a](https://github.com/rudderlabs/rudder-server/commit/a28984a96b5550f3a3e79550f09d813f262a4a8b))
* dedup based on message ID ([#3289](https://github.com/rudderlabs/rudder-server/issues/3289)) ([a28984a](https://github.com/rudderlabs/rudder-server/commit/a28984a96b5550f3a3e79550f09d813f262a4a8b))
* dedup based on message ID ([#3289](https://github.com/rudderlabs/rudder-server/issues/3289)) ([a2679d5](https://github.com/rudderlabs/rudder-server/commit/a2679d59d1c42e7fb289fa82e1d338753ddc3a75))
* **deps:** bump cloud.google.com/go/bigquery from 1.51.0 to 1.51.1 ([#3303](https://github.com/rudderlabs/rudder-server/issues/3303)) ([a28984a](https://github.com/rudderlabs/rudder-server/commit/a28984a96b5550f3a3e79550f09d813f262a4a8b))
* **deps:** bump cloud.google.com/go/bigquery from 1.51.1 to 1.51.2 ([#3309](https://github.com/rudderlabs/rudder-server/issues/3309)) ([465ad41](https://github.com/rudderlabs/rudder-server/commit/465ad41c2d8a4695bf00923b3b04cad0b31c686f))
* **deps:** bump github.com/aws/aws-sdk-go from 1.44.256 to 1.44.259 ([#3302](https://github.com/rudderlabs/rudder-server/issues/3302)) ([a28984a](https://github.com/rudderlabs/rudder-server/commit/a28984a96b5550f3a3e79550f09d813f262a4a8b))
* **deps:** bump github.com/aws/aws-sdk-go from 1.44.256 to 1.44.259 ([#3302](https://github.com/rudderlabs/rudder-server/issues/3302)) ([2476215](https://github.com/rudderlabs/rudder-server/commit/24762152d85c99a501b365844b44fa113ea8ecd1))
* **deps:** bump github.com/aws/aws-sdk-go from 1.44.259 to 1.44.261 ([#3316](https://github.com/rudderlabs/rudder-server/issues/3316)) ([f4abcb1](https://github.com/rudderlabs/rudder-server/commit/f4abcb17a4a2037411daf9d1dcbe65e2b31b86ab))
* **deps:** bump github.com/aws/aws-sdk-go from 1.44.261 to 1.44.262 ([#3322](https://github.com/rudderlabs/rudder-server/issues/3322)) ([a2dd313](https://github.com/rudderlabs/rudder-server/commit/a2dd313c97a127bf6d62e91ccb306518e79b07e5))
* **deps:** bump github.com/aws/aws-sdk-go from 1.44.262 to 1.44.264 ([#3343](https://github.com/rudderlabs/rudder-server/issues/3343)) ([de5c605](https://github.com/rudderlabs/rudder-server/commit/de5c605d7cc8633310ba630426af76cd6f3ae52e))
* **deps:** bump github.com/confluentinc/confluent-kafka-go/v2 from 2.1.0 to 2.1.1 ([#3266](https://github.com/rudderlabs/rudder-server/issues/3266)) ([a28984a](https://github.com/rudderlabs/rudder-server/commit/a28984a96b5550f3a3e79550f09d813f262a4a8b))
* **deps:** bump github.com/onsi/ginkgo/v2 from 2.9.4 to 2.9.5 ([#3336](https://github.com/rudderlabs/rudder-server/issues/3336)) ([8aefa7a](https://github.com/rudderlabs/rudder-server/commit/8aefa7a7fd0b1d54238a9964d85047a8d17401a0))
* **deps:** bump github.com/prometheus/common from 0.42.0 to 0.43.0 ([#3293](https://github.com/rudderlabs/rudder-server/issues/3293)) ([a28984a](https://github.com/rudderlabs/rudder-server/commit/a28984a96b5550f3a3e79550f09d813f262a4a8b))
* **deps:** bump github.com/rudderlabs/rudder-go-kit from 0.13.0 to 0.13.1 ([#3284](https://github.com/rudderlabs/rudder-server/issues/3284)) ([a28984a](https://github.com/rudderlabs/rudder-server/commit/a28984a96b5550f3a3e79550f09d813f262a4a8b))
* **deps:** bump github.com/rudderlabs/rudder-go-kit from 0.13.1 to 0.13.3 ([#3296](https://github.com/rudderlabs/rudder-server/issues/3296)) ([a28984a](https://github.com/rudderlabs/rudder-server/commit/a28984a96b5550f3a3e79550f09d813f262a4a8b))
* **deps:** bump github.com/segmentio/kafka-go from 0.4.39 to 0.4.40 ([#3294](https://github.com/rudderlabs/rudder-server/issues/3294)) ([a28984a](https://github.com/rudderlabs/rudder-server/commit/a28984a96b5550f3a3e79550f09d813f262a4a8b))
* **deps:** bump go.etcd.io/etcd/client/v3 from 3.5.8 to 3.5.9 ([#3323](https://github.com/rudderlabs/rudder-server/issues/3323)) ([c54f7d4](https://github.com/rudderlabs/rudder-server/commit/c54f7d496aaec106384a2d4f1a4b23c277a0a78f))
* **deps:** bump golang.org/x/oauth2 from 0.7.0 to 0.8.0 ([#3300](https://github.com/rudderlabs/rudder-server/issues/3300)) ([a28984a](https://github.com/rudderlabs/rudder-server/commit/a28984a96b5550f3a3e79550f09d813f262a4a8b))
* **deps:** bump golang.org/x/sync from 0.1.0 to 0.2.0 ([#3301](https://github.com/rudderlabs/rudder-server/issues/3301)) ([a28984a](https://github.com/rudderlabs/rudder-server/commit/a28984a96b5550f3a3e79550f09d813f262a4a8b))
* **deps:** bump golang.org/x/sync from 0.1.0 to 0.2.0 ([#3301](https://github.com/rudderlabs/rudder-server/issues/3301)) ([feb07aa](https://github.com/rudderlabs/rudder-server/commit/feb07aacc34f9bc0386728d900da441684ce52fd))
* **deps:** bump google.golang.org/api from 0.120.0 to 0.121.0 ([#3286](https://github.com/rudderlabs/rudder-server/issues/3286)) ([a28984a](https://github.com/rudderlabs/rudder-server/commit/a28984a96b5550f3a3e79550f09d813f262a4a8b))
* **deps:** bump google.golang.org/api from 0.121.0 to 0.122.0 ([#3310](https://github.com/rudderlabs/rudder-server/issues/3310)) ([d5a506a](https://github.com/rudderlabs/rudder-server/commit/d5a506a4743df6910a375fc7802eb317649c88eb))
* **deps:** bump google.golang.org/grpc from 1.54.0 to 1.55.0 ([#3283](https://github.com/rudderlabs/rudder-server/issues/3283)) ([a28984a](https://github.com/rudderlabs/rudder-server/commit/a28984a96b5550f3a3e79550f09d813f262a4a8b))
* fix flaky test ([#3325](https://github.com/rudderlabs/rudder-server/issues/3325)) ([22f2510](https://github.com/rudderlabs/rudder-server/commit/22f251062ad7225196eb4ef4773dffe25278a20b))
* go-kit v0.13.5 ([#3375](https://github.com/rudderlabs/rudder-server/issues/3375)) ([7352e64](https://github.com/rudderlabs/rudder-server/commit/7352e640813d448727404c7e0816945b36c296fe))
* include error message in error log ([#3348](https://github.com/rudderlabs/rudder-server/issues/3348)) ([bb01437](https://github.com/rudderlabs/rudder-server/commit/bb01437d77b8bac180b27bad3fdeea6d4418a517))
* increase default jobsdb operation timeouts ([#3172](https://github.com/rudderlabs/rudder-server/issues/3172)) ([a28984a](https://github.com/rudderlabs/rudder-server/commit/a28984a96b5550f3a3e79550f09d813f262a4a8b))
* **jobsdb:** granular table count and cache hit stats ([#3372](https://github.com/rudderlabs/rudder-server/issues/3372)) ([a481131](https://github.com/rudderlabs/rudder-server/commit/a481131eb0eddfa504d16cff591e22725ae72fe3))
* moved from gorilla to chi ([#3263](https://github.com/rudderlabs/rudder-server/issues/3263)) ([57231be](https://github.com/rudderlabs/rudder-server/commit/57231befe58b30ebba2b3a64ca1a60f8d749a679))
* rudder-go-kit v0.13.4 ([#3365](https://github.com/rudderlabs/rudder-server/issues/3365)) ([dfb8745](https://github.com/rudderlabs/rudder-server/commit/dfb8745166d5ecfb54a89398bfc15e5affe4ff8f))
* upgraded chi v1 middleware to v5 ([#3353](https://github.com/rudderlabs/rudder-server/issues/3353)) ([a1b37d1](https://github.com/rudderlabs/rudder-server/commit/a1b37d1e839cb0a6cb42cc6efd93c6c0f0c008ab))
* warehouse integration test improvements ([#3264](https://github.com/rudderlabs/rudder-server/issues/3264)) ([4f8f54d](https://github.com/rudderlabs/rudder-server/commit/4f8f54d16ccd4395b3b13b834f509aaabef032c9))

## [1.8.5](https://github.com/rudderlabs/rudder-server/compare/v1.8.4...v1.8.5) (2023-05-17)


### Bug Fixes

* max connections for warehouse slaves ([#3341](https://github.com/rudderlabs/rudder-server/issues/3341)) ([b355eb2](https://github.com/rudderlabs/rudder-server/commit/b355eb2e87aaf7985e1d225f9b24a7ff89b3c33b))

## [1.8.4](https://github.com/rudderlabs/rudder-server/compare/v1.8.3...v1.8.4) (2023-05-17)


### Bug Fixes

* include source definition type when destination has UT ([#3338](https://github.com/rudderlabs/rudder-server/issues/3338)) ([3343017](https://github.com/rudderlabs/rudder-server/commit/334301754f2862a75ab1b633475538cbb435e186))

## [1.8.3](https://github.com/rudderlabs/rudder-server/compare/v1.8.2...v1.8.3) (2023-05-12)


### Miscellaneous

* add toggle for backendconfig db caching ([#3320](https://github.com/rudderlabs/rudder-server/issues/3320)) ([e75fea0](https://github.com/rudderlabs/rudder-server/commit/e75fea005243e4c55127ac2c7a51bcd30ff20fa8))

## [1.8.2](https://github.com/rudderlabs/rudder-server/compare/v1.8.1...v1.8.2) (2023-05-10)


### Bug Fixes

* stash loop not sleeping when no jobs are present ([#3312](https://github.com/rudderlabs/rudder-server/issues/3312)) ([b446ae7](https://github.com/rudderlabs/rudder-server/commit/b446ae756fa96186326f2c1cf14c4756c50a64cd))

## [1.8.1](https://github.com/rudderlabs/rudder-server/compare/v1.8.0...v1.8.1) (2023-05-10)


### Bug Fixes

* kafka secret comes first ([#3307](https://github.com/rudderlabs/rudder-server/issues/3307)) ([757f608](https://github.com/rudderlabs/rudder-server/commit/757f6085d89ee08599e28c2218e110403d303f54))

## [1.8.0](https://github.com/rudderlabs/rudder-server/compare/v1.7.0...v1.8.0) (2023-05-09)


### Features

* databricks compatible go driver ([#3148](https://github.com/rudderlabs/rudder-server/issues/3148)) ([7660520](https://github.com/rudderlabs/rudder-server/commit/76605201e3af7db8b9497475896412be13b602bf))
* **destination:** support event filtering in a scalable way for hybrid & cloud modes ([#3188](https://github.com/rudderlabs/rudder-server/issues/3188)) ([2c2c0e1](https://github.com/rudderlabs/rudder-server/commit/2c2c0e1d305dfecc2bc141ede0e01e58b7d918d3))
* embed avro schema id ([#3118](https://github.com/rudderlabs/rudder-server/issues/3118)) ([8c98631](https://github.com/rudderlabs/rudder-server/commit/8c98631f30d5bd54d9cc9adfbee0e805ff87403b))
* kafka compression ([#3179](https://github.com/rudderlabs/rudder-server/issues/3179)) ([acb3918](https://github.com/rudderlabs/rudder-server/commit/acb3918cd1282825c0cc0a17a208cbd565a839c1))
* modifed kafka to use config received from config-be. ([#3205](https://github.com/rudderlabs/rudder-server/issues/3205)) ([0d1c75f](https://github.com/rudderlabs/rudder-server/commit/0d1c75f6e70b365701558660e5f81043b60d6d0e))
* **processor:** enhance reports to hold transformation and tracking plan metrics ([#3138](https://github.com/rudderlabs/rudder-server/issues/3138)) ([865ad6c](https://github.com/rudderlabs/rudder-server/commit/865ad6c0814c317a4ea2f952b4c24026c570b435))
* **processor:** forward events to new events-schema jobsDB ([#3201](https://github.com/rudderlabs/rudder-server/issues/3201)) ([5a69ce9](https://github.com/rudderlabs/rudder-server/commit/5a69ce97860cb51b20b181e73e35254c87ba4933))
* **router:** limit number of jobs with the same ordering key in worker's queue ([#3243](https://github.com/rudderlabs/rudder-server/issues/3243)) ([ed5722d](https://github.com/rudderlabs/rudder-server/commit/ed5722df3f95e2ce897209ab043d6fbb760f5308))
* rudder-server modification to use suppression-backup service. ([#3116](https://github.com/rudderlabs/rudder-server/issues/3116)) ([daf3e26](https://github.com/rudderlabs/rudder-server/commit/daf3e26f9351607e8ee841213b189b4286f592cf))
* schema forwarder ([#3146](https://github.com/rudderlabs/rudder-server/issues/3146)) ([9accebf](https://github.com/rudderlabs/rudder-server/commit/9accebf25c86eefd1f3b5ccee368e6ce61a7fe78))
* source level isolation at processor ([#3187](https://github.com/rudderlabs/rudder-server/issues/3187)) ([a3f687e](https://github.com/rudderlabs/rudder-server/commit/a3f687e8eed7a6f6cb32d20d71318986301c0da1))
* suppression backup service ([#2910](https://github.com/rudderlabs/rudder-server/issues/2910)) ([5b68307](https://github.com/rudderlabs/rudder-server/commit/5b6830781b486fffc864a851814419b795593351))
* update transformer image to rudderstack org ([#3115](https://github.com/rudderlabs/rudder-server/issues/3115)) ([8f74bbb](https://github.com/rudderlabs/rudder-server/commit/8f74bbb55fdb2d172352e2dbe54c9f64e8915edd))
* **warehouse:** middleware for sql warehouse integrations queries ([#3190](https://github.com/rudderlabs/rudder-server/issues/3190)) ([85f064a](https://github.com/rudderlabs/rudder-server/commit/85f064a35c6a6767ec2f0a867985d681bb8ae955))
* **warehouse:** warehouse schema repository ([#3087](https://github.com/rudderlabs/rudder-server/issues/3087)) ([7becd87](https://github.com/rudderlabs/rudder-server/commit/7becd873521f40ffc2f5405c52815ae076c9e9c4))


### Bug Fixes

* add externalID to S3 file uploader config ([#3153](https://github.com/rudderlabs/rudder-server/issues/3153)) ([d7ac3a8](https://github.com/rudderlabs/rudder-server/commit/d7ac3a809d9023f0ddf7e20ed8c680369deec3b9))
* add mandatory env for shopify oss ([#3227](https://github.com/rudderlabs/rudder-server/issues/3227)) ([460890d](https://github.com/rudderlabs/rudder-server/commit/460890d29ab4d897faca18da0717dae6fac24aba))
* add numeric scale precision to snowflake ([#3239](https://github.com/rudderlabs/rudder-server/issues/3239)) ([ef127ef](https://github.com/rudderlabs/rudder-server/commit/ef127ef360d5195463fd63cef777c433e4527edb))
* controller conn ([#3142](https://github.com/rudderlabs/rudder-server/issues/3142)) ([f65a67a](https://github.com/rudderlabs/rudder-server/commit/f65a67ac94fbd22d50b5e2dc0213368668d1a4dd))
* databricks retries ([#3305](https://github.com/rudderlabs/rudder-server/issues/3305)) ([6cacb2e](https://github.com/rudderlabs/rudder-server/commit/6cacb2ee0a6237345ff7302fb86483a631cde46c))
* error messages map !initialized in gateway ([#3191](https://github.com/rudderlabs/rudder-server/issues/3191)) ([03cd911](https://github.com/rudderlabs/rudder-server/commit/03cd911ddb9591192bed3d725e4f4165291a8fa4))
* identity tables load files are not generated ([0aab933](https://github.com/rudderlabs/rudder-server/commit/0aab933f0ab612965748780caf243e1a8d094ebb))
* illegal job order with maxRetriesLimitReached ([#3163](https://github.com/rudderlabs/rudder-server/issues/3163)) ([f328e43](https://github.com/rudderlabs/rudder-server/commit/f328e43a68147c56fe8f446cf9d1e3614be8363b))
* kafka with CP admin client ([#3297](https://github.com/rudderlabs/rudder-server/issues/3297)) ([c9fcb8c](https://github.com/rudderlabs/rudder-server/commit/c9fcb8c6c0925691f7a8e782114d71ca22fa84e2))
* no need for setup during warehouse crash recovery ([#3203](https://github.com/rudderlabs/rudder-server/issues/3203)) ([1f7d542](https://github.com/rudderlabs/rudder-server/commit/1f7d5428bbf66ba4a19271b3e91825247ec5e9e4))
* postgres support for loading users and identifies in same transactions ([#3237](https://github.com/rudderlabs/rudder-server/issues/3237)) ([2c3697a](https://github.com/rudderlabs/rudder-server/commit/2c3697a62ccb4dfcd2e1c7a70fba65b523921975))
* redshift staging table creation ([#3180](https://github.com/rudderlabs/rudder-server/issues/3180)) ([8ec737d](https://github.com/rudderlabs/rudder-server/commit/8ec737dd5a0474407ac8f67f79242dd4e2f8f1f3))
* remove logging in messages potentially leaking creds ([#3195](https://github.com/rudderlabs/rudder-server/issues/3195)) ([04a31a0](https://github.com/rudderlabs/rudder-server/commit/04a31a0ce984006c3fad574555a4ecfea053d508))
* roleBasedAuth parameter wasn't being set correctly ([#3134](https://github.com/rudderlabs/rudder-server/issues/3134)) ([c2f6dd8](https://github.com/rudderlabs/rudder-server/commit/c2f6dd8ec8c647e95f9b3b0d9b0e0dd89d3f9c2c))
* stash loop is not sleeping even though query limits are not being reached ([#3291](https://github.com/rudderlabs/rudder-server/issues/3291)) ([5a0947e](https://github.com/rudderlabs/rudder-server/commit/5a0947ee9f853e7b2b449126c7f07d9edd3f457b))
* stats come first ([#3159](https://github.com/rudderlabs/rudder-server/issues/3159)) ([c7210fa](https://github.com/rudderlabs/rudder-server/commit/c7210fa0a80383a4b22efca18a014ddb2bba2e7b))
* update lo to slices in eventfilter package ([#3238](https://github.com/rudderlabs/rudder-server/issues/3238)) ([0a91574](https://github.com/rudderlabs/rudder-server/commit/0a9157422e7670a28906119007e61f1aa1aab4a8))
* **warehouse:** all staging files processing failed ([#3137](https://github.com/rudderlabs/rudder-server/issues/3137)) ([9dc4b8f](https://github.com/rudderlabs/rudder-server/commit/9dc4b8f44e9913e1d34b41a625af4e29043cb960))
* **warehouse:** table uploads were not getting updated ([6dec777](https://github.com/rudderlabs/rudder-server/commit/6dec777f3d0cc51d069c1392baed0ac3c2fe4a6d))


### Miscellaneous

* access denied for s3 datalake ([#3186](https://github.com/rudderlabs/rudder-server/issues/3186)) ([29c818a](https://github.com/rudderlabs/rudder-server/commit/29c818ac206fce60dc5344a58f2becabd4483682))
* add metrics for file sizes for badgerDB ([#3092](https://github.com/rudderlabs/rudder-server/issues/3092)) ([f659b49](https://github.com/rudderlabs/rudder-server/commit/f659b49d1171fe02246370135a3349b71a78659d))
* add sql middleware to for warehouse ([#3212](https://github.com/rudderlabs/rudder-server/issues/3212)) ([b8bc531](https://github.com/rudderlabs/rudder-server/commit/b8bc53132c3f50727b5e18ef0dab6308fed9c145))
* added postgres legacy and new implementation tests ([#3216](https://github.com/rudderlabs/rudder-server/issues/3216)) ([e22428c](https://github.com/rudderlabs/rudder-server/commit/e22428c19ab83b0ce92f47015cfb88ecd259ff59))
* added threshold for commit for sqlwrapper ([#3273](https://github.com/rudderlabs/rudder-server/issues/3273)) ([a4f3b56](https://github.com/rudderlabs/rudder-server/commit/a4f3b56d5b90f42a2f3e68cb94a5a41f21146dc3))
* added transactions to sqlquerywrapper ([#3231](https://github.com/rudderlabs/rudder-server/issues/3231)) ([bba4894](https://github.com/rudderlabs/rudder-server/commit/bba48948b5e7e68bac791f3ce4e8a83fcc1e4be4))
* adding BeginTx to sqlwrapper ([#3260](https://github.com/rudderlabs/rudder-server/issues/3260)) ([d4b30d6](https://github.com/rudderlabs/rudder-server/commit/d4b30d6506a3f7fa5b5dc0452d9593286f99e99b))
* addressing snyk vulnerabilities ([#3224](https://github.com/rudderlabs/rudder-server/issues/3224)) ([9f5dc99](https://github.com/rudderlabs/rudder-server/commit/9f5dc99325279ad672010e2a56918d7a09470d6f))
* bigquery query rate limited ([#3185](https://github.com/rudderlabs/rudder-server/issues/3185)) ([60437df](https://github.com/rudderlabs/rudder-server/commit/60437df5913edbe96ea032bf058077aa083644a1))
* bq column count error mappings ([#3184](https://github.com/rudderlabs/rudder-server/issues/3184)) ([0b47f65](https://github.com/rudderlabs/rudder-server/commit/0b47f6542d8b17474bb75c3bb1e2db87bb7538bb))
* bump github.com/rudderlabs/sql-tunnels from 0.1.2 to 0.1.3 ([#3225](https://github.com/rudderlabs/rudder-server/issues/3225)) ([698fe5e](https://github.com/rudderlabs/rudder-server/commit/698fe5e925361cf45c3c3aaad2a7b9823104d1c1))
* bump kafka-go and gosnowflake ([#3217](https://github.com/rudderlabs/rudder-server/issues/3217)) ([1b4698e](https://github.com/rudderlabs/rudder-server/commit/1b4698e3aad3dcc80b1c87dbab9f112684ae52aa))
* change stash defaults ([#3136](https://github.com/rudderlabs/rudder-server/issues/3136)) ([e2f019d](https://github.com/rudderlabs/rudder-server/commit/e2f019d8f833e418af874451d180e8fee9fb5b44))
* cleanup of unnecessary connection-tester ([#3208](https://github.com/rudderlabs/rudder-server/issues/3208)) ([4204f7a](https://github.com/rudderlabs/rudder-server/commit/4204f7a65eb8e45547b1098b340a41975a18d0cc))
* cleanup warehouse handlers ([#3211](https://github.com/rudderlabs/rudder-server/issues/3211)) ([f65f0e7](https://github.com/rudderlabs/rudder-server/commit/f65f0e76ea9c3eef39f0483c8a026614004cdab1))
* deltalake parquet support ([#3135](https://github.com/rudderlabs/rudder-server/issues/3135)) ([f54b9fd](https://github.com/rudderlabs/rudder-server/commit/f54b9fd577fcc7a3589bfd7d5106936783188e10))
* **deps:** bump cloud.google.com/go/bigquery from 1.46.0 to 1.50.0 ([#3171](https://github.com/rudderlabs/rudder-server/issues/3171)) ([07cde26](https://github.com/rudderlabs/rudder-server/commit/07cde26dc9312290cc3d73e2ac24f31490b6e80b))
* **deps:** bump cloud.google.com/go/pubsub from 1.30.0 to 1.30.1 ([#3277](https://github.com/rudderlabs/rudder-server/issues/3277)) ([c9d12c1](https://github.com/rudderlabs/rudder-server/commit/c9d12c1df21a83d462508dfbb9c656218cd1b7c1))
* **deps:** bump github.com/aws/aws-sdk-go from 1.44.245 to 1.44.246 ([#3229](https://github.com/rudderlabs/rudder-server/issues/3229)) ([cae0093](https://github.com/rudderlabs/rudder-server/commit/cae00931e9e0a12ee6a4e9ad2391dddf136cd03f))
* **deps:** bump github.com/aws/aws-sdk-go from 1.44.246 to 1.44.252 ([#3254](https://github.com/rudderlabs/rudder-server/issues/3254)) ([42ee1b5](https://github.com/rudderlabs/rudder-server/commit/42ee1b502f1db183ff645baf0a73baf1c671657e))
* **deps:** bump github.com/aws/aws-sdk-go from 1.44.252 to 1.44.253 ([#3258](https://github.com/rudderlabs/rudder-server/issues/3258)) ([be1fef9](https://github.com/rudderlabs/rudder-server/commit/be1fef98fd9751111eb5c849258342817b299a4d))
* **deps:** bump github.com/aws/aws-sdk-go from 1.44.253 to 1.44.254 ([#3261](https://github.com/rudderlabs/rudder-server/issues/3261)) ([0f87463](https://github.com/rudderlabs/rudder-server/commit/0f8746308d4d1412c9f36c8bbe95642e428eea73))
* **deps:** bump github.com/aws/aws-sdk-go from 1.44.254 to 1.44.256 ([#3276](https://github.com/rudderlabs/rudder-server/issues/3276)) ([804ec2c](https://github.com/rudderlabs/rudder-server/commit/804ec2c692cc10a285186286fc3bcefdeee68820))
* **deps:** bump github.com/databricks/databricks-sql-go from 1.1.0 to 1.2.0 ([#3234](https://github.com/rudderlabs/rudder-server/issues/3234)) ([7b7cfe7](https://github.com/rudderlabs/rudder-server/commit/7b7cfe7508974e7b16ebf1abdf05af31dcdf4d64))
* **deps:** bump github.com/docker/docker from 20.10.21+incompatible to 23.0.3+incompatible ([#3167](https://github.com/rudderlabs/rudder-server/issues/3167)) ([0600834](https://github.com/rudderlabs/rudder-server/commit/06008346a0b305c25b1303f2d6ff91586c2812c6))
* **deps:** bump github.com/lib/pq from 1.10.8 to 1.10.9 ([#3247](https://github.com/rudderlabs/rudder-server/issues/3247)) ([15baff9](https://github.com/rudderlabs/rudder-server/commit/15baff9439b2b99070b5c774492f58c4f9dc8a02))
* **deps:** bump github.com/onsi/ginkgo/v2 from 2.9.2 to 2.9.4 ([#3278](https://github.com/rudderlabs/rudder-server/issues/3278)) ([114f710](https://github.com/rudderlabs/rudder-server/commit/114f710cec0dedb0e2d4ae0255495e37409dfe18))
* **deps:** bump github.com/onsi/gomega from 1.27.1 to 1.27.6 ([#3213](https://github.com/rudderlabs/rudder-server/issues/3213)) ([48e6913](https://github.com/rudderlabs/rudder-server/commit/48e6913e630e198cf9139e6042a0a7b90f48a463))
* **deps:** bump github.com/opencontainers/runc from 1.1.4 to 1.1.5 ([#3140](https://github.com/rudderlabs/rudder-server/issues/3140)) ([e3bd948](https://github.com/rudderlabs/rudder-server/commit/e3bd94844d88df7e249d6577df7a5d7b5febe664))
* **deps:** bump github.com/rudderlabs/sql-tunnels from 0.1.2 to 0.1.3 ([698fe5e](https://github.com/rudderlabs/rudder-server/commit/698fe5e925361cf45c3c3aaad2a7b9823104d1c1))
* **deps:** bump github.com/samber/lo from 1.37.0 to 1.38.1 ([#3194](https://github.com/rudderlabs/rudder-server/issues/3194)) ([d66b63f](https://github.com/rudderlabs/rudder-server/commit/d66b63f639942610e6390cf8f620e786a6795a89))
* **deps:** bump github.com/shirou/gopsutil/v3 from 3.23.1 to 3.23.3 ([#3170](https://github.com/rudderlabs/rudder-server/issues/3170)) ([e9bb277](https://github.com/rudderlabs/rudder-server/commit/e9bb2771f0e53eba5d66bf8d04023a4abab5538f))
* **deps:** bump github.com/shirou/gopsutil/v3 from 3.23.3 to 3.23.4 ([#3259](https://github.com/rudderlabs/rudder-server/issues/3259)) ([4eb3a64](https://github.com/rudderlabs/rudder-server/commit/4eb3a6454b61c52e4820ad4a5ac9281d77f554f6))
* **deps:** bump github.com/stretchr/testify from 1.8.1 to 1.8.2 ([#3082](https://github.com/rudderlabs/rudder-server/issues/3082)) ([109b652](https://github.com/rudderlabs/rudder-server/commit/109b6526d816d03fa55287d8e62549a2957fa24a))
* **deps:** bump github.com/urfave/cli/v2 from 2.25.1 to 2.25.3 ([#3262](https://github.com/rudderlabs/rudder-server/issues/3262)) ([b1bab9a](https://github.com/rudderlabs/rudder-server/commit/b1bab9a94aceedd3f6fd98a1c5868d3609d16db1))
* **deps:** bump go.uber.org/automaxprocs from 1.5.1 to 1.5.2 ([#3193](https://github.com/rudderlabs/rudder-server/issues/3193)) ([833a4f7](https://github.com/rudderlabs/rudder-server/commit/833a4f7dfc10e1fd9f171476ef9c22e912ae3d80))
* **deps:** bump golang.org/x/oauth2 from 0.6.0 to 0.7.0 ([#3196](https://github.com/rudderlabs/rudder-server/issues/3196)) ([10b62ea](https://github.com/rudderlabs/rudder-server/commit/10b62eabcde2d210cc225fad9c068424b99f3388))
* **deps:** bump google.golang.org/api from 0.110.0 to 0.116.0 ([#3177](https://github.com/rudderlabs/rudder-server/issues/3177)) ([4227912](https://github.com/rudderlabs/rudder-server/commit/42279124fb85f62c902ea25e733eb612e8fabea6))
* **deps:** bump google.golang.org/api from 0.118.0 to 0.120.0 ([#3246](https://github.com/rudderlabs/rudder-server/issues/3246)) ([a05ae14](https://github.com/rudderlabs/rudder-server/commit/a05ae1402568a5e6d396c3ebd004d18df9946614))
* disable KeepAlives by default in transformer HTTP clients ([#3147](https://github.com/rudderlabs/rudder-server/issues/3147)) ([6c308c4](https://github.com/rudderlabs/rudder-server/commit/6c308c45155f63db180d682df7385172bf4ce5c8))
* enable backoff when event ordering is disabled ([#3121](https://github.com/rudderlabs/rudder-server/issues/3121)) ([bbc14c1](https://github.com/rudderlabs/rudder-server/commit/bbc14c1c2d5bce82f13e8c179c7aa8a430ffa797))
* enable setting of `embedAvroSchemaID` from config ([#3282](https://github.com/rudderlabs/rudder-server/issues/3282)) ([e2d729d](https://github.com/rudderlabs/rudder-server/commit/e2d729d4200345b07b85f1509e4094f74cac0e8d))
* error enrichment for redshift ([#3182](https://github.com/rudderlabs/rudder-server/issues/3182)) ([3e8a962](https://github.com/rudderlabs/rudder-server/commit/3e8a962a54947be3519798fc287be34eb3f5f4ee))
* fix deadlock issue uploader ([#3272](https://github.com/rudderlabs/rudder-server/issues/3272)) ([71b5ac8](https://github.com/rudderlabs/rudder-server/commit/71b5ac8f388645396af29da30f9e8fd3cc5e1923))
* fix typecast panic ([#3269](https://github.com/rudderlabs/rudder-server/issues/3269)) ([e0280e4](https://github.com/rudderlabs/rudder-server/commit/e0280e4bf35323844fc0436a56f4803a739b8cd7))
* improve pending events api ([#3253](https://github.com/rudderlabs/rudder-server/issues/3253)) ([3704619](https://github.com/rudderlabs/rudder-server/commit/3704619cd2cac41f02d05fc945dc7d73de9dfcb3))
* initiate db handle warehouse slave ([#3270](https://github.com/rudderlabs/rudder-server/issues/3270)) ([bb897b4](https://github.com/rudderlabs/rudder-server/commit/bb897b4cb8857db0d885865775e497e30cc78ef4))
* kafka batching ([#3151](https://github.com/rudderlabs/rudder-server/issues/3151)) ([cdbbc7f](https://github.com/rudderlabs/rudder-server/commit/cdbbc7f1cb929a7221c1181ee7d3f47cbce09673))
* kit v0.12.0 ([#3209](https://github.com/rudderlabs/rudder-server/issues/3209)) ([1475ece](https://github.com/rudderlabs/rudder-server/commit/1475ece547a917f140e935f7ad1afe7ca419159e))
* lock bitnami test image versions ([#3232](https://github.com/rudderlabs/rudder-server/issues/3232)) ([7087f63](https://github.com/rudderlabs/rudder-server/commit/7087f636488f4c31505de37adc5a74248cf30f87))
* otel buckets ([#3120](https://github.com/rudderlabs/rudder-server/issues/3120)) ([803ac03](https://github.com/rudderlabs/rudder-server/commit/803ac032c151ad5f119f36d5ef16d51641111f09))
* refactor regulation worker integration test ([#3230](https://github.com/rudderlabs/rudder-server/issues/3230)) ([381ca72](https://github.com/rudderlabs/rudder-server/commit/381ca720c1bed91459679d42ca906ea9050e45dd))
* remove global pkglogger from warehouse integrations ([#3202](https://github.com/rudderlabs/rudder-server/issues/3202)) ([1211e51](https://github.com/rudderlabs/rudder-server/commit/1211e51aef0f7567d257e671997c76463e750da9))
* remove jobsdb status handler ([#3192](https://github.com/rudderlabs/rudder-server/issues/3192)) ([73c295a](https://github.com/rudderlabs/rudder-server/commit/73c295a4fd747604f8429186018bc8d318326baf))
* remove readonlyjobsdb and some status handlers ([#3168](https://github.com/rudderlabs/rudder-server/issues/3168)) ([7d82354](https://github.com/rudderlabs/rudder-server/commit/7d82354fa539399282dd29720711e6fec156061d))
* replace readme header link with slack link ([#3183](https://github.com/rudderlabs/rudder-server/issues/3183)) ([db9fdc5](https://github.com/rudderlabs/rudder-server/commit/db9fdc5afb3a7342a0ab54fb7793222b5affbc1d))
* restore sample configuration files ([#3122](https://github.com/rudderlabs/rudder-server/issues/3122)) ([0ac3a6e](https://github.com/rudderlabs/rudder-server/commit/0ac3a6e0cd24fce8efb13e9bd2fbd799ee15f300))
* **router:** don't include job in metadata during transformation requests ([#3111](https://github.com/rudderlabs/rudder-server/issues/3111)) ([d2910d2](https://github.com/rudderlabs/rudder-server/commit/d2910d2babbb5b2ed62dc610d407aa2f186e0c99))
* **router:** skip full workers during the generator loop ([#3206](https://github.com/rudderlabs/rudder-server/issues/3206)) ([bed02f7](https://github.com/rudderlabs/rudder-server/commit/bed02f7caef90839b231ec5e2477501d2357066f))
* start server in normal mode when there is no instruction by scheduler ([#3103](https://github.com/rudderlabs/rudder-server/issues/3103)) ([bad6a82](https://github.com/rudderlabs/rudder-server/commit/bad6a82857edc2b206c62bc466f08c3167f61aa6))
* stop warehouse panics ([#3105](https://github.com/rudderlabs/rudder-server/issues/3105)) ([7becd87](https://github.com/rudderlabs/rudder-server/commit/7becd873521f40ffc2f5405c52815ae076c9e9c4))
* updating error mappings ([#3150](https://github.com/rudderlabs/rudder-server/issues/3150)) ([0851cb9](https://github.com/rudderlabs/rudder-server/commit/0851cb9e31736c441b0d32e5942f8f8fe325cfe1))
* upgrade direct dependencies ([#3223](https://github.com/rudderlabs/rudder-server/issues/3223)) ([309727d](https://github.com/rudderlabs/rudder-server/commit/309727d7fdfaf9ab9d94ddc83ccfb95c521cf1ae))
* upgrading workflow to use action v3. ([#3164](https://github.com/rudderlabs/rudder-server/issues/3164)) ([f7cf6c9](https://github.com/rudderlabs/rudder-server/commit/f7cf6c96f23538df81b29e039af76d7af01dab67))
* use rudder-go-kit ([#3104](https://github.com/rudderlabs/rudder-server/issues/3104)) ([7becd87](https://github.com/rudderlabs/rudder-server/commit/7becd873521f40ffc2f5405c52815ae076c9e9c4))
* use the common PAT for release-please ([#3204](https://github.com/rudderlabs/rudder-server/issues/3204)) ([14e3b91](https://github.com/rudderlabs/rudder-server/commit/14e3b91fd50431de34e0644e35a32cab7173b139))
* **warehouse:** cleanup for test connection ([#3226](https://github.com/rudderlabs/rudder-server/issues/3226)) ([51c1ac0](https://github.com/rudderlabs/rudder-server/commit/51c1ac0a767738fd6e4f15251830e07a51d0f6c3))
* **warehouse:** snowflake application identifier ([#3124](https://github.com/rudderlabs/rudder-server/issues/3124)) ([cdff792](https://github.com/rudderlabs/rudder-server/commit/cdff792dee438d9981d30e662a1ff2715227361c))

## [1.7.3](https://github.com/rudderlabs/rudder-server/compare/v1.7.2...v1.7.3) (2023-04-04)


### Bug Fixes

* add externalID to S3 file uploader config ([#3153](https://github.com/rudderlabs/rudder-server/issues/3153)) ([355dda2](https://github.com/rudderlabs/rudder-server/commit/355dda276470721c39b9a729f4ae019db9c64135))

## [1.7.2](https://github.com/rudderlabs/rudder-server/compare/v1.7.1...v1.7.2) (2023-03-30)


### Bug Fixes

* identity tables load files are not generated ([be569c1](https://github.com/rudderlabs/rudder-server/commit/be569c13db4f34286c05295dfffdfcc95c9d3863))

## [1.7.1](https://github.com/rudderlabs/rudder-server/compare/v1.7.0...v1.7.1) (2023-03-24)


### Bug Fixes

* **warehouse:** table uploads were not getting updated ([abb8bfb](https://github.com/rudderlabs/rudder-server/commit/abb8bfb3c1ed77584d75c50e4bbc2f305aa4ea69))

## [1.7.0](https://github.com/rudderlabs/rudder-server/compare/v1.6.0...v1.7.0) (2023-03-17)


### Features

* default retention period set to 7 days for rudder backups ([#3038](https://github.com/rudderlabs/rudder-server/issues/3038)) ([0d9af35](https://github.com/rudderlabs/rudder-server/commit/0d9af35aa8fcc690b4c8601ae8f59bb86fcae9ec))
* **gateway:** support new event type extract ([#2999](https://github.com/rudderlabs/rudder-server/issues/2999)) ([63dc940](https://github.com/rudderlabs/rudder-server/commit/63dc9402b3e263eeb97c62e8902a0d68718b9556))
* kafka over ssh ([#3007](https://github.com/rudderlabs/rudder-server/issues/3007)) ([99262c3](https://github.com/rudderlabs/rudder-server/commit/99262c3736d5ab3b03e5b0ad7c582d73e168f7a7))
* **warehouse:** added duplicates stats for snowflake ([#3097](https://github.com/rudderlabs/rudder-server/issues/3097)) ([0eeaeb5](https://github.com/rudderlabs/rudder-server/commit/0eeaeb5287d0481427df7bfbb13bdbc1538ed87a))
* **warehouse:** snowflake roles support. ([#3031](https://github.com/rudderlabs/rudder-server/issues/3031)) ([98a413c](https://github.com/rudderlabs/rudder-server/commit/98a413c5df1e3011b2c774d3ea4d9054eab44a72))
* **warehouse:** temp table support for postgres ([#2964](https://github.com/rudderlabs/rudder-server/issues/2964)) ([9a80f45](https://github.com/rudderlabs/rudder-server/commit/9a80f459f5289df3d99d25fc7b09ed1804bd6522))


### Bug Fixes

* enabled data race ([#3041](https://github.com/rudderlabs/rudder-server/issues/3041)) ([7136be4](https://github.com/rudderlabs/rudder-server/commit/7136be40e7cf02886c0f99053ff32c0c6996e14e))
* inconsistent and leaky retry delay logic in router ([#3002](https://github.com/rudderlabs/rudder-server/issues/3002)) ([20c8644](https://github.com/rudderlabs/rudder-server/commit/20c8644298e2d4cc3338b8aa5d2f7d9cd717ba62))
* kafka create topic ([#3102](https://github.com/rudderlabs/rudder-server/issues/3102)) ([10ccbf3](https://github.com/rudderlabs/rudder-server/commit/10ccbf332ed661a501a47afe6f7687d78fc05cff))
* limiter deadlock while trying to notify a dynamic priority item ([#3056](https://github.com/rudderlabs/rudder-server/issues/3056)) ([ade2e4d](https://github.com/rudderlabs/rudder-server/commit/ade2e4d456ec42262b037ee3839f1823561d2bee))
* minio error while creating bucket if already exists ([#3109](https://github.com/rudderlabs/rudder-server/issues/3109)) ([2abecaa](https://github.com/rudderlabs/rudder-server/commit/2abecaa6020d033e2ac8dd0a57c6badeb40e57bc))
* payload limiter gauge reports invalid value type ([#3048](https://github.com/rudderlabs/rudder-server/issues/3048)) ([828f70d](https://github.com/rudderlabs/rudder-server/commit/828f70d271832ff56a463d9618c9a02dd1f408c7))
* pending events all ([#3075](https://github.com/rudderlabs/rudder-server/issues/3075)) ([1d9f63b](https://github.com/rudderlabs/rudder-server/commit/1d9f63b3df72a038a6b1f4f5d402a04ab00befa5))
* **router:** retry backoff causing out-of-order job processing ([#3098](https://github.com/rudderlabs/rudder-server/issues/3098)) ([eda4525](https://github.com/rudderlabs/rudder-server/commit/eda4525edaa6f3af6ae05bd41a11ddb6d94ab202))
* user/anonymous id read at gateway ([#3051](https://github.com/rudderlabs/rudder-server/issues/3051)) ([828f70d](https://github.com/rudderlabs/rudder-server/commit/828f70d271832ff56a463d9618c9a02dd1f408c7))
* **warehouse:** added support for filtering on the uploads and calculating aborted events for task_run_id ([#2975](https://github.com/rudderlabs/rudder-server/issues/2975)) ([8ab58b8](https://github.com/rudderlabs/rudder-server/commit/8ab58b80914a602d786e9fb37747770f97f325f0))
* **warehouse:** grouping uploads for processing pickup ([#3039](https://github.com/rudderlabs/rudder-server/issues/3039)) ([4832630](https://github.com/rudderlabs/rudder-server/commit/4832630e58db4aeaaaf5c65c634247ad5da8e12a))
* **warehouse:** infinites retries ([#3050](https://github.com/rudderlabs/rudder-server/issues/3050)) ([d3320fa](https://github.com/rudderlabs/rudder-server/commit/d3320fa6f407d65368cbbf32278fcff5927aa7f9))
* **warehouse:** snowflake default timestamp to timestamp with time zone ([#3100](https://github.com/rudderlabs/rudder-server/issues/3100)) ([cef4a18](https://github.com/rudderlabs/rudder-server/commit/cef4a18578332a16455cde4127731c6ea0bf7ba7))
* **warehouse:** snowflakes user identifies table getting skipped ([#3113](https://github.com/rudderlabs/rudder-server/issues/3113)) ([dd626b3](https://github.com/rudderlabs/rudder-server/commit/dd626b310fe0e90c1a01eb811ddf4cacc8d2900f))


### Miscellaneous

* abort job after 5 attempts at deletion-worker ([#3012](https://github.com/rudderlabs/rudder-server/issues/3012)) ([ed83eaa](https://github.com/rudderlabs/rudder-server/commit/ed83eaac2f56a7825a7ddd0e830bab0f21b13713))
* add live events stats ([#2930](https://github.com/rudderlabs/rudder-server/issues/2930)) ([7ca1b0b](https://github.com/rudderlabs/rudder-server/commit/7ca1b0bb03bbb8499d87bcc84b7c1919f9ac69f7))
* add transformations challenge announcement ([#3072](https://github.com/rudderlabs/rudder-server/issues/3072)) ([f128b93](https://github.com/rudderlabs/rudder-server/commit/f128b931047dfb1bef0ec50d32994ab3a3028f3f))
* badgerDB improvements for debugger ([#3101](https://github.com/rudderlabs/rudder-server/issues/3101)) ([0c525d0](https://github.com/rudderlabs/rudder-server/commit/0c525d0ba9ef85decfeba3b5be5aba6a2d2dd446))
* debugger's badgerdb cache optimisations ([#3042](https://github.com/rudderlabs/rudder-server/issues/3042)) ([e6e9933](https://github.com/rudderlabs/rudder-server/commit/e6e99337b218f844bb893371246b54a3b7f710b8))
* **deps:** bump github.com/minio/minio-go/v7 from 7.0.48 to 7.0.49 ([#3018](https://github.com/rudderlabs/rudder-server/issues/3018)) ([f4ea7b3](https://github.com/rudderlabs/rudder-server/commit/f4ea7b31c0d9b8da1b8f0f71e14d10c390ae3ae2))
* **deps:** bump github.com/onsi/ginkgo/v2 from 2.1.6 to 2.9.0 ([#3068](https://github.com/rudderlabs/rudder-server/issues/3068)) ([6bdbb7a](https://github.com/rudderlabs/rudder-server/commit/6bdbb7aa3d79d06bce27a230d32ee4af2bc05b89))
* **deps:** bump github.com/prometheus/common from 0.37.0 to 0.41.0 ([#3062](https://github.com/rudderlabs/rudder-server/issues/3062)) ([bb04a8b](https://github.com/rudderlabs/rudder-server/commit/bb04a8b21af714ed7ed03d4769db6fa92b781b67))
* **deps:** bump github.com/urfave/cli/v2 from 2.20.3 to 2.25.0 ([#3067](https://github.com/rudderlabs/rudder-server/issues/3067)) ([6b429b7](https://github.com/rudderlabs/rudder-server/commit/6b429b7baa9795668960b07b2a73e56cad8fb7f9))
* **deps:** bump go.uber.org/goleak from 1.2.0 to 1.2.1 ([#3017](https://github.com/rudderlabs/rudder-server/issues/3017)) ([2eb92ca](https://github.com/rudderlabs/rudder-server/commit/2eb92ca6d89738b33830e4d31dbcb82baf31744a))
* don't include prereleases in docker latest tag ([#3006](https://github.com/rudderlabs/rudder-server/issues/3006)) ([20c8644](https://github.com/rudderlabs/rudder-server/commit/20c8644298e2d4cc3338b8aa5d2f7d9cd717ba62))
* enable pipeline level sync frequency ([#3094](https://github.com/rudderlabs/rudder-server/issues/3094)) ([ea3bbd5](https://github.com/rudderlabs/rudder-server/commit/ea3bbd502d7ed5f6f9be0e08e1a9af389589831a))
* fix namespace bug ([#3110](https://github.com/rudderlabs/rudder-server/issues/3110)) ([7b6fa35](https://github.com/rudderlabs/rudder-server/commit/7b6fa35cd688374ed64150e511704107294a8703))
* improve regulation-worker status capture ([#2837](https://github.com/rudderlabs/rudder-server/issues/2837)) ([6f1d07d](https://github.com/rudderlabs/rudder-server/commit/6f1d07df234a2a2c32cf0ecaac465a08ddfca2bc))
* increase parallel loads ([#3073](https://github.com/rudderlabs/rudder-server/issues/3073)) ([7dcc756](https://github.com/rudderlabs/rudder-server/commit/7dcc756f7fc39a7b66a69f0df42feb3957ba821c))
* **jobsdb:** omit workspaceId tag when it doesn't correspond to an actual workspace ([#3057](https://github.com/rudderlabs/rudder-server/issues/3057)) ([f936260](https://github.com/rudderlabs/rudder-server/commit/f936260589a1058025ca67ea5dda6f7425e25157))
* migrate stats to otel ([#2989](https://github.com/rudderlabs/rudder-server/issues/2989)) ([a4243de](https://github.com/rudderlabs/rudder-server/commit/a4243de1d52dff56e2e1619b00d4ba9a6291f560))
* perform rss/wss-aware cgroups memory usage calculation ([#3052](https://github.com/rudderlabs/rudder-server/issues/3052)) ([1b6af85](https://github.com/rudderlabs/rudder-server/commit/1b6af85e6410184d1f84fd57c6a76f7e30a5c97f))
* prefer using lo.BufferWithTimeout ([#2998](https://github.com/rudderlabs/rudder-server/issues/2998)) ([20c8644](https://github.com/rudderlabs/rudder-server/commit/20c8644298e2d4cc3338b8aa5d2f7d9cd717ba62))
* reduce parquet file size datalake ([#3035](https://github.com/rudderlabs/rudder-server/issues/3035)) ([4cb5907](https://github.com/rudderlabs/rudder-server/commit/4cb59075ab6430a3b4f898bb349e343d40d85944))
* remove workerID tag ([#3055](https://github.com/rudderlabs/rudder-server/issues/3055)) ([b732919](https://github.com/rudderlabs/rudder-server/commit/b7329199fc9e1db4097c6d21376deec8effb2310))
* upgrade all dependencies ([#2996](https://github.com/rudderlabs/rudder-server/issues/2996)) ([20c8644](https://github.com/rudderlabs/rudder-server/commit/20c8644298e2d4cc3338b8aa5d2f7d9cd717ba62))
* use gcra rate limiter gateway ([#3086](https://github.com/rudderlabs/rudder-server/issues/3086)) ([61d9275](https://github.com/rudderlabs/rudder-server/commit/61d927500b8262e06d92d08c24bb2310ca4b3cef))
* use official bitnami images for arm64 ([#3047](https://github.com/rudderlabs/rudder-server/issues/3047)) ([3aeb4f6](https://github.com/rudderlabs/rudder-server/commit/3aeb4f60139b99938476ecbdb1b786caf1f7c41c))
* use token on protoc setup to avoid rate-limit ([#3083](https://github.com/rudderlabs/rudder-server/issues/3083)) ([0f89b26](https://github.com/rudderlabs/rudder-server/commit/0f89b265f524b04f9f0445e3502b02ac25420ccc))
* use upload_id for staging files ([#3066](https://github.com/rudderlabs/rudder-server/issues/3066)) ([3ec2433](https://github.com/rudderlabs/rudder-server/commit/3ec2433b671428a7f5504af8e9cb85b2f4373c51))
* **warehouse:** added support for observability for loading tables and made dedup optional for Redshift ([#3037](https://github.com/rudderlabs/rudder-server/issues/3037)) ([63fd288](https://github.com/rudderlabs/rudder-server/commit/63fd28852af9278ebaa7e9c55ee59a1449c139f1))
* **warehouse:** added warehouse handling for s3 with glue and other improvements ([#2940](https://github.com/rudderlabs/rudder-server/issues/2940)) ([3495797](https://github.com/rudderlabs/rudder-server/commit/34957976512f658f5165d296671ee4adfc24d321))
* **warehouse:** additional error mappings ([#2994](https://github.com/rudderlabs/rudder-server/issues/2994)) ([20c8644](https://github.com/rudderlabs/rudder-server/commit/20c8644298e2d4cc3338b8aa5d2f7d9cd717ba62))
* **warehouse:** allow empty properties schema for event models ([#3095](https://github.com/rudderlabs/rudder-server/issues/3095)) ([b9deb4a](https://github.com/rudderlabs/rudder-server/commit/b9deb4abe620ca80bf64ce5b2b0b278d4e36d654))
* **warehouse:** default warehouse priority set to 100 ([#3026](https://github.com/rudderlabs/rudder-server/issues/3026)) ([20c8644](https://github.com/rudderlabs/rudder-server/commit/20c8644298e2d4cc3338b8aa5d2f7d9cd717ba62))
* **warehouse:** encoding package with readers, loaders, writers ([#3077](https://github.com/rudderlabs/rudder-server/issues/3077)) ([06c0a71](https://github.com/rudderlabs/rudder-server/commit/06c0a7179ff1f65a72ab6e518aaa3487c21ce9a5))
* **warehouse:** naming conventions ([#3076](https://github.com/rudderlabs/rudder-server/issues/3076)) ([f2e99c7](https://github.com/rudderlabs/rudder-server/commit/f2e99c7f4b56e1d595722133fc479df505e96029))
* **warehouse:** use first_event_at while pickup for warehouse processing jobs ([#3036](https://github.com/rudderlabs/rudder-server/issues/3036)) ([7aeed3b](https://github.com/rudderlabs/rudder-server/commit/7aeed3bfcf9f3679eabfb9f6689f49ba04c62ad9))
* **warehouse:** warehouse integration tests improvements ([#3059](https://github.com/rudderlabs/rudder-server/issues/3059)) ([e57362e](https://github.com/rudderlabs/rudder-server/commit/e57362e514a2731a7f9419467fc626efdc3d316f))

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
