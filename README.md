![Build Status][build status]
[![Go Report Card][goreportcardbadge]][goreportcard]
[![Release]][release]

# ![RudderStack][rudderlogo]

# What is RudderStack?

RudderStack is an **open-source**, **enterprise-ready** platform for collecting, storing and routing customer event data **securely** to your data warehouse and dozens of other tools. It provides a powerful transformation framework to process your event data on the fly.

RudderStack's backend is written in Go, with a rich UI written in React.js.

# License

RudderStack server is released under the [AGPLv3 License][agplv3_license]

# Why RudderStack?

- **Privacy and Security**: You can collect and store your customer data without sending everything to a third-party vendor. With RudderStack, you get fine-grained control over what data to forward to which analytical tool.

- **Processing Flexibility**: With RudderStack's powerful JavaScript-based event transformation framework, you can enhance or transform your event data by combining it with your other internal data. Furthermore, since RudderStack runs inside your cloud or on-premise environment, you can easily access your production data to join with the event data.

- **Unlimited Events**: Event volume-based pricing of most of the commercial systems is broken. With RudderStack, you are be able to collect as much data as possible without worrying about overrunning event budgets.

- **Stand-alone system**: RudderStack runs as a single Go binary with the only dependency being on the PostgreSQL database. There is no other dependency required to use RudderStack.

- **Seamless integration**: RudderStack currently supports integration with over 30 popular destination platforms such as Google Analytics, Amplitude, Mixpanel, Amazon S3 and more. Moreover, it also supports integration with popular data warehouses such as Snowflake, Redshift and Google BigQuery.

- **High Performance**: On a single m4.2xlarge, RudderStack can process 3000 events/second

- **Client-side SDK support**: RudderStack offers client-side SDKs for JavaScript, Android, iOS, and Unity.

# Install RudderStack

You can go through our [detailed documentation](https://docs.rudderstack.com/) to get up and running with RudderStack on your platform of choice:

- [Hosted Demo Account](https://docs.rudderstack.com/getting-started/installing-and-setting-up-rudderstack#setup-instructions-for-a-hosted-demo-account)
- [Docker](https://docs.rudderstack.com/getting-started/installing-and-setting-up-rudderstack#setup-instructions-for-docker)
- [Kubernetes](https://docs.rudderstack.com/getting-started/installing-and-setting-up-rudderstack#setup-instructions-for-kubernetes)
- [Native Installation](https://docs.rudderstack.com/getting-started/installing-and-setting-up-rudderstack#setup-instructions-for-a-native-installation)
- [Developer Machine Setup](https://docs.rudderstack.com/getting-started/installing-and-setting-up-rudderstack/developer-machine-setup)

Once you have installed and set up RudderStack, learn [how to send test events](https://docs.rudderstack.com/getting-started/installing-and-setting-up-rudderstack#how-to-send-test-events)

# Contribute

We would love to see you contribute to RudderStack. See [CONTRIBUTING.md](CONTRIBUTING.md) for more information on how to contribute.

# Stay Connected

- See the [HackerNews][hackernews] discussion around RudderStack.
- Join our [Discord][discord] channel
- Follow us [RudderStack][twitter]
- Get the latest news from the [RudderStack blog][rudderstackblog]
- Subscribe to our newsletter

<!----variable's---->

[build status]: https://codebuild.us-east-1.amazonaws.com/badges?uuid=eyJlbmNyeXB0ZWREYXRhIjoiT01EQkVPc0NBbDJLV2txTURidkRTMTNmWFRZWUY2dEtia3FRVmFXdXhWeUwzaC9aV3dsWWNNT0NwaVZKd1hKTFVMazB2cDQ5UHlaZTgvbFRER3R5SXRvPSIsIml2UGFyYW1ldGVyU3BlYyI6IktJQVMveHIzQnExZVE5b0YiLCJtYXRlcmlhbFNldFNlcmlhbCI6MX0%3D&branch=master
[release]: https://img.shields.io/github/v/release/rudderlabs/rudder-server?color=blue&sort=semver
[discord]: https://discordapp.com/invite/xNEdEGw
[twitter]: https://twitter.com/rudderstack
[goreportcard]: https://goreportcard.com/report/github.com/rudderlabs/rudder-server
[goreportcardbadge]: https://goreportcard.com/badge/github.com/rudderlabs/rudder-server
[ssh]: https://help.github.com/en/articles/which-remote-url-should-i-use#cloning-with-ssh-urls
[dashboard]: https://app.rudderlabs.com
[agplv3_license]: https://www.gnu.org/licenses/agpl-3.0-standalone.html
[sspl_license]: https://www.mongodb.com/licensing/server-side-public-license
[hackernews]: https://news.ycombinator.com/item?id=21081756
[helmscriptsgitrepo]: https://github.com/rudderlabs/rudderstack-helm
[terraformscriptsgitrepo]: https://github.com/rudderlabs/rudder-terraform
[golang]: https://golang.org/dl/
[node]: https://nodejs.org/en/download/
[ruddersdkjsgitrepo]: https://github.com/rudderlabs/rudder-sdk-js
[ruddersdkandroidgitrepo]: https://github.com/rudderlabs/rudder-sdk-android
[ruddersdkiosgitrepo]: https://github.com/rudderlabs/rudder-sdk-ios
[rudderlogo]: https://repository-images.githubusercontent.com/197743848/b352c900-dbc8-11e9-9d45-4deb9274101f
[rudderserverreleases]: https://github.com/rudderlabs/rudder-server/releases
[ruddertransformerreleases]: https://github.com/rudderlabs/rudder-transformer/releases
[rudderstackblog]: https://rudderstack.com/blog/
[rudderserversampleenv]: https://github.com/rudderlabs/rudder-server/blob/master/config/sample.env
[rudderdockeryml]: https://github.com/rudderlabs/rudder-server/blob/master/rudder-docker.yml
