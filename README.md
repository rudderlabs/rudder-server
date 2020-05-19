![Build Status][build status]
[![Release]][release]

![RudderStack](https://github.com/ameypv-rudder/rudder-server/blob/readme-update/resources/RudderStack.png)

# What is RudderStack?

[RudderStack](https://rudderstack.com/) is an **open-source Segment alternative**  for collecting, storing and routing customer event data **securely** to your data warehouse and dozens of other tools. It is **enterprise-ready**, and provides a powerful transformation framework to process your event data on the fly.

RudderStack's backend is written in Go, with a rich UI written in React.js.

You can also use the [cloud-hosted](https://app.rudderstack.com/signup?type=freetrial&utm_source=github&utm_medium=rdr-srv&utm_campaign=hosted&utm_content=intro) RudderStack instance to experience the product. Click [here](https://app.rudderstack.com/signup?type=freetrial&utm_source=github&utm_medium=rdr-srv&utm_campaign=hosted&utm_content=intro).

# Why Use RudderStack?
- **Production-ready**: Companies like **MatterMost**, **IFTTT**, **Grofers**, and **1mg** use RudderStack for collecting their events

- **Extreme Scale**: One of our largest installations currently sends **300 Million** events/day with peak of **40K** req/sec, via a multi-node RudderStack setup

- **Privacy and Security**: You can collect and store your customer data without sending everything to a third-party vendor. With RudderStack, you get fine-grained control over what data to forward to which analytical tool

- **Seamless Integration**: RudderStack currently supports integration with over 30 popular destination platforms such as Google Analytics, Amplitude, Mixpanel, Amazon S3,and more. Moreover, it also supports integration with popular data warehouses such as Snowflake, Redshift, and Google BigQuery

- **Client-side SDK Support**: RudderStack offers client-side SDKs for [JavaScript](https://docs.rudderstack.com/sdk-integration-guide/getting-started-with-javascript-sdk), [Android](https://docs.rudderstack.com/sdk-integration-guide/getting-started-with-android-sdk), [iOS](https://docs.rudderstack.com/sdk-integration-guide/getting-started-with-ios-sdk), and [Unity](https://docs.rudderstack.com/sdk-integration-guide/getting-started-with-unity-sdk)

- **User-specified Transformation**: RudderStack gives you the power to filter or transform your events before sending them to the desired destinations

# Key Features

- **Segment API Compatible**: RudderStack is Segment API and library compatible. So you don't need to change your app if you are using Segment.

- **Processing Flexibility**: With RudderStack's powerful JavaScript-based event transformation framework, you can enhance or transform your event data by combining it with your other internal data. Furthermore, since RudderStack runs inside your cloud or on-premise environment, you can easily access your production data to join with the event data.

- **Unlimited Events**: Event volume-based pricing of most of the commercial systems is broken. With RudderStack, you are be able to collect as much data as possible without worrying about overrunning event budgets.

- **Stand-alone System**: RudderStack runs as a single Go binary with the only dependency being on the PostgreSQL database. There is no other dependency required to use RudderStack.

- **High Performance**: On a single m4.2xlarge, RudderStack can process 3000 events/second.

- **Enhanced Telemetry**: To help us improve RudderStack, we collect performance and diagnostic metrics about how you use RudderStack, and how it is working. **No customer data is present in the metrics**.


# Install RudderStack

You can go through our [detailed documentation](https://docs.rudderstack.com/) to get up and running with RudderStack on your platform of choice:

- [Hosted Demo Account](https://docs.rudderstack.com/getting-started/installing-and-setting-up-rudderstack#setup-instructions-for-a-hosted-demo-account)
- [Docker](https://docs.rudderstack.com/getting-started/installing-and-setting-up-rudderstack#setup-instructions-for-docker)
- [Kubernetes](https://docs.rudderstack.com/getting-started/installing-and-setting-up-rudderstack#setup-instructions-for-kubernetes)
- [Native Installation](https://docs.rudderstack.com/getting-started/installing-and-setting-up-rudderstack#setup-instructions-for-a-native-installation)
- [Developer Machine Setup](https://docs.rudderstack.com/getting-started/installing-and-setting-up-rudderstack/developer-machine-setup)

Once you have installed RudderStack, [send test events](https://docs.rudderstack.com/getting-started/installing-and-setting-up-rudderstack#how-to-send-test-events) to verify the setup.

You can also learn more about the [RudderStack Config Generator](https://github.com/ameypv-rudder/rudder-server/wiki/RudderStack-Config-Generator) which allows you to manage your source and destination configurations without using our hosted services.

# UI Pages

### Connections Page

![Connections Page](https://gblobscdn.gitbook.com/assets%2F-Lq5Ea6fHVg3dSxMCgyQ%2F-M7WR4_0fzJ-eVn7ZeOE%2F-M7WTXFwcky90UyQmwuq%2FConnections.png?alt=media&token=99ff7da7-f172-486c-9e05-1f113ffcfeef)

### Events Page

![Events Page](https://gblobscdn.gitbook.com/assets%2F-Lq5Ea6fHVg3dSxMCgyQ%2F-M7WR4_0fzJ-eVn7ZeOE%2F-M7WU8HlfCDWIB6jHkWl%2FEvents.png?alt=media&token=c332f302-1862-45c2-bf84-dbeca07f4f82)

# License

RudderStack server is released under the [AGPLv3 License][agplv3_license].

# Contribute

We would love to see you contribute to RudderStack. Get more information on how to contribute [here](CONTRIBUTING.md).

# Wiki

For more information on RudderStack's features and functionalities, make sure you check out our [Wiki]() page

# Stay Connected

- See the [HackerNews][hackernews] discussion around RudderStack.
- Join our [Discord][discord] channel
- Follow us on [Twitter][twitter]
- Get the latest news from the [RudderStack blog][rudderstack-blog]
- Subscribe to our newsletter

<!----variables---->

[build status]: https://codebuild.us-east-1.amazonaws.com/badges?uuid=eyJlbmNyeXB0ZWREYXRhIjoiT01EQkVPc0NBbDJLV2txTURidkRTMTNmWFRZWUY2dEtia3FRVmFXdXhWeUwzaC9aV3dsWWNNT0NwaVZKd1hKTFVMazB2cDQ5UHlaZTgvbFRER3R5SXRvPSIsIml2UGFyYW1ldGVyU3BlYyI6IktJQVMveHIzQnExZVE5b0YiLCJtYXRlcmlhbFNldFNlcmlhbCI6MX0%3D&branch=master
[release]: https://img.shields.io/github/v/release/rudderlabs/rudder-server?color=blue&sort=semver
[discord]: https://discordapp.com/invite/xNEdEGw
[docs]: https://docs.rudderstack.com/
[twitter]: https://twitter.com/rudderstack
[go-report-card]: https://go-report-card.com/report/github.com/rudderlabs/rudder-server
[go-report-card-badge]: https://go-report-card.com/badge/github.com/rudderlabs/rudder-server
[ssh]: https://help.github.com/en/articles/which-remote-url-should-i-use#cloning-with-ssh-urls
[dashboard]: https://app.rudderlabs.com
[agplv3_license]: https://www.gnu.org/licenses/agpl-3.0-standalone.html
[sspl_license]: https://www.mongodb.com/licensing/server-side-public-license
[hackernews]: https://news.ycombinator.com/item?id=21081756
[helm-scripts-git-repo]: https://github.com/rudderlabs/rudderstack-helm
[terraform-scripts-git-repo]: https://github.com/rudderlabs/rudder-terraform
[golang]: https://golang.org/dl/
[node]: https://nodejs.org/en/download/
[rudder-sdk-js-git-repo]: https://github.com/rudderlabs/rudder-sdk-js
[rudder-sdk-android-git-repo]: https://github.com/rudderlabs/rudder-sdk-android
[rudder-sdk-ios-git-repo]: https://github.com/rudderlabs/rudder-sdk-ios
[config-generator]: https://github.com/rudderlabs/config-generator
[config-generator-section]: https://github.com/rudderlabs/rudder-server/blob/master/README.md#rudderstack-config-generator
[rudder-logo]: https://repository-images.githubusercontent.com/197743848/b352c900-dbc8-11e9-9d45-4deb9274101f
[rudder-server-releases]: https://github.com/rudderlabs/rudder-server/releases
[rudder-transformer-releases]: https://github.com/rudderlabs/rudder-transformer/releases
[rudderstack-blog]: https://rudderstack.com/blog/
[rudder-server-sample-env]: https://github.com/rudderlabs/rudder-server/blob/master/config/sample.env
[rudder-docker-yml]: https://github.com/rudderlabs/rudder-server/blob/master/rudder-docker.yml
