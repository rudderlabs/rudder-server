![Build Status][build status]
[![Release]][release]

# [![RudderStack](https://raw.githubusercontent.com/rudderlabs/rudder-server/master/resources/RudderStack.png)](https://rudderstack.com)

# What is RudderStack?

[RudderStack](https://rudderstack.com/) is an **open-source Segment alternative** for collecting, storing and routing customer event data **securely** to your data warehouse and dozens of other tools. It is **enterprise-ready**, and provides a powerful transformation framework to process your event data on the fly.

RudderStack's backend is written in Go, with a rich UI written in React.js.

You can also use the [cloud-hosted](https://app.rudderstack.com/signup?type=freetrial&utm_source=github&utm_medium=rdr-srv&utm_campaign=hosted&utm_content=intro) RudderStack instance to experience the product. Click [here](https://app.rudderstack.com/signup?type=freetrial&utm_source=github&utm_medium=rdr-srv&utm_campaign=hosted&utm_content=intro).

Questions? Please [join](https://discordapp.com/invite/xNEdEGw) our [discord channel](https://discordapp.com/invite/xNEdEGw), or follow us on [Twitter](https://twitter.com/rudderstack).

# Why Use RudderStack?

- **Production-ready**: Companies like **Mattermost**, **IFTTT**, **Torpedo**, **Grofers**, **1mg**, **Nana**, **OnceHub**,  and dozens of large companies use RudderStack for collecting their events. 
   Note: If you are using RudderStack and your name is not on the list, please submit a PR.

- **Extreme Scale**: One of our largest installations currently sends **300 Million** events/day with peak of **40K** req/sec, via a two-node RudderStack setup.

- **Privacy and Security**: You can collect and store your customer data without sending everything to a third-party vendor. With RudderStack, you get fine-grained control over what data to forward to which analytical tool.

- **Seamless Integration**: RudderStack currently supports integration with over 45 popular [destination platforms](https://docs.rudderstack.com/destinations/) such as Google Analytics, Amplitude, Mixpanel, Amazon S3, and more. Moreover, it also supports [data warehouse integrations](https://docs.rudderstack.com/data-warehouse-integrations) for Snowflake, Redshift, and Google BigQuery.

- **Client-side SDK Support**: RudderStack offers client-side SDKs for [JavaScript](https://docs.rudderstack.com/sdk-integration-guide/getting-started-with-javascript-sdk), [Android](https://docs.rudderstack.com/sdk-integration-guide/getting-started-with-android-sdk), [iOS](https://docs.rudderstack.com/sdk-integration-guide/getting-started-with-ios-sdk), and [Unity](https://docs.rudderstack.com/sdk-integration-guide/getting-started-with-unity-sdk).

- **User-specified Transformation**: RudderStack gives you the power to filter or transform your events before sending them to the desired destinations.

# Key Features

- **Segment API Compatible**: RudderStack is Segment API compatible. So you don't need to change your app if you are using Segment, just integrate the RudderStack SDKs into your app and your events will keep flowing as before (including data-warehouse).

- **Processing Flexibility**: With RudderStack's powerful JavaScript-based event transformation framework, you can enhance or transform your event data by combining it with your other internal data. Furthermore, since RudderStack runs inside your cloud or on-premise environment, you can easily access your production data to join with the event data.

- **Unlimited Events**: Event volume-based pricing of most of the commercial systems is broken. With RudderStack, you are be able to collect as much data as possible without worrying about overrunning event budgets.

- **Stand-alone System**: RudderStack runs as a single Go binary with the dependencies being on a PostgreSQL server and a Node.js service. There is no other dependency required to run RudderStack.

- **Platform-independent**: RudderStack is Kubernetes-native and can run on any Kubernetes cluster with our Helm charts. RudderStack is cloud-agnostic and can run on stand-alone machines in all popular cloud platforms, namely AWS, Microsoft Azure, GCP, and others.

- **High Performance**: On a single m4.2xlarge AWS EC2 instance, RudderStack can process 3000 events/second.

- **Enhanced Telemetry**: To help us improve RudderStack, we collect performance and diagnostic metrics about how you use RudderStack, and how it is working. **No customer data is present in the metrics**. For technical details, please check out our wiki page on [Telemetry](https://github.com/rudderlabs/rudder-server/wiki/RudderStack-Telemetry).

# RudderStack Setup

The easiest way to try RudderStack is through our hosted service. Please signup [here](https://app.rudderstack.com/signup?type=freetrial)

You can also set up and use RudderStack on your platform of choice. Please refer to the following guides for detailed instructions:

- [Setting up RudderStack on Docker](https://docs.rudderstack.com/administrators-guide/installing-and-setting-up-rudderstack/docker)
- [Setting up RudderStack on Kubernetes](https://docs.rudderstack.com/administrators-guide/installing-and-setting-up-rudderstack/kubernetes)
- [Setting up RudderStack on a Native Installation](https://docs.rudderstack.com/administrators-guide/installing-and-setting-up-rudderstack/native-installation)
- [Developer Machine Setup](https://docs.rudderstack.com/administrators-guide/installing-and-setting-up-rudderstack/developer-machine-setup)

Once you have installed RudderStack, [send test events](https://docs.rudderstack.com/getting-started/installing-and-setting-up-rudderstack#how-to-send-test-events) to verify the setup.

# UI Pages

### Connections Page

![Connections Page](https://gblobscdn.gitbook.com/assets%2F-Lq5Ea6fHVg3dSxMCgyQ%2F-M8Fo18nKM8Y3sHNQwW3%2F-M8Fo6hu_qKB4XX0STNZ%2FScreenshot%202020-05-26%20at%205.02.38%20PM.png?alt=media&token=adbd68bd-5b55-4e65-a19a-a1a29fc616e8)

### Events Page

![Events Page](https://gblobscdn.gitbook.com/assets%2F-Lq5Ea6fHVg3dSxMCgyQ%2F-M8Fo18nKM8Y3sHNQwW3%2F-M8FoF_Gnu9CBQgUujZW%2FScreenshot%202020-05-26%20at%205.12.19%20PM.png?alt=media&token=71165ae7-964c-4370-9826-29315ab3e3b4)

# RudderStack Architecture

RudderStack has two major components, namely:

- **Control Plane**: The control plane allows you to manage the configuration of your sources and destinations. There are two options for setting up the connections:

  - [Managed control plane](https://app.rudderstack.com/): This is the easiest way to manage your source and destination configurations.
  - [RudderStack Config Generator](https://github.com/rudderlabs/rudder-server/wiki/RudderStack-Config-Generator): RudderStack also allows you to manage your source and destination configurations without having to sign up and use our hosted services. Please note that you cannot create transformations with the RudderStack Config Generator.

- **Data Plane**: This is the core engine that is responsible for:

  - Receiving and buffering the event data
  - Transforming the event data into the required destination format, and
  - Relaying it to the destination

For a detailed understanding of the RudderStack architecture, please check our [documentation](https://docs.rudderstack.com/getting-started/rudderstack-data-plane-architecture).

A high-level view of RudderStack's architecture is as shown:
![Architecture](https://gblobscdn.gitbook.com/assets%2F-Lq5Ea6fHVg3dSxMCgyQ%2F-Lz111ICiMeHdy_Gu6JX%2F-Lz1A_NxMgbjhbSrVL2h%2FRudder%20Core%20Architecture.png?alt=media&token=2c524db9-7c5c-44e9-a351-cbb1c46a8063)

# License

RudderStack server is released under the [AGPLv3 License][agplv3_license].

# Contribute

We would love to see you contribute to RudderStack. Get more information on how to contribute [here](CONTRIBUTING.md).

# Wiki

For more information on RudderStack's features and functionalities, make sure you check out our [Wiki](https://github.com/rudderlabs/rudder-server/wiki) page.

# Follow Us

- [Discord][discord]
- [LinkedIn](https://www.linkedin.com/company/rudderlabs/)
- [Twitter][twitter]
- [HackerNews][hackernews]
- [RudderStack Blog][rudderstack-blog]

<!----variables---->

[build status]: https://codebuild.us-east-1.amazonaws.com/badges?uuid=eyJlbmNyeXB0ZWREYXRhIjoiT01EQkVPc0NBbDJLV2txTURidkRTMTNmWFRZWUY2dEtia3FRVmFXdXhWeUwzaC9aV3dsWWNNT0NwaVZKd1hKTFVMazB2cDQ5UHlaZTgvbFRER3R5SXRvPSIsIml2UGFyYW1ldGVyU3BlYyI6IktJQVMveHIzQnExZVE5b0YiLCJtYXRlcmlhbFNldFNlcmlhbCI6MX0%3D&branch=master
[release]: https://img.shields.io/github/v/release/rudderlabs/rudder-server?color=blue&sort=semver
[discord]: https://discordapp.com/invite/xNEdEGw
[docs]: https://docs.rudderstack.com/
[twitter]: https://twitter.com/rudderstack
[go-report-card]: https://go-report-card.com/report/github.com/rudderlabs/rudder-server
[go-report-card-badge]: https://go-report-card.com/badge/github.com/rudderlabs/rudder-server
[ssh]: https://help.github.com/en/articles/which-remote-url-should-i-use#cloning-with-ssh-urls
[dashboard]: https://app.rudderstack.com
[dashboard-on]: https://app.rudderstack.com/signup?type=freetrial
[dashboard-intro]: https://app.rudderstack.com/signup?type=freetrial&utm_source=github&utm_medium=rdr-srv&utm_campaign=hosted&utm_content=intro
[dashboard-setup]: https://app.rudderstack.com/signup?type=freetrial&utm_source=github&utm_medium=rdr-srv&utm_campaign=hosted&utm_content=setup-instructions
[dashboard-docker]: https://app.rudderstack.com/signup?utm_source=github&utm_medium=rdr-srv&utm_campaign=selfhosted&utm_content=docker
[dashboard-k8s]: https://app.rudderstack.com/signup?utm_source=github&utm_medium=rdr-srv&utm_campaign=selfhosted&utm_content=k8s
[dashboard-native]: https://app.rudderstack.com/signup?utm_source=github&utm_medium=rdr-srv&utm_campaign=selfhosted&utm_content=native
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
