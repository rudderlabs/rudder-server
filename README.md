<p align="center"><a href="https://rudderstack.com"><img src="https://raw.githubusercontent.com/rudderlabs/rudder-server/master/resources/RudderStack.png" alt="RudderStack - The smart customer data pipeline" height="90"/></a></p>
<h1 align="center"></h1>
<p align="center"><b>The smart customer data pipeline</b></p>

<p align="center">
	<a href="https://rudderstack.com"><img src="https://codebuild.us-east-1.amazonaws.com/badges?uuid=eyJlbmNyeXB0ZWREYXRhIjoiT01EQkVPc0NBbDJLV2txTURidkRTMTNmWFRZWUY2dEtia3FRVmFXdXhWeUwzaC9aV3dsWWNNT0NwaVZKd1hKTFVMazB2cDQ5UHlaZTgvbFRER3R5SXRvPSIsIml2UGFyYW1ldGVyU3BlYyI6IktJQVMveHIzQnExZVE5b0YiLCJtYXRlcmlhbFNldFNlcmlhbCI6MX0%3D&branch=master" height="22"/></a>
    <a href="https://rudderstack.com"><img src="https://img.shields.io/github/v/release/rudderlabs/rudder-server?color=blue&sort=semver" alt="Release" height="22"/></a>
</p>
<br/>

# What is RudderStack?

[RudderStack](https://rudderstack.com/) is a **customer data pipeline tool** for collecting, routing and processing data from your websites, apps, cloud tools, and data warehouse.

With RudderStack, you can build customer data pipelines that connect your whole customer data stack and then make them smarter by triggering enrichment and activation in customer tools based on analysis in your data warehouse. Its easy-to-use SDKs and event source integrations, Cloud Extract integrations, transformations, and expansive library of destination and warehouse integrations makes building customer data pipelines for both event streaming and cloud-to-warehouse ELT simple. 


Questions? Please join our [Slack channel](https://resources.rudderstack.com/join-rudderstack-slack) for support on the product.

| Try **RudderStack Cloud Free** - a free tier of [RudderStack Cloud](https://resources.rudderstack.com/rudderstack-cloud). Click [here](https://app.rudderlabs.com/signup?type=freetrial) to start building a smarter customer data pipeline today, with RudderStack Cloud. |
|:------|

# Why Use RudderStack?

- **Developer-focused**: RudderStack is built API-first. So it integrates seamlessly with the tools that the developers already use and love. Its backend is written in Go, with a rich UI written in React.js.

- **Warehouse-first**: RudderStack treats your data warehouse as a first class citizen among destinations, with advanced features and configurable, near real-time sync.

- **Production-ready**: Companies like **Mattermost**, **IFTTT**, **Torpedo**, **Grofers**, **1mg**, **Nana**, **OnceHub**,  and dozens of large companies use RudderStack for collecting their events. **Note**: If you're using RudderStack and would like to add your name in this list, please submit a PR.

- **Extreme Scale**: One of our largest installations currently sends **300 Million** events/day with peak of **40K** req/sec, via a two-node RudderStack setup.

- **High Availability**: RudderStack comes with at least 99.99% uptime. We have built a sophisticated error handling and retry system that ensures that your data will be delivered even in the event of network partitions or destinations downtime.

- **Privacy and Security**: You can collect and store your customer data without sending everything to a third-party vendor. With RudderStack, you get fine-grained control over what data to forward to which analytical tool.

- **Seamless Integration**: RudderStack currently supports integration with over 70 popular [destination platforms](https://docs.rudderstack.com/destinations-guides) such as Google Analytics, Amplitude, Hotjar, Mixpanel, Amazon S3, and more. Moreover, it also supports [data warehouse integrations](https://docs.rudderstack.com/data-warehouse-integration-guides) for Snowflake, Redshift, ClickHouse, PostgreSQL and Google BigQuery.

- **Client-side SDK Support**: RudderStack offers client-side SDKs for [JavaScript](https://docs.rudderstack.com/rudderstack-sdk-integration-guides/rudderstack-javascript-sdk), [Android](https://docs.rudderstack.com/rudderstack-sdk-integration-guides/rudderstack-android-sdk), [iOS](https://docs.rudderstack.com/rudderstack-sdk-integration-guides/rudderstack-ios-sdk), and [Unity](https://docs.rudderstack.com/rudderstack-sdk-integration-guides/getting-started-with-unity-sdk), and more.

- **User-specified Transformation**: RudderStack gives you the power to filter or transform your events before sending them to the desired destinations.

# Key Features

- **Segment API Compatible**: RudderStack is Segment API compatible. So you don't need to change your app if you are using Segment, just integrate the RudderStack SDKs into your app and your events will keep flowing as before (including data-warehouse).

- **Processing Flexibility**: With RudderStack's powerful JavaScript-based event transformation framework, you can enhance or transform your event data by combining it with your other internal data. Furthermore, since RudderStack runs inside your cloud or on-premise environment, you can easily access your production data to join with the event data.

- **Unlimited Events**: Event volume-based pricing of most of the commercial systems is broken. With RudderStack, you are be able to collect as much data as possible without worrying about overrunning event budgets.

- **Stand-alone System**: RudderStack runs as a single Go binary with the dependencies being on a PostgreSQL server and a Node.js service. There is no other dependency required to run RudderStack.

- **Platform-independent**: RudderStack is Kubernetes-native and can run on any Kubernetes cluster with our Helm charts. RudderStack is cloud-agnostic and can run on stand-alone machines in all popular cloud platforms, namely AWS, Microsoft Azure, GCP, and others.

- **High Performance**: On a single m4.2xlarge AWS EC2 instance, RudderStack can process 3000 events/second.

- **Enhanced Telemetry**: To help us improve RudderStack, we collect performance and diagnostic metrics about how you use RudderStack, and how it is working. **No customer data is present in the metrics**. For technical details, please check out our wiki page on [Telemetry](https://github.com/rudderlabs/rudder-server/wiki/RudderStack-Telemetry).

# Our Customers

[![1mg](https://user-images.githubusercontent.com/59817155/95971154-74745e80-0e2e-11eb-8468-9f9cf1bfcf46.png)](https://www.1mg.com/) [![Grofers](https://user-images.githubusercontent.com/59817155/95970797-fe6ff780-0e2d-11eb-9c08-d4550b2c68b5.png)](https://grofers.com/) [![IFTTT](https://user-images.githubusercontent.com/59817155/95970944-2cedd280-0e2e-11eb-83d3-500c46c3a290.png)](https://ifttt.com/) [![Mattermost](https://user-images.githubusercontent.com/59817155/95971043-4c84fb00-0e2e-11eb-8ef8-2e47970221c6.png)](https://mattermost.com/) [![Wynn Casino](https://user-images.githubusercontent.com/59817155/95969926-f9f70f00-0e2c-11eb-8985-27b62d34fc65.png)](https://www.wynnlasvegas.com/) [![Acorns](https://user-images.githubusercontent.com/59817155/95970244-58bc8880-0e2d-11eb-9c7b-2ca08e2b11d7.png)](https://www.acorns.com/) [![Hinge](https://user-images.githubusercontent.com/59817155/95970463-9de0ba80-0e2d-11eb-91b1-bbfe55b91228.png)](https://hinge.co/) [![Proposify](https://user-images.githubusercontent.com/59817155/95971522-f82e4b00-0e2e-11eb-8c58-95fcb8a0e76c.png)](https://www.proposify.com/) [![Barstool Sports](https://user-images.githubusercontent.com/59817155/95971526-f95f7800-0e2e-11eb-85d3-fa81dbf9aa6e.png)](https://www.barstoolsports.com/)


# Get Started

The easiest way to experience RudderStack is to [sign up](https://app.rudderlabs.com/signup?type=freetrial) for **RudderStack Cloud Free** - a completely free tier of [RudderStack Cloud](https://resources.rudderstack.com/rudderstack-cloud). Click [here](https://app.rudderlabs.com/signup?type=freetrial) to get started.

You can also set up and use RudderStack on your platform of choice.

**Note:** If you are planning to use RudderStack in production, we **strongly** recommend the Kubernetes Helm charts. We update our Docker images with bug fixes etc much more frequently than our GitHub repo (where we release once a month).

- [Setting up RudderStack on Docker](https://docs.rudderstack.com/get-started/installing-and-setting-up-rudderstack/docker)
- [Setting up RudderStack on Kubernetes](https://docs.rudderstack.com/get-started/installing-and-setting-up-rudderstack/kubernetes)
- [Setting up RudderStack on a Native Installation](https://docs.rudderstack.com/get-started/installing-and-setting-up-rudderstack/native-installation)
- [Developer Machine Setup](https://docs.rudderstack.com/get-started/installing-and-setting-up-rudderstack/developer-machine-setup)

Once you have installed RudderStack, [send test events](https://docs.rudderstack.com/get-started/installing-and-setting-up-rudderstack#how-to-send-test-events) to verify the setup.

# UI Pages

### Connections Page

![Connections Page](https://gblobscdn.gitbook.com/assets%2F-Lq5Ea6fHVg3dSxMCgyQ%2F-M8Fo18nKM8Y3sHNQwW3%2F-M8Fo6hu_qKB4XX0STNZ%2FScreenshot%202020-05-26%20at%205.02.38%20PM.png?alt=media&token=adbd68bd-5b55-4e65-a19a-a1a29fc616e8)

### Events Page

![Events Page](https://gblobscdn.gitbook.com/assets%2F-Lq5Ea6fHVg3dSxMCgyQ%2F-M8Fo18nKM8Y3sHNQwW3%2F-M8FoF_Gnu9CBQgUujZW%2FScreenshot%202020-05-26%20at%205.12.19%20PM.png?alt=media&token=71165ae7-964c-4370-9826-29315ab3e3b4)

### Connecting a Source to a Destination

![Sources Destinations](https://user-images.githubusercontent.com/59817155/98811482-f021f500-2446-11eb-9abd-5951fe8b2546.gif)

# RudderStack Architecture

RudderStack has two major components, namely:

- **Control Plane**: The control plane allows you to manage the configuration of your sources and destinations. There are two options for setting up the connections:

  - [Managed Control Plane](https://app.rudderlabs.com/signup?type=freetrial): This is the easiest way to manage your source and destination configurations.
  - [RudderStack Config Generator](https://github.com/rudderlabs/rudder-server/wiki/RudderStack-Config-Generator): RudderStack also allows you to manage your source and destination configurations without having to sign up and use our hosted services. **Please note that you cannot create transformations or Live Debugger with the RudderStack Config Generator**.

- **Data Plane**: This is the core engine that is responsible for:
  - Receiving and buffering the event data
  - Transforming the event data into the required destination format, and
  - Relaying it to the destination

For a detailed understanding of the RudderStack architecture, please check our [documentation](https://docs.rudderstack.com/get-started/rudderstack-architecture).

A high-level view of RudderStack's architecture is as shown:

![Architecture](https://user-images.githubusercontent.com/59817155/98810368-23fc1b00-2445-11eb-9519-025867b94de1.png)

# License

RudderStack server is released under the [AGPLv3 License][agplv3_license].

# Contribute

We would love to see you contribute to RudderStack. Get more information on how to contribute [here](CONTRIBUTING.md).

# Wiki

For more information on RudderStack's features and functionalities, make sure you check out our [Wiki](https://github.com/rudderlabs/rudder-server/wiki) page.

# Follow Us

- [RudderStack Blog][rudderstack-blog]
- [Slack][slack]
- [Twitter][twitter]
- [LinkedIn][linkedin]
- [dev.to][devto]
- [Medium][medium]
- [YouTube][youtube]
- [HackerNews][hackernews]
- [Product Hunt][producthunt]

# :clap:  Our Supporters
[![Stargazers repo roster for @rudderlabs/rudder-server](https://reporoster.com/stars/rudderlabs/rudder-server)](https://github.com/rudderlabs/rudder-server/stargazers)
[![Forkers repo roster for @rudderlabs/rudder-server](https://reporoster.com/forks/rudderlabs/rudder-server)](https://github.com/rudderlabs/rudder-server/network/members)

<!----variables---->

[build status]: https://codebuild.us-east-1.amazonaws.com/badges?uuid=eyJlbmNyeXB0ZWREYXRhIjoiT01EQkVPc0NBbDJLV2txTURidkRTMTNmWFRZWUY2dEtia3FRVmFXdXhWeUwzaC9aV3dsWWNNT0NwaVZKd1hKTFVMazB2cDQ5UHlaZTgvbFRER3R5SXRvPSIsIml2UGFyYW1ldGVyU3BlYyI6IktJQVMveHIzQnExZVE5b0YiLCJtYXRlcmlhbFNldFNlcmlhbCI6MX0%3D&branch=master
[release]: https://img.shields.io/github/v/release/rudderlabs/rudder-server?color=blue&sort=semver
[docs]: https://docs.rudderstack.com/
[slack]: https://resources.rudderstack.com/join-rudderstack-slack
[twitter]: https://twitter.com/rudderstack
[linkedin]: https://www.linkedin.com/company/rudderlabs/
[devto]: https://dev.to/rudderstack
[medium]: https://rudderstack.medium.com/
[youtube]: https://www.youtube.com/channel/UCgV-B77bV_-LOmKYHw8jvBw
[rudderstack-blog]: https://rudderstack.com/blog/
[hackernews]: https://news.ycombinator.com/item?id=21081756
[producthunt]: https://www.producthunt.com/posts/rudderstack
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
[rudder-server-sample-env]: https://github.com/rudderlabs/rudder-server/blob/master/config/sample.env
[rudder-docker-yml]: https://github.com/rudderlabs/rudder-server/blob/master/rudder-docker.yml
