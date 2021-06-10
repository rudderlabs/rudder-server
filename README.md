<p align="center"><a href="https://rudderstack.com"><img src="https://user-images.githubusercontent.com/59817155/121357083-1c571300-c94f-11eb-8cc7-ce6df13855c9.png" alt="RudderStack - The smart customer data pipeline" height="80"/></a></p>
<h1 align="center"></h1>
<p align="center"><b>Customer Data Platform for Developers</b></p>

<p align="center">
	<a href="https://rudderstack.com"><img src="https://codebuild.us-east-1.amazonaws.com/badges?uuid=eyJlbmNyeXB0ZWREYXRhIjoiT01EQkVPc0NBbDJLV2txTURidkRTMTNmWFRZWUY2dEtia3FRVmFXdXhWeUwzaC9aV3dsWWNNT0NwaVZKd1hKTFVMazB2cDQ5UHlaZTgvbFRER3R5SXRvPSIsIml2UGFyYW1ldGVyU3BlYyI6IktJQVMveHIzQnExZVE5b0YiLCJtYXRlcmlhbFNldFNlcmlhbCI6MX0%3D&branch=master" height="22"/></a>
    <a href="https://rudderstack.com"><img src="https://img.shields.io/github/v/release/rudderlabs/rudder-server?color=blue&sort=semver" alt="Release" height="22"/></a>
</p>
<br/>

### Table of Contents

- [What is RudderStack?](https://github.com/rudderlabs/rudder-server#what-is-rudderstack)
- [Why use RudderStack?](https://github.com/rudderlabs/rudder-server#why-use-rudderstack)
- [Set up RudderStack](https://github.com/rudderlabs/rudder-server#get-started-with-rudderstack)
- [Our customers](https://github.com/rudderlabs/rudder-server#our-customers)
- [Contribute to RudderStack](https://github.com/rudderlabs/rudder-server#contribute)
- [Follow us](https://github.com/rudderlabs/rudder-server#follow-us)


## What is RudderStack?

As the leading **open-source Customer Data Platform** (CDP), [**RudderStack**](https://rudderstack.com/) provides data pipelines that make it easy to collect data from every application, website and SaaS platform, then activate it in your warehouse and business tools.

With RudderStack, you can build customer data pipelines that connect your whole customer data stack and then make them smarter by triggering enrichment and activation in customer tools based on analysis in your data warehouse. Its easy-to-use SDKs and event source integrations, Cloud Extract integrations, transformations, and expansive library of destination and warehouse integrations makes building customer data pipelines for both event streaming and cloud-to-warehouse ELT simple.

<p align="center">
<a href="https://rudderstack.com"><img src="https://user-images.githubusercontent.com/59817155/121467292-76e78200-c9d6-11eb-8696-cb3c45d94ef3.gif" alt="RudderStack"></a></p>


Questions? Please join our [**Slack channel**](https://resources.rudderstack.com/join-rudderstack-slack) for support on the product.

| Try **RudderStack Cloud Free** - a free tier of [RudderStack Cloud](https://resources.rudderstack.com/rudderstack-cloud). Click [here](https://app.rudderlabs.com/signup?type=freetrial) to start building a smarter customer data pipeline today, with RudderStack Cloud. |
|:------|

## Why use RudderStack?

- **Unlimited Events**: Event volume-based pricing of most of the commercial systems is broken. With RudderStack, you are be able to collect as much data as possible without worrying about overrunning your event budgets.

- **Warehouse-first**: RudderStack treats your data warehouse as a first class citizen among destinations, with advanced features and configurable, near real-time sync.

- **Developer-focused**: RudderStack is built API-first. It integrates seamlessly with the tools that the developers already use and love.

- **Segment API Compatible**: RudderStack is Segment API compatible. So you don't need to change your app if you are using Segment, just integrate the RudderStack SDKs into your app and your events will keep flowing as before (including data-warehouse).

- **Production-ready**: Companies like **Mattermost**, **IFTTT**, **Torpedo**, **Grofers**, **1mg**, **Nana**, **OnceHub**,  and dozens of large companies use RudderStack for collecting their events. **Note**: If you're using RudderStack and would like to add your name in this list, please submit a PR.

- **Seamless Integration**: RudderStack currently supports integration with over 80 popular [tool](https://docs.rudderstack.com/destinations-guides) and [warehouse](https://docs.rudderstack.com/data-warehouse-integration-guides) destinations.

- **User-specified Transformation**: RudderStack offers a powerful JavaScript-based event transformation framework which lets you enhance or transform your event data by combining it with your other internal data. Furthermore, RudderStack runs inside your cloud or on-premise environment, so you can easily access your production data to join with the event data.

- **High Availability**: RudderStack comes with at least 99.99% uptime. We have built a sophisticated error handling and retry system that ensures that your data will be delivered even in the event of network partitions or destinations downtime.

- **Privacy and Security**: You can collect and store your customer data without sending everything to a third-party vendor. With RudderStack, you get fine-grained control over what data to forward to which analytical tool.

## Our customers

[![1mg](https://user-images.githubusercontent.com/59817155/95971154-74745e80-0e2e-11eb-8468-9f9cf1bfcf46.png)](https://www.1mg.com/) [![Grofers](https://user-images.githubusercontent.com/59817155/95970797-fe6ff780-0e2d-11eb-9c08-d4550b2c68b5.png)](https://grofers.com/) [![IFTTT](https://user-images.githubusercontent.com/59817155/95970944-2cedd280-0e2e-11eb-83d3-500c46c3a290.png)](https://ifttt.com/) [![Mattermost](https://user-images.githubusercontent.com/59817155/95971043-4c84fb00-0e2e-11eb-8ef8-2e47970221c6.png)](https://mattermost.com/) [![Wynn Casino](https://user-images.githubusercontent.com/59817155/95969926-f9f70f00-0e2c-11eb-8985-27b62d34fc65.png)](https://www.wynnlasvegas.com/) [![Acorns](https://user-images.githubusercontent.com/59817155/95970244-58bc8880-0e2d-11eb-9c7b-2ca08e2b11d7.png)](https://www.acorns.com/) [![Hinge](https://user-images.githubusercontent.com/59817155/95970463-9de0ba80-0e2d-11eb-91b1-bbfe55b91228.png)](https://hinge.co/) [![Proposify](https://user-images.githubusercontent.com/59817155/95971522-f82e4b00-0e2e-11eb-8c58-95fcb8a0e76c.png)](https://www.proposify.com/) [![Barstool Sports](https://user-images.githubusercontent.com/59817155/95971526-f95f7800-0e2e-11eb-85d3-fa81dbf9aa6e.png)](https://www.barstoolsports.com/)


## Get started with RudderStack

The easiest way to experience RudderStack is to [sign up](https://app.rudderlabs.com/signup?type=freetrial) for **RudderStack Cloud Free** - a completely free tier of [RudderStack Cloud](https://resources.rudderstack.com/rudderstack-cloud). Click [here](https://app.rudderlabs.com/signup?type=freetrial) to get started.

You can also set up RudderStack on your platform of choice with these two easy steps:

### Step 1: Set up RudderStack

- [Setting up RudderStack on Docker](https://docs.rudderstack.com/get-started/installing-and-setting-up-rudderstack/docker)
- [Setting up RudderStack on Kubernetes](https://docs.rudderstack.com/get-started/installing-and-setting-up-rudderstack/kubernetes)
- [Developer Machine Setup](https://docs.rudderstack.com/installing-and-setting-up-rudderstack/developer-machine-setup)

| If you are planning to use RudderStack in production, we STRONGLY recommend using our Kubernetes Helm charts. We update our Docker images with bug fixes much more frequently than our GitHub repo. |
|:------|

### Step 2: Verify the installation

Once you have installed RudderStack, [send test events](https://docs.rudderstack.com/get-started/installing-and-setting-up-rudderstack#how-to-send-test-events) to verify the setup.

## UI pages

#### Connections page

![Connections Page](https://user-images.githubusercontent.com/59817155/113102943-489e3700-921c-11eb-87c0-d4b6f9e3270c.png)

#### Live events

![Events Page](https://user-images.githubusercontent.com/59817155/113098485-25708900-9216-11eb-8f61-2b0a4443afe1.JPG)

#### Connecting a RudderStack source to a destination

![Connection](https://user-images.githubusercontent.com/59817155/113098528-35886880-9216-11eb-97e9-6575bca23ae6.gif)

## RudderStack architecture

Read about RudderStack's architecture in our [wiki](https://github.com/rudderlabs/rudder-server/wiki/RudderStack's-Architecture).

## License

RudderStack server is released under the [AGPLv3 License][agplv3_license].

## Contribute

We would love to see you contribute to RudderStack. Get more information on how to contribute [here](CONTRIBUTING.md).

## Follow us

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
