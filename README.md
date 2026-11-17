<!-- HEADER: rudderlabs README header template. Source: devrel brand/github-readme-header/TEMPLATE.md -->
<p align="center">
  <a href="https://www.rudderstack.com/?utm_source=github&utm_medium=readme&utm_content=rudder-server-logo">
    <picture>
      <source media="(prefers-color-scheme: dark)" srcset="resources/rudderstack-logo-white.svg">
      <img src="resources/rudderstack-logo-dark.svg" height="48" alt="RudderStack">
    </picture>
  </a>
</p>

<h3 align="center">The agentic CDP that runs on your warehouse</h3>

<p align="center">
  Collect events from every app, website and server, transform them in flight, and deliver them to your warehouse and 200+ tools. Segment API compatible.
</p>

<p align="center">
  <a href="https://github.com/rudderlabs/rudder-server/stargazers"><img src="https://img.shields.io/github/stars/rudderlabs/rudder-server?style=social" alt="GitHub stars"></a>
  <a href="https://github.com/rudderlabs/rudder-server/releases"><img src="https://img.shields.io/github/v/release/rudderlabs/rudder-server?color=blue&sort=semver" alt="Release"></a>
  <a href="https://hub.docker.com/r/rudderlabs/rudder-server"><img src="https://img.shields.io/docker/pulls/rudderlabs/rudder-server" alt="Docker pulls"></a>
  <a href="https://github.com/rudderlabs/rudder-server/actions/workflows/builds.yml"><img src="https://github.com/rudderlabs/rudder-server/actions/workflows/builds.yml/badge.svg" alt="Builds"></a>
  <a href="LICENSE"><img src="https://img.shields.io/static/v1?label=license&message=ELv2&color=7447fc" alt="License: ELv2"></a>
</p>

<p align="center">
  <b>
    <a href="https://www.rudderstack.com/?utm_source=github&utm_medium=readme&utm_content=rudder-server-nav">Website</a> ·
    <a href="https://www.rudderstack.com/docs/?utm_source=github&utm_medium=readme&utm_content=rudder-server-nav">Documentation</a> ·
    <a href="CHANGELOG.md">Changelog</a> ·
    <a href="https://www.rudderstack.com/blog/?utm_source=github&utm_medium=readme&utm_content=rudder-server-nav">Blog</a> ·
    <a href="https://www.rudderstack.com/join-rudderstack-slack-community/">Slack</a> ·
    <a href="https://x.com/RudderStack">X</a>
  </b>
</p>

---

<p align="center">
  <b>New:</b> <a href="https://www.rudderstack.com/product/lookout/?utm_source=github&utm_medium=readme&utm_content=rudder-server-launch-line">Lookout</a> just launched. Go from "who should we target" to a live audience in minutes.
</p>
<!-- /HEADER -->

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="resources/rudder-server-flow-dark.png">
    <img src="resources/rudder-server-flow-light.png" alt="RudderStack data plane (rudder-server): customer data events from website, mobile and server-side SDKs reach the Gateway, which stores them in PostgreSQL and sends an ACK. The Processor runs transformations, and the Router delivers events to warehouses like Snowflake, BigQuery and Databricks and to 200+ tools" width="100%">
  </picture>
</p>

## Key features

- **Warehouse-first**: RudderStack treats your data warehouse as a first-class citizen among destinations, with advanced features and configurable, near real-time sync.

- **Developer-focused**: RudderStack is built API-first. It integrates with the tools developers already use and love, and works with AI agents through the [RudderStack MCP](#rudderstack-mcp) server.

- **High availability**: RudderStack is built for at least 99.99% uptime. A sophisticated error handling and retry system makes sure your data is delivered even during network partitions or destination downtime.

- **Privacy and security**: Collect and store your customer data without sending everything to a third-party vendor. You get fine-grained control over what data goes to which tool.

- **Unlimited events**: Event volume-based pricing is broken. With RudderStack open source, you can collect as much data as you need without worrying about overrunning your event budget.

- **Segment API-compatible**: RudderStack is fully compatible with the Segment API. You don't need to change your app if you use Segment; integrate the RudderStack SDKs and your events keep flowing to the same destinations, including data warehouses.

- **Production-ready**: Companies like Mattermost, IFTTT, Torpedo, Grofers, 1mg, Nana, OnceHub and dozens of large companies use RudderStack to collect their events.

- **Seamless integration**: RudderStack supports 200+ [tool](https://www.rudderstack.com/docs/destinations/) and [warehouse](https://www.rudderstack.com/docs/destinations/warehouse-destinations/) destinations. Every event follows the [RudderStack Event Specification](https://www.rudderstack.com/docs/event-spec/standard-events/) (`identify`, `track`, `page`, `screen`, `group`, `alias`), so you instrument once and RudderStack transforms and routes each event to every destination.

- **User-specified transformations**: A JavaScript-based transformation framework lets you enrich or transform events by combining them with your other internal data. Because RudderStack can run inside your cloud or on-premise environment, you can join your production data with the event data.

## Get started

The easiest way to experience RudderStack is to [**sign up**](https://app.rudderstack.com/signup?type=freetrial&utm_source=github&utm_medium=readme&utm_content=rudder-server-get-started) for **RudderStack Cloud Free**, a completely free tier of [**RudderStack Cloud**](https://www.rudderstack.com/cloud/).

You can also set up RudderStack on your platform of choice with these two easy steps:

### Step 1: Set up RudderStack

- [**Docker**](https://www.rudderstack.com/docs/get-started/rudderstack-open-source/data-plane-setup/docker/)
- [**Kubernetes**](https://www.rudderstack.com/docs/get-started/rudderstack-open-source/data-plane-setup/kubernetes/)
- [**Developer machine setup**](https://www.rudderstack.com/docs/get-started/rudderstack-open-source/data-plane-setup/developer-machine-setup/)

> **Note**: If you are planning to use RudderStack in production, we STRONGLY recommend using our Kubernetes Helm charts. We update our Docker images with bug fixes much more frequently than our GitHub repo.

### Step 2: Verify the installation

Once you have installed RudderStack, [**send test events**](https://www.rudderstack.com/docs/get-started/rudderstack-open-source/sending-test-events/) to verify the setup.

## Architecture

RudderStack is an independent, stand-alone system with a dependency only on the database (PostgreSQL). Its backend is written in **Go**.

Wondering how far a Postgres-backed queue scales? Read [**Lessons from scaling PostgreSQL queues to 100K events**](https://www.rudderstack.com/blog/scaling-postgres-queue/?utm_source=github&utm_medium=readme&utm_content=rudder-server-postgres).

A high-level view of RudderStack's architecture is shown below:

![RudderStack architecture: control plane, data plane and destinations](resources/rudderstack-architecture.svg)

For more details on the various architectural components, refer to our [**documentation**](https://www.rudderstack.com/docs/get-started/rudderstack-open-source/rudderstack-architecture/).

## Contribute

We would love to see you contribute to RudderStack. Get more information on how to contribute [**here**](CONTRIBUTING.md).

If rudder-server is useful to you, a ⭐ helps more people find it.

## License

RudderStack server is released under the [**Elastic License 2.0**](LICENSE).

## More from RudderStack

### Lookout

<a href="https://www.rudderstack.com/product/lookout/?utm_source=github&utm_medium=readme&utm_content=rudder-server-lookout-banner"><img src="resources/banner-lookout.png" alt="Lookout, an AI assistant for your customer data: from who should we target to live audience in minutes. Try the sandbox." width="100%"></a>

Describe a goal, get an audience built on your warehouse and synced to your tools. **[Try the sandbox →](https://lookout.rudderstack.com/w/lookout-sandbox/join?utm_source=github&utm_medium=readme&utm_content=rudder-server-lookout)**

### RudderStack MCP

<a href="https://github.com/rudderlabs/rudderstack-mcp"><img src="resources/banner-mcp.png" alt="RudderStack MCP: connect AI agents to your CDP. Works with Claude, ChatGPT, Cursor, VS Code and Gemini." width="100%"></a>

Hosted, OAuth sign-in, nothing to install.

[![Add to Claude](https://img.shields.io/badge/Claude-Add_connector-D97757?style=for-the-badge&logo=claude&logoColor=white)](https://claude.ai/directory/rudderstack) [![Add to Cursor](https://img.shields.io/badge/Cursor-Install-000000?style=for-the-badge&logo=cursor&logoColor=white)](https://cursor.com/install-mcp?name=RudderStack&config=eyJ1cmwiOiJodHRwczovL21jcC5ydWRkZXJzdGFjay5jb20vbWNwIn0%3D) [![Install in VS Code](https://img.shields.io/badge/VS_Code-Install-0098FF?style=for-the-badge)](https://insiders.vscode.dev/redirect/mcp/install?name=rudderstack&config=%7B%22type%22%3A%22http%22%2C%22url%22%3A%22https%3A%2F%2Fmcp.rudderstack.com%2Fmcp%22%7D) [![ChatGPT, Codex and more](https://img.shields.io/badge/ChatGPT_·_Codex_·_more-Setup-555555?style=for-the-badge)](https://github.com/rudderlabs/rudderstack-mcp#connect)
