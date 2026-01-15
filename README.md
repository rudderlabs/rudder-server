<p align="center">
📖 Just launched <b><a href="https://www.rudderstack.com/learn/">Data Learning Center</a></b> - Resources on data engineering and data infrastructure
  <br/>
 </p>

<p align="center">
  <a href="https://www.rudderstack.com/">
    <img src="resources/rs-logo-full-duotone-dark.jpg" height="64px">
  </a>
</p>

<p align="center"><b>The Customer Data Platform for Developers</b></p>

<p align="center">
  <a href="https://github.com/rudderlabs/rudder-server/actions/workflows/tests.yaml">
    <img src="https://github.com/rudderlabs/rudder-server/actions/workflows/tests.yaml/badge.svg">
  </a>
  <a href="https://github.com/rudderlabs/rudder-server/actions/workflows/builds.yml">
    <img src="https://github.com/rudderlabs/rudder-server/actions/workflows/builds.yml/badge.svg">
  </a>
  <a href="https://goreportcard.com/report/github.com/rudderlabs/rudder-server">
    <img src="https://goreportcard.com/badge/github.com/rudderlabs/rudder-server">
  </a>
  <a href="https://github.com/rudderlabs/rudder-server/releases">
    <img src="https://img.shields.io/github/v/release/rudderlabs/rudder-server?color=blue&sort=semver">
  </a>
  <a href="https://www.rudderstack.com/docs/get-started/installing-and-setting-up-rudderstack/docker/">
    <img src="https://img.shields.io/docker/pulls/rudderlabs/rudder-server">
  </a>
  <a href="https://github.com/rudderlabs/rudder-server/blob/master/LICENSE">
    <img src="https://img.shields.io/static/v1?label=license&message=ELv2&color=7447fc">
  </a>
</p>

<p align="center">
  <b>
    <a href="https://www.rudderstack.com/">Website</a>
    ·
    <a href="https://www.rudderstack.com/docs/">Documentation</a>
    ·
    <a href="https://github.com/rudderlabs/rudder-server/blob/master/CHANGELOG.md">Changelog</a>
    ·
    <a href="https://www.rudderstack.com/blog/">Blog</a>
    ·
    <a href="https://www.rudderstack.com/join-rudderstack-slack-community/">Slack</a>
    ·
    <a href="https://twitter.com/rudderstack">Twitter</a>
  </b>
</p>

---

## Table of Contents

- [Overview](#overview)
- [Key Features](#key-features)
- [Quick Start](#quick-start)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Architecture](#architecture)
- [Development](#development)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)
- [Community and Support](#community-and-support)
- [License](#license)

## Overview

As the leading open source Customer Data Platform (CDP), [**RudderStack**](https://www.rudderstack.com/) provides data pipelines that make it easy to collect data from every application, website and SaaS platform, then activate it in your warehouse and business tools.

With RudderStack, you can build customer data pipelines that connect your whole customer data stack and then make them smarter by triggering enrichment and activation in customer tools based on analysis in your data warehouse. Its easy-to-use SDKs and event source integrations, Cloud Extract integrations, transformations, and expansive library of destination and warehouse integrations makes building customer data pipelines for both event streaming and cloud-to-warehouse ELT simple.

<p align="center">
  <a href="https://www.rudderstack.com/">
    <img src="https://user-images.githubusercontent.com/59817155/121468374-4ef91e00-c9d8-11eb-8611-28bea18f609d.gif" alt="RudderStack">
  </a>
</p>

| Try **RudderStack Cloud Free** - a free tier of [**RudderStack Cloud**](https://www.rudderstack.com/cloud/). Click [**here**](https://app.rudderstack.com/signup?type=freetrial) to start building a smarter customer data pipeline today, with RudderStack Cloud. |
| :----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |

## Key Features

### 🏢 Warehouse-First Architecture
RudderStack treats your data warehouse as a first-class citizen among destinations, with advanced features and configurable, near real-time sync. Your data warehouse becomes the single source of truth for all your customer data.

### 👨‍💻 Developer-Focused
Built API-first, RudderStack integrates seamlessly with the tools that developers already use and love. Comprehensive SDKs, detailed documentation, and a developer-friendly approach make integration straightforward.

### ⚡ High Availability
RudderStack comes with at least 99.99% uptime. We have built a sophisticated error handling and retry system that ensures that your data will be delivered even in the event of network partitions or destination downtime.

### 🔒 Privacy and Security
You can collect and store your customer data without sending everything to a third-party vendor. With RudderStack, you get fine-grained control over what data to forward to which analytical tool, ensuring compliance with privacy regulations.

### 📊 Unlimited Events
Event volume-based pricing of most commercial systems is broken. With RudderStack Open Source, you can collect as much data as possible without worrying about overrunning your event budgets.

### 🔄 Segment API-Compatible
RudderStack is fully compatible with the Segment API. So you don't need to change your app if you are using Segment; just integrate the RudderStack SDKs into your app and your events will keep flowing to the destinations (including data warehouses) as before.

### 🚀 Production-Ready
Companies like Mattermost, IFTTT, Torpedo, Grofers, 1mg, Nana, OnceHub, and dozens of large companies use RudderStack for collecting their events.

### 🔌 Seamless Integration
RudderStack currently supports integration with over 90 popular [**tool**](https://www.rudderstack.com/docs/destinations/) and [**warehouse**](https://www.rudderstack.com/docs/data-warehouse-integrations/) destinations.

### 🛠️ User-Specified Transformation
RudderStack offers a powerful JavaScript-based event transformation framework which lets you enhance or transform your event data by combining it with your other internal data. Furthermore, as RudderStack runs inside your cloud or on-premise environment, you can easily access your production data to join with the event data.

## Quick Start

The fastest way to get started with RudderStack:

```bash
# Using Docker Compose
git clone https://github.com/rudderlabs/rudder-server.git
cd rudder-server
docker-compose up
```

Your RudderStack server will be available at `http://localhost:8080`

For a managed solution, [**sign up**](https://app.rudderstack.com/signup?type=freetrial) for **RudderStack Cloud Free** - a completely free tier of [**RudderStack Cloud**](https://www.rudderstack.com/cloud/).

## Prerequisites

Before installing RudderStack, ensure you have:

- **Docker** (version 20.10 or later) and **Docker Compose** (version 1.29 or later)
- **PostgreSQL** (version 10 or later) - for data persistence
- **Go** (version 1.21 or later) - for development
- **Node.js** (version 16 or later) - for UI development
- At least **4GB RAM** and **2 CPU cores** for optimal performance

## Installation

You can set up RudderStack on your platform of choice:

### Docker (Recommended for Quick Setup)

```bash
docker pull rudderlabs/rudder-server:latest
docker run -p 8080:8080 rudderlabs/rudder-server:latest
```

For detailed Docker setup, see [**Docker Installation Guide**](https://www.rudderstack.com/docs/rudderstack-open-source/installing-and-setting-up-rudderstack/docker/).

### Kubernetes (Recommended for Production)

```bash
helm repo add rudderlabs https://rudderlabs.github.io/rudderstack-helm-charts/
helm install my-release rudderlabs/rudderstack
```

For detailed Kubernetes setup, see [**Kubernetes Installation Guide**](https://www.rudderstack.com/docs/rudderstack-open-source/installing-and-setting-up-rudderstack/kubernetes/).

> **Note**: If you are planning to use RudderStack in production, we **STRONGLY** recommend using our Kubernetes Helm charts. We update our Docker images with bug fixes much more frequently than our GitHub repo.

### Developer Machine Setup

For local development:

```bash
git clone https://github.com/rudderlabs/rudder-server.git
cd rudder-server
go mod download
make build
./rudder-server
```

For detailed developer setup, see [**Developer Machine Setup Guide**](https://www.rudderstack.com/docs/rudderstack-open-source/installing-and-setting-up-rudderstack/developer-machine-setup/).

### Verify Installation

Once you have installed RudderStack, [**send test events**](https://www.rudderstack.com/docs/get-started/installing-and-setting-up-rudderstack/sending-test-events/) to verify the setup.

## Architecture

RudderStack is an independent, stand-alone system with a dependency only on the database (PostgreSQL). Its backend is written in **Go** with a rich UI written in **React.js**.

### High-Level Architecture

![Architecture](resources/rudder-server-architecture.png)

### Core Components

- **Gateway**: Receives events from various sources (SDKs, APIs, webhooks)
- **Processor**: Transforms and enriches events based on user-defined rules
- **Router**: Routes processed events to configured destinations
- **Backend Config**: Manages configuration and syncs with control plane
- **JobsDB**: Persistent queue for reliable event delivery
- **Warehouse**: Handles data warehouse syncing and schema management

For more details on the various architectural components, refer to our [**documentation**](https://www.rudderstack.com/docs/get-started/rudderstack-architecture/).

## Development

### Building from Source

```bash
# Clone the repository
git clone https://github.com/rudderlabs/rudder-server.git
cd rudder-server

# Install dependencies
go mod download

# Build the project
make build

# Run tests
make test

# Run linter
make lint
```

### Running Locally

```bash
# Start PostgreSQL (if not already running)
docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=password postgres:13

# Set environment variables
export JOBS_DB_HOST=localhost
export JOBS_DB_PORT=5432
export JOBS_DB_USER=postgres
export JOBS_DB_PASSWORD=password
export JOBS_DB_NAME=jobsdb

# Run the server
./rudder-server
```

## Troubleshooting

### Common Issues

#### Server won't start
- Ensure PostgreSQL is running and accessible
- Check that ports 8080 and 8081 are not in use
- Verify environment variables are set correctly

#### Events not being delivered
- Check destination configuration in the control plane
- Verify network connectivity to destinations
- Review logs for error messages: `docker logs rudder-server`

#### High memory usage
- Adjust `RSERVER_JOBS_DB_MAX_POOL_SIZE` to limit database connections
- Configure `RSERVER_GATEWAY_MAX_BATCH_SIZE` to control batch sizes
- Monitor and tune based on your workload

For more troubleshooting tips, visit our [**documentation**](https://www.rudderstack.com/docs/).

## Contributing

We would love to see you contribute to RudderStack! Here's how you can get started:

1. **Fork the repository** and create your branch from `master`
2. **Make your changes** and ensure tests pass
3. **Submit a pull request** with a clear description of your changes

Get more information on how to contribute [**here**](https://github.com/rudderlabs/rudder-server/blob/master/CONTRIBUTING.md).

### Development Guidelines

- Follow Go best practices and conventions
- Write tests for new features
- Update documentation as needed
- Run `make fmt` before committing
- Ensure all tests pass with `make test`

## Community and Support

### Get Help

- 💬 [**Slack Community**](https://www.rudderstack.com/join-rudderstack-slack-community/) - Join our active community
- 📖 [**Documentation**](https://www.rudderstack.com/docs/) - Comprehensive guides and API references
- 🐛 [**GitHub Issues**](https://github.com/rudderlabs/rudder-server/issues) - Report bugs or request features
- 📧 [**Email Support**](mailto:support@rudderstack.com) - Direct support from our team

### Stay Updated

- 🐦 [**Twitter**](https://twitter.com/rudderstack) - Follow us for updates
- 📝 [**Blog**](https://www.rudderstack.com/blog/) - Technical articles and announcements
- 📺 [**YouTube**](https://www.youtube.com/c/RudderStack) - Video tutorials and demos

## License

RudderStack server is released under the [**Elastic License 2.0**](LICENSE).

---

<p align="center">
  Made with ❤️ by the RudderStack team
</p>
