![Build Status][build status]
[![Release]][release]

# What is RudderStack?

**Short answer:** RudderStack is an open-source Segment alternative written in Go, built for the enterprise. .

**Long answer:** RudderStack is a platform for collecting, storing and routing customer event data to dozens of tools. RudderStack is open-source, can run in your cloud environment (AWS, GCP, Azure or even your data-centre) and provides a powerful transformation framework to process your event data on the fly.

RudderStack runs as a single go binary with Postgres. It also needs the destination (e.g. GA, Amplitude) specific transformation code which are node scripts. This repo contains the core backend and the transformation modules of Rudder.
The client SDKs are in a separate repo (link below).

Rudder server is released under [AGPLv3 License][agplv3_license]

See the [HackerNews][hackernews] discussion around RudderStack.

Questions? Read our [Docs][docs] OR join our [Discord][discord] channel. Or please email soumyadeb at rudderlabs.com.

# Features

1. **Production Ready:** Multiple companies from startups to large engerprieses are running RudderStack for collecting events.
2. **Extreme Scale:** One of our largest installations is sending **300M events/day** with peak of **40K req/sec** via a multi-node RudderStack setup.
3. **Segment API Compatibile:** RudderStack is Segment API and library compatible so don't need to change your app if you are using Segment.
4. **Cloud Destinations:** Google Analytics, Amplitude, MixPanel, Adjust, AppsFlyer and dozens more destinations.
5. **Warehouse Destinations:** S3, Minio, Redshift, Snowflake, Google BigQuery support.
6. **Transformations:** User-specified transformation to filter/transform events.
7. **Rich UI:** Written in react
8. **SDKs:** [Javascript][rudder-sdk-js-git-repo], [Android][rudder-sdk-android-git-repo] or [iOS][rudder-sdk-android-git-repo] and server-side SDKs.
9. **Detailed Docs:** [Docs][docs]


# Why RudderStack ?

We are building RudderStack because we believe open-source and cloud-prem is important for three main reasons

1. **Privacy & Security:** You should be able to collect and store your customer data without sending everything to a 3rd party vendor or embedding proprietary SDKs. With RudderStack, the event data is always in your control. Besides, RudderStack gives you fine-grained control over what data to forward to what analytical tool.

2. **Processing Flexibility:** You should be able to enhance OR transform your event data by combining it with your other _internal_ data, e.g. stored in your transactional systems. RudderStack makes that possible because it provides a powerful JS-based event transformation framework. Furthermore, since RudderStack runs _inside_ your cloud or on-prem environment, you can access your production data to join with the event data.

3. **Unlimited Events:** Event volume-based pricing of most commercial systems is broken. You should be able to collect as much data as possible without worrying about overrunning event budgets. RudderStack's core BE is open-source and free to use.


# Contribution

We would love to see people contributing to RudderStack. see [CONTRIBUTING.md](CONTRIBUTING.md) for more information on contributing to RudderStack.

# Stay Connected

1. Join our [Discord][discord]
2. Follow [RudderStack][twitter] on Twitter

# UI Pages

## Connections Page

![image](https://user-images.githubusercontent.com/411699/65309691-36b0f200-dbaa-11e9-9631-8a9f81cea606.png)

## Events Page

![image](https://user-images.githubusercontent.com/52487451/65647230-e2937c80-dfb2-11e9-88bd-3b015c4b576f.png)

---

# Setup Instructions (Hosted Demo Account)

1. Go to the [dashboard][dashboard] `https://app.rudderlabs.com` and set up your account.
2. Select `RudderStack Hosted Service` from the top right corner after you login.
3. Follow (Send Test Events) instructions below to send test event.

---

# Setup Instructions (Docker)

The docker setup is the easiest & fastest way to try out RudderStack.

1. Go to the [dashboard][dashboard] `https://app.rudderlabs.com` and set up your account. Copy your workspace token from top of the home page.

   (Note) Instead of our full featured hosted UI, you can also use the open-source [config-generator-UI][config-generator-section] to create the source & destination configs and pass it to RudderStack.

2. If you have a Github account with SSH key added, then clone the repo with `git clone git@github.com:rudderlabs/rudder-server.git`. Move to the directory `cd rudder-server` and update the _rudder-transformer_ with `git submodule init && git submodule update`

   (Optional) If you don't have SSH enabled Github account or prefer HTTPS, then clone the repo with `git clone https://github.com/rudderlabs/rudder-server.git`. Move to the directory `cd rudder-server` and change the _rudder-transformer_ submodule path to HTTPS
   `sed -i.bak 's,git@github.com:rudderlabs/rudder-transformer.git,https://github.com/rudderlabs/rudder-transformer.git,g' .gitmodules`. Update the _rudder-transformer_ with `git submodule init && git submodule update`

3. Replace `<your_workspace_token>` in `build/docker.env` with the above token.
4. (Optional) Uncomment and set `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` in `build/docker.env` if you want to add S3 as a destination on the UI.
5. Run the command `docker-compose up --build` to bring up all the services.
6. Follow (Send Test Events) instructions below to send test event.

---

# Setup Instructions (Kubernetes)

1. Go to the [dashboard][dashboard] `https://app.rudderlabs.com` and set up your account. Copy your workspace token from top of the home page.

2. Our helm scripts and instructions are in a separate repo - [Download Here][helm-scripts-git-repo]

---

# Setup Instructions (Native Installation)

Disclaimer: This is not the easiest way of installing RudderStack. Please use this if you want to know more about the internals.

1. Install Golang 1.13 or above. [Download Here][golang]
2. Install NodeJS 10.6 or above. [Download Here][node]
3. Install PostgreSQL 10 or above and set up the DB

```
psql -c "CREATE DATABASE jobsdb"
psql -c "CREATE USER rudder SUPERUSER"
psql "jobsdb" -c "ALTER USER rudder WITH ENCRYPTED PASSWORD 'rudder'";
psql "jobsdb" -c "GRANT ALL PRIVILEGES ON DATABASE jobsdb to rudder";
```

4. Go to the [dashboard][dashboard] and set up your account. Copy your workspace token from top of the home page
5. Clone this repository. Run `git submodule init` and `git submodule update` to fetch the rudder-transformer repo.
   and navigate to the transformer directory `cd rudder-transformer`
6. Install dependencies `npm i` and start the destination transformer `node destTransformer.js`
7. Navigate back to main directory `cd rudder-server`. Copy the sample.env to the main directory `cp config/sample.env .env`
8. Update the `WORKSPACE_TOKEN` environment variable with the token fetched in step 4
9. Run the backend server `go run -mod=vendor main.go`
10. Follow (Send Test Events) instructions below to send test event.

---

# Send Test Events

1. If you already have a Google Analytics account, keep the tracking ID handy. If not, please create one and get the tracking ID. The Google Analytics account needs to have a **Web** Property (**Web+App** does't seem to work)
2. Create one source (Android or iOS) and configure a Google Analytics destination for the same with the above tracking ID
3. We have bundled a shell script that can generate test events. Get the source “writeKey” from our app dashboard and then run the following command. Run `cd scripts; ./generate-event <writeKeyHere> http://localhost:8080/v1/batch`. NOTE: `writeKey` is different from the `your_workspace_token` in step 2. Former is associated with the source while the latter is for your account.
4. You can then login to your Google Analytics account and verify that events are delivered. Go to `MainPage->RealTime->Events`. `RealTime` view is important as the other dashboard can sometimes take 24-48 hrs to refresh.
5. You can use our [Javascript][rudder-sdk-js-git-repo], [Android][rudder-sdk-android-git-repo] or [iOS][rudder-sdk-ios-git-repo] SDKs for sending events from your app.

---

# RudderStack Config Generator

Rudderstack has two components _control plane_ and _data plane_.
Data plane reliably delivers your event data. Control plane manages the configuration of your sources and destinations.
This configuration can also be read from a file instead of from Control plane, if you don't want to use our hosted control plane.

Config-generator provides the UI to manage the source and destination configurations without needing to signup, etc.
All the source and destination configuration stays on your local storage. You can export/import config to a JSON file.

## Setup

1. `cd utils/config-gen`
2. `npm install`
3. `npm start`

RudderStack config generator starts on the default port i.e., http://localhost:3000.
On a successful setup, you should see the following

![image](https://blobscdn.gitbook.com/v0/b/gitbook-28427.appspot.com/o/assets%2F-Lq586FOQtfjJPKbd01W%2F-M0LidOVklHOkLYEulS4%2F-M0M3fzyrb-UHiNBG7j0%2FScreenshot%202020-02-18%20at%2012.20.57%20PM.png?alt=media&token=a3f24ad8-fe72-4fed-8953-8e4c790f6cfd)

## Export workspace config

After adding the required sources and destinations, export your workspace config. This workspace-config is required by the RudderStack Server.
To learn more about adding sources and destinations in RudderStack, refer [Adding a Source and Destination in RudderStack](https://docs.rudderstack.com/getting-started/adding-source-and-destination-rudderstack)

Update the [config](https://docs.rudderstack.com/administrators-guide/config-parameters) variables `configFromFile` and `configJSONPath` in rudder-server to read workspace config from the exported JSON file.

## Start RudderStack with the workspace config file

- Download the workspace config file on your machine.
- In `docker-compose.yml`, uncomment `volumes` section under `backend` service. Specify the path to your workspace config.
- In `build/docker.env`, set the environment variable `RSERVER_BACKEND_CONFIG_CONFIG_FROM_FILE=true`

---

# RudderStack Architecture

The following is a brief overview of the major components of RudderStack.
![image](https://user-images.githubusercontent.com/52487451/64673994-471beb00-d48d-11e9-854f-2c3fbc021e63.jpg)

## RudderStack Control Plane

The UI to configure the sources, destinations etc. It consists of

**Config backend**: This is the backend service that handles the sources, destinations and their connections. User management and access based roles are defined here.

**Customer webapp**: This is the front end application that enables the teams to set up their customer data routing with RudderStack. These will show you high-level data on event deliveries and more stats. It also provides access to custom enterprise features.

## RudderStack Data Plane

Data plane is our core engine that receives the events, stores, transforms them and reliably delivers to the destinations. This engine can be customized to your business requirements by a wide variety of configuration options. Eg. You can choose to enable backing up events to an S3 bucket, the maximum size of the event for the server to reject malicious requests. Sticking to defaults will work well for most of the companies but you have the flexibility to customize the data plane.

The data plane uses Postgres as the store for events. We built our streaming framework on top of Postgres – that’s a topic for a future blog post. Reliable delivery and order of the events are the first principles in our design.

## RudderStack Destination Transformation

Conversion of events from RudderStack format into destination-specific format is handled by the transformation module. The transformation codes are written in Javascript. I

The following blogs provide an overview of our transformation module

https://rudderlabs.com/transformations-in-rudder-part-1/

https://rudderlabs.com/transformations-in-rudder-part-2/

If you are missing a transformation, please feel free to add it to the repository.

## RudderStack User Transformation

RudderStack also supports user-specific transformations for real-time operations like aggregation, sampling, modifying events etc. The following blog describes one real-life use case of the transformation module

https://rudderlabs.com/customer-case-study-casino-game/

## Client SDKs

The client SDKs provide APIs collecting events and sending it to the RudderStack Backend.

# Coming Soon

1. More performance benchmarks. On a single m4.2xlarge, RudderStack can process ~3K events/sec. We will evaluate other instance types and publish numbers soon.
2. More documentation
3. More destination support
4. HA support
5. More SDKs (or Segment compatibility)

<!----variable's---->

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
