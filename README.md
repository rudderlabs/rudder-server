![Build Status][Build Status]
[![Go Report Card][GoReportCardBadge]][GoReportCard]
# What is Rudder?

**Short answer:** Rudder is an open-source Segment alternative written in Go, built for the enterprise. .

**Long answer:** Rudder is a platform for collecting, storing and routing customer event data to dozens of tools. Rudder is open-source, can run in your cloud environment (AWS, GCP, Azure or even your data-centre) and provides a powerful transformation framework to process your event data on the fly.

Rudder runs as a single go binary with Postgres. It also needs the destination (e.g. GA, Amplitude) specific transformation code which are node scripts. This repo contains the core backend and the transformation modules of Rudder.
The client SDKs are in a separate repo (link below).

Rudder server is released under [SSPL License][SSPL_License]

Questions? Join our [Discord][Discord] channel. Or please email soumyadeb at rudderlabs.com.

# Why Rudder ?

We are building Rudder because we believe open-source and cloud-prem is important for three main reasons

1. **Privacy & Security:** You should be able to collect and store your customer data without sending everything to a 3rd party vendor or embedding proprietary SDKs. With Rudder, the event data is always in your control. Besides, Rudder gives you fine-grained control over what data to forward to what analytical tool.

2. **Processing Flexibility:** You should be able to enhance OR transform your event data by combining it with your other _internal_ data, e.g. stored in your transactional systems. Rudder makes that possible because it provides a powerful JS-based event transformation framework. Furthermore, since Rudder runs _inside_ your cloud or on-prem environment, you can access your production data to join with the event data.

3. **Unlimited Events:** Event volume-based pricing of most commercial systems is broken. You should be able to collect as much data as possible without worrying about overrunning event budgets. Rudder's core BE is open-source and free to use.

See the [HackerNews][HackerNews] discussion around Rudder.

# Contribution
We would love to see people contributing to rudder. see [CONTRIBUTING.md](CONTRIBUTING.md) for more information on contributing to rudder.
# Stay Connected

1. Join our [Discord][Discord]
2. Follow [Rudderlabs][Twitter] on Twitter

# Setup Instructions (Hosted Demo Account)

1. Go to the [dashboard][Dashboard] `https://app.rudderlabs.com` and set up your account.
2. Select `Rudder Hosted Service` from the top right corner after you login.
3. Follow (Send Test Events) instructions below to send test event.

# Setup Instructions (Docker)

The docker setup is the easiest & fastest way to try out Rudder.

1. Go to the [dashboard][Dashboard] `https://app.rudderlabs.com` and set up your account. Copy your workspace token from top of the home page.
2. Clone this repository with [SSH](https://help.github.com/en/articles/which-remote-url-should-i-use#cloning-with-ssh-urls) and
3. Replace `<your_workspace_token>` in `build/docker.env` with the above token.
4. (Optional) Uncomment and set `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` in `build/docker.env` if you want to add S3 as a destination on the UI.
5. Run `git submodule init` and `git submodule update` to fetch the rudder-transformer repo.
6. Run the command `docker-compose up --build` to bring up all the services.
7. Follow (Send Test Events) instructions below to send test event.

# Setup Instructions (Kubernetes)

Our helm scripts and instructions are in a separate repo - [Download Here][HelmScriptsGitRepo]

# Setup Instructions (Terraform)

Our terraform scripts and instructions are in a separate repo - [Download Here][TerraformScriptsGitRepo]


# Setup Instructions (Native Installation)

Disclaimer: This is not the easiest way of installing Rudder.  Please use this if you want to know more about the internals.

1. Install Golang 1.12 or above. [Download Here][Golang]
2. Install NodeJS 10.6 or above. [Download Here][Node]
3. Install PostgreSQL 10 or above and set up the DB

```
createdb jobsdb
createuser --superuser rudder
psql "jobsdb" -c "alter user rudder with encrypted password 'rudder'";
psql "jobsdb" -c "grant all privileges on database jobsdb to rudder";
```

4. Go to the [dashboard][Dashboard] and set up your account. Copy your workspace token from top of the home page
5. Clone this repository. Run `git submodule init` and `git submodule update` to fetch the rudder-transformer repo.
 and navigate to the transformer directory `cd rudder-transformer`
6. Start the destination transformer `node destTransformer.js`
7. Navigate back to main directory `cd rudder-server`. Copy the sample.env to the main directory `cp config/sample.env .env`
8. Update the `CONFIG_BACKEND_TOKEN` environment variable with the token fetched in step 4
9. Run the backend server `go run -mod=vendor main.go`
10. Follow (Send Test Events) instructions below to send test event.

# Send Test Events

1. If you already have a Google Analytics account, keep the tracking ID handy. If not, please create one and get the tracking ID. The Google Analytics account needs to have a **Web** Property (**Web+App** does't seem to work)
2. Create one source (Android or iOS) and configure a Google Analytics destination for the same with the above tracking ID
3. We have bundled a shell script that can generate test events. Get the source “writeKey” from our app dashboard and then run the following command. Run `./scripts/generate-event <writeKeyHere> http://localhost:8080/v1/batch`. NOTE: `writeKey` is different from the `your_workspace_token` in step 2. Former is associated with the source while the latter is for your account.
4. You can then login to your Google Analytics account and verify that events are delivered. Go to `MainPage->RealTime->Events`. `RealTime` view is important as the other dashboard can sometimes take 24-48 hrs to refresh.
5. You can use our [Javascript][RudderSdkJsGitRepo], [Android][RudderSdkAndroidGitRepo] or [iOS][RudderSdkIOSGitRepo] SDKs for sending events from your app.

## Features

1. Google Analytics, Amplitude, MixPanel, Adjust, AppsFlyer & Facebook destinations. Lot more coming soon.
2. S3 dump. Redshift and other data warehouses coming soon.
3. User-specified transformation to filter/transform events.
4. Stand-alone system. The only dependency is on Postgres.
5. High performance. On a single m4.2xlarge, Rudder can process ~3K events/sec. Performance numbers on other instance types soon.
6. Rich UI written in react.
7. [Javascript][RudderSdkJsGitRepo], [Android][RudderSdkAndroidGitRepo] or [iOS][[RudderSdkAndroidGitRepo]]. Server-side SDKs coming soon.


# UI Pages

## Connections Page
![image](https://user-images.githubusercontent.com/411699/65309691-36b0f200-dbaa-11e9-9631-8a9f81cea606.png)

## Events Page
![image](https://user-images.githubusercontent.com/52487451/65647230-e2937c80-dfb2-11e9-88bd-3b015c4b576f.png)

## Stats Page
![image](https://user-images.githubusercontent.com/52487451/65647158-a2cc9500-dfb2-11e9-8397-d2642f0b8801.png)

# Architecture

The following is a brief overview of the major components of Rudder Stack.
![image](https://user-images.githubusercontent.com/52487451/64673994-471beb00-d48d-11e9-854f-2c3fbc021e63.jpg)

## Rudder Control Plane

The UI to configure the sources, destinations etc. It consists of

**Config backend**: This is the backend service that handles the sources, destinations and their connections. User management and access based roles are defined here.

**Customer webapp**: This is the front end application that enables the teams to set up their customer data routing with Rudder. These will show you high-level data on event deliveries and more stats. It also provides access to custom enterprise features.

## Rudder Data Plane

Data plane is our core engine that receives the events, stores, transforms them and reliably delivers to the destinations. This engine can be customized to your business requirements by a wide variety of configuration options. Eg. You can choose to enable backing up events to an S3 bucket, the maximum size of the event for the server to reject malicious requests. Sticking to defaults will work well for most of the companies but you have the flexibility to customize the data plane.

The data plane uses Postgres as the store for events. We built our streaming framework on top of Postgres – that’s a topic for a future blog post. Reliable delivery and order of the events are the first principles in our design.

## Rudder Destination Transformation

Conversion of events from Rudder format into destination-specific format is handled by the transformation module. The transformation codes are written in Javascript. I

The following blogs provide an overview of our transformation module

https://rudderlabs.com/transformations-in-rudder-part-1/

https://rudderlabs.com/transformations-in-rudder-part-2/

If you are missing a transformation, please feel free to add it to the repository.

## Rudder User Transformation

Rudder also supports user-specific transformations for real-time operations like aggregation, sampling, modifying events etc. The following blog describes one real-life use case of the transformation module

https://rudderlabs.com/customer-case-study-casino-game/

## Client SDKs

The client SDKs provide APIs collecting events and sending it to the Rudder Backend.

# Coming Soon

1. More performance benchmarks. On a single m4.2xlarge, Rudder can process ~3K events/sec. We will evaluate other instance types and publish numbers soon.
2. More documentation
3. More destination support
4. HA support
5. More SDKs (or Segment compatibility)
6. Transformations from UI

<!----variable's---->

[Build Status]: https://codebuild.us-east-1.amazonaws.com/badges?uuid=eyJlbmNyeXB0ZWREYXRhIjoiT01EQkVPc0NBbDJLV2txTURidkRTMTNmWFRZWUY2dEtia3FRVmFXdXhWeUwzaC9aV3dsWWNNT0NwaVZKd1hKTFVMazB2cDQ5UHlaZTgvbFRER3R5SXRvPSIsIml2UGFyYW1ldGVyU3BlYyI6IktJQVMveHIzQnExZVE5b0YiLCJtYXRlcmlhbFNldFNlcmlhbCI6MX0%3D&branch=master
[Discord]: https://discordapp.com/invite/xNEdEGw
[Twitter]: https://twitter.com/rudderlabs
[GoReportCard]: https://goreportcard.com/report/github.com/rudderlabs/rudder-server
[GoReportCardBadge]: https://goreportcard.com/badge/github.com/rudderlabs/rudder-server
[Dashboard]: https://app.rudderlabs.com
[SSPL_License]: https://www.mongodb.com/licensing/server-side-public-license
[HackerNews]: https://news.ycombinator.com/item?id=21081756
[HelmScriptsGitRepo]: https://github.com/rudderlabs/rudderstack-helm
[TerraformScriptsGitRepo]: https://github.com/rudderlabs/rudder-terraform
[Golang]: https://golang.org/dl/
[Node]: https://nodejs.org/en/download/
[RudderSdkJsGitRepo]: https://github.com/rudderlabs/rudder-sdk-js
[RudderSdkAndroidGitRepo]: https://github.com/rudderlabs/rudder-sdk-android
[RudderSdkIOSGitRepo]: https://github.com/rudderlabs/rudder-sdk-ios
