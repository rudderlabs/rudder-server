
Rudder is an open source Segment alternative written in Go. https://rudderlabs.com.
 
What is Rudder?
===============

Rudder is a platform for collecting, storing and routing customer event data to dozens of tools. Rudder is open-source, can run in your own cloud environment (AWS, GCP, Azure or even your own data-center) and provides a powerful transformation framework to process your event data.


Why Rudder ?
============

We believe open-source and cloud-prem is important for three main reasons

1. Privacy & Security: You should be able to collect and store your customer data without sending everything to 3rd party vendors or embedding proprietary SDKs. Rudder gives you fine grained control over what data to forward to what analytical tool.

2. Processing Flexibility: You should be able to enhance OR transform your event data by combining it with your other *internal* data stored in your transactional systems. Running *inside* your cloud environment makes that possible.

3. Pricing: Event volume based pricing of most commercial system is broken. You should be able to collect as much data as possible without worrying about overrunning event budgets.


Main Page
=========

![image](https://user-images.githubusercontent.com/52487451/64673168-0b802180-d48b-11e9-8535-9292eff0aa45.png)

Setup Instructions (Docker)
===========================

The docker setup is the easiest & fastest way to try out Rudder. 

1. Checkout this repo https://github.com/rudderlabs/rudder-oss
2. Run the command `docker-compose up` to bring up all the services.
3. If you already have a Google Analytics account, keep the tracking ID handy. If not, please create one and get the tracking ID.
4. Go to http://localhost:3000 to set up source and destinations. Add a new source from the dropdown for Android/iOS source definitions. Configure your Google Analytics destination with the tracking ID from step above.

5. We have bundled a shell script that can generate test events. Get the “writeKey” from our app dashboard and then run the following command. Run `./generate-event <writeKeyHere>`
 The script generates a sample event and sends it to the backend container that is running in docker. Based on our destination configuration, the backend will transform the event and forward it to the configured destination.
 
6. You can then login to your Google Analytics account and verify that events are delivered in the correct order.

7. You can use our Android, iOS or Javascript SDKs for sending events from your app.

Setup Instructions (this repo)
==============================

If you want to run each of the services without docker please follow the following steps

1. Install Golang 1.12 or above
2. Install NodeJS 10.6 or above
3. Install PostgreSQL 10 or above
4. Login to https://app.rudderlabs.com and setup your account. Copy your workspace token from top of the home page
5. Clone this repository and navigate to the transformer directory `cd rudder-transformer`
6. Start the user and destination transformers as separate processes `node userTransformer.js` and `node destTransformer.js`
7. Navigate back to main directory `cd rudder-server`. Copy the sample.env to the main directory `cp build/sample.env .env`
8. Update the `CONFIG_BACKEND_TOKEN` environment variable with the token fetched in step 4
9. Run the backend server `go run -mod=vendor main.go`
10. Setup your sources from the dashboard `https://app.rudderlabs.com` and start sending events using the test script (mentioned in step 5 of Docker setup instructions) or our SDKs.


Architecture
============
The following is a  brief overview of the major components of Rudder Stack.
![image](https://user-images.githubusercontent.com/52487451/64673994-471beb00-d48d-11e9-854f-2c3fbc021e63.jpg)

## Rudder Control Plane

The UI to configure the sources, destinations etc. It consists of 

**Config backend**: This is the backend service that handles the sources, destinations and their connections. User management and access based roles are defined here.

**Customer webapp**: This is the front end application that enables the teams to set up their customer data routing with Rudder. These will show you high-level data on event deliveries and more stats. It also provides access to custom enterprise features. 
 
## Rudder Data Plane  

Data plane is our core engine that receives the events, stores, transforms them and reliably delivers to the destinations. This engine can be customized to your business requirements by a wide variety of configuration options. Eg. You can choose to enable backing up events to any S3 bucket, maximum size of the event for server to reject malicious requests. Sticking to defaults will work well for most of the companies but you have the flexibility to customize the data plane.

The data plane uses Postgres as the store for events. We built our own streaming framework on top of Postgres – that’s a topic for a future blog post. Reliable delivery and ordering of the events are the first principles in our design.

## Rudder Destination Transformation

Conversion of events from Rudder format into destination specific format is handled by the transformation module. The transformation codes are written in Javascript. I

The following blogs provide an overview of our transformation module

https://rudderlabs.com/transformations-in-rudder-part-1/

https://rudderlabs.com/transformations-in-rudder-part-2/

If you are missing a transformation, please feel free to add it to the repository.

## Rudder User Transformation

Rudder also supports user specific transformations  for real time operations like aggregation, sampling, modifying events etc. The following blog describes one real life use case of the transformation module

https://rudderlabs.com/customer-case-study-casino-game/

## Client SDKs

The client SDKs provide APIs collecting events and sending it to the Rudder Backend.

