# Contributing to RudderStack #

Thanks for taking the time and for your help in improving this project!

## Getting Help ##

If you have a question about RudderStack or have encountered problems using it, start by asking a question on our [Slack](https://resources.rudderstack.com/join-rudderstack-slack) channel.

## RudderStack Contributor Agreement ##

To contribute to this project, we need you to sign to [Contributor License Agreement (“CLA”)][CLA] for the first commit you make. By agreeing to the [CLA][CLA]
we can add you to list of approved contributors and review the changes proposed by you.

## Installing and Setting Up RudderStack

To contribute to this project, you need to install RudderStack on your machine. By following a few simple instructions, you can get your machine up and running to use RudderStack in no time.


1. Download and install [Golang 1.13](https://golang.org/dl/) or above.

2. Download and install [Node.js 10.6](https://nodej.org/en/download/) or above.

3. Download and install [PostgreSQL 10](https://www.postgresql.org/download/) or above, and set up the database using the following commands:

 ```
 createdb jobsdb
 createuser --superuser rudder
 psql "jobsdb" -c "alter user rudder with encrypted password 'rudder'";
 psql "jobsdb" -c "grant all privileges on database jobsdb to rudder";

 ```

4. Go to the [RudderStack dashboard](https://app.rudderstack.com/signup) and set up your account. Copy your workspace token from the top of the home page.

5. Clone the RudderStack server repository. Run `git submodule init` and `git submodule update` to fetch the [**rudder-transformer**](https://github.com/rudderlabs/rudder-transformer) repository. Then, navigate to the transformer directory using the following command:

```
cd rudder-transformer
```

6. Install dependencies using the command `npm install` and start the destination transformer using the following command:

```
npm start

```

7. Navigate back to the main directory using the following command:

```
cd rudder-server

```

8. Copy the `sample.env` to the main directory using the following command:

```
cp config/sample.env .env

```

9. Update the `WORKSPACE_TOKEN` environment variable with the `workspace token fetched` from the RudderStack dashboard.

10. Run the backend server using the following command:

```
go run -mod=vendor main.go

```

Once you have successfully followed the steps above, follow our guide on [How to Send Test Events](https://docs.rudderstack.com/getting-started/installing-and-setting-up-rudderstack#how-to-send-test-events) in order to test if there are any issues with the installation.

There you go! You can now start using RudderStack on your machine.

## The Best Way to Contribute to RudderStack ##

The best way to contribute to our open source software is to create an integration. An integration is a connection between RudderStack and a downstream destination where you would like your event data to flow to. There are a several reasons that you may want to build an integration
- If you would like to send data to a certain destination but RudderStack doesn't offer it yet.
- If you have developed a tool that you would like RudderStack to integrate with to expand your user-base.
- If you want to add features to an already existing integration.
- And many more! Feel free to chat with us on our [Slack](https://resources.rudderstack.com/join-rudderstack-slack) channel and share your ideas!

You can also contribute to any open source RudderStack project. View our GitHub Page to see all of the different projects.

> **_NOTE:_**  The note content.For creating an integration, the primary GitHub Repository you will need to work with will be [`rudder-transformer`](https://github.com/rudderlabs/rudder-transformer)

### Submitting a Pull Request ###

- For creating a pull request for an integration contribution, follow [these instructions from the docs](https://docs.rudderstack.com/user-guides/how-to-guides/how-to-submit-an-integration-pull-request)
- For any other pull requests, you can refer to [this specific section of the doc](https://docs.rudderstack.com/user-guides/how-to-guides/how-to-submit-an-integration-pull-request#creating-a-pull-request). 

> **_NOTE:_**  The type of change you make will dictate what repositories you will need to make pull requests for. Please reach out to us on our [Slack](https://resources.rudderstack.com/join-rudderstack-slack) channel if you have any questions.

### Committing ###

We prefer squash or rebase commits so that all changes from a branch are
committed to master as a single commit. All pull requests are squashed when
merged, but rebasing prior to merge gives you better control over the commit
message.

We look forward to your feedback on improving this project.


<!----variable's---->

[issue]: https://github.com/rudderlabs/rudder-server/issues/new
[CLA]: https://rudderlabs.wufoo.com/forms/rudderlabs-contributor-license-agreement
