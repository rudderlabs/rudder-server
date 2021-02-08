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

## Submitting a Pull Request ##

Do you have an improvement?

1. Submit an [issue][issue] describing your proposed change.
2. We will try to respond to your issue promptly.
3. Fork this repo, develop and test your code changes. See the project's [README](README.md) for further information about working in this repository.
4. Submit a pull request against this repo's `master` branch.
    - Include instructions on how to test your changes.
5. Your branch may be merged once all configured checks pass, including:
    - A review from appropriate maintainers

## Committing ##

We prefer squash or rebase commits so that all changes from a branch are
committed to master as a single commit. All pull requests are squashed when
merged, but rebasing prior to merge gives you better control over the commit
message.

We look forward to your feedback on improving this project.


<!----variable's---->

[issue]: https://github.com/rudderlabs/rudder-server/issues/new
[CLA]: https://rudderlabs.wufoo.com/forms/rudderlabs-contributor-license-agreement
