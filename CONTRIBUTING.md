# Contributing to Rudder #

:heart: Thanks for taking the time and for your help improving this project!

## Getting Help ##

If you have a question about rudder or have encountered problems using it,
start by asking a question on [slack][slack] or [discord][discord]

## Developer Certificate of Origin ##

To contribute to this project, you must agree to the Developer Certificate of
Origin (DCO) for each commit you make. The DCO is a simple statement that you,
as a contributor, have the legal right to make the contribution.

See the [DCO](DCO) file for the full text of what you must agree to.

To signify that you agree to the DCO for a commit, you add a line to the
git commit message:

```
Signed-off-by: John Doe <john.doe@example.com>
```

In most cases, you can add this signoff to your commit automatically with the
`-s` flag to `git commit`. You must use your real name and a reachable email
address (sorry, no pseudonyms or anonymous contributions).

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

### Commit messages ###

Finalized commit messages should be in the following format:

```
Subject

Problem

Solution

Validation

Fixes #[GitHub issue ID]
```

#### Subject ####

- one line, <= 50 characters
- describe what is done; not the result
- use the active voice
- capitalize first word and proper nouns
- do not end in a period — this is a title/subject
- reference the GitHub issue by number

##### Examples #####



#### Problem ####

Explain the context and why you're making that change.  What is the problem
you're trying to solve? In some cases there is not a problem and this can be
thought of as being the motivation for your change.

#### Solution ####

Describe the modifications you've made.

If this PR changes a behavior, it is helpful to describe the difference between
the old behavior and the new behavior. Provide before and after screenshots,
example CLI output, or changed YAML where applicable.

Describe any implementation changes which are particularly complex or
unintuitive.

List any follow-up work that will need to be done in a future PR and link to
any relevant Github issues.

#### Validation ####

Describe the testing you've done to validate your change.  Give instructions
for reviewers to replicate your tests.  Performance-related changes should
include before- and after- benchmark results.

<!----variable's---->

[discord]: https://discordapp.com/invite/xNEdEGw
[issue]: https://github.com/rudderlabs/rudder-server/issues/new
[slack]: https://rudderlabs.slack.com/
