# pytransformer contract tests

These tests run **two rudder-pytransformer containers side by side** and assert they behave identically. One is the
*baseline* (a released version), the other the *candidate* (what you're checking). Requests go through the real
`usertransformer.Client`, so the suite covers the actual rudder-server → PyT path rather than a stand-in.

A subtest that only exercises one container is a property test (connection pooling, DNS caching, redirects); those use
the candidate only. Everything else compares.

## Which two images

**`PYT_CANDIDATE_TAG` and `PYT_BASELINE_TAG` are both required.** There is no default pair, because a hardcoded one
goes stale: the suite would keep passing while comparing two versions nobody ships any more. `TestMain` fails the
package if either is missing, before any container starts.

```sh
PYT_CANDIDATE_TAG=0.11.0 PYT_BASELINE_TAG=0.10.2 go test ./integration_test/pytransformer_contract/ -count=1
```

CI resolves the two most recent released tags from ECR and exports them, so a new release is picked up without editing
anything here.

If the two resolve to the same tag, `startBaselinePytransformer` panics rather than let every comparison pass against
itself.

## Running

You need to be able to pull from ECR (see the Notion docs), and both tags set. To check your own PyT build, build it
first in the rudder-pytransformer repo — `make build-ecr-latest` tags it `main` — then point the candidate at it:

```sh
PYT_CANDIDATE_TAG=main PYT_BASELINE_TAG=<last released tag> make test package=integration_test/pytransformer_contract
# or one suite while iterating:
PYT_CANDIDATE_TAG=main PYT_BASELINE_TAG=<last released tag> go test ./integration_test/pytransformer_contract/ \
   -run TestBackwardsCompatibility -count=1 -timeout=20m
```

The rudder-pytransformer Makefile recipe that builds and runs this suite sets both for you, and requires the baseline
tag as an argument.

**Docker only pulls a tag it doesn't already have.** Skip the rebuild and `main` is whatever you last built or pulled,
and the suite compares that instead, silently. Rebuild before trusting a green run.

## Adding a test

`TestBaseContract` in `base_test.go` is the smallest complete example — copy it and change the Python code and events.
For a case that belongs with the existing table-driven suites, add an entry to the `subtests`
slice in `backwards_compatibility_test.go` or `geolocation_backwards_compatibility_test.go`.

Two rules worth knowing:

- **Assert on both sides, then compare.** `baselineResp.Equal(&candidateResp)` is what makes it a contract test;
  per-field asserts on each side keep the failure output readable when it breaks.
- **Give every subtest its own `versionID`.** Containers are shared across subtests, so the versionID is the only
  isolation boundary between them — a collision serves one subtest's transformation code to another. The suites assert
  on this, so a duplicate fails loudly.
