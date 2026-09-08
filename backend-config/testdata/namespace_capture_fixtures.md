# Namespace config fixtures

`sample_namespace_v1.json.gpg` and `sample_namespace_v2.json.gpg` are a **paired** capture of the
same namespace, taken moments apart from both control plane endpoints:

| fixture                        | endpoint                                                                |
| ------------------------------ | ----------------------------------------------------------------------- |
| `sample_namespace_v1.json.gpg` | `GET $CONFIG_BACKEND_URL/data-plane/v1/namespaces/{ns}/config`           |
| `sample_namespace_v2.json.gpg` | `GET $CONFIG_BACKEND_URL/configuration/v2/namespaces/{ns}?secrets=embed` |

They are scrubbed by `anonymize_namespace_capture.py` and encrypted on top of that.

## Running the test

The test decrypts the fixtures itself, so the passphrase is all it needs:

```sh
WORKSPACE_CONFIG_FIXTURE_PASSPHRASE=... go test ./backend-config/ -run TestV2MapperAgainstFixtures
```

Without those two variables the test skips, which is also what happens on a pull request from a
fork, where the passphrase secret is not available.

The passphrase lives in the `WORKSPACE_CONFIG_FIXTURE_PASSPHRASE` repository secret (Actions and
Dependabot).

## Refreshing the pair

Capture both endpoints **in one go** - they are only a golden pair if nothing changed in between -
scrub, verify, then encrypt. Work in a scratch directory outside the repository: the raw captures
carry live credentials and must never be committed. The scrub refuses to run inside a git work tree
for that reason, and both the raw and the scrubbed filenames are gitignored.

```sh
cd "$(mktemp -d)"
REPO=/path/to/rudder-server

kubectl exec <rudderstack-pod> -- sh -c \
  'wget -qO- --header="Authorization: Basic $(printf "%s:" "$HOSTED_SERVICE_SECRET" | base64)" \
   "$CONFIG_BACKEND_URL/data-plane/v1/namespaces/$WORKSPACE_NAMESPACE/config"' > v1-raw.json

kubectl exec <rudderstack-pod> -- sh -c \
  'wget -qO- --header="Authorization: Basic $(printf "%s:" "$HOSTED_SERVICE_SECRET" | base64)" \
   "$CONFIG_BACKEND_URL/configuration/v2/namespaces/$WORKSPACE_NAMESPACE?secrets=embed"' > v2-raw.json

python3 $REPO/backend-config/testdata/anonymize_namespace_capture.py   # writes v1-anon.json / v2-anon.json

CAPTURE=$PWD
for v in v1 v2; do
  gpg --batch --yes --symmetric --cipher-algo AES256 --compress-algo bzip2 \
      --passphrase "$WORKSPACE_CONFIG_FIXTURE_PASSPHRASE" \
      -o $REPO/backend-config/testdata/sample_namespace_$v.json.gpg $CAPTURE/$v-anon.json
done

cd $REPO && go test ./backend-config/ -run TestV2MapperAgainstFixtures -count=1
make sec # gitleaks, in case a capture escaped the scratch directory anyway
```
