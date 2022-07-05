# rudder-server-enterprise
Enterprise specific features of the rudder-server repo

**This repo is not standalone. It is intended to be used as part of [rudder-server](https://github.com/rudderlabs/rudder-server)**

## Overview

This repo contains enterprise-only feature implementations of **rudder-server**, and needs to be build as part of it. The provided **Makefile** will check the existence of this repo, and will integrate the features in the code accordingly.

## Setup
In order to setup the enterprise version:

```
git checkout https://github.com/rudderlabs/rudder-server
cd rudder-server
make enterprise-init
```

## Building enterprise version

Provided this repo is linked with the parent project, simply running `make` on rudder-server will call the `import.sh` script, to auto-generate the `imports/enterprise.go` file. This file is responsible for importing any provided enterprise packages. It should not be modified or commited to the parent repo, and is ignored via `.gitignore`.
