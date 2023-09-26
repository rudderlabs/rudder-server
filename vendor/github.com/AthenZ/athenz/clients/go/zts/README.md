# zts-go-client

A Go client library to talk to Athenz ZTS.

The model.go and client.go files are generated from zts_core, and checked in so users of this library need not know that.

Release Notes:
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Version 1.0 (2016-09-06)
 - Initial opensource release

## Usage

To get it into your workspace:

    go get github.com/AthenZ/athenz/clients/go/zts

Then in your Go code:

    import (
        zts "github.com/AthenZ/athenz/clients/go/zts"
    )
    func main() {
         var principal rdl.Principal /* NToken */
         ...
         client := zts.NewClient()
         client.AddCredentials(principal.GetHTTPHeaderName(), principal.GetCredentials())
         svc, err := client.GetServiceIdentity("athenz", "storage")
         ...
    }

## License

Copyright 2016 Yahoo Inc.

Licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)
