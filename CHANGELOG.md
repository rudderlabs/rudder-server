## v0.1.5 (February 7, 2020)

ENHANCEMENTS:

* RudderStack can route events to Redshift and BigQuery warehouses in a single node mode

* Support for Rate-limiting events via configuration

* Support for object storage destinations Azure Cloud Storage and Google Cloud Storage

* New stats to profile the processor module and JobsDB

* JobsDB table size can be configured by size or number of rows

* Table dumps file name changes to support faster replays

* Flexible API format between the backend and the transformer. Support added more HTTP methods and content-types to send data to destinations 

* Improved user transformations memory management

* Kubernetes support for enterprise edition

BUG FIXES:

* OOM crash during table dumps

* Postgres invalid JSON content crash

* Multiple alerts issue when the server enters the degraded mode

* Multiple maintenance databases

* Recommendations for production deployments

* Gateway latency fix

* Created_at timestamp is generated at Postgres now


BREAKING CHANGES:

* Events disk format hasn't changed. To handle the inflight changes, bring down both the services and bring them up back with the latest releases in the following order the transformer and then the backend. This can be a rolling update node by node.


