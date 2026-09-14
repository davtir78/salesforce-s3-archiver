# Third Party Notices

salesforce-s3-archiver is a derivative work of
[newrelic/newrelic-salesforce-exporter](https://github.com/newrelic/newrelic-salesforce-exporter),
Copyright New Relic, Inc., licensed under the Apache License 2.0. Files originating from that
project have been modified.

The file `internal/integration/stream/pubsub/proto/pubsub_api.pb.go` and
`pubsub_api_grpc.pb.go` are generated from the Salesforce Pub/Sub API protocol definition
published at [forcedotcom/pub-sub-api](https://github.com/forcedotcom/pub-sub-api).

This project uses the following third party libraries, which carry their own copyright notices
and license terms:

| Library | License |
|---|---|
| [github.com/aws/aws-sdk-go-v2](https://github.com/aws/aws-sdk-go-v2) | Apache-2.0 |
| [github.com/go-viper/mapstructure](https://github.com/go-viper/mapstructure) | MIT |
| [github.com/golang-jwt/jwt](https://github.com/golang-jwt/jwt) | MIT |
| [github.com/linkedin/goavro](https://github.com/linkedin/goavro) | Apache-2.0 |
| [github.com/prometheus/client_golang](https://github.com/prometheus/client_golang) | Apache-2.0 |
| [github.com/redis/go-redis](https://github.com/redis/go-redis) | BSD-2-Clause |
| [github.com/spf13/viper](https://github.com/spf13/viper) | MIT |
| [google.golang.org/grpc](https://github.com/grpc/grpc-go) | Apache-2.0 |
| [google.golang.org/protobuf](https://github.com/protocolbuffers/protobuf-go) | BSD-3-Clause |

Transitive dependencies are listed in `go.sum`; their licenses are available in the Go module cache.
