module github.com/gallowaysoftware/murmur/pkg/exec/batch/sparkconnect

go 1.25.0

// Spark Connect support is shipped as its own Go module so that the rest of
// Murmur (95% of consumers) doesn't need to mirror this fork-replace into
// their own go.mod. Users of THIS module must still mirror the replace below
// in their own go.mod — Go does not propagate replace directives transitively.
replace github.com/apache/spark-connect-go => github.com/pequalsnp/spark-connect-go v0.0.0-20250826122459-0e3d565b63e6

// In-tree development: bind the parent module to the working tree so
// `go mod tidy` resolves pkg/state etc. against the local source rather than
// fetching the root module from the module proxy (which would shadow the
// in-tree submodule path and produce an ambiguous-import error).
//
// The version on the right side is irrelevant for the local replace; users
// outside the workspace who depend on this submodule will satisfy
// github.com/gallowaysoftware/murmur from their own go.mod's require line.
replace github.com/gallowaysoftware/murmur => ../../../..

require (
	github.com/apache/spark-connect-go v0.0.0-00010101000000-000000000000
	github.com/aws/aws-sdk-go-v2 v1.45.1
	github.com/aws/aws-sdk-go-v2/config v1.33.1
	github.com/aws/aws-sdk-go-v2/credentials v1.20.1
	github.com/aws/aws-sdk-go-v2/service/dynamodb v1.65.1
	github.com/gallowaysoftware/murmur v0.0.0-00010101000000-000000000000
)

require (
	cloud.google.com/go/compute/metadata v0.9.0 // indirect
	github.com/apache/arrow-go/v18 v18.6.0 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.19.1 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.5.1 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.8.1 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.5.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.13.19 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/endpoint-discovery v1.13.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.14.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/signin v1.7.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.35.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.40.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.47.1 // indirect
	github.com/aws/smithy-go v1.28.1 // indirect
	github.com/go-errors/errors v1.5.1 // indirect
	github.com/goccy/go-json v0.10.6 // indirect
	github.com/google/flatbuffers v25.12.19+incompatible // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/klauspost/compress v1.19.2 // indirect
	github.com/klauspost/cpuid/v2 v2.3.0 // indirect
	github.com/pierrec/lz4/v4 v4.1.26 // indirect
	github.com/zeebo/xxh3 v1.1.0 // indirect
	golang.org/x/exp v0.0.0-20260112195511-716be5621a96 // indirect
	golang.org/x/net v0.58.0 // indirect
	golang.org/x/oauth2 v0.36.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.41.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260526163538-3dc84a4a5aaa // indirect
	google.golang.org/grpc v1.83.2 // indirect
	google.golang.org/protobuf v1.36.12 // indirect
)
