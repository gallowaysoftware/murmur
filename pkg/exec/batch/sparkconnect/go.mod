module github.com/gallowaysoftware/murmur/pkg/exec/batch/sparkconnect

go 1.26.2

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
	github.com/gallowaysoftware/murmur v0.0.0-00010101000000-000000000000
)

require (
	cloud.google.com/go/compute/metadata v0.9.0 // indirect
	github.com/apache/arrow-go/v18 v18.4.0 // indirect
	github.com/go-errors/errors v1.5.1 // indirect
	github.com/goccy/go-json v0.10.5 // indirect
	github.com/google/flatbuffers v25.2.10+incompatible // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/klauspost/compress v1.18.5 // indirect
	github.com/klauspost/cpuid/v2 v2.2.11 // indirect
	github.com/pierrec/lz4/v4 v4.1.26 // indirect
	github.com/zeebo/xxh3 v1.0.2 // indirect
	golang.org/x/exp v0.0.0-20250408133849-7e4ce0ab07d0 // indirect
	golang.org/x/mod v0.34.0 // indirect
	golang.org/x/net v0.53.0 // indirect
	golang.org/x/oauth2 v0.36.0 // indirect
	golang.org/x/sync v0.20.0 // indirect
	golang.org/x/sys v0.43.0 // indirect
	golang.org/x/telemetry v0.0.0-20260311193753-579e4da9a98c // indirect
	golang.org/x/text v0.36.0 // indirect
	golang.org/x/tools v0.43.0 // indirect
	golang.org/x/xerrors v0.0.0-20240903120638-7835f813f4da // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260226221140-a57be14db171 // indirect
	google.golang.org/grpc v1.81.0 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)
