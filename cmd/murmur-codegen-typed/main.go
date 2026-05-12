// murmur-codegen-typed generates a typed Connect-RPC service from a
// pipeline-spec YAML.
//
// Usage:
//
//	murmur-codegen-typed --in pipeline-spec.yaml --out gen/
//
// Writes:
//
//	gen/<service>.proto         — proto messages + service definition
//	gen/<service>_server.go     — Go server stub using pkg/query/typed
//
// The user runs buf (or protoc + connect-go plugin) on the .proto to
// generate the typed message + connect-handler types, then compiles
// the _server.go alongside their app.
//
// What this tool is NOT: it does not run protoc/buf for you. The
// generated .proto file is the contract; the user owns the proto
// pipeline. That keeps the dependency footprint zero and lets the
// user pin their preferred protoc plugin versions.
//
// Pipeline kinds supported: sum, hll, topk, bloom.
// Method kinds supported: get_all_time, get_window, get_window_many,
// get_many, get_range. get_many and get_range are sum-only (the typed
// clients in pkg/query/typed only expose those methods on SumClient).
package main

import (
	"flag"
	"fmt"
	"go/format"
	"os"
	"path/filepath"
	"strings"
)

func main() {
	in := flag.String("in", "", "path to pipeline-spec YAML (required)")
	out := flag.String("out", "gen/", "output directory")
	flag.Parse()

	if *in == "" {
		flag.Usage()
		os.Exit(2)
	}

	if err := generate(*in, *out); err != nil {
		fmt.Fprintf(os.Stderr, "murmur-codegen-typed: %v\n", err)
		os.Exit(1)
	}
}

// generate is the testable entry point — main wraps it.
func generate(specPath, outDir string) error {
	spec, err := loadSpec(specPath)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", outDir, err)
	}

	protoBytes, err := renderProto(spec)
	if err != nil {
		return fmt.Errorf("render proto: %w", err)
	}
	protoPath := filepath.Join(outDir, fileBaseFromService(spec.Service.Name)+".proto")
	if err := os.WriteFile(protoPath, protoBytes, 0o644); err != nil {
		return fmt.Errorf("write %s: %w", protoPath, err)
	}

	serverBytes, err := renderServer(spec)
	if err != nil {
		return fmt.Errorf("render server: %w", err)
	}
	// gofmt the server output. If gofmt fails (e.g. invalid Go), we
	// still write the unformatted bytes so the user can debug.
	if formatted, ferr := format.Source(serverBytes); ferr == nil {
		serverBytes = formatted
	}
	serverPath := filepath.Join(outDir, fileBaseFromService(spec.Service.Name)+"_server.go")
	if err := os.WriteFile(serverPath, serverBytes, 0o644); err != nil {
		return fmt.Errorf("write %s: %w", serverPath, err)
	}

	return nil
}

// fileBaseFromService converts "BotInteractionCountService" to
// "bot_interaction_count_service".
func fileBaseFromService(name string) string {
	var sb strings.Builder
	for i, r := range name {
		if i > 0 && r >= 'A' && r <= 'Z' {
			sb.WriteByte('_')
		}
		if r >= 'A' && r <= 'Z' {
			sb.WriteRune(r + 32)
		} else {
			sb.WriteRune(r)
		}
	}
	return sb.String()
}
