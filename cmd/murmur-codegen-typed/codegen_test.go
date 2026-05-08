package main

import (
	"bytes"
	"go/format"
	"os"
	"path/filepath"
	"testing"
)

// TestGenerate_BotInteractions exercises the codegen end-to-end against
// the worked example in examples/typed-rpc-codegen/bot-interactions.
// Compares output to the checked-in expected/ files.
//
// To regenerate the expected files after intentional template changes:
//
//	go run ./cmd/murmur-codegen-typed \
//	   --in examples/typed-rpc-codegen/bot-interactions/pipeline-spec.yaml \
//	   --out examples/typed-rpc-codegen/bot-interactions/expected/
func TestGenerate_BotInteractions(t *testing.T) {
	tmp := t.TempDir()
	specPath := filepath.Join("..", "..", "examples", "typed-rpc-codegen", "bot-interactions", "pipeline-spec.yaml")
	expectedDir := filepath.Join("..", "..", "examples", "typed-rpc-codegen", "bot-interactions", "expected")

	if err := generate(specPath, tmp); err != nil {
		t.Fatalf("generate: %v", err)
	}

	for _, name := range []string{
		"bot_interaction_count_service.proto",
		"bot_interaction_count_service_server.go",
	} {
		got, err := os.ReadFile(filepath.Join(tmp, name))
		if err != nil {
			t.Errorf("%s: read got: %v", name, err)
			continue
		}
		want, err := os.ReadFile(filepath.Join(expectedDir, name))
		if err != nil {
			t.Errorf("%s: read expected: %v", name, err)
			continue
		}
		if !bytes.Equal(got, want) {
			t.Errorf("%s: output differs from expected.\n--- want ---\n%s\n--- got ---\n%s",
				name, string(want), string(got))
		}
	}
}

// TestGenerate_GoStubCompiles verifies that the generated Go server stub
// is at least gofmt-clean and would survive `go/format.Source`. We can't
// compile-link it without the buf-generated proto types, but `format.Source`
// catches structural issues like missing imports, syntax errors, or
// mismatched braces.
func TestGenerate_GoStubCompiles(t *testing.T) {
	tmp := t.TempDir()
	specPath := filepath.Join("..", "..", "examples", "typed-rpc-codegen", "bot-interactions", "pipeline-spec.yaml")
	if err := generate(specPath, tmp); err != nil {
		t.Fatalf("generate: %v", err)
	}
	src, err := os.ReadFile(filepath.Join(tmp, "bot_interaction_count_service_server.go"))
	if err != nil {
		t.Fatalf("read server stub: %v", err)
	}
	if _, err := format.Source(src); err != nil {
		t.Fatalf("format.Source on generated stub: %v", err)
	}
}

func TestSpec_ValidateRejectsMissingFields(t *testing.T) {
	cases := []struct {
		name string
		spec Spec
		want string
	}{
		{
			name: "no proto_package",
			spec: Spec{},
			want: "proto_package is required",
		},
		{
			name: "no go_package",
			spec: Spec{ProtoPackage: "x.y.z"},
			want: "go_package is required",
		},
		{
			name: "no service.name",
			spec: Spec{ProtoPackage: "x.y", GoPackage: "x/y"},
			want: "service.name is required",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.spec.validate()
			if err == nil {
				t.Fatal("validate: nil error, want failure")
			}
			if err.Error() != tc.want {
				t.Errorf("error: got %q, want %q", err.Error(), tc.want)
			}
		})
	}
}

func TestMethod_ValidateRejectsBadKeyTemplate(t *testing.T) {
	m := Method{
		Name:        "GetX",
		Kind:        MethodGetAllTime,
		KeyTemplate: "x:{undefined_field}",
		Request:     []Field{{Name: "bot_id", Type: "string"}},
	}
	err := m.validate()
	if err == nil {
		t.Fatal("validate: nil error, want failure on undefined template ref")
	}
}

func TestMethod_ValidateRequiresWindowDurationField(t *testing.T) {
	m := Method{
		Name:        "GetX",
		Kind:        MethodGetWindow,
		KeyTemplate: "x:{bot_id}",
		Request:     []Field{{Name: "bot_id", Type: "string"}},
	}
	err := m.validate()
	if err == nil {
		t.Fatal("validate: nil error, want window_duration_field requirement")
	}
}

func TestRenderKeyTemplate(t *testing.T) {
	cases := []struct {
		name string
		m    Method
		want string
	}{
		{
			name: "two string refs",
			m: Method{
				KeyTemplate: "bot:{bot_id}|user:{user_id}",
				Request:     []Field{{Name: "bot_id", Type: "string"}, {Name: "user_id", Type: "string"}},
			},
			want: `fmt.Sprintf("bot:%s|user:%s", msg.BotId, msg.UserId)`,
		},
		{
			name: "int and string",
			m: Method{
				KeyTemplate: "shard:{shard_id}|user:{user_id}",
				Request:     []Field{{Name: "shard_id", Type: "int64"}, {Name: "user_id", Type: "string"}},
			},
			want: `fmt.Sprintf("shard:%d|user:%s", msg.ShardId, msg.UserId)`,
		},
		{
			name: "literal (no refs)",
			m: Method{
				KeyTemplate: "global",
				Request:     []Field{{Name: "x", Type: "string"}}, // unused
			},
			want: `"global"`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := renderKeyTemplate(tc.m)
			if got != tc.want {
				t.Errorf("renderKeyTemplate: got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestGoFieldName(t *testing.T) {
	cases := map[string]string{
		"bot_id":           "BotId",
		"user_id":          "UserId",
		"duration_seconds": "DurationSeconds",
		"single":           "Single",
	}
	for in, want := range cases {
		if got := goFieldName(in); got != want {
			t.Errorf("goFieldName(%q): got %q, want %q", in, got, want)
		}
	}
}

func TestFileBaseFromService(t *testing.T) {
	cases := map[string]string{
		"BotInteractionCountService": "bot_interaction_count_service",
		"PageViewCounter":            "page_view_counter",
		"X":                          "x",
	}
	for in, want := range cases {
		if got := fileBaseFromService(in); got != want {
			t.Errorf("fileBaseFromService(%q): got %q, want %q", in, got, want)
		}
	}
}
