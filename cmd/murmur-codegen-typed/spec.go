// spec.go defines the YAML pipeline-spec schema.
//
// One YAML file per service. The codegen emits one .proto and one
// _server.go file per service. Multiple methods per service share
// the same underlying Murmur pipeline — different RPCs are different
// query shapes (GetAllTime / GetWindow / GetWindowMany) over the
// same monoid.
package main

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

// Spec is the top-level service spec.
type Spec struct {
	// ProtoPackage is the proto package (e.g. "example.bot.v1"). Required.
	ProtoPackage string `yaml:"proto_package"`

	// GoPackage is the Go import path for the generated server stub
	// (e.g. "github.com/example/count-core/gen/bot/v1"). Required.
	GoPackage string `yaml:"go_package"`

	// ProtoGoPackage is the Go import path of the buf-generated types
	// from the proto file (e.g. "github.com/example/count-core/gen/bot/v1").
	// Defaults to GoPackage if empty.
	ProtoGoPackage string `yaml:"proto_go_package"`

	// Service describes the RPC service.
	Service Service `yaml:"service"`
}

// Service is one RPC service backed by one Murmur pipeline.
type Service struct {
	// Name is the proto service name (e.g. "BotInteractionCountService"). Required.
	Name string `yaml:"name"`

	// PipelineName is the Murmur pipeline name (the path component
	// in /<pipeline>.QueryService/Get). Required.
	PipelineName string `yaml:"pipeline_name"`

	// PipelineKind is the monoid family. One of: sum, hll, topk, bloom.
	PipelineKind PipelineKind `yaml:"pipeline_kind"`

	// Methods lists the RPCs exposed on this service.
	Methods []Method `yaml:"methods"`
}

// PipelineKind tags which typed-client (Sum, HLL, TopK, Bloom) the
// codegen will use in the generated server stub. Each kind drives a
// different response-message shape:
//
//   - sum   → { value int64, present bool }
//   - hll   → { value int64, present bool } (cardinality)
//   - topk  → { items repeated TopKItem, present bool }
//   - bloom → { capacity_bits int64, hash_functions int32, approx_size int64, present bool }
type PipelineKind string

const (
	PipelineSum   PipelineKind = "sum"
	PipelineHLL   PipelineKind = "hll"
	PipelineTopK  PipelineKind = "topk"
	PipelineBloom PipelineKind = "bloom"
)

// Method is one RPC.
type Method struct {
	// Name is the RPC method name (e.g. "GetCount"). Required.
	Name string `yaml:"name"`

	// Kind is one of: get_all_time, get_window, get_window_many,
	// get_many, get_range.
	Kind MethodKind `yaml:"kind"`

	// Request lists the fields on the request message. Order is
	// preserved in the proto.
	Request []Field `yaml:"request"`

	// KeyTemplate is a printf-style template referencing request
	// field names in {curly braces}, e.g. "bot:{bot_id}|user:{user_id}".
	// The codegen emits Go code that interpolates the request fields
	// at call time.
	KeyTemplate string `yaml:"key_template"`

	// WindowDurationField is the request-field name (must be int64-typed)
	// that supplies the window duration in seconds. Required for
	// kind=get_window and kind=get_window_many.
	WindowDurationField string `yaml:"window_duration_field"`

	// ManyKeyField is the request-field name (must be repeated-string-typed)
	// holding the list of entity values to query. Required for
	// kind=get_window_many and kind=get_many. The KeyTemplate is
	// applied per element.
	ManyKeyField string `yaml:"many_key_field"`

	// RangeStartField / RangeEndField are int64 request-field names
	// supplying Unix-second start/end timestamps for kind=get_range.
	// The runtime converts them with time.Unix(v, 0).
	RangeStartField string `yaml:"range_start_field"`
	RangeEndField   string `yaml:"range_end_field"`
}

// MethodKind tags which typed-client method (Get / GetMany / GetWindow /
// GetWindowMany / GetRange) the generated handler will call. All four
// pipeline kinds (sum / hll / topk / bloom) now support every method
// kind — the typed clients in pkg/query/typed expose GetMany / GetRange
// on every client.
type MethodKind string

const (
	MethodGetAllTime    MethodKind = "get_all_time"
	MethodGetWindow     MethodKind = "get_window"
	MethodGetWindowMany MethodKind = "get_window_many"
	MethodGetMany       MethodKind = "get_many"
	MethodGetRange      MethodKind = "get_range"
)

// Field is one field on a proto message.
type Field struct {
	// Name is the proto field name (snake_case).
	Name string `yaml:"name"`

	// Type is the proto type. Supported: string, int64, repeated string.
	Type string `yaml:"type"`
}

// loadSpec reads a YAML spec file and validates it.
func loadSpec(path string) (*Spec, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read spec %s: %w", path, err)
	}
	var s Spec
	if err := yaml.Unmarshal(b, &s); err != nil {
		return nil, fmt.Errorf("parse spec %s: %w", path, err)
	}
	if err := s.validate(); err != nil {
		return nil, fmt.Errorf("validate spec %s: %w", path, err)
	}
	return &s, nil
}

func (s *Spec) validate() error {
	if s.ProtoPackage == "" {
		return errors.New("proto_package is required")
	}
	if s.GoPackage == "" {
		return errors.New("go_package is required")
	}
	if s.ProtoGoPackage == "" {
		s.ProtoGoPackage = s.GoPackage
	}
	if s.Service.Name == "" {
		return errors.New("service.name is required")
	}
	if s.Service.PipelineName == "" {
		return errors.New("service.pipeline_name is required")
	}
	switch s.Service.PipelineKind {
	case PipelineSum, PipelineHLL, PipelineTopK, PipelineBloom:
	default:
		return fmt.Errorf("service.pipeline_kind: unsupported %q (want sum, hll, topk, or bloom)", s.Service.PipelineKind)
	}
	if len(s.Service.Methods) == 0 {
		return errors.New("service.methods is empty")
	}
	for i, m := range s.Service.Methods {
		if err := m.validate(s.Service.PipelineKind); err != nil {
			return fmt.Errorf("methods[%d] (%s): %w", i, m.Name, err)
		}
	}
	return nil
}

func (m *Method) validate(serviceKind PipelineKind) error {
	if m.Name == "" {
		return errors.New("method.name is required")
	}
	if !startsUpper(m.Name) {
		return fmt.Errorf("method.name %q must be UpperCamelCase", m.Name)
	}
	switch m.Kind {
	case MethodGetAllTime, MethodGetWindow, MethodGetWindowMany, MethodGetMany, MethodGetRange:
	default:
		return fmt.Errorf("method.kind: unsupported %q", m.Kind)
	}
	if len(m.Request) == 0 {
		return errors.New("method.request is empty")
	}
	if m.KeyTemplate == "" {
		return errors.New("method.key_template is required")
	}
	// KeyTemplate references must match request fields (best-effort check).
	for _, ref := range templateRefs(m.KeyTemplate) {
		found := false
		for _, f := range m.Request {
			if f.Name == ref {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("key_template references {%s} but no such request field", ref)
		}
	}
	if m.Kind == MethodGetWindow || m.Kind == MethodGetWindowMany {
		if m.WindowDurationField == "" {
			return fmt.Errorf("kind=%s requires window_duration_field", m.Kind)
		}
		f := findField(m.Request, m.WindowDurationField)
		if f == nil || f.Type != "int64" {
			return fmt.Errorf("window_duration_field %q must be an int64 request field", m.WindowDurationField)
		}
	}
	if m.Kind == MethodGetWindowMany || m.Kind == MethodGetMany {
		if m.ManyKeyField == "" {
			return fmt.Errorf("kind=%s requires many_key_field", m.Kind)
		}
		f := findField(m.Request, m.ManyKeyField)
		if f == nil || f.Type != "repeated string" {
			return fmt.Errorf("many_key_field %q must be a 'repeated string' request field", m.ManyKeyField)
		}
		// The key_template must reference the many_key_field — otherwise every
		// element of the batch produces the same key and the batched call is
		// pointless (also leaves the generated loop variable unused).
		found := false
		for _, ref := range templateRefs(m.KeyTemplate) {
			if ref == m.ManyKeyField {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("kind=%s: key_template must reference {%s}", m.Kind, m.ManyKeyField)
		}
	}
	if m.Kind == MethodGetRange {
		if m.RangeStartField == "" || m.RangeEndField == "" {
			return fmt.Errorf("kind=%s requires both range_start_field and range_end_field", m.Kind)
		}
		fs := findField(m.Request, m.RangeStartField)
		fe := findField(m.Request, m.RangeEndField)
		if fs == nil || fs.Type != "int64" {
			return fmt.Errorf("range_start_field %q must be an int64 request field", m.RangeStartField)
		}
		if fe == nil || fe.Type != "int64" {
			return fmt.Errorf("range_end_field %q must be an int64 request field", m.RangeEndField)
		}
	}
	return nil
}

func findField(fs []Field, name string) *Field {
	for i := range fs {
		if fs[i].Name == name {
			return &fs[i]
		}
	}
	return nil
}

func startsUpper(s string) bool {
	if s == "" {
		return false
	}
	c := s[0]
	return c >= 'A' && c <= 'Z'
}

// templateRefs extracts the {field} references from a KeyTemplate.
// "bot:{bot_id}|user:{user_id}" → ["bot_id", "user_id"].
func templateRefs(t string) []string {
	var out []string
	for {
		i := strings.IndexByte(t, '{')
		if i < 0 {
			return out
		}
		j := strings.IndexByte(t[i:], '}')
		if j < 0 {
			return out
		}
		out = append(out, t[i+1:i+j])
		t = t[i+j+1:]
	}
}
