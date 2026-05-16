package kafka

import (
	"errors"
	"testing"
)

func TestNewDLQProducer_RequiresTopic(t *testing.T) {
	_, err := NewDLQProducer(DLQConfig{Brokers: []string{"localhost:9092"}, Source: "x"})
	if err == nil {
		t.Fatal("nil error, want failure on missing topic")
	}
}

func TestNewDLQProducer_RequiresBrokers(t *testing.T) {
	_, err := NewDLQProducer(DLQConfig{Topic: "x.dlq", Source: "x"})
	if err == nil {
		t.Fatal("nil error, want failure on missing brokers")
	}
}

func TestNewDLQProducer_RequiresSource(t *testing.T) {
	_, err := NewDLQProducer(DLQConfig{Topic: "x.dlq", Brokers: []string{"localhost:9092"}})
	if err == nil {
		t.Fatal("nil error, want failure on missing source")
	}
}

// TestDLQProducer_CallbackShape pins the DLQProducer's method-value types
// against the Config[T] callback fields at compile time. If either method
// signature drifts, the package stops compiling — which is exactly what we
// want, since downstream wiring (Config.OnDecodeError = dlq.OnDecodeError)
// would otherwise break silently for users until they upgraded.
func TestDLQProducer_CallbackShape(t *testing.T) {
	d := &DLQProducer{topic: "x.dlq", source: "x"}

	// Compile-time pins. The package fails to compile if the method-value
	// shapes don't match the Config[T] field types.
	cfg := Config[struct{}]{
		OnDecodeError: d.OnDecodeError,
		OnFetchError:  d.OnFetchError,
	}
	if cfg.OnDecodeError == nil || cfg.OnFetchError == nil {
		t.Fatal("callback fields not populated")
	}
}

func TestDLQProducer_recordErrCapturesFirst(t *testing.T) {
	d := &DLQProducer{}
	first := errors.New("first")
	second := errors.New("second")
	d.recordErr(first)
	d.recordErr(second)
	if d.firstErr != first {
		t.Errorf("firstErr: got %v, want %v", d.firstErr, first)
	}
}
