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

// TestDLQProducer_OnDecodeErrorCallbackShape ensures the callback matches
// the signature consumed by Config[T].OnDecodeError — i.e., a DLQProducer's
// method value is assignable to that field directly. The body is exercised
// in the integration tests against a real broker.
func TestDLQProducer_OnDecodeErrorCallbackShape(t *testing.T) {
	var assign func(raw []byte, partition int32, offset int64, err error)
	d := &DLQProducer{topic: "x.dlq", source: "x"}
	assign = d.OnDecodeError
	if assign == nil {
		t.Fatal("OnDecodeError method value is nil")
	}
}

func TestDLQProducer_OnFetchErrorCallbackShape(t *testing.T) {
	var assign func(topic string, partition int32, err error)
	d := &DLQProducer{topic: "x.dlq", source: "x"}
	assign = d.OnFetchError
	if assign == nil {
		t.Fatal("OnFetchError method value is nil")
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
