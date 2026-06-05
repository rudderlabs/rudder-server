package firstevent

import (
	"testing"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
)

func TestShouldFire_DedupPerSource(t *testing.T) {
	tr := &Tracker{enabled: true}

	if !tr.ShouldFire("src1") {
		t.Fatal("first event for src1 should fire")
	}
	if tr.ShouldFire("src1") {
		t.Fatal("second event for src1 must be deduped")
	}
	if !tr.ShouldFire("src2") {
		t.Fatal("first event for a different source should fire")
	}
}

func TestShouldFire_DisabledAndEmpty(t *testing.T) {
	if (&Tracker{enabled: true}).ShouldFire("") {
		t.Fatal("empty sourceID must not fire")
	}
	if (&Tracker{enabled: false}).ShouldFire("src1") {
		t.Fatal("disabled tracker must not fire")
	}
}

func TestNewTracker_DisabledWithoutWriteKey(t *testing.T) {
	tr := NewTracker(config.New(), logger.NOP)
	if tr.enabled {
		t.Fatal("tracker must be disabled when no PipelineActivation.writekey is set")
	}
	if tr.ShouldFire("src1") {
		t.Fatal("disabled tracker must be a no-op")
	}
}
