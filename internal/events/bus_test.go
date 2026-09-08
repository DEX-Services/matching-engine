package events

import "testing"

func TestNextOutOfBandSequence_MonotonicAndDistinct(t *testing.T) {
	b := NewBus()
	seen := make(map[uint64]bool)
	var prev uint64
	for i := 0; i < 100; i++ {
		n := b.NextOutOfBandSequence()
		if n == 0 {
			t.Fatal("expected a non-zero sequence number")
		}
		if seen[n] {
			t.Fatalf("sequence number %d repeated", n)
		}
		seen[n] = true
		if i > 0 && n <= prev {
			t.Fatalf("expected strictly increasing sequence numbers, got %d after %d", n, prev)
		}
		prev = n
	}
}

func TestNextOutOfBandSequence_SeparateFromBookRange(t *testing.T) {
	b := NewBus()
	n := b.NextOutOfBandSequence()
	// Book-sequenced events for a symbol start at 1 and increment per real
	// matching-goroutine event; any realistic run stays far below this base,
	// so out-of-band numbers must never collide with them under the
	// persistence.events UNIQUE(symbol, sequence_number) index.
	const bookSequenceCeiling = uint64(1) << 40
	if n <= bookSequenceCeiling {
		t.Fatalf("expected an out-of-band sequence number well above any realistic book sequence, got %d", n)
	}
}
