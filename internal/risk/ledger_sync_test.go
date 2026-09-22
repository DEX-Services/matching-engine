package risk

import (
	"testing"

	"github.com/dex/matching-engine/internal/fixedpoint"
)

func TestCredit_IncreasesAvailable(t *testing.T) {
	l := NewLedger()
	l.Credit("user1", "USDC", fixedpoint.MustFromString("40"))
	if got := l.Available("user1", "USDC"); !got.Equal(fixedpoint.MustFromString("40")) {
		t.Fatalf("Available = %s, want 40", got)
	}
}

func TestCredit_Accumulates(t *testing.T) {
	l := NewLedger()
	l.Credit("user1", "USDC", fixedpoint.MustFromString("40"))
	l.Credit("user1", "USDC", fixedpoint.MustFromString("10"))
	if got := l.Available("user1", "USDC"); !got.Equal(fixedpoint.MustFromString("50")) {
		t.Fatalf("Available = %s, want 50", got)
	}
}

func TestDebit_DecreasesAvailable(t *testing.T) {
	l := NewLedger()
	l.Credit("user1", "USDC", fixedpoint.MustFromString("40"))
	if err := l.Debit("user1", "USDC", fixedpoint.MustFromString("15")); err != nil {
		t.Fatalf("Debit returned error: %v", err)
	}
	if got := l.Available("user1", "USDC"); !got.Equal(fixedpoint.MustFromString("25")) {
		t.Fatalf("Available = %s, want 25", got)
	}
}

func TestDebit_InsufficientBalanceErrors(t *testing.T) {
	l := NewLedger()
	l.Credit("user1", "USDC", fixedpoint.MustFromString("5"))
	if err := l.Debit("user1", "USDC", fixedpoint.MustFromString("10")); err == nil {
		t.Fatal("expected error debiting more than available, got nil")
	}
	if got := l.Available("user1", "USDC"); !got.Equal(fixedpoint.MustFromString("5")) {
		t.Fatalf("Available should be unchanged after failed debit, got %s", got)
	}
}

func TestDebit_UnknownAccountErrors(t *testing.T) {
	l := NewLedger()
	if err := l.Debit("nobody", "USDC", fixedpoint.MustFromString("1")); err == nil {
		t.Fatal("expected error debiting unfunded/unknown account, got nil")
	}
}
