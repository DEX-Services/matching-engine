package risk

import (
	"testing"

	"github.com/shopspring/decimal"
)

func TestCredit_IncreasesAvailable(t *testing.T) {
	l := NewLedger()
	l.Credit("user1", "USDC", decimal.RequireFromString("40"))
	if got := l.Available("user1", "USDC"); !got.Equal(decimal.RequireFromString("40")) {
		t.Fatalf("Available = %s, want 40", got)
	}
}

func TestCredit_Accumulates(t *testing.T) {
	l := NewLedger()
	l.Credit("user1", "USDC", decimal.RequireFromString("40"))
	l.Credit("user1", "USDC", decimal.RequireFromString("10"))
	if got := l.Available("user1", "USDC"); !got.Equal(decimal.RequireFromString("50")) {
		t.Fatalf("Available = %s, want 50", got)
	}
}

func TestDebit_DecreasesAvailable(t *testing.T) {
	l := NewLedger()
	l.Credit("user1", "USDC", decimal.RequireFromString("40"))
	if err := l.Debit("user1", "USDC", decimal.RequireFromString("15")); err != nil {
		t.Fatalf("Debit returned error: %v", err)
	}
	if got := l.Available("user1", "USDC"); !got.Equal(decimal.RequireFromString("25")) {
		t.Fatalf("Available = %s, want 25", got)
	}
}

func TestDebit_InsufficientBalanceErrors(t *testing.T) {
	l := NewLedger()
	l.Credit("user1", "USDC", decimal.RequireFromString("5"))
	if err := l.Debit("user1", "USDC", decimal.RequireFromString("10")); err == nil {
		t.Fatal("expected error debiting more than available, got nil")
	}
	if got := l.Available("user1", "USDC"); !got.Equal(decimal.RequireFromString("5")) {
		t.Fatalf("Available should be unchanged after failed debit, got %s", got)
	}
}

func TestDebit_UnknownAccountErrors(t *testing.T) {
	l := NewLedger()
	if err := l.Debit("nobody", "USDC", decimal.RequireFromString("1")); err == nil {
		t.Fatal("expected error debiting unfunded/unknown account, got nil")
	}
}
