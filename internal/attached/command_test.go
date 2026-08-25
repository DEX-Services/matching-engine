package attached

import (
	"github.com/dex/matching-engine/internal/models"
	"github.com/shopspring/decimal"
	"testing"
)

func TestExecuteActivatesOnlyActualFill(t *testing.T) {
	r := NewRegistry()
	e := &models.Order{ID: "p", AccountID: "a", Symbol: "BTC-USDC"}
	g := Group{ID: "g", ParentOrderID: "p", StopLoss: &Leg{ID: "sl"}}
	out, got, err := Execute(r, Command{g, e}, func(o *models.Order) (*models.Order, error) { o.Filled = decimal.NewFromInt(2); return o, nil })
	if err != nil || out == nil || got == nil || !got.ProtectedQty.Equal(decimal.NewFromInt(2)) {
		t.Fatalf("got %#v, %v", got, err)
	}
}
