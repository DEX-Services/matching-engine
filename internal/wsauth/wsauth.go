// Package wsauth verifies the user's session JWT (issued by Dex-Backend) so
// the /ws WebSocket hub can identify which account a connection belongs to.
// This engine shares Dex-Backend's JWT_SECRET (same mechanism bots/internal/
// auth already uses); it never issues its own tokens.
//
// This exists specifically to fix a real information leak: /ws was
// completely unauthenticated, and its broadcast included every account's
// AccountID on order/liquidation/funding/realized-PnL events with no
// redaction, so any visitor to the page (no login required) could watch
// every account's trading activity in real time. A connection now optionally
// carries a verified UserID, and the hub only includes AccountID fields that
// belong to that connection's own account, dropping them (rather than
// refusing the event) for anyone else's — the same event still carries
// price/quantity/symbol data other consumers (market-wide depth/ticker
// listeners) may legitimately want, just not whose order it was.
package wsauth

import (
	"fmt"

	"github.com/golang-jwt/jwt/v5"
)

// Claims mirrors Dex-Backend's internal/auth.Claims (uid + addr) and
// bots/internal/auth's identical copy.
type Claims struct {
	UserID        string `json:"uid"`
	WalletAddress string `json:"addr"`
	jwt.RegisteredClaims
}

// Verifier validates session JWTs against the shared HMAC secret.
type Verifier struct {
	secret []byte
}

// NewVerifier builds a Verifier from JWT_SECRET. secret == "" disables
// verification (Verify always fails), which is the safe default: a
// connection that can't be identified is simply treated as no account,
// getting AccountID-redacted events like everyone else, never full access.
func NewVerifier(secret string) *Verifier {
	return &Verifier{secret: []byte(secret)}
}

func (v *Verifier) Enabled() bool { return len(v.secret) > 0 }

// Verify parses and validates a token, returning the claims on success.
func (v *Verifier) Verify(tokenString string) (*Claims, error) {
	if !v.Enabled() {
		return nil, fmt.Errorf("ws auth not configured")
	}
	claims := &Claims{}
	token, err := jwt.ParseWithClaims(tokenString, claims, func(t *jwt.Token) (interface{}, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method")
		}
		return v.secret, nil
	})
	if err != nil || !token.Valid {
		return nil, fmt.Errorf("invalid token")
	}
	return claims, nil
}
