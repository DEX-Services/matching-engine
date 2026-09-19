package persistence

import (
	"context"
	"log/slog"
	"time"

	"github.com/dex/matching-engine/internal/backendclient"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PendingSyncWriter durably records a balance-sync call that exhausted
// backendclient.Async's retries (M3), so PendingSyncSweeper can replay it
// later instead of it being lost the moment it's logged. Wire it in as
// backendclient.OnAsyncExhausted at startup.
type PendingSyncWriter struct {
	pool *pgxpool.Pool
	log  *slog.Logger
}

func NewPendingSyncWriter(pool *pgxpool.Pool) *PendingSyncWriter {
	return &PendingSyncWriter{pool: pool, log: slog.Default()}
}

// Write matches backendclient.OnAsyncExhausted's signature.
func (w *PendingSyncWriter) Write(ctx context.Context, sync backendclient.PendingSync) {
	writeCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	_, err := w.pool.Exec(writeCtx,
		`INSERT INTO pending_backend_sync (op, account_id, asset, amount, category, idempotency_key) VALUES ($1, $2, $3, $4, $5, $6)`,
		sync.Op, sync.AccountID, sync.Asset, sync.Amount, nullIfEmpty(sync.Category), sync.IdempotencyKey,
	)
	if err != nil {
		// This is already the fallback path for a failure with nowhere
		// further to fall back to — log loudly so a sustained Postgres
		// outage on top of a Dex-Backend outage is at least visible.
		w.log.Error("pending backend sync durable write failed; call is now unrecoverable",
			"op", sync.Op, "account", sync.AccountID, "asset", sync.Asset, "err", err)
	}
}

func nullIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// PendingSyncSweeper periodically retries every row in pending_backend_sync
// against Dex-Backend, deleting a row once its replay succeeds. Each
// replayed call reuses its original idempotency_key, so a replay is safe
// even if the original attempt actually landed on Dex-Backend before the
// failure that produced the row (see Dex-Backend's dedup on
// internal_idempotency_keys).
type PendingSyncSweeper struct {
	pool     *pgxpool.Pool
	backend  *backendclient.Client
	log      *slog.Logger
	interval time.Duration
}

func NewPendingSyncSweeper(pool *pgxpool.Pool, backend *backendclient.Client, interval time.Duration) *PendingSyncSweeper {
	return &PendingSyncSweeper{pool: pool, backend: backend, log: slog.Default(), interval: interval}
}

// Run starts the sweep loop. Call in a dedicated goroutine.
func (s *PendingSyncSweeper) Run(ctx context.Context) {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	s.sweepOnce(ctx)
	for {
		select {
		case <-ticker.C:
			s.sweepOnce(ctx)
		case <-ctx.Done():
			return
		}
	}
}

type pendingSyncRow struct {
	id             int64
	op             string
	accountID      string
	asset          string
	amount         string
	category       *string
	idempotencyKey string
}

func (s *PendingSyncSweeper) sweepOnce(ctx context.Context) {
	const batchSize = 100
	rows, err := s.pool.Query(ctx,
		`SELECT id, op, account_id, asset, amount, category, idempotency_key FROM pending_backend_sync ORDER BY id ASC LIMIT $1`,
		batchSize)
	if err != nil {
		s.log.Error("pending backend sync sweep query failed", "err", err)
		return
	}
	var batch []pendingSyncRow
	for rows.Next() {
		var r pendingSyncRow
		if err := rows.Scan(&r.id, &r.op, &r.accountID, &r.asset, &r.amount, &r.category, &r.idempotencyKey); err != nil {
			rows.Close()
			s.log.Error("pending backend sync row scan failed", "err", err)
			return
		}
		batch = append(batch, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		s.log.Error("pending backend sync sweep iteration failed", "err", err)
		return
	}

	delivered := 0
	for _, r := range batch {
		if err := s.replay(ctx, r); err != nil {
			s.log.Warn("pending backend sync replay failed, will retry next sweep",
				"id", r.id, "op", r.op, "account", r.accountID, "err", err)
			if _, updErr := s.pool.Exec(ctx,
				`UPDATE pending_backend_sync SET attempts = attempts + 1, last_error = $2 WHERE id = $1`,
				r.id, err.Error(),
			); updErr != nil {
				s.log.Error("pending backend sync attempt-count update failed", "id", r.id, "err", updErr)
			}
			continue
		}
		if _, err := s.pool.Exec(ctx, `DELETE FROM pending_backend_sync WHERE id = $1`, r.id); err != nil {
			s.log.Error("pending backend sync delete after replay failed", "id", r.id, "err", err)
			continue
		}
		delivered++
	}
	if delivered > 0 {
		s.log.Info("pending backend sync drained", "count", delivered)
	}
}

func (s *PendingSyncSweeper) replay(ctx context.Context, r pendingSyncRow) error {
	replayCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	switch r.op {
	case "lock":
		return s.backend.LockIdempotent(replayCtx, r.accountID, r.asset, r.amount, r.idempotencyKey)
	case "unlock":
		return s.backend.UnlockIdempotent(replayCtx, r.accountID, r.asset, r.amount, r.idempotencyKey)
	case "settle":
		return s.backend.SettleIdempotent(replayCtx, r.accountID, r.asset, r.amount, r.idempotencyKey)
	case "credit":
		return s.backend.CreditIdempotent(replayCtx, r.accountID, r.asset, r.amount, r.idempotencyKey)
	case "settleFee":
		category := ""
		if r.category != nil {
			category = *r.category
		}
		return s.backend.SettleFeeIdempotent(replayCtx, r.accountID, r.asset, r.amount, category, r.idempotencyKey)
	default:
		s.log.Error("pending backend sync row has unknown op, discarding", "id", r.id, "op", r.op)
		return nil
	}
}
