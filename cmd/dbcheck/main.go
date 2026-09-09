// Operations health-check tool for the persistence pipeline: inspects the
// shared cloud Postgres without touching engine state. Modes:
//
//	dbcheck count   -> events row count, recent activity, outbox backlog
//	dbcheck types   -> per-type event counts over the last 3 hours
//	dbcheck conns   -> who is connected to this database (pg_stat_activity)
//	dbcheck queries -> last query per foreign connection (identify writers)
//
// Requires the engine's .env (POSTGRES_* vars) in the working directory.
package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

func main() {
	_ = godotenv.Load()
	dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=require",
		os.Getenv("POSTGRES_USER"), os.Getenv("POSTGRES_PASSWORD"),
		os.Getenv("POSTGRES_HOST"), os.Getenv("POSTGRES_PORT"), os.Getenv("POSTGRES_DB"))
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		panic(err)
	}
	defer pool.Close()

	count := func() int {
		var total, recent, outbox int
		var last time.Time
		q1 := `SELECT count(*), count(*) FILTER (WHERE created_at > now() - interval '120 minutes'), coalesce(max(created_at), '1970-01-01'::timestamptz) FROM events`
		if err := pool.QueryRow(context.Background(), q1).Scan(&total, &recent, &last); err != nil {
			panic(err)
		}
		_ = pool.QueryRow(context.Background(), `SELECT count(*) FROM event_outbox`).Scan(&outbox)
		fmt.Printf("events total=%d recent2h=%d outbox=%d last_event=%s (%s ago)\n",
			total, recent, outbox, last.Format(time.RFC3339), time.Since(last).Round(time.Second))
		return recent
	}

	switch os.Args[1] {
	case "types":
		rows, err := pool.Query(context.Background(),
			`SELECT type, count(*), min(created_at), max(created_at) FROM events
			 WHERE created_at > now() - interval '3 hours' GROUP BY type ORDER BY 2 DESC`)
		if err != nil {
			panic(err)
		}
		defer rows.Close()
		for rows.Next() {
			var t string
			var c int
			var mn, mx time.Time
			if err := rows.Scan(&t, &c, &mn, &mx); err != nil {
				panic(err)
			}
			fmt.Printf("%-20s %6d  %s .. %s\n", t, c, mn.Format("15:04:05"), mx.Format("15:04:05"))
		}
	case "queries":
		rows, err := pool.Query(context.Background(),
			`SELECT pid, coalesce(client_addr::text,'local'), state, state_change::time, left(coalesce(query,''),90)
			 FROM pg_stat_activity WHERE datname = current_database() AND client_addr IS NOT NULL
			 ORDER BY pid`)
		if err != nil {
			panic(err)
		}
		defer rows.Close()
		for rows.Next() {
			var pid int
			var addr, state, q string
			var sc time.Time
			if err := rows.Scan(&pid, &addr, &state, &sc, &q); err != nil {
				panic(err)
			}
			fmt.Printf("%-6d %-18s %-16s %s  %s\n", pid, addr, state, sc.Format("15:04:05"), q)
		}
	case "conns":
		rows, err := pool.Query(context.Background(),
			`SELECT coalesce(application_name,''), coalesce(client_addr::text,'local'), usename, state, count(*)
			 FROM pg_stat_activity WHERE datname = current_database()
			 GROUP BY 1,2,3,4 ORDER BY 5 DESC`)
		if err != nil {
			panic(err)
		}
		defer rows.Close()
		for rows.Next() {
			var app, addr, user, state string
			var c int
			if err := rows.Scan(&app, &addr, &user, &state, &c); err != nil {
				panic(err)
			}
			fmt.Printf("%-22s %-18s %-10s %-14s %d\n", app, addr, user, state, c)
		}
	case "count":
		fmt.Println(count())
	case "wait":
		target, _ := strconv.Atoi(os.Args[2])
		start := time.Now()
		last := -1
		for time.Since(start) < 60*time.Second {
			n := count()
			if n != last {
				fmt.Printf("t+%.1fs: %d rows\n", time.Since(start).Seconds(), n)
				last = n
				if n >= target {
					fmt.Printf("DONE: reached %d rows in %.1fs\n", n, time.Since(start).Seconds())
					return
				}
			}
			time.Sleep(300 * time.Millisecond)
		}
		fmt.Println("TIMEOUT")
	}
}
