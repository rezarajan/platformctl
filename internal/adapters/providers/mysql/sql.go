package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"strings"
	"time"

	// Also registers the "mysql" database/sql driver.
	godriver "github.com/go-sql-driver/mysql"

	"github.com/rezarajan/platformctl/internal/adapters/providers/providerkit"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

// dsnAddr builds the DSN through the driver's own Config type so credentials
// containing @, :, /, #, spaces, or quotes survive intact — secret values
// must not depend on lucky demo passwords (docs/planning/07 §2.2). addr is
// a "host:port" this process can dial right now (docs/planning/08 B8: from
// Provider.reachableAddr/runtime.EnsureReachable — MySQL's wire protocol has
// no broker-style redirect, so this can be used directly for a whole call).
//
// tlsPosture is always nil today: this Provider only ever administers a
// self-hosted, same-network MySQL/MariaDB instance it created itself, which
// has no external Connection to resolve an outbound TLS posture from
// (docs/planning/08 I2's mode field is external-Connection-only) — the
// parameter exists so this DSN builder no longer carries an unconditionally
// bare (TLS-less) DSN, and so it's independently unit-testable across every
// mode the moment a future caller has one to pass. Non-nil registers a
// go-sql-driver TLS config and references it via the "tls" DSN param — the
// driver's own documented mechanism (there is no inline-PEM query param).
func dsnAddr(addr, user, pass, db string, tlsPosture *providerkit.DatabaseTLS) string {
	cfg := godriver.NewConfig()
	cfg.User = user
	cfg.Passwd = pass
	cfg.Net = "tcp"
	cfg.Addr = addr
	cfg.DBName = db
	cfg.Timeout = 10 * time.Second
	if tlsPosture != nil {
		if tlsCfg, err := tlsPosture.TLSConfig(hostOnly(addr)); err == nil {
			name := "platformctl-" + addr + "-" + tlsPosture.Mode
			if regErr := godriver.RegisterTLSConfig(name, tlsCfg); regErr == nil {
				cfg.TLSConfig = name
			}
		}
	}
	return cfg.FormatDSN()
}

// hostOnly strips the port off a "host:port" address for use as a TLS
// ServerName — verify-full's hostname check must never include the port.
func hostOnly(addr string) string {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	return host
}

func open(conn string) (*sql.DB, error) {
	db, err := sql.Open("mysql", conn)
	if err != nil {
		return nil, fmt.Errorf("connect to mysql: %w", err)
	}
	return db, nil
}

// waitReadyReachable ping-loops until the server accepts authenticated
// connections — the images serve their health ping during an init phase
// that still refuses real logins. providerkit.WaitReachable owns the
// re-resolve-on-every-attempt rule (docs/planning/09 Class 2 / F1) rather
// than retrying against one address resolved before the wait began.
func waitReadyReachable(ctx context.Context, rt runtime.ContainerRuntime, name string, port int, buildConn func(addr string) string, timeout time.Duration) error {
	return providerkit.WaitReachable(ctx, rt, name, port, timeout, func(ctx context.Context, addr string) error {
		return ping(ctx, buildConn(addr))
	})
}

func rotateRootPassword(ctx context.Context, adminConn, newPass string) error {
	db, err := open(adminConn)
	if err != nil {
		return err
	}
	defer db.Close()
	rows, err := db.QueryContext(ctx, "SELECT Host FROM mysql.user WHERE User = 'root'")
	if err != nil {
		return fmt.Errorf("list root accounts: %w", err)
	}
	defer rows.Close()
	var hosts []string
	for rows.Next() {
		var host string
		if err := rows.Scan(&host); err != nil {
			return fmt.Errorf("scan root account: %w", err)
		}
		hosts = append(hosts, host)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("list root accounts: %w", err)
	}
	if len(hosts) == 0 {
		return fmt.Errorf("no root accounts found")
	}
	for _, host := range hosts {
		account := quoteString("root") + "@" + quoteString(host)
		if _, err := db.ExecContext(ctx, fmt.Sprintf("ALTER USER %s IDENTIFIED BY %s", account, quoteString(newPass))); err != nil {
			return fmt.Errorf("rotate root password for root@%s: %w", host, err)
		}
	}
	return nil
}

// quoteIdent backtick-quotes an identifier (CREATE DATABASE/USER cannot be
// parameterized).
func quoteIdent(s string) string { return "`" + strings.ReplaceAll(s, "`", "``") + "`" }

func quoteString(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

func ensureDatabase(ctx context.Context, adminConn, name string) error {
	db, err := open(adminConn)
	if err != nil {
		return err
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS "+quoteIdent(name)); err != nil {
		return fmt.Errorf("create database %q: %w", name, err)
	}
	return nil
}

// dropDatabase removes the database if it exists — only reached through an
// explicit Source deletionPolicy: delete (docs/planning/07 §2.2).
func dropDatabase(ctx context.Context, adminConn, name string) error {
	db, err := open(adminConn)
	if err != nil {
		return err
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, "DROP DATABASE IF EXISTS "+quoteIdent(name)); err != nil {
		return fmt.Errorf("drop database %q: %w", name, err)
	}
	return nil
}

// schemaTables lists the base tables (excluding views) in the named schema —
// I13's promote step needs this to know exactly which tables a batched
// RENAME TABLE statement must move (docs/adr/007-backup-restore.md
// addendum 2). Views are excluded deliberately: mysqldump/mariadb-dump's
// plain-SQL output recreates a view via CREATE VIEW after its underlying
// base tables already exist in the target schema, so a restored scratch
// schema's views already reference scratch-local table names correctly and
// need no separate rename handling — renaming a schema's tables alone
// already makes every view inside it resolve against the newly-promoted
// tables (a view is schema-scoped, not identity-bound to specific renamed
// table objects the way a foreign key is).
func schemaTables(ctx context.Context, adminConn, schema string) ([]string, error) {
	db, err := open(adminConn)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	rows, err := db.QueryContext(ctx, `SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_type = 'BASE TABLE'`, schema)
	if err != nil {
		return nil, fmt.Errorf("list tables in schema %q: %w", schema, err)
	}
	defer rows.Close()
	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("scan table name in schema %q: %w", schema, err)
		}
		tables = append(tables, name)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list tables in schema %q: %w", schema, err)
	}
	return tables, nil
}

// promoteDatabase implements I13's atomic promote step
// (docs/adr/007-backup-restore.md addendum 2, verified live against a real
// MySQL instance before this code was written): a single batched RENAME
// TABLE statement moves every target table aside into oldName and every
// scratch table into target's place, in one statement. MySQL/MariaDB
// execute a multi-table RENAME TABLE batch atomically — a mid-batch
// failure (a destination-schema collision, a missing schema, ...) rolls
// back every rename in the statement, reproduced live by renaming into a
// nonexistent schema and confirming the source tables were left exactly as
// they started. Unlike Postgres, RENAME TABLE does not require other
// connections to the schema to be closed first — it takes the necessary
// metadata locks itself. oldName/target/scratch schemas must already exist
// (CREATE DATABASE IF NOT EXISTS, the caller's job) before this runs; an
// empty target (no tables) is valid and simply contributes no rename pairs
// for that half.
func promoteDatabase(ctx context.Context, adminConn, target, scratch, oldName string) error {
	targetTables, err := schemaTables(ctx, adminConn, target)
	if err != nil {
		return fmt.Errorf("promote %q: %w", target, err)
	}
	scratchTables, err := schemaTables(ctx, adminConn, scratch)
	if err != nil {
		return fmt.Errorf("promote %q: %w", target, err)
	}
	if len(scratchTables) == 0 {
		return fmt.Errorf("promote %q: scratch schema %q has no tables to promote", target, scratch)
	}
	var pairs []string
	for _, t := range targetTables {
		pairs = append(pairs, fmt.Sprintf("%s.%s TO %s.%s", quoteIdent(target), quoteIdent(t), quoteIdent(oldName), quoteIdent(t)))
	}
	for _, t := range scratchTables {
		pairs = append(pairs, fmt.Sprintf("%s.%s TO %s.%s", quoteIdent(scratch), quoteIdent(t), quoteIdent(target), quoteIdent(t)))
	}
	db, err := open(adminConn)
	if err != nil {
		return err
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, "RENAME TABLE "+strings.Join(pairs, ", ")); err != nil {
		return fmt.Errorf("promote %q: rename table batch: %w", target, err)
	}
	return nil
}

func databaseExists(ctx context.Context, adminConn, name string) (bool, error) {
	db, err := open(adminConn)
	if err != nil {
		return false, err
	}
	defer db.Close()
	var count int
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM information_schema.schemata WHERE schema_name = ?", name).Scan(&count); err != nil {
		return false, fmt.Errorf("check database %q: %w", name, err)
	}
	return count > 0, nil
}

// ensureReplicationUser provisions the grants Debezium's MySQL/MariaDB
// connector documents: SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE,
// REPLICATION CLIENT.
func ensureReplicationUser(ctx context.Context, adminConn, user, pass string) error {
	if user == "" || pass == "" {
		return fmt.Errorf("replication secret must provide username and password keys")
	}
	db, err := open(adminConn)
	if err != nil {
		return err
	}
	defer db.Close()
	account := quoteIdent(user) + "@'%'"
	if _, err := db.ExecContext(ctx, fmt.Sprintf("CREATE USER IF NOT EXISTS %s IDENTIFIED BY %s", account, quoteString(pass))); err != nil {
		return fmt.Errorf("create replication user %q: %w", user, err)
	}
	if _, err := db.ExecContext(ctx, fmt.Sprintf("ALTER USER %s IDENTIFIED BY %s", account, quoteString(pass))); err != nil {
		return fmt.Errorf("update replication user %q: %w", user, err)
	}
	if _, err := db.ExecContext(ctx, "GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO "+account); err != nil {
		return fmt.Errorf("grant replication privileges to %q: %w", user, err)
	}
	return nil
}

// ensureMonitoringUser provisions (or repassword-syncs) the dedicated
// least-privilege monitoring user the mysqld_exporter sidecar
// authenticates as (docs/planning/08 C9 completion) — mysqld_exporter's own
// documented minimum grant set (PROCESS, REPLICATION CLIENT, SELECT on
// *.*), deliberately narrower than the replication user's REPLICATION
// SLAVE grant. This credential is entirely platform-generated (never a
// user-declared SecretReference), so no try-desired/try-previous rotation
// state machine is needed the way the root credential's externally-supplied
// value requires — the root connection can simply (re)set it
// unconditionally.
func ensureMonitoringUser(ctx context.Context, adminConn, user, pass string) error {
	db, err := open(adminConn)
	if err != nil {
		return err
	}
	defer db.Close()
	account := quoteIdent(user) + "@'%'"
	if _, err := db.ExecContext(ctx, fmt.Sprintf("CREATE USER IF NOT EXISTS %s IDENTIFIED BY %s", account, quoteString(pass))); err != nil {
		return fmt.Errorf("create monitoring user %q: %w", user, err)
	}
	if _, err := db.ExecContext(ctx, fmt.Sprintf("ALTER USER %s IDENTIFIED BY %s", account, quoteString(pass))); err != nil {
		return fmt.Errorf("update monitoring user %q: %w", user, err)
	}
	if _, err := db.ExecContext(ctx, "GRANT PROCESS, REPLICATION CLIENT, SELECT ON *.* TO "+account); err != nil {
		return fmt.Errorf("grant monitoring privileges to %q: %w", user, err)
	}
	return nil
}

// parseMyCnfPassword extracts the "password = ..." value from a [client]
// my.cnf stanza — the exact, minimal format ensureExporter itself writes
// (metrics.go), so this only ever needs to parse this package's own output
// back (liveMonitorPassword's read-back-for-idempotency call site), not
// arbitrary my.cnf syntax.
func parseMyCnfPassword(cnf string) string {
	const prefix = "password = "
	for _, line := range strings.Split(cnf, "\n") {
		if strings.HasPrefix(line, prefix) {
			return strings.TrimPrefix(line, prefix)
		}
	}
	return ""
}

func verifyBinlog(ctx context.Context, adminConn string) error {
	db, err := open(adminConn)
	if err != nil {
		return err
	}
	defer db.Close()
	var name, value string
	if err := db.QueryRowContext(ctx, "SHOW VARIABLES LIKE 'log_bin'").Scan(&name, &value); err != nil {
		return fmt.Errorf("check log_bin: %w", err)
	}
	if !strings.EqualFold(value, "ON") && value != "1" {
		return fmt.Errorf("log_bin is %q, expected ON (instance misconfigured for CDC)", value)
	}
	return nil
}

// globalVariable returns a server variable (e.g. binlog_format) — probe
// support for CDC-readiness drift (docs/planning/07 §2.1).
func globalVariable(ctx context.Context, adminConn, name string) (string, error) {
	db, err := open(adminConn)
	if err != nil {
		return "", err
	}
	defer db.Close()
	var ignored, value string
	if err := db.QueryRowContext(ctx, "SHOW GLOBAL VARIABLES LIKE '"+name+"'").Scan(&ignored, &value); err != nil {
		return "", fmt.Errorf("read variable %q: %w", name, err)
	}
	return value, nil
}

// ping verifies the DSN authenticates — probe support for
// credential-validity drift.
func ping(ctx context.Context, conn string) error {
	db, err := open(conn)
	if err != nil {
		return err
	}
	defer db.Close()
	return db.PingContext(ctx)
}
