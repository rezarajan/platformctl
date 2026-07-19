package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	// Also registers the "mysql" database/sql driver.
	godriver "github.com/go-sql-driver/mysql"
)

// dsnAddr builds the DSN through the driver's own Config type so credentials
// containing @, :, /, #, spaces, or quotes survive intact — secret values
// must not depend on lucky demo passwords (docs/planning/07 §2.2). addr is
// a "host:port" this process can dial right now (docs/planning/08 B8: from
// Provider.reachableAddr/runtime.EnsureReachable — MySQL's wire protocol has
// no broker-style redirect, so this can be used directly for a whole call).
func dsnAddr(addr, user, pass, db string) string {
	cfg := godriver.NewConfig()
	cfg.User = user
	cfg.Passwd = pass
	cfg.Net = "tcp"
	cfg.Addr = addr
	cfg.DBName = db
	cfg.Timeout = 10 * time.Second
	return cfg.FormatDSN()
}

func open(conn string) (*sql.DB, error) {
	db, err := sql.Open("mysql", conn)
	if err != nil {
		return nil, fmt.Errorf("connect to mysql: %w", err)
	}
	return db, nil
}

// waitReady ping-loops until the server accepts authenticated connections —
// the images serve their health ping during an init phase that still
// refuses real logins.
func waitReady(ctx context.Context, conn string, timeout time.Duration) error {
	db, err := open(conn)
	if err != nil {
		return err
	}
	defer db.Close()
	deadline := time.Now().Add(timeout)
	var lastErr error
	for {
		lastErr = db.PingContext(ctx)
		if lastErr == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("mysql not reachable within %s: %w", timeout, lastErr)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
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
