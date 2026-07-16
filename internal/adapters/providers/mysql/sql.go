package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	// Registers the "mysql" database/sql driver.
	_ "github.com/go-sql-driver/mysql"
)

func dsn(host string, port int, user, pass, db string) string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?timeout=10s", user, pass, host, port, db)
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
