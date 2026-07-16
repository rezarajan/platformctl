package postgres

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
)

// connString builds the connection URL through net/url so credentials
// containing @, :, /, #, spaces, or quotes survive intact — secret values
// must not depend on lucky demo passwords (docs/planning/07 §2.2).
func connString(host string, port int, user, pass, db string) string {
	u := url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(user, pass),
		Host:     net.JoinHostPort(host, strconv.Itoa(port)),
		Path:     "/" + db,
		RawQuery: "sslmode=disable",
	}
	return u.String()
}

func connect(ctx context.Context, conn string) (*pgx.Conn, error) {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	c, err := pgx.Connect(ctx, conn)
	if err != nil {
		return nil, fmt.Errorf("connect to postgres: %w", err)
	}
	return c, nil
}

// waitReady ping-loops until the server accepts connections. Container
// health and host-port reachability are not the same instant on every
// runtime; provisioning right after WaitHealthy must tolerate the gap.
func waitReady(ctx context.Context, conn string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for {
		c, err := connect(ctx, conn)
		if err == nil {
			_ = c.Close(ctx)
			return nil
		}
		lastErr = err
		if time.Now().After(deadline) {
			return fmt.Errorf("postgres not reachable within %s: %w", timeout, lastErr)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

func ensureSuperuserCredentials(ctx context.Context, adminConn, user, pass string) error {
	c, err := connect(ctx, adminConn)
	if err != nil {
		return err
	}
	defer c.Close(ctx)
	var count int
	if err := c.QueryRow(ctx, `SELECT count(*) FROM pg_roles WHERE rolname = $1`, user).Scan(&count); err != nil {
		return fmt.Errorf("check superuser role %q: %w", user, err)
	}
	quotedUser := pgx.Identifier{user}.Sanitize()
	if count == 0 {
		if _, err := c.Exec(ctx, fmt.Sprintf(`CREATE ROLE %s WITH LOGIN SUPERUSER PASSWORD '%s'`, quotedUser, escapeLiteral(pass))); err != nil {
			return fmt.Errorf("create superuser role %q: %w", user, err)
		}
		return nil
	}
	if _, err := c.Exec(ctx, fmt.Sprintf(`ALTER ROLE %s WITH LOGIN SUPERUSER PASSWORD '%s'`, quotedUser, escapeLiteral(pass))); err != nil {
		return fmt.Errorf("update superuser role %q: %w", user, err)
	}
	return nil
}

func ensureDatabase(ctx context.Context, adminConn, name string) error {
	exists, err := databaseExists(ctx, adminConn, name)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	c, err := connect(ctx, adminConn)
	if err != nil {
		return err
	}
	defer c.Close(ctx)
	// CREATE DATABASE cannot be parameterized; the identifier is quoted.
	if _, err := c.Exec(ctx, fmt.Sprintf(`CREATE DATABASE %s`, pgx.Identifier{name}.Sanitize())); err != nil {
		return fmt.Errorf("create database %q: %w", name, err)
	}
	return nil
}

// dropDatabase removes the database if it exists — only reached through an
// explicit Source deletionPolicy: delete (docs/planning/07 §2.2).
func dropDatabase(ctx context.Context, adminConn, name string) error {
	exists, err := databaseExists(ctx, adminConn, name)
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}
	c, err := connect(ctx, adminConn)
	if err != nil {
		return err
	}
	defer c.Close(ctx)
	// FORCE terminates lingering connections (replication slots aside);
	// identifier quoted, cannot be parameterized.
	if _, err := c.Exec(ctx, fmt.Sprintf(`DROP DATABASE %s WITH (FORCE)`, pgx.Identifier{name}.Sanitize())); err != nil {
		return fmt.Errorf("drop database %q: %w", name, err)
	}
	return nil
}

func databaseExists(ctx context.Context, adminConn, name string) (bool, error) {
	c, err := connect(ctx, adminConn)
	if err != nil {
		return false, err
	}
	defer c.Close(ctx)
	var count int
	if err := c.QueryRow(ctx, `SELECT count(*) FROM pg_database WHERE datname = $1`, name).Scan(&count); err != nil {
		return false, fmt.Errorf("check database %q: %w", name, err)
	}
	return count > 0, nil
}

func ensureReplicationRole(ctx context.Context, adminConn, user, pass string) error {
	c, err := connect(ctx, adminConn)
	if err != nil {
		return err
	}
	defer c.Close(ctx)
	var count int
	if err := c.QueryRow(ctx, `SELECT count(*) FROM pg_roles WHERE rolname = $1`, user).Scan(&count); err != nil {
		return fmt.Errorf("check role %q: %w", user, err)
	}
	quotedUser := pgx.Identifier{user}.Sanitize()
	if count == 0 {
		if _, err := c.Exec(ctx, fmt.Sprintf(`CREATE ROLE %s WITH LOGIN REPLICATION PASSWORD '%s'`, quotedUser, escapeLiteral(pass))); err != nil {
			return fmt.Errorf("create replication role %q: %w", user, err)
		}
	} else if _, err := c.Exec(ctx, fmt.Sprintf(`ALTER ROLE %s WITH LOGIN REPLICATION PASSWORD '%s'`, quotedUser, escapeLiteral(pass))); err != nil {
		return fmt.Errorf("update replication role %q: %w", user, err)
	}
	// Snapshot reads need SELECT on current and future tables; the role is
	// deliberately not a superuser.
	if _, err := c.Exec(ctx, fmt.Sprintf(`GRANT pg_read_all_data TO %s`, quotedUser)); err != nil {
		return fmt.Errorf("grant pg_read_all_data to %q: %w", user, err)
	}
	return nil
}

// ensurePublication pre-creates the pgoutput publication in the source
// database as the superuser. Debezium's autocreate path would otherwise need
// the replication role to own every table (or be superuser); pre-creating it
// is the documented least-privilege setup.
func ensurePublication(ctx context.Context, dbConn, name string) error {
	c, err := connect(ctx, dbConn)
	if err != nil {
		return err
	}
	defer c.Close(ctx)
	var count int
	if err := c.QueryRow(ctx, `SELECT count(*) FROM pg_publication WHERE pubname = $1`, name).Scan(&count); err != nil {
		return fmt.Errorf("check publication %q: %w", name, err)
	}
	if count > 0 {
		return nil
	}
	if _, err := c.Exec(ctx, fmt.Sprintf(`CREATE PUBLICATION %s FOR ALL TABLES`, pgx.Identifier{name}.Sanitize())); err != nil {
		return fmt.Errorf("create publication %q: %w", name, err)
	}
	return nil
}

func verifyLogicalWAL(ctx context.Context, adminConn string) error {
	c, err := connect(ctx, adminConn)
	if err != nil {
		return err
	}
	defer c.Close(ctx)
	var level string
	if err := c.QueryRow(ctx, `SHOW wal_level`).Scan(&level); err != nil {
		return fmt.Errorf("check wal_level: %w", err)
	}
	if level != "logical" {
		return fmt.Errorf("wal_level is %q, expected \"logical\" (instance misconfigured)", level)
	}
	return nil
}

func escapeLiteral(s string) string {
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		if s[i] == '\'' {
			out = append(out, '\'')
		}
		out = append(out, s[i])
	}
	return string(out)
}

// showSetting returns a server configuration value (e.g. wal_level) — probe
// support for CDC-readiness drift (docs/planning/07 §2.1).
func showSetting(ctx context.Context, adminConn, name string) (string, error) {
	c, err := connect(ctx, adminConn)
	if err != nil {
		return "", err
	}
	defer c.Close(ctx)
	var value string
	if err := c.QueryRow(ctx, `SELECT setting FROM pg_settings WHERE name = $1`, name).Scan(&value); err != nil {
		return "", fmt.Errorf("read setting %q: %w", name, err)
	}
	return value, nil
}

// ping verifies the connection string authenticates — probe support for
// credential-validity drift.
func ping(ctx context.Context, conn string) error {
	c, err := connect(ctx, conn)
	if err != nil {
		return err
	}
	return c.Close(ctx)
}
