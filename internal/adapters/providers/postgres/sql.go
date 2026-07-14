package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

func connString(host string, port int, user, pass, db string) string {
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable", user, pass, host, port, db)
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
		return nil
	}
	if _, err := c.Exec(ctx, fmt.Sprintf(`ALTER ROLE %s WITH LOGIN REPLICATION PASSWORD '%s'`, quotedUser, escapeLiteral(pass))); err != nil {
		return fmt.Errorf("update replication role %q: %w", user, err)
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
