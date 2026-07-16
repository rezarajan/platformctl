package postgres

import (
	"testing"

	"github.com/jackc/pgx/v5"
)

// TestConnStringSurvivesSpecialCharacterCredentials guards docs/planning/07
// §2.2: secrets commonly contain @ : / # spaces and quotes; the connection
// URL must parse back to the exact same credentials.
func TestConnStringSurvivesSpecialCharacterCredentials(t *testing.T) {
	cases := []struct{ user, pass string }{
		{"admin", "p@ss:w/rd#1 x"},
		{"we?rd@user", `quo"te'and\slash`},
		{"admin", "perfectly-normal"},
	}
	for _, tc := range cases {
		conn := connString("127.0.0.1", 5432, tc.user, tc.pass, "postgres")
		cfg, err := pgx.ParseConfig(conn)
		if err != nil {
			t.Fatalf("ParseConfig(%q): %v", conn, err)
		}
		if cfg.User != tc.user || cfg.Password != tc.pass {
			t.Errorf("round-trip user/pass = %q/%q, want %q/%q", cfg.User, cfg.Password, tc.user, tc.pass)
		}
		if cfg.Database != "postgres" {
			t.Errorf("database = %q, want postgres", cfg.Database)
		}
	}
}
