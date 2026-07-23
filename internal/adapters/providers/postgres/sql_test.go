package postgres

import (
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/rezarajan/platformctl/internal/adapters/providers/providerkit"
)

// TestConnStringSurvivesSpecialCharacterCredentials guards docs/planning/07
// §2.2: secrets commonly contain @ : / # spaces and quotes; the connection
// URL must parse back to the exact same credentials.
func TestConnStringSurvivesSpecialCharacterCredentials(t *testing.T) {
	t.Parallel()
	cases := []struct{ user, pass string }{
		{"admin", "p@ss:w/rd#1 x"},
		{"we?rd@user", `quo"te'and\slash`},
		{"admin", "perfectly-normal"},
	}
	for _, tc := range cases {
		conn := connStringAddr("127.0.0.1:5432", tc.user, tc.pass, "postgres", nil)
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

// TestConnStringSSLMode covers docs/planning/08 I2's four outbound TLS
// postures this DSN builder must produce: nil (back-compat plaintext) and
// each of connection.TLSModeRequire/VerifyCA/VerifyFull.
func TestConnStringSSLMode(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		tls     *providerkit.DatabaseTLS
		wantSSL string
	}{
		{"nil posture disables TLS", nil, "disable"},
		{"require", &providerkit.DatabaseTLS{Mode: "require"}, "require"},
		{"verify-ca", &providerkit.DatabaseTLS{Mode: "verify-ca"}, "verify-ca"},
		{"verify-full", &providerkit.DatabaseTLS{Mode: "verify-full"}, "verify-full"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			conn := connStringAddr("db.example.com:5432", "u", "p", "orders", tc.tls)
			cfg, err := pgx.ParseConfig(conn)
			if err != nil {
				t.Fatalf("ParseConfig(%q): %v", conn, err)
			}
			_ = cfg
			if !strings.Contains(conn, "sslmode="+tc.wantSSL) {
				t.Errorf("connStringAddr = %q, want sslmode=%s", conn, tc.wantSSL)
			}
		})
	}
}
