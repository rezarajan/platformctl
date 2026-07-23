package mysql

import (
	"testing"

	godriver "github.com/go-sql-driver/mysql"

	"github.com/rezarajan/platformctl/internal/adapters/providers/providerkit"
)

// TestDSNSurvivesSpecialCharacterCredentials guards docs/planning/07 §2.2:
// the DSN must round-trip credentials containing @ : / # spaces and quotes.
func TestDSNSurvivesSpecialCharacterCredentials(t *testing.T) {
	t.Parallel()
	cases := []struct{ user, pass string }{
		{"root", "p@ss:w/rd#1 x"},
		{"we(rd)user", `quo"te'and\slash`},
		{"root", "perfectly-normal"},
	}
	for _, tc := range cases {
		conn := dsnAddr("127.0.0.1:3306", tc.user, tc.pass, "app", nil)
		cfg, err := godriver.ParseDSN(conn)
		if err != nil {
			t.Fatalf("ParseDSN(%q): %v", conn, err)
		}
		if cfg.User != tc.user || cfg.Passwd != tc.pass {
			t.Errorf("round-trip user/pass = %q/%q, want %q/%q", cfg.User, cfg.Passwd, tc.user, tc.pass)
		}
		if cfg.DBName != "app" {
			t.Errorf("dbname = %q, want app", cfg.DBName)
		}
	}
}

// TestDSNTLSMode covers docs/planning/08 I2's four outbound TLS postures:
// nil (back-compat, no "tls" DSN param at all) and each of
// connection.TLSModeRequire/VerifyCA/VerifyFull, each registering a
// go-sql-driver TLS config and referencing it via the "tls" param — the
// driver's own documented mechanism (there is no inline-PEM query param).
func TestDSNTLSMode(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		tls  *providerkit.DatabaseTLS
	}{
		{"nil posture", nil},
		{"require", &providerkit.DatabaseTLS{Mode: "require"}},
		{"verify-ca", &providerkit.DatabaseTLS{Mode: "verify-ca"}},
		{"verify-full", &providerkit.DatabaseTLS{Mode: "verify-full"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			conn := dsnAddr("db.example.com:3306", "root", "pw", "app", tc.tls)
			cfg, err := godriver.ParseDSN(conn)
			if err != nil {
				t.Fatalf("ParseDSN(%q): %v", conn, err)
			}
			if tc.tls == nil {
				if cfg.TLSConfig != "" {
					t.Errorf("TLSConfig = %q, want empty for a nil posture", cfg.TLSConfig)
				}
				return
			}
			if cfg.TLSConfig == "" {
				t.Fatalf("TLSConfig unset for mode %q, want a registered config name", tc.tls.Mode)
			}
			if _, err := godriver.ParseDSN(conn); err != nil {
				t.Fatalf("registered TLS config name did not survive a DSN round-trip: %v", err)
			}
		})
	}
}
