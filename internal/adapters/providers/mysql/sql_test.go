package mysql

import (
	"testing"

	godriver "github.com/go-sql-driver/mysql"
)

// TestDSNSurvivesSpecialCharacterCredentials guards docs/planning/07 §2.2:
// the DSN must round-trip credentials containing @ : / # spaces and quotes.
func TestDSNSurvivesSpecialCharacterCredentials(t *testing.T) {
	cases := []struct{ user, pass string }{
		{"root", "p@ss:w/rd#1 x"},
		{"we(rd)user", `quo"te'and\slash`},
		{"root", "perfectly-normal"},
	}
	for _, tc := range cases {
		conn := dsnAddr("127.0.0.1:3306", tc.user, tc.pass, "app")
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
