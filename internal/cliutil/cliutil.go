// Package cliutil provides output formatting and the documented exit-code
// contract. See docs/planning/02-architecture.md §8.
package cliutil

import (
	"encoding/json"
	"fmt"
	"io"
	"text/tabwriter"

	"gopkg.in/yaml.v3"
)

// Exit codes — deterministic so CI can branch without parsing text.
const (
	ExitOK          = 0
	ExitPlanChanges = 1
	ExitExecution   = 2
	ExitValidation  = 3
	ExitLockHeld    = 4
)

// ExitError carries an exit code up to main.
type ExitError struct {
	Code int
	Err  error
}

func (e ExitError) Error() string {
	if e.Err == nil {
		return fmt.Sprintf("exit %d", e.Code)
	}
	return e.Err.Error()
}

func Exit(code int, err error) error { return ExitError{Code: code, Err: err} }

// WriteOutput renders v as table (via rows), json, or yaml to w.
// rows is used only for table format: header + data rows.
func WriteOutput(w io.Writer, format string, v any, rows [][]string) error {
	switch format {
	case "json":
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		return enc.Encode(v)
	case "yaml":
		return yaml.NewEncoder(w).Encode(v)
	case "table", "":
		tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
		for _, row := range rows {
			for i, cell := range row {
				if i > 0 {
					fmt.Fprint(tw, "\t")
				}
				fmt.Fprint(tw, cell)
			}
			fmt.Fprintln(tw)
		}
		return tw.Flush()
	default:
		return fmt.Errorf("unknown output format %q (allowed: table, json, yaml)", format)
	}
}
