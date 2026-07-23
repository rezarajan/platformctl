package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
)

// textLineHandler backs --log-format text (the default, docs/planning/08
// I11): it renders exactly what Engine.logf's pre-slog stderr prose looked
// like — the record's Message plus a trailing newline, nothing else. No
// timestamp/level prefix, no attrs. This is a deliberate departure from
// slog's own TextHandler (which renders "time=... level=... msg=...
// key=value ..."): I11's accept bar requires the default log stream stay
// byte-compatible with the fmt.Fprintf(os.Stderr, format+"\n", args...)
// seam it replaces, and Engine.logAction always renders the same prose into
// the record's Message that the old seam printed — this handler just gets
// out of the way of it. --log-format json swaps this out for slog's
// standard JSON handler, which DOES render attrs (resource/action/outcome/
// duration per NFR-4) alongside that same message.
type textLineHandler struct {
	w io.Writer
}

func (h *textLineHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *textLineHandler) Handle(_ context.Context, r slog.Record) error {
	_, err := fmt.Fprintln(h.w, r.Message)
	return err
}

func (h *textLineHandler) WithAttrs(_ []slog.Attr) slog.Handler { return h }
func (h *textLineHandler) WithGroup(_ string) slog.Handler      { return h }

// newEngineLogger builds the slog.Logger that backs engine.Engine.Logger
// (docs/planning/08 I11), per --log-format:
//   - "text" (default): textLineHandler above — byte-compatible prose.
//   - "json": slog's standard JSON handler — one parseable object per
//     reconciliation action, carrying NFR-4's resource/action/outcome/
//     duration attrs plus the same prose in "msg".
//
// Any other value is rejected by (*app).init before this is ever called.
func newEngineLogger(w io.Writer, format string) *slog.Logger {
	var h slog.Handler
	if format == "json" {
		h = slog.NewJSONHandler(w, nil)
	} else {
		h = &textLineHandler{w: w}
	}
	return slog.New(h)
}
