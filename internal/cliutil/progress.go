package cliutil

import (
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/rezarajan/platformctl/internal/domain/resource"
)

// ProgressReporter renders apply progress in a Docker-style, ordered,
// countable form: a header with the total, a "starting" line and a
// "done/failed" line per step (each tagged with its [n/total] position and,
// on completion, its duration), plus drift-heal and skip notes. It is safe
// for concurrent steps (parallel apply) — output is serialized by a mutex —
// and for non-TTY sinks (CI, pipes), which is why it streams plain lines
// rather than repainting in place.
type ProgressReporter struct {
	w     io.Writer
	color bool
	mu    sync.Mutex
}

// NewProgressReporter writes to w. color enables ANSI status glyphs when w is
// a terminal.
func NewProgressReporter(w io.Writer, color bool) *ProgressReporter {
	return &ProgressReporter{w: w, color: color}
}

const (
	ansiReset  = "\033[0m"
	ansiGreen  = "\033[32m"
	ansiRed    = "\033[31m"
	ansiYellow = "\033[33m"
	ansiDim    = "\033[2m"
	ansiCyan   = "\033[36m"
)

func (p *ProgressReporter) tint(code, s string) string {
	if !p.color {
		return s
	}
	return code + s + ansiReset
}

func (p *ProgressReporter) Begin(total int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if total == 0 {
		// A no-changes / drift-probe apply: the command already explained
		// itself; healing steps (if any) stream below.
		return
	}
	noun := "resources"
	if total == 1 {
		noun = "resource"
	}
	fmt.Fprintf(p.w, "Reconciling %d %s:\n", total, noun)
}

func (p *ProgressReporter) StepStarted(seq, total int, key resource.Key, action string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	fmt.Fprintf(p.w, "  %s %s %s %s\n",
		p.tint(ansiCyan, counter(seq, total)),
		p.tint(ansiCyan, "◐"),
		action,
		key.String())
}

func (p *ProgressReporter) StepFinished(seq, total int, key resource.Key, action string, d time.Duration, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err != nil {
		fmt.Fprintf(p.w, "  %s %s %s %s %s\n      %s\n",
			p.tint(ansiRed, counter(seq, total)),
			p.tint(ansiRed, "✗"),
			action,
			key.String(),
			p.tint(ansiDim, "("+d.String()+")"),
			p.tint(ansiRed, err.Error()))
		return
	}
	fmt.Fprintf(p.w, "  %s %s %s %s %s\n",
		p.tint(ansiGreen, counter(seq, total)),
		p.tint(ansiGreen, "✓"),
		action,
		key.String(),
		p.tint(ansiDim, "("+d.String()+")"))
}

func (p *ProgressReporter) StepSkipped(key resource.Key, reason string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	fmt.Fprintf(p.w, "  %s %s %s %s\n",
		p.tint(ansiYellow, "  -  "),
		p.tint(ansiYellow, "⊘"),
		"skip",
		key.String()+" — "+reason)
}

func (p *ProgressReporter) StepHealing(key resource.Key, reason string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	fmt.Fprintf(p.w, "  %s %s drift %s — %s\n",
		p.tint(ansiYellow, "  ~  "),
		p.tint(ansiYellow, "⟳"),
		key.String(),
		reason)
}

func (p *ProgressReporter) End(succeeded, failed, skipped int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if succeeded == 0 && failed == 0 && skipped == 0 {
		return // a clean no-op / drift-probe apply changed nothing
	}
	summary := fmt.Sprintf("%d applied", succeeded)
	if failed > 0 {
		summary += fmt.Sprintf(", %s", p.tint(ansiRed, fmt.Sprintf("%d failed", failed)))
	}
	if skipped > 0 {
		summary += fmt.Sprintf(", %s", p.tint(ansiYellow, fmt.Sprintf("%d skipped", skipped)))
	}
	glyph := p.tint(ansiGreen, "✓")
	if failed > 0 {
		glyph = p.tint(ansiRed, "✗")
	}
	fmt.Fprintf(p.w, "%s %s\n", glyph, summary)
}

func counter(seq, total int) string {
	if seq > total {
		// A healing step discovered at runtime, beyond the planned count.
		return fmt.Sprintf("[%d]", seq)
	}
	return fmt.Sprintf("[%d/%d]", seq, total)
}
