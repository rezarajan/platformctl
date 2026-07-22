package cliutil

import (
	"fmt"
	"os"

	huh "charm.land/huh/v2"
	"golang.org/x/term"
)

// Interactive reports whether stdin is a TTY huh forms can actually run
// against — the gate every add/wire/expose command uses to decide between
// prompting and the CI-safe "non-TTY + incomplete flags = hard error"
// path (docs/planning/08 E9, docs/adr/024-interactive-composition.md).
// Confined here (and cmd/platformctl) per the charm-import confinement
// rule ADR 024's "Interaction layer" section states and
// internal/archtest's confinement test enforces.
func Interactive() bool {
	return term.IsTerminal(int(os.Stdin.Fd()))
}

// Option builds a huh select option with a distinct label/value — the one
// piece of huh's API a caller needs beyond the helpers below to build an
// option list (e.g. from compose.Candidate).
func Option(label, value string) huh.Option[string] {
	return huh.NewOption(label, value)
}

// SelectString runs a single-select form over options, returning the
// chosen value.
func SelectString(title string, options []huh.Option[string]) (string, error) {
	var choice string
	form := huh.NewForm(huh.NewGroup(
		huh.NewSelect[string]().Title(title).Options(options...).Value(&choice),
	))
	if err := form.Run(); err != nil {
		return "", err
	}
	return choice, nil
}

// InputString runs a single free-text form, pre-filled with defaultValue.
// When required is true, an empty answer is rejected in-form.
func InputString(title, defaultValue string, required bool) (string, error) {
	value := defaultValue
	input := huh.NewInput().Title(title).Value(&value)
	if required {
		input = input.Validate(func(s string) error {
			if s == "" {
				return fmt.Errorf("%s is required", title)
			}
			return nil
		})
	}
	form := huh.NewForm(huh.NewGroup(input))
	if err := form.Run(); err != nil {
		return "", err
	}
	return value, nil
}

// Confirm runs a yes/no form.
func Confirm(title string, defaultValue bool) (bool, error) {
	value := defaultValue
	form := huh.NewForm(huh.NewGroup(
		huh.NewConfirm().Title(title).Value(&value),
	))
	if err := form.Run(); err != nil {
		return false, err
	}
	return value, nil
}
