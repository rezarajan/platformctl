// Package clock defines a Clock interface so time is fakeable in tests.
package clock

import "time"

type Clock interface {
	Now() time.Time
}

// Real is the wall-clock implementation.
type Real struct{}

func (Real) Now() time.Time { return time.Now() }

// Fake is a settable clock for tests.
type Fake struct{ T time.Time }

func (f *Fake) Now() time.Time { return f.T }

func (f *Fake) Advance(d time.Duration) { f.T = f.T.Add(d) }
