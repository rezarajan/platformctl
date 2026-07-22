package lint

import (
	"reflect"
	"testing"

	"github.com/rezarajan/platformctl/internal/domain/resource"
)

func TestParseWaivers(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want []Waiver
	}{
		{"empty", "", nil},
		{"single", "DL010: intentionally unread", []Waiver{{Code: "DL010", Reason: "intentionally unread"}}},
		{"no reason", "DL010", []Waiver{{Code: "DL010"}}},
		{
			"reason containing a comma is preserved verbatim (newline is the multi-entry separator, not comma)",
			"DL013: this blueprint only demonstrates capture, no sink is added",
			[]Waiver{{Code: "DL013", Reason: "this blueprint only demonstrates capture, no sink is added"}},
		},
		{
			"multiple entries, newline-separated",
			"DL010: reason one\nDL012: reason two",
			[]Waiver{{Code: "DL010", Reason: "reason one"}, {Code: "DL012", Reason: "reason two"}},
		},
		{
			"blank lines and surrounding whitespace are ignored",
			"\n  DL010:   reason one  \n\n  DL012: reason two\n",
			[]Waiver{{Code: "DL010", Reason: "reason one"}, {Code: "DL012", Reason: "reason two"}},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := ParseWaivers(map[string]string{WaiveAnnotation: c.raw})
			if !reflect.DeepEqual(got, c.want) {
				t.Errorf("ParseWaivers(%q) = %#v, want %#v", c.raw, got, c.want)
			}
		})
	}

	t.Run("missing annotation returns nil", func(t *testing.T) {
		if got := ParseWaivers(map[string]string{}); got != nil {
			t.Errorf("ParseWaivers(no annotation) = %#v, want nil", got)
		}
	})
}

func TestLessOrdersBySeverityThenCodeThenResource(t *testing.T) {
	a := Finding{Code: "DL002", Severity: Warning, Resource: resource.Key{Kind: "Binding", Name: "b"}}
	b := Finding{Code: "DL002", Severity: Warning, Resource: resource.Key{Kind: "Binding", Name: "a"}}
	c := Finding{Code: "DL001", Severity: Warning, Resource: resource.Key{Kind: "Binding", Name: "z"}}
	d := Finding{Code: "DL010", Severity: Info, Resource: resource.Key{Kind: "EventStream", Name: "a"}}

	if !Less(c, a) {
		t.Error("lower code should sort first within the same severity")
	}
	if !Less(b, a) {
		t.Error("same code/severity should sort by resource key")
	}
	if !Less(a, d) {
		t.Error("warning should sort before info regardless of code")
	}
	if Less(d, a) {
		t.Error("info should not sort before warning")
	}
}
