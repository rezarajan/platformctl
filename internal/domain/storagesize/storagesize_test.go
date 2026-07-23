package storagesize

import "testing"

func TestParseBytes(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in      string
		want    int64
		wantErr bool
	}{
		{"50Gi", 50 * 1 << 30, false},
		{"100Mi", 100 * 1 << 20, false},
		{"1Ti", 1 << 40, false},
		{"10G", 10_000_000_000, false},
		{"1048576", 1048576, false},
		{"", 0, true},
		{"garbage", 0, true},
		{"-1Gi", 0, true},
	}
	for _, tc := range cases {
		got, err := ParseBytes(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Errorf("ParseBytes(%q) = %d, nil; want an error", tc.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("ParseBytes(%q) unexpected error: %v", tc.in, err)
			continue
		}
		if got != tc.want {
			t.Errorf("ParseBytes(%q) = %d, want %d", tc.in, got, tc.want)
		}
	}
}
