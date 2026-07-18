// Package storagesize parses human-friendly volume size strings
// ("50Gi", "100Mi", "10G") into raw bytes for runtime.VolumeSpec.SizeBytes.
//
// The suffix vocabulary matches Kubernetes' own resource.Quantity syntax
// (binary Ki/Mi/Gi/Ti and decimal K/M/G/T) since that's the convention
// docs/planning/08 B3's spec.configuration.storage.size stanza uses — but
// this package has no Kubernetes dependency: providers are runtime-agnostic,
// and the Docker adapter ignores VolumeSpec.SizeBytes entirely, so parsing
// the string shouldn't require pulling in a Kubernetes-specific library.
package storagesize

import (
	"fmt"
	"strconv"
	"strings"
)

var binarySuffixes = map[string]int64{
	"Ki": 1 << 10,
	"Mi": 1 << 20,
	"Gi": 1 << 30,
	"Ti": 1 << 40,
}

var decimalSuffixes = map[string]int64{
	"K": 1e3,
	"M": 1e6,
	"G": 1e9,
	"T": 1e12,
}

// ParseBytes parses a size string like "50Gi", "100Mi", "10G", or a bare
// byte count ("1048576"), returning the size in bytes.
func ParseBytes(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty size")
	}
	for suffix, mult := range binarySuffixes {
		if strings.HasSuffix(s, suffix) {
			return parseWithMultiplier(s, suffix, mult)
		}
	}
	for suffix, mult := range decimalSuffixes {
		if strings.HasSuffix(s, suffix) {
			return parseWithMultiplier(s, suffix, mult)
		}
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("size %q: not a recognized size (want a bare byte count or a Ki/Mi/Gi/Ti/K/M/G/T-suffixed value)", s)
	}
	return n, nil
}

func parseWithMultiplier(s, suffix string, mult int64) (int64, error) {
	numPart := strings.TrimSuffix(s, suffix)
	n, err := strconv.ParseFloat(numPart, 64)
	if err != nil {
		return 0, fmt.Errorf("size %q: invalid number before %q suffix", s, suffix)
	}
	if n < 0 {
		return 0, fmt.Errorf("size %q: must not be negative", s)
	}
	return int64(n * float64(mult)), nil
}
