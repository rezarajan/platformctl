package providerkit

import (
	"fmt"

	"github.com/rezarajan/platformctl/internal/domain/provider"
)

// ResolveCredential resolves the SecretReference named by
// cfg.Configuration[configKey] — defaulting to the first entry in
// spec.secretRefs when configKey is unset — and returns its resolved
// key/value map plus the reference name (for the caller's own per-key
// validation error messages, which name what keys they require: postgres
// needs username+password, mysql needs only password).
func ResolveCredential(cfg provider.Provider, secrets map[string]map[string]string, configKey, name string) (creds map[string]string, refName string, err error) {
	refName, _ = cfg.Configuration[configKey].(string)
	if refName == "" && len(cfg.SecretRefs) > 0 {
		refName = cfg.SecretRefs[0]
	}
	creds, ok := secrets[refName]
	if !ok {
		return nil, refName, fmt.Errorf("Provider %q (type: %s): no resolved credentials for secretRef %q", name, cfg.Type, refName)
	}
	return creds, refName, nil
}
