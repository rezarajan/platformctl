package policy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/santhosh-tekuri/jsonschema/v6"

	"github.com/rezarajan/platformctl/schemas"
)

// compiledSchema mirrors internal/application/manifest/schema.go's
// compilation approach (same jsonschema library, same embed-then-compile
// shape) but against schemas.PolicyFS/PolicyKindFiles — the parallel,
// deliberately separate schema set ADR 021 §1 calls for.
var (
	compiledOnce sync.Once
	compiled     *jsonschema.Schema
	compileErr   error
)

func compiledPolicySchema() (*jsonschema.Schema, error) {
	compiledOnce.Do(func() {
		c := jsonschema.NewCompiler()
		files := schemas.PolicyKindFiles["policy.datascape.io/v1alpha1"]
		path, ok := files["Policy"]
		if !ok {
			compileErr = fmt.Errorf("no embedded schema registered for Policy")
			return
		}
		data, err := schemas.PolicyFS.ReadFile(path)
		if err != nil {
			compileErr = fmt.Errorf("read embedded schema %s: %w", path, err)
			return
		}
		doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(data))
		if err != nil {
			compileErr = fmt.Errorf("parse embedded schema %s: %w", path, err)
			return
		}
		var id struct {
			ID string `json:"$id"`
		}
		if err := json.Unmarshal(data, &id); err != nil || id.ID == "" {
			compileErr = fmt.Errorf("embedded schema %s has no $id", path)
			return
		}
		if err := c.AddResource(id.ID, doc); err != nil {
			compileErr = fmt.Errorf("register embedded schema %s: %w", path, err)
			return
		}
		sch, err := c.Compile(id.ID)
		if err != nil {
			compileErr = fmt.Errorf("compile schema for Policy: %w", err)
			return
		}
		compiled = sch
	})
	return compiled, compileErr
}

// validateAgainstSchema checks one raw decoded Policy document against its
// embedded JSON Schema, round-tripping through JSON exactly as
// manifest.validateAgainstSchema does (YAML decoders and the jsonschema
// library disagree on Go numeric types otherwise).
func validateAgainstSchema(raw map[string]any) error {
	sch, err := compiledPolicySchema()
	if err != nil {
		// A build defect (the embedded schema itself failing to compile),
		// not a user input problem — surface loudly, matching
		// manifest.compiledMustLoad's panic-on-build-defect precedent.
		panic(err)
	}
	buf, err := json.Marshal(raw)
	if err != nil {
		return err
	}
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(buf))
	if err != nil {
		return err
	}
	name := "?"
	if meta, ok := raw["metadata"].(map[string]any); ok {
		if n, ok := meta["name"].(string); ok && n != "" {
			name = n
		}
	}
	if err := sch.Validate(doc); err != nil {
		return fmt.Errorf("Policy %q: schema validation failed: %w", name, err)
	}
	return nil
}
