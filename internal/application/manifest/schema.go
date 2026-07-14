// JSON Schema validation: every raw manifest document is checked against its
// Kind's schema (schemas/v1alpha1/*.json) before Go-level decoding — the
// FR-9 guarantee that malformed shapes (including any field that could smuggle
// a plaintext secret) fail at schema time with a precise path.
package manifest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/santhosh-tekuri/jsonschema/v6"

	"github.com/rezarajan/platformctl/schemas"
)

var (
	compiledOnce sync.Once
	compiled     map[string]map[string]*jsonschema.Schema // apiVersion → kind → schema
	compileErr   error
)

func compiledSchemas() (map[string]map[string]*jsonschema.Schema, error) {
	compiledOnce.Do(func() {
		c := jsonschema.NewCompiler()
		// Register every embedded schema under its $id so cross-file $refs
		// (meta.json) resolve without touching the network.
		for _, files := range schemas.KindFiles {
			for _, path := range files {
				if err := addResource(c, path); err != nil {
					compileErr = err
					return
				}
			}
		}
		if err := addResource(c, "v1alpha1/meta.json"); err != nil {
			compileErr = err
			return
		}
		compiled = make(map[string]map[string]*jsonschema.Schema)
		for apiVersion, files := range schemas.KindFiles {
			compiled[apiVersion] = make(map[string]*jsonschema.Schema, len(files))
			for kind, path := range files {
				sch, err := c.Compile("https://datascape.io/schemas/" + path)
				if err != nil {
					compileErr = fmt.Errorf("compile schema for %s: %w", kind, err)
					return
				}
				compiled[apiVersion][kind] = sch
			}
		}
	})
	return compiled, compileErr
}

func addResource(c *jsonschema.Compiler, path string) error {
	data, err := schemas.FS.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read embedded schema %s: %w", path, err)
	}
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("parse embedded schema %s: %w", path, err)
	}
	var id struct {
		ID string `json:"$id"`
	}
	if err := json.Unmarshal(data, &id); err != nil || id.ID == "" {
		return fmt.Errorf("embedded schema %s has no $id", path)
	}
	return c.AddResource(id.ID, doc)
}

// validateAgainstSchema checks one raw decoded document against its Kind's
// JSON Schema. Unknown kinds/apiVersions are left to Validate's clearer
// errors.
func validateAgainstSchema(raw map[string]any) error {
	apiVersion, _ := raw["apiVersion"].(string)
	kind, _ := raw["kind"].(string)
	byKind, ok := compiledMustLoad()[apiVersion]
	if !ok {
		return nil
	}
	sch, ok := byKind[kind]
	if !ok {
		return nil
	}
	// Round-trip through JSON so numeric/nested types match what the
	// validator expects, independent of the YAML decoder's Go types.
	buf, err := json.Marshal(raw)
	if err != nil {
		return err
	}
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(buf))
	if err != nil {
		return err
	}
	name, _ := raw["metadata"].(map[string]any)["name"]
	if err := sch.Validate(doc); err != nil {
		return fmt.Errorf("%s %q: schema validation failed: %w", kind, name, err)
	}
	return nil
}

func compiledMustLoad() map[string]map[string]*jsonschema.Schema {
	c, err := compiledSchemas()
	if err != nil {
		// Embedded schemas failing to compile is a build defect, not a user
		// input problem; surface loudly.
		panic(err)
	}
	return c
}
