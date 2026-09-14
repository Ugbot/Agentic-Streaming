package core

import (
	"encoding/json"
	"fmt"
	"strings"
)

// ValidateSchema validates value against a JSON-schema-lite contract
// ({type, required, properties}). Returns a list of errors (empty == valid).
func ValidateSchema(value any, schema map[string]any) []string {
	expected, _ := schema["type"].(string)
	if expected == "" {
		expected = "object"
	}
	if !typeMatches(expected, value) {
		return []string{fmt.Sprintf("expected %s, got %T", expected, value)}
	}
	var errors []string
	if expected == "object" {
		obj, _ := value.(map[string]any)
		for _, req := range toAnySlice(schema["required"]) {
			if _, ok := obj[fmt.Sprint(req)]; !ok {
				errors = append(errors, fmt.Sprintf("missing required field %q", req))
			}
		}
		if props, ok := schema["properties"].(map[string]any); ok {
			for field, sub := range props {
				if v, present := obj[field]; present {
					if subSchema, ok := sub.(map[string]any); ok {
						for _, e := range ValidateSchema(v, subSchema) {
							errors = append(errors, field+"."+e)
						}
					}
				}
			}
		}
	}
	return errors
}

func typeMatches(expected string, value any) bool {
	switch expected {
	case "string":
		_, ok := value.(string)
		return ok
	case "number", "integer":
		_, ok := value.(float64)
		return ok
	case "boolean":
		_, ok := value.(bool)
		return ok
	case "array":
		_, ok := value.([]any)
		return ok
	case "object":
		_, ok := value.(map[string]any)
		return ok
	}
	return true
}

// ParseStructured parses text as one JSON object (tolerant of prose) and validates it.
func ParseStructured(text string, schema map[string]any) (map[string]any, []string) {
	s := strings.TrimSpace(text)
	start, end := strings.Index(s, "{"), strings.LastIndex(s, "}")
	if start == -1 || end <= start {
		return nil, []string{"no JSON object found in output"}
	}
	var obj map[string]any
	if err := json.Unmarshal([]byte(s[start:end+1]), &obj); err != nil {
		return nil, []string{"invalid JSON: " + err.Error()}
	}
	return obj, ValidateSchema(obj, schema)
}

// SchemaInstruction is a system-prompt fragment telling the model to answer with
// conforming JSON.
func SchemaInstruction(schema map[string]any) string {
	b, _ := json.Marshal(schema)
	return "Respond with a single JSON object conforming to this schema: " + string(b)
}
