package config

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
)

// ArrInstanceConfig holds the connection and routing for a single Sonarr or
// Radarr instance.
//
// The API key is masked by String and MarshalJSON. Config is logged on startup
// and dumped by the health endpoint, so a plain struct here would put the key
// in both.
type ArrInstanceConfig struct {
	URL        string
	APIKey     string
	Categories []string
}

// String returns a redacted representation suitable for logging.
func (c ArrInstanceConfig) String() string {
	return fmt.Sprintf("ArrInstanceConfig{URL:%q, APIKey:%s, Categories:%v}",
		c.URL, redactAPIKey(c.APIKey), c.Categories)
}

// MarshalJSON masks the API key in JSON output.
func (c ArrInstanceConfig) MarshalJSON() ([]byte, error) {
	type alias struct {
		URL        string   `json:"url"`
		APIKey     string   `json:"api_key"`
		Categories []string `json:"categories"`
	}

	return json.Marshal(alias{
		URL:        c.URL,
		APIKey:     redactAPIKey(c.APIKey),
		Categories: c.Categories,
	})
}

// IsZero reports whether the instance is unconfigured. An unconfigured
// instance disables filtering for its categories rather than failing.
func (c ArrInstanceConfig) IsZero() bool {
	return c.URL == "" && c.APIKey == "" && len(c.Categories) == 0
}

// redactAPIKey returns a fixed mask rather than a prefix: even a few characters
// of a key are worth withholding, and a fixed mask cannot leak length either.
func redactAPIKey(key string) string {
	if key == "" {
		return "<unset>"
	}
	return "***"
}

// validateArrInstance rejects a partially configured instance. All three fields
// or none: a URL with no key, or categories with no URL, is a deployment
// mistake that would otherwise present as the filter silently never running.
func validateArrInstance(name string, c ArrInstanceConfig) error {
	if c.IsZero() {
		return nil
	}
	if c.URL == "" {
		return fmt.Errorf("%s: URL is required when %s is configured", name, name)
	}
	if c.APIKey == "" {
		return fmt.Errorf("%s: API key is required when %s is configured", name, name)
	}
	if len(c.Categories) == 0 {
		return fmt.Errorf("%s: at least one category is required when %s is configured", name, name)
	}
	for _, category := range c.Categories {
		if strings.TrimSpace(category) == "" {
			return fmt.Errorf("%s: categories must not contain an empty value", name)
		}
	}
	return nil
}

// overlappingCategory returns the first category present in both lists, or "".
// A category routed to two instances has no correct answer, so it is rejected
// at startup rather than resolved arbitrarily at lookup time.
func overlappingCategory(a, b []string) string {
	for _, category := range a {
		if slices.Contains(b, category) {
			return category
		}
	}
	return ""
}
