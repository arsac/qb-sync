package config

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestArrInstanceConfigStringRedactsAPIKey(t *testing.T) {
	c := ArrInstanceConfig{URL: "http://radarr:7878", APIKey: "secret-12345", Categories: []string{"radarr"}}
	got := c.String()
	if strings.Contains(got, "secret-12345") {
		t.Fatalf("String() leaks API key: %q", got)
	}
	if !strings.Contains(got, "http://radarr:7878") {
		t.Fatalf("String() should include URL: %q", got)
	}
}

func TestArrInstanceConfigMarshalJSONRedactsAPIKey(t *testing.T) {
	c := ArrInstanceConfig{URL: "http://radarr:7878", APIKey: "secret-12345", Categories: []string{"radarr"}}
	out, err := json.Marshal(c)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(string(out), "secret-12345") {
		t.Fatalf("MarshalJSON leaks API key: %s", string(out))
	}
}
