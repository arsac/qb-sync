package arr

import (
	"encoding/json"
	"strings"
	"testing"
)

// The API key is redacted because config is logged at startup and dumped by the
// health endpoint, so an unredacted struct puts the key in both.
func TestInstanceConfigRedactsAPIKey(t *testing.T) {
	t.Parallel()

	c := InstanceConfig{URL: "http://radarr:7878", APIKey: "secret-12345", Categories: []string{"radarr"}}

	got := c.String()
	if strings.Contains(got, "secret-12345") {
		t.Errorf("String() leaks the API key: %q", got)
	}
	if !strings.Contains(got, "http://radarr:7878") {
		t.Errorf("String() should still show the URL: %q", got)
	}

	out, err := json.Marshal(c)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(string(out), "secret-12345") {
		t.Errorf("MarshalJSON leaks the API key: %s", out)
	}
}

// One rule set, applied wherever a Config comes from. Process config delegates
// here rather than keeping a copy, so these are the only rules there are.
func TestConfigValidate(t *testing.T) {
	t.Parallel()

	full := InstanceConfig{URL: "http://radarr:7878", APIKey: "k", Categories: []string{"radarr"}}

	tests := []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{name: "nothing configured is valid", cfg: Config{}},
		{name: "one complete instance", cfg: Config{Radarr: full}},
		{
			name: "both instances with distinct categories",
			cfg: Config{
				Radarr: full,
				Sonarr: InstanceConfig{URL: "http://sonarr:8989", APIKey: "k", Categories: []string{"tv"}},
			},
		},
		{
			name:    "url without key",
			cfg:     Config{Radarr: InstanceConfig{URL: "http://radarr:7878", Categories: []string{"radarr"}}},
			wantErr: true,
		},
		{
			name:    "key without url",
			cfg:     Config{Radarr: InstanceConfig{APIKey: "k", Categories: []string{"radarr"}}},
			wantErr: true,
		},
		{
			name:    "configured but no categories never routes anything",
			cfg:     Config{Radarr: InstanceConfig{URL: "http://radarr:7878", APIKey: "k"}},
			wantErr: true,
		},
		{
			name:    "blank category",
			cfg:     Config{Radarr: InstanceConfig{URL: "http://radarr:7878", APIKey: "k", Categories: []string{" "}}},
			wantErr: true,
		},
		{
			// No correct answer at lookup time, so it is rejected at startup.
			name: "same category routed to both",
			cfg: Config{
				Radarr: full,
				Sonarr: InstanceConfig{URL: "http://sonarr:8989", APIKey: "k", Categories: []string{"radarr"}},
			},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if err := tc.cfg.Validate(); (err != nil) != tc.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}
