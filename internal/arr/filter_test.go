package arr

import (
	"context"
	"testing"
)

func TestNoopFilterAlwaysSyncs(t *testing.T) {
	var f Filter = noopFilter{}
	d := f.ShouldSync(context.Background(), "abc123", "tv-sonarr")
	if !d.Sync {
		t.Fatalf("expected Sync=true, got %+v", d)
	}
	if d.Reason != ReasonNoCategory {
		t.Fatalf("expected Reason=%q, got %q", ReasonNoCategory, d.Reason)
	}
}
