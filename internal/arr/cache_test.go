package arr

import (
	"testing"
	"time"
)

func TestVerdictCacheHitAndMiss(t *testing.T) {
	c := newVerdictCache(50 * time.Millisecond)
	k := verdictKey{instance: "radarr", hash: "abc"}

	if _, ok := c.Get(k); ok {
		t.Fatalf("expected miss on empty cache")
	}

	c.Set(k, Decision{Sync: false, Reason: ReasonIgnored})
	got, ok := c.Get(k)
	if !ok {
		t.Fatalf("expected hit after Set")
	}
	if got.Sync || got.Reason != ReasonIgnored {
		t.Fatalf("unexpected decision %+v", got)
	}
}

func TestVerdictCacheTTLExpiry(t *testing.T) {
	c := newVerdictCache(10 * time.Millisecond)
	k := verdictKey{instance: "sonarr", hash: "xyz"}
	c.Set(k, Decision{Sync: true, Reason: ReasonNotRejected})

	time.Sleep(20 * time.Millisecond)

	if _, ok := c.Get(k); ok {
		t.Fatalf("expected miss after TTL expiry")
	}
}

func TestVerdictCacheKeyIsCaseInsensitiveOnHash(t *testing.T) {
	c := newVerdictCache(time.Second)
	c.Set(verdictKey{instance: "radarr", hash: "ABC"}, Decision{Sync: false, Reason: ReasonFailed})
	if _, ok := c.Get(verdictKey{instance: "radarr", hash: "abc"}); !ok {
		t.Fatalf("expected case-insensitive hash match")
	}
}
