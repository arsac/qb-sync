package arr

import (
	"strings"
	"sync"
	"time"
)

// verdictKey identifies a cached decision. Hash is stored lowercased so callers
// can lookup with either case.
type verdictKey struct {
	instance string
	hash     string
}

// normalize lowercases the hash so the key is case-insensitive.
func (k verdictKey) normalize() verdictKey {
	return verdictKey{instance: k.instance, hash: strings.ToLower(k.hash)}
}

// verdictEntry wraps a Decision with its expiry timestamp.
type verdictEntry struct {
	decision Decision
	expiry   time.Time
}

// verdictCache is a TTL-based, self-expiring decision cache. It is safe for
// concurrent use and does not run a background goroutine — entries are
// considered stale on read.
type verdictCache struct {
	mu      sync.Mutex
	entries map[verdictKey]verdictEntry
	ttl     time.Duration
	now     func() time.Time // overridable for tests
}

func newVerdictCache(ttl time.Duration) *verdictCache {
	return &verdictCache{
		entries: make(map[verdictKey]verdictEntry),
		ttl:     ttl,
		now:     time.Now,
	}
}

// Get returns the decision if present and not expired.
func (c *verdictCache) Get(k verdictKey) (Decision, bool) {
	k = k.normalize()
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[k]
	if !ok {
		return Decision{}, false
	}
	if c.now().After(e.expiry) {
		delete(c.entries, k)
		return Decision{}, false
	}
	return e.decision, true
}

// Set stores a decision with TTL = c.ttl from now.
func (c *verdictCache) Set(k verdictKey, d Decision) {
	k = k.normalize()
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries[k] = verdictEntry{decision: d, expiry: c.now().Add(c.ttl)}
}
