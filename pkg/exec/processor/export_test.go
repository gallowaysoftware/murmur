package processor

import "time"

// SetCoalescerClock is a test-only hook that swaps the wall-clock used by
// Coalescer for synchronous time-tick-flush checks. Production callers use the
// real time.Now via NewCoalescer.
func SetCoalescerClock[V any](c *Coalescer[V], now func() time.Time) {
	c.withClock(now)
}
