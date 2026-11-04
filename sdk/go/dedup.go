package botsight

import (
	"sync"
	"time"
)

// dedup drops updates already counted: after a restart the bot asks Telegram for the same
// unconfirmed updates again, and webhooks are retried. Per bot it keeps the highest
// update_id and a bitmap of the 256 below it, like the anti-replay window of IPsec
// (RFC 4303, 3.4.3). A window untouched for a day starts anew: Telegram keeps updates at
// most 24 hours, and after a week of silence the next id is random.
type dedup struct {
	mu   sync.Mutex
	bots map[int64]*window
	now  func() time.Time
}

const windowSize = 256

type window struct {
	top  int64
	bits [windowSize / 64]uint64 // bit i: top-i was seen
	seen time.Time
}

func newDedup() *dedup { return &dedup{bots: map[int64]*window{}, now: time.Now} }

func (d *dedup) fresh(botID, id int64) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	now := d.now()
	w := d.bots[botID]
	if w == nil || now.Sub(w.seen) > 24*time.Hour {
		w = &window{top: id, seen: now}
		w.bits[0] = 1
		d.bots[botID] = w
		return true
	}
	w.seen = now
	switch diff := id - w.top; {
	case diff > 0:
		w.shift(diff)
		w.top = id
		w.bits[0] |= 1
		return true
	case -diff >= windowSize:
		return false
	default:
		i := uint(-diff)
		bit := uint64(1) << (i % 64)
		if w.bits[i/64]&bit != 0 {
			return false
		}
		w.bits[i/64] |= bit
		return true
	}
}

// shift moves the bitmap n positions towards older ids.
func (w *window) shift(n int64) {
	if n >= windowSize {
		w.bits = [windowSize / 64]uint64{}
		return
	}
	words, bits := int(n/64), uint(n%64)
	for i := len(w.bits) - 1; i >= 0; i-- {
		var v uint64
		if j := i - words; j >= 0 {
			v = w.bits[j] << bits
			if bits > 0 && j > 0 {
				v |= w.bits[j-1] >> (64 - bits)
			}
		}
		w.bits[i] = v
	}
}
