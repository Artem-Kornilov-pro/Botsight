package botproxy

import (
	"sync"
	"time"
)

// Dedup drops updates already seen (Botsight.md, 20): after a restart a bot asks for the
// same unconfirmed updates again, and a webhook may be retried.
//
// update_id grows by one per update of a bot, but webhooks with max_connections > 1
// deliver them slightly out of order. So, like the anti-replay window of IPsec
// (RFC 4303, 3.4.3), it keeps per bot the highest id and a bitmap of the 256 ids below
// it: 40 bytes per bot instead of a set of ids.
//
// Two rules of Telegram bound the state in time: an update is kept at most 24 hours, so
// a duplicate is never older than that; and after a week without updates the next id is
// random and may be lower. A window not touched for Forget is therefore started anew.
type Dedup struct {
	mu     sync.Mutex
	bots   map[int64]*window
	Forget time.Duration
	now    func() time.Time
}

const windowSize = 256

type window struct {
	top  int64
	bits [windowSize / 64]uint64 // bit i: top-i was seen; bit 0 is top itself
	seen time.Time
}

func NewDedup() *Dedup {
	return &Dedup{bots: map[int64]*window{}, Forget: 24 * time.Hour, now: time.Now}
}

// Fresh reports whether the update is new and records it.
func (d *Dedup) Fresh(botID, updateID int64) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	now := d.now()
	w := d.bots[botID]
	if w == nil || now.Sub(w.seen) > d.Forget {
		w = &window{top: updateID}
		w.bits[0] = 1
		w.seen = now
		d.bots[botID] = w
		return true
	}
	w.seen = now
	switch diff := updateID - w.top; {
	case diff > 0: // newer than anything: slide the window
		w.shift(diff)
		w.top = updateID
		w.bits[0] |= 1
		return true
	case -diff >= windowSize: // too old to tell: within 24 hours this is a repeat
		return false
	default:
		i := uint(-diff)
		word, bit := i/64, uint64(1)<<(i%64)
		if w.bits[word]&bit != 0 {
			return false
		}
		w.bits[word] |= bit
		return true
	}
}

// shift moves the bitmap n positions towards older ids.
func (w *window) shift(n int64) {
	if n >= windowSize {
		w.bits = [windowSize / 64]uint64{}
		return
	}
	words, bitsN := int(n/64), uint(n%64)
	for i := len(w.bits) - 1; i >= 0; i-- {
		var v uint64
		if j := i - words; j >= 0 {
			v = w.bits[j] << bitsN
			if bitsN > 0 && j-1 >= 0 {
				v |= w.bits[j-1] >> (64 - bitsN)
			}
		}
		w.bits[i] = v
	}
}

// Bots is the number of bots with a window, for the metrics.
func (d *Dedup) Bots() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.bots)
}

// Sweep drops windows not touched for Forget, so stopped bots do not keep memory.
func (d *Dedup) Sweep() {
	d.mu.Lock()
	defer d.mu.Unlock()
	now := d.now()
	for id, w := range d.bots {
		if now.Sub(w.seen) > d.Forget {
			delete(d.bots, id)
		}
	}
}
