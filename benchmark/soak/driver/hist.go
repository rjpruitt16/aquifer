package main

import (
	"math"
	"sync"
	"time"
)

// hist is a log-bucketed latency histogram (5% wide buckets), cheap enough to
// record every request and merge per minute and per hour.
const histBuckets = 512

var histBase = math.Log(1.05)

type hist struct {
	mu sync.Mutex
	b  [histBuckets]uint64
	n  uint64
}

func bucketOf(d time.Duration) int {
	us := d.Microseconds()
	if us < 1 {
		us = 1
	}
	i := int(math.Log(float64(us)) / histBase)
	if i >= histBuckets {
		i = histBuckets - 1
	}
	return i
}

func (h *hist) add(d time.Duration) {
	i := bucketOf(d)
	h.mu.Lock()
	h.b[i]++
	h.n++
	h.mu.Unlock()
}

// take returns the counts so far and resets the histogram.
func (h *hist) take() histCounts {
	h.mu.Lock()
	defer h.mu.Unlock()
	c := histCounts{B: h.b, N: h.n}
	h.b = [histBuckets]uint64{}
	h.n = 0
	return c
}

type histCounts struct {
	B [histBuckets]uint64
	N uint64
}

func (c *histCounts) merge(o histCounts) {
	for i := range c.B {
		c.B[i] += o.B[i]
	}
	c.N += o.N
}

// ms returns the p-th percentile in milliseconds (bucket upper edge).
func (c histCounts) ms(p float64) float64 {
	if c.N == 0 {
		return 0
	}
	want := uint64(math.Ceil(float64(c.N) * p))
	var seen uint64
	for i, n := range c.B {
		seen += n
		if seen >= want {
			return math.Round(math.Exp(float64(i+1)*histBase)/10) / 100
		}
	}
	return 0
}
