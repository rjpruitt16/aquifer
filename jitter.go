package aquifer

import (
	"math/rand"
	"time"
)

const defaultJitterRatio = 0.10

func withJitter(d time.Duration) time.Duration {
	if d <= 0 {
		return d
	}
	ceiling := time.Duration(float64(d) * defaultJitterRatio)
	if ceiling <= 0 {
		ceiling = time.Nanosecond
	}
	return d + time.Duration(rand.Int63n(int64(ceiling)+1))
}
