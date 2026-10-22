// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package aquifer

import (
	"math"
	"runtime"
	"strconv"
	"sync"
	"time"
)

// Load is how many HTTP users and WebSocket sessions this instance has, and
// how many of each it can hold, for whatever is in front of it (Canalis):
// reported in the registration ping and on every response. See
// REGISTRATION.md.
//
// Both kinds of traffic spend the same CPU, so they share one budget:
// whatever one isn't using, the other can have. The CPU in use is split
// between the two by what each costs (benchmark.md: about 750 jobs/s and
// 1,500 WebSocket messages/s per CPU), which gives the CPU one user or one
// session costs, and:
//
//	headroom      = 80% - CPU in use
//	http_capacity = users + headroom / CPU per user
//	ws_capacity   = least of: sessions + headroom / CPU per session,
//	                sessions + memory left under AQUIFER_MEMORY_LIMIT_MB / 90 KB,
//	                AQUIFER_WS_MAX_CLIENT_CONNECTIONS
//
// Neither is ever below what the instance already has. With nothing to
// measure yet, a user is taken to send 10 jobs/s and a session 1 message/s.
// Spillway reports the same four numbers the same way, with its own costs.
type Load struct {
	HTTPUsers    int64 `json:"http_users"`
	HTTPCapacity int64 `json:"http_capacity"`
	WSSessions   int64 `json:"ws_sessions"`
	WSCapacity   int64 `json:"ws_capacity"`
}

const (
	loadSampleInterval = 5 * time.Second
	loadTargetCPU      = 0.8
	// Weight of the newest reading in the running averages: about 30
	// seconds of memory at one reading every 5 seconds.
	loadSmoothing = 0.3

	// Measured per CPU: benchmark.md section 11 (jobs end to end, webhook
	// included) and section 13 (WebSocket round trips).
	loadJobsPerCPU       = 750
	loadWSMessagesPerCPU = 1500
	loadSessionMemoryKB  = 90

	// Assumed while there's no traffic to measure.
	loadAssumedJobsPerUser        = 10
	loadAssumedMessagesPerSession = 1

	loadMaxHTTPCapacity = 10_000
)

// loadReadings is what computeLoad works from. CPU is the share of the
// instance's CPU in use (0-1); rates are per second.
type loadReadings struct {
	CPU           float64
	CPUs          int
	JobRate       float64
	MessageRate   float64
	HTTPUsers     int64
	WSSessions    int64
	MaxSessions   int64
	MemoryLimitMB int64
	MemoryMB      int64
}

func computeLoad(r loadReadings) Load {
	cpus := float64(max(r.CPUs, 1))
	httpNeed := r.JobRate / (loadJobsPerCPU * cpus)
	wsNeed := r.MessageRate / (loadWSMessagesPerCPU * cpus)
	var httpCPU, wsCPU float64
	if need := httpNeed + wsNeed; need > 0 {
		httpCPU, wsCPU = r.CPU*httpNeed/need, r.CPU*wsNeed/need
	}
	headroom := math.Max(loadTargetCPU-r.CPU, 0)

	perUser := loadCost(httpCPU, r.HTTPUsers, loadAssumedJobsPerUser/(loadJobsPerCPU*cpus))
	perSession := loadCost(wsCPU, r.WSSessions, loadAssumedMessagesPerSession/(loadWSMessagesPerCPU*cpus))

	httpCapacity := min(r.HTTPUsers+int64(math.Floor(headroom/perUser)), loadMaxHTTPCapacity)
	wsCapacity := min(r.WSSessions+int64(math.Floor(headroom/perSession)), r.MaxSessions)
	if r.MemoryLimitMB > 0 {
		free := max(r.MemoryLimitMB-r.MemoryMB, 0)
		wsCapacity = min(wsCapacity, r.WSSessions+free*1024/loadSessionMemoryKB)
	}
	return Load{
		HTTPUsers:    r.HTTPUsers,
		HTTPCapacity: max(httpCapacity, r.HTTPUsers),
		WSSessions:   r.WSSessions,
		WSCapacity:   max(wsCapacity, r.WSSessions),
	}
}

// loadCost is the CPU one user or session costs: measured when there are
// some and they used any, assumed otherwise.
func loadCost(cpu float64, count int64, assumed float64) float64 {
	if count > 0 && cpu > 0 {
		return cpu / float64(count)
	}
	return assumed
}

// loadSampler takes a new reading when the last one is older than
// loadSampleInterval, on whichever call finds it stale, so it needs no
// goroutine of its own.
type loadSampler struct {
	mu       sync.Mutex
	at       time.Time
	cpuTime  time.Duration
	jobs     int64
	messages int64
	averages *loadAverages
	last     Load
}

type loadAverages struct{ cpu, jobRate, messageRate float64 }

// Load is this instance's current load (see Load).
func (a *Aquifer) Load() Load {
	s := &a.loadSampler
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	if !s.at.IsZero() && now.Sub(s.at) < loadSampleInterval {
		return s.last
	}

	cpuTime := processCPUTime()
	jobs := a.registry.acceptedJobs()
	messages := a.webSockets.Messages()
	reading := loadAverages{}
	if !s.at.IsZero() {
		seconds := now.Sub(s.at).Seconds()
		cpus := runtime.GOMAXPROCS(0)
		reading = loadAverages{
			cpu:         min((cpuTime-s.cpuTime).Seconds()/(seconds*float64(cpus)), 1),
			jobRate:     float64(jobs-s.jobs) / seconds,
			messageRate: float64(messages-s.messages) / seconds,
		}
	}
	if s.averages == nil {
		s.averages = &reading
	} else {
		s.averages.cpu += loadSmoothing * (reading.cpu - s.averages.cpu)
		s.averages.jobRate += loadSmoothing * (reading.jobRate - s.averages.jobRate)
		s.averages.messageRate += loadSmoothing * (reading.messageRate - s.averages.messageRate)
	}

	sessions, maxSessions := a.webSockets.Sessions()
	memoryMB, memoryLimitMB := a.admission.Memory()
	s.last = computeLoad(loadReadings{
		CPU:           s.averages.cpu,
		CPUs:          runtime.GOMAXPROCS(0),
		JobRate:       s.averages.jobRate,
		MessageRate:   s.averages.messageRate,
		HTTPUsers:     a.registry.activeUsers(),
		WSSessions:    int64(sessions),
		MaxSessions:   int64(maxSessions),
		MemoryLimitMB: memoryLimitMB,
		MemoryMB:      memoryMB,
	})
	s.at, s.cpuTime, s.jobs, s.messages = now, cpuTime, jobs, messages
	return s.last
}

// setLoadHeaders puts Load on a response, next to X-Aqueduct-Fullness.
func setLoadHeaders(h interface{ Set(string, string) }, load Load) {
	h.Set("X-Aqueduct-Http-Users", strconv.FormatInt(load.HTTPUsers, 10))
	h.Set("X-Aqueduct-Http-Capacity", strconv.FormatInt(load.HTTPCapacity, 10))
	h.Set("X-Aqueduct-WS-Sessions", strconv.FormatInt(load.WSSessions, 10))
	h.Set("X-Aqueduct-WS-Capacity", strconv.FormatInt(load.WSCapacity, 10))
}
