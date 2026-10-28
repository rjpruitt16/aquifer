// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package aquifer

import (
	"net"
	"sync"
	"sync/atomic"

	"github.com/rjpruitt16/aquifer/internal/env"
)

// Every connection and every held stream uses a file descriptor, and so does
// every request Aquifer sends to a target API. A node holding one stream per
// waiting caller ran out of them under a long backlog: it could no longer
// accept connections or open new ones to the target API, so it stopped
// dispatching altogether. These limits keep a share of descriptors free.

// connectionLimit is how many client connections the HTTP adapter keeps
// open at once (AQUIFER_MAX_CONNECTIONS; default three quarters of the open
// file limit). Past it, new connections wait in the kernel's accept queue.
// 0 or less: no limit.
func connectionLimit() int {
	return int(env.Int64("AQUIFER_MAX_CONNECTIONS", int64(openFileLimit()*3/4)))
}

// heldStreamLimit is how many SSE streams Aquifer holds open for queued jobs
// at once (AQUIFER_MAX_HELD_STREAMS; default half the open file limit). Past
// it, a caller that would have been streamed gets a 202 telling it where
// and when to poll. 0 or less: no limit.
func heldStreamLimit() int {
	return int(env.Int64("AQUIFER_MAX_HELD_STREAMS", int64(openFileLimit()/2)))
}

// streamLimiter counts held streams against heldStreamLimit.
type streamLimiter struct {
	max int64
	n   atomic.Int64
}

func newStreamLimiter(max int) *streamLimiter {
	return &streamLimiter{max: int64(max)}
}

// acquire takes a stream slot, reporting false when none is free.
func (l *streamLimiter) acquire() bool {
	if l == nil || l.max <= 0 {
		return true
	}
	if l.n.Add(1) > l.max {
		l.n.Add(-1)
		return false
	}
	return true
}

func (l *streamLimiter) release() {
	if l == nil || l.max <= 0 {
		return
	}
	l.n.Add(-1)
}

// limitListener accepts at most cap(sem) connections at once; Accept waits
// for one to close before taking the next.
type limitListener struct {
	net.Listener
	sem       chan struct{}
	done      chan struct{}
	closeOnce sync.Once
}

// limitListen wraps l to hold at most max connections; max <= 0 returns l.
func limitListen(l net.Listener, max int) net.Listener {
	if max <= 0 {
		return l
	}
	return &limitListener{Listener: l, sem: make(chan struct{}, max), done: make(chan struct{})}
}

func (l *limitListener) Accept() (net.Conn, error) {
	select {
	case l.sem <- struct{}{}:
	case <-l.done:
		return nil, net.ErrClosed
	}
	c, err := l.Listener.Accept()
	if err != nil {
		<-l.sem
		return nil, err
	}
	return &limitConn{Conn: c, release: sync.OnceFunc(func() { <-l.sem })}, nil
}

func (l *limitListener) Close() error {
	l.closeOnce.Do(func() { close(l.done) })
	return l.Listener.Close()
}

type limitConn struct {
	net.Conn
	release func()
}

func (c *limitConn) Close() error {
	err := c.Conn.Close()
	c.release()
	return err
}
