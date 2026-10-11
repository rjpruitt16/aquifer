package aquifer

import (
	"context"
	"hash/fnv"
	"sync"

	"github.com/redis/go-redis/v9"
)

// WebSocketFollow tells a live session when its transcript may have entries
// it hasn't delivered. The session still reads and delivers them itself,
// from its own cursor, so replay and ordering are unchanged; the follow only
// decides when to read.
type WebSocketFollow interface {
	// C is signaled when entries may exist that the session hasn't read.
	// Spurious signals are possible; a missed entry is not.
	C() <-chan struct{}
	// Advance records that the session has delivered through cursor.
	Advance(cursor string)
	Close()
}

// streamFollower replaces one blocking XREAD per live session with Valkey
// pub/sub. Every append also publishes on the stream's notify channel (in
// the same pipeline), and each shard holds one pub/sub connection subscribed
// to the channels of the sessions it serves. Valkey pushes a message only
// when a stream actually gets an entry, so an idle session costs nothing and
// a busy one costs one pushed message per entry.
//
// Per-session blocking reads re-issued every AQUIFER_WS_READ_BLOCK_MS even
// when idle and held one Valkey connection each; those loops took half of a
// CPU at 5,000 sessions (benchmark.md §13). A single multi-stream XREAD per
// shard was tried first: it cut idle CPU but re-read every followed stream
// on each wake, which under load cost more than the per-session reads.
//
// Pub/sub is fire-and-forget, so correctness never depends on a message
// arriving. Whenever a subscription is confirmed (on first follow, and again
// whenever go-redis reconnects and resubscribes) the shard signals that
// stream's followers, and they re-read from their own cursors. Any entry
// recorded while a channel wasn't subscribed is picked up by that read.
type streamFollower struct {
	shards []*followShard
}

type followShard struct {
	ps *redis.PubSub

	mu   sync.Mutex
	subs map[string]map[*followSub]struct{} // notify channel -> followers
	done chan struct{}
}

type followSub struct {
	shard   *followShard
	channel string
	c       chan struct{}
	once    sync.Once
}

func newStreamFollower(client *redis.Client, shards int) *streamFollower {
	if shards <= 0 {
		shards = defaultWebSocketReaderShards
	}
	f := &streamFollower{}
	for i := 0; i < shards; i++ {
		s := &followShard{
			ps:   client.Subscribe(context.Background()),
			subs: make(map[string]map[*followSub]struct{}),
			done: make(chan struct{}),
		}
		f.shards = append(f.shards, s)
		go s.run()
	}
	return f
}

func (f *streamFollower) close() {
	for _, s := range f.shards {
		s.ps.Close()
		<-s.done
	}
}

func (f *streamFollower) follow(ctx context.Context, channel string) (*followSub, error) {
	h := fnv.New32a()
	h.Write([]byte(channel))
	shard := f.shards[h.Sum32()%uint32(len(f.shards))]
	sub := &followSub{shard: shard, channel: channel, c: make(chan struct{}, 1)}

	shard.mu.Lock()
	first := shard.subs[channel] == nil
	if first {
		shard.subs[channel] = make(map[*followSub]struct{})
	}
	shard.subs[channel][sub] = struct{}{}
	shard.mu.Unlock()

	if first {
		if err := shard.ps.Subscribe(ctx, channel); err != nil {
			sub.Close()
			return nil, err
		}
	} else {
		// Already subscribed: the confirmation that would have signaled this
		// follower happened before it joined, so signal it now.
		sub.signal()
	}
	return sub, nil
}

func (s *followSub) C() <-chan struct{} { return s.c }

// Advance is a no-op: notifications come from Valkey per entry, and the
// session reads from its own cursor, so the follower tracks no positions.
func (s *followSub) Advance(string) {}

func (s *followSub) Close() {
	s.once.Do(func() {
		sh := s.shard
		sh.mu.Lock()
		delete(sh.subs[s.channel], s)
		last := len(sh.subs[s.channel]) == 0
		if last {
			delete(sh.subs, s.channel)
		}
		sh.mu.Unlock()
		if last {
			sh.ps.Unsubscribe(context.Background(), s.channel)
		}
	})
}

func (s *followSub) signal() {
	select {
	case s.c <- struct{}{}:
	default: // already pending
	}
}

func (sh *followShard) run() {
	defer close(sh.done)
	for msg := range sh.ps.ChannelWithSubscriptions(redis.WithChannelSize(4096)) {
		switch m := msg.(type) {
		case *redis.Message:
			sh.signal(m.Channel)
		case *redis.Subscription:
			if m.Kind == "subscribe" {
				sh.signal(m.Channel) // first subscribe, or resubscribe after a reconnect
			}
		}
	}
}

func (sh *followShard) signal(channel string) {
	sh.mu.Lock()
	defer sh.mu.Unlock()
	for sub := range sh.subs[channel] {
		sub.signal()
	}
}
