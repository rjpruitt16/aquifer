// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

type ValkeyStateStore struct {
	client *redis.Client
	prefix string
}

func NewValkeyStateStore(rawURL, prefix string) (*ValkeyStateStore, error) {
	if rawURL == "" {
		return nil, errors.New("AQUIFER_CLUSTER_VALKEY_URL or AQUIFER_VALKEY_URL is required for the Valkey cluster provider")
	}
	parsed, err := NormalizeValkeyURL(rawURL)
	if err != nil {
		return nil, err
	}
	opts, err := redis.ParseURL(parsed)
	if err != nil {
		return nil, fmt.Errorf("parse cluster Valkey URL: %w", err)
	}
	if prefix == "" {
		prefix = defaultClusterValkeyPrefix
	}
	if !strings.HasSuffix(prefix, ":") {
		prefix += ":"
	}
	return &ValkeyStateStore{client: redis.NewClient(opts), prefix: prefix}, nil
}

func NormalizeValkeyURL(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("parse cluster Valkey URL: %w", err)
	}
	switch strings.ToLower(u.Scheme) {
	case "redis", "rediss":
	case "valkey":
		u.Scheme = "redis"
	case "valkeys":
		u.Scheme = "rediss"
	default:
		return "", fmt.Errorf("unsupported cluster Valkey URL scheme %q", u.Scheme)
	}
	return u.String(), nil
}

func (s *ValkeyStateStore) Heartbeat(ctx context.Context, member Member, ttl time.Duration) error {
	payload, err := json.Marshal(member)
	if err != nil {
		return err
	}
	pipe := s.client.TxPipeline()
	pipe.SAdd(ctx, s.instancesKey(), member.ID)
	pipe.Set(ctx, s.instanceKey(member.ID), payload, ttl)
	_, err = pipe.Exec(ctx)
	return err
}

func (s *ValkeyStateStore) Members(ctx context.Context) ([]Member, error) {
	ids, err := s.client.SMembers(ctx, s.instancesKey()).Result()
	if err != nil || len(ids) == 0 {
		return nil, err
	}
	keys := make([]string, len(ids))
	for i, id := range ids {
		keys[i] = s.instanceKey(id)
	}
	values, err := s.client.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, err
	}
	members := make([]Member, 0, len(values))
	var stale []any
	for i, value := range values {
		if value == nil {
			stale = append(stale, ids[i])
			continue
		}
		var member Member
		var payload []byte
		switch value := value.(type) {
		case string:
			payload = []byte(value)
		case []byte:
			payload = value
		default:
			continue
		}
		if err := json.Unmarshal(payload, &member); err != nil {
			stale = append(stale, ids[i])
			continue
		}
		if member.ID == "" || member.Address == "" {
			stale = append(stale, ids[i])
			continue
		}
		members = append(members, member)
	}
	if len(stale) > 0 {
		pipe := s.client.TxPipeline()
		pipe.SRem(ctx, s.instancesKey(), stale...)
		for _, id := range stale {
			pipe.Del(ctx, s.instanceAssignmentsKey(id.(string)))
		}
		_, _ = pipe.Exec(ctx)
	}
	return members, nil
}

// assignClusterScopeScript claims a user for an instance. Canalis runs this
// same script against the same keys (canalis-rs src/assign.rs), and
// aqueduct-runner's test-claim-script fails CI if the two copies drift.
// One optional trailing argument after the candidate pairs, "1", makes it
// strict: when no candidate is under capacity, return nothing rather than
// the least-loaded one. Canalis passes it, because it has its own line to
// queue a user in; Aquifer doesn't, because it has nowhere else to put the
// work, so for Aquifer nothing changes.
var assignClusterScopeScript = redis.NewScript(`
local assignment = redis.call('GET', KEYS[1])
local clock = redis.call('TIME')
local now = clock[1] * 1000 + math.floor(clock[2] / 1000)
local ttl = tonumber(ARGV[1])
local scope = ARGV[2]
local count = tonumber(ARGV[3])
local prefix = ARGV[4]

local function active_member(payload)
  if not payload then return nil end
  local ok, member = pcall(cjson.decode, payload)
  if ok and member['state'] ~= 'draining' and member['state'] ~= 'offline' then
    return member
  end
  return nil
end

if assignment then
  local payload = redis.call('GET', prefix .. 'instance:' .. assignment)
  if active_member(payload) then
    redis.call('PEXPIRE', KEYS[1], ttl)
    redis.call('ZADD', prefix .. 'instance:' .. assignment .. ':assignments', now + ttl, scope)
    return payload
  end
  redis.call('DEL', KEYS[1])
  redis.call('ZREM', prefix .. 'instance:' .. assignment .. ':assignments', scope)
end

local selected = 0
local least = 0
local least_ratio = nil
for i = 1, count do
  local payload = redis.call('GET', KEYS[2 + (i - 1) * 2])
  local member = active_member(payload)
  if member then
    local assignments = KEYS[3 + (i - 1) * 2]
    redis.call('ZREMRANGEBYSCORE', assignments, '-inf', now)
    local used = redis.call('ZCARD', assignments)
    local reported = tonumber(member['active_users']) or 0
    if reported > used then used = reported end
    local capacity = tonumber(member['user_capacity']) or tonumber(ARGV[4 + (i - 1) * 2 + 2])
    if capacity < 1 then capacity = 1 end
    if selected == 0 and used < capacity then selected = i end
    local ratio = used / capacity
    if least == 0 or ratio < least_ratio then
      least = i
      least_ratio = ratio
    end
  end
end
if selected == 0 and ARGV[5 + count * 2] ~= '1' then selected = least end
if selected == 0 then return '' end

local id = ARGV[4 + (selected - 1) * 2 + 1]
redis.call('SET', KEYS[1], id, 'PX', ttl)
redis.call('ZADD', KEYS[3 + (selected - 1) * 2], now + ttl, scope)
return redis.call('GET', KEYS[2 + (selected - 1) * 2])
`)

func (s *ValkeyStateStore) Assign(ctx context.Context, scope string, candidates []Member, ttl time.Duration) (Member, error) {
	if len(candidates) == 0 {
		return Member{}, nil
	}
	hashed := hashKey(scope)
	keys := make([]string, 1, 1+len(candidates)*2)
	keys[0] = s.assignmentKey(hashed)
	args := make([]any, 0, 4+len(candidates)*2)
	args = append(args, ttl.Milliseconds(), hashed, len(candidates), s.prefix)
	for _, candidate := range candidates {
		keys = append(keys, s.instanceKey(candidate.ID), s.instanceAssignmentsKey(candidate.ID))
		capacity := candidate.Capacity
		if capacity <= 0 {
			capacity = defaultClusterMaxActiveUsers
		}
		args = append(args, candidate.ID, capacity)
	}
	payload, err := assignClusterScopeScript.Run(ctx, s.client, keys, args...).Text()
	if err != nil {
		return Member{}, err
	}
	if payload == "" {
		return Member{}, nil
	}
	var member Member
	if err := json.Unmarshal([]byte(payload), &member); err != nil {
		return Member{}, fmt.Errorf("decode assigned cluster member: %w", err)
	}
	return member, nil
}

const renewClusterScopeLua = `
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
local clock = redis.call('TIME')
local now = clock[1] * 1000 + math.floor(clock[2] / 1000)
redis.call('PEXPIRE', KEYS[1], ARGV[2])
redis.call('ZADD', KEYS[2], now + ARGV[2], ARGV[3])
return 1
`

func (s *ValkeyStateStore) Renew(ctx context.Context, scopes []string, nodeID string, ttl time.Duration) error {
	if len(scopes) == 0 {
		return nil
	}
	_, err := s.client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for _, scope := range scopes {
			hashed := hashKey(scope)
			pipe.Eval(ctx, renewClusterScopeLua,
				[]string{s.assignmentKey(hashed), s.instanceAssignmentsKey(nodeID)},
				nodeID, ttl.Milliseconds(), hashed,
			)
		}
		return nil
	})
	return err
}

var releaseClusterScopeScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
redis.call('DEL', KEYS[1])
redis.call('ZREM', KEYS[2], ARGV[2])
return 1
`)

func (s *ValkeyStateStore) Release(ctx context.Context, scope, nodeID string) error {
	hashed := hashKey(scope)
	return releaseClusterScopeScript.Run(ctx, s.client,
		[]string{s.assignmentKey(hashed), s.instanceAssignmentsKey(nodeID)},
		nodeID, hashed,
	).Err()
}

func (s *ValkeyStateStore) RemoveInstance(ctx context.Context, nodeID string) error {
	pipe := s.client.TxPipeline()
	pipe.Del(ctx, s.instanceKey(nodeID), s.instanceAssignmentsKey(nodeID))
	pipe.SRem(ctx, s.instancesKey(), nodeID)
	_, err := pipe.Exec(ctx)
	return err
}

func (s *ValkeyStateStore) Close() error { return s.client.Close() }

func (s *ValkeyStateStore) instancesKey() string         { return s.prefix + "instances" }
func (s *ValkeyStateStore) instanceKey(id string) string { return s.prefix + "instance:" + id }
func (s *ValkeyStateStore) instanceAssignmentsKey(id string) string {
	return s.prefix + "instance:" + id + ":assignments"
}
func (s *ValkeyStateStore) assignmentKey(scopeHash string) string {
	return s.prefix + "assignment:" + scopeHash
}

// hashKey keeps scope names out of Valkey keys, the same hash the job
// store uses for idempotency keys.
func hashKey(key string) string {
	h := sha256.Sum256([]byte(key))
	return hex.EncodeToString(h[:])
}
