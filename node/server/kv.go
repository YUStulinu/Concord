package server

import (
	"encoding/json"
	"sync"
)

// Command is one client operation, as stored in the Raft log.
type Command struct {
	Op     string  `json:"op"` // put, get, delete, cas
	Key    string  `json:"key"`
	Value  string  `json:"value,omitempty"`
	Expect *string `json:"expect,omitempty"` // cas: the value expected (null: the key must be absent)
	Client uint64  `json:"client,omitempty"`
	Seq    uint64  `json:"seq,omitempty"`
}

type Result struct {
	OK    bool   `json:"ok"`
	Value string `json:"value,omitempty"`
	Found bool   `json:"found"`
}

type session struct {
	Seq    uint64
	Result Result
}

// KV is the replicated state machine: a map, plus each client's last request and its result,
// so a request retried after a timeout is applied once (exactly-once with the client's help).
type KV struct {
	mu       sync.RWMutex
	data     map[string]string
	sessions map[uint64]session
}

func NewKV() *KV { return &KV{data: map[string]string{}, sessions: map[uint64]session{}} }

func (kv *KV) Apply(c Command) Result {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if c.Client != 0 {
		if s, ok := kv.sessions[c.Client]; ok && c.Seq <= s.Seq {
			return s.Result // a retry of what was already applied
		}
	}
	var r Result
	switch c.Op {
	case "put":
		kv.data[c.Key] = c.Value
		r = Result{OK: true}
	case "delete":
		_, r.Found = kv.data[c.Key]
		delete(kv.data, c.Key)
		r.OK = true
	case "cas":
		cur, found := kv.data[c.Key]
		match := (c.Expect == nil && !found) || (c.Expect != nil && found && *c.Expect == cur)
		if match {
			kv.data[c.Key] = c.Value
		}
		r = Result{OK: match, Value: cur, Found: found}
	case "get":
		r.Value, r.Found = kv.data[c.Key]
		r.OK = true
	}
	if c.Client != 0 {
		kv.sessions[c.Client] = session{Seq: c.Seq, Result: r}
	}
	return r
}

func (kv *KV) Get(key string) (string, bool) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	v, ok := kv.data[key]
	return v, ok
}

func (kv *KV) Len() int {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	return len(kv.data)
}

type kvSnapshot struct {
	Data     map[string]string  `json:"data"`
	Sessions map[uint64]session `json:"sessions"`
}

func (kv *KV) Snapshot() []byte {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	b, _ := json.Marshal(kvSnapshot{Data: kv.data, Sessions: kv.sessions})
	return b
}

func (kv *KV) Restore(b []byte) error {
	var s kvSnapshot
	if len(b) > 0 {
		if err := json.Unmarshal(b, &s); err != nil {
			return err
		}
	}
	if s.Data == nil {
		s.Data = map[string]string{}
	}
	if s.Sessions == nil {
		s.Sessions = map[uint64]session{}
	}
	kv.mu.Lock()
	kv.data, kv.sessions = s.Data, s.Sessions
	kv.mu.Unlock()
	return nil
}
