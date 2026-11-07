// Package raft is the Raft consensus algorithm (Ongaro and Ousterhout, 2014, and Ongaro's
// dissertation), written as a deterministic state machine: it takes messages, ticks and
// proposals, and hands back a Ready -- what to persist, what to send, what has committed. The
// caller does the I/O. Nothing here reads a clock or touches the network, so tests can drive
// clusters through any schedule of failures.
//
// Included: leader election with pre-vote and check-quorum (a partitioned node cannot disrupt
// a healthy cluster when it returns), log replication with fast backtracking, commitment only
// of current-term entries, ReadIndex for linearizable reads without writing to the log,
// snapshots with InstallSnapshot, and membership changes one server at a time.
package raft

import (
	"encoding/binary"
	"errors"
	"fmt"
)

type EntryType uint8

const (
	EntryNormal EntryType = iota
	EntryConfChange
)

type Entry struct {
	Term  uint64
	Index uint64
	Type  EntryType
	Data  []byte
}

type HardState struct {
	Term   uint64
	Vote   uint64
	Commit uint64
}

func (h HardState) IsEmpty() bool { return h == HardState{} }

// Member is a server of the cluster: its Raft address and the address clients use.
type Member struct {
	ID   uint64
	Raft string
	HTTP string
}

// Config is the cluster's membership.
type Config struct {
	Members []Member
}

func (c Config) Has(id uint64) bool {
	for _, m := range c.Members {
		if m.ID == id {
			return true
		}
	}
	return false
}

func (c Config) Get(id uint64) (Member, bool) {
	for _, m := range c.Members {
		if m.ID == id {
			return m, true
		}
	}
	return Member{}, false
}

func (c Config) Quorum() int { return len(c.Members)/2 + 1 }

func (c Config) clone() Config {
	return Config{Members: append([]Member(nil), c.Members...)}
}

type ConfChangeOp uint8

const (
	AddMember ConfChangeOp = iota + 1
	RemoveMember
)

type ConfChange struct {
	Op     ConfChangeOp
	Member Member
}

func (cc ConfChange) Encode() []byte {
	b := []byte{byte(cc.Op)}
	b = binary.AppendUvarint(b, cc.Member.ID)
	b = binary.AppendUvarint(b, uint64(len(cc.Member.Raft)))
	b = append(b, cc.Member.Raft...)
	b = binary.AppendUvarint(b, uint64(len(cc.Member.HTTP)))
	return append(b, cc.Member.HTTP...)
}

func DecodeConfChange(b []byte) (ConfChange, error) {
	bad := errors.New("bad conf change")
	if len(b) < 2 {
		return ConfChange{}, bad
	}
	cc := ConfChange{Op: ConfChangeOp(b[0])}
	b = b[1:]
	id, n := binary.Uvarint(b)
	if n <= 0 {
		return cc, bad
	}
	b = b[n:]
	str := func() (string, bool) {
		l, n := binary.Uvarint(b)
		if n <= 0 || uint64(len(b)-n) < l {
			return "", false
		}
		s := string(b[n : n+int(l)])
		b = b[n+int(l):]
		return s, true
	}
	r, ok1 := str()
	h, ok2 := str()
	if !ok1 || !ok2 {
		return cc, bad
	}
	cc.Member = Member{ID: id, Raft: r, HTTP: h}
	return cc, nil
}

func (c Config) apply(cc ConfChange) Config {
	out := Config{}
	for _, m := range c.Members {
		if m.ID != cc.Member.ID {
			out.Members = append(out.Members, m)
		}
	}
	if cc.Op == AddMember {
		out.Members = append(out.Members, cc.Member)
	}
	return out
}

type Snapshot struct {
	Index  uint64
	Term   uint64
	Config Config
	Data   []byte
}

type MsgType uint8

const (
	MsgPreVote MsgType = iota + 1
	MsgPreVoteResp
	MsgVote
	MsgVoteResp
	MsgApp
	MsgAppResp
	MsgHeartbeat
	MsgHeartbeatResp
	MsgSnap
	MsgTimeoutNow
)

func (t MsgType) String() string {
	names := [...]string{"", "PreVote", "PreVoteResp", "Vote", "VoteResp", "App", "AppResp", "Heartbeat", "HeartbeatResp", "Snap", "TimeoutNow"}
	if int(t) < len(names) {
		return names[t]
	}
	return fmt.Sprintf("Msg(%d)", t)
}

type Message struct {
	Type    MsgType
	From    uint64
	To      uint64
	Term    uint64
	Index   uint64 // App: index before the entries; AppResp: last index matched (or hint when rejected)
	LogTerm uint64 // App: term at Index; votes: candidate's last log term
	Commit  uint64
	Entries []Entry
	Reject  bool
	Hint    uint64 // AppResp reject: where the follower's log may start to differ
	Snap    *Snapshot
	Ctx     uint64 // ReadIndex request carried on heartbeats
}

// ReadState says that a read requested with Ctx may be served once Index is applied.
type ReadState struct {
	Ctx   uint64
	Index uint64
}

// Ready is what the caller must do, in order: persist HardState, Snapshot and Entries (with
// fsync), then send Messages, then apply Committed and serve Reads whose index is applied.
type Ready struct {
	HardState HardState
	Snapshot  *Snapshot
	Entries   []Entry
	Messages  []Message
	Committed []Entry
	Reads     []ReadState
	// Ctx values of reads that could not be served (not leader, lost leadership).
	FailedReads []uint64
}

func (rd Ready) Empty() bool {
	return rd.HardState.IsEmpty() && rd.Snapshot == nil && len(rd.Entries) == 0 && len(rd.Messages) == 0 &&
		len(rd.Committed) == 0 && len(rd.Reads) == 0 && len(rd.FailedReads) == 0
}

var (
	ErrNotLeader       = errors.New("not the leader")
	ErrConfChangeAhead = errors.New("a membership change is already in progress")
)
