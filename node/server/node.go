package server

import (
	"encoding/json"
	"errors"
	"log"
	"sync/atomic"
	"time"

	"github.com/YUStulinu/Concord/node/raft"
)

var (
	ErrNotLeader = errors.New("not the leader")
	ErrLost      = errors.New("the entry was replaced by another leader's; it did not take effect")
	ErrUnknown   = errors.New("the outcome is unknown")
	ErrStopped   = errors.New("node stopped")
)

type Config struct {
	ID            uint64
	RaftAddr      string
	DataDir       string
	Bootstrap     []raft.Member // empty when joining a cluster
	Tick          time.Duration
	ElectionTick  int
	SnapshotEvery uint64 // compact the log after this many applied entries
	Logger        *log.Logger
}

type proposal struct {
	data []byte
	conf *raft.ConfChange
	done chan outcome
}

type outcome struct {
	res Result
	err error
}

type readRequest struct {
	done chan error
}

type waiter struct {
	term uint64
	done chan outcome
}

// Node runs one Raft server: a single goroutine owns the Raft state machine and does its I/O
// in the order Ready requires -- persist (one fsync for the batch), send, apply.
type Node struct {
	cfg   Config
	r     *raft.Raft
	st    *Storage
	tr    *Transport
	KV    *KV
	recvc chan raft.Message
	propc chan proposal
	readc chan readRequest
	statc chan chan raft.Status
	skewc chan float64
	stopc chan struct{}
	done  chan struct{}

	waiters  map[uint64]waiter
	reads    map[uint64]readRequest
	ready    []pendingRead // confirmed, waiting for apply
	readCtx  uint64
	applied  uint64
	lastSnap uint64

	members atomic.Pointer[[]raft.Member]
	leader  atomic.Uint64
	role    atomic.Int32
	term    atomic.Uint64

	Applied, Elections atomic.Int64
}

type pendingRead struct {
	index uint64
	done  chan error
}

func Start(cfg Config) (*Node, error) {
	if cfg.Tick == 0 {
		cfg.Tick = 10 * time.Millisecond
	}
	if cfg.ElectionTick == 0 {
		cfg.ElectionTick = 15
	}
	if cfg.SnapshotEvery == 0 {
		cfg.SnapshotEvery = 10000
	}
	st, state, err := OpenStorage(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	n := &Node{cfg: cfg, st: st, KV: NewKV(), recvc: make(chan raft.Message, 8192), propc: make(chan proposal, 4096),
		readc: make(chan readRequest, 4096), statc: make(chan chan raft.Status), skewc: make(chan float64, 1),
		stopc: make(chan struct{}), done: make(chan struct{}), waiters: map[uint64]waiter{}, reads: map[uint64]readRequest{}}
	if state.Snapshot != nil {
		if err := n.KV.Restore(state.Snapshot.Data); err != nil {
			return nil, err
		}
		n.applied, n.lastSnap = state.Snapshot.Index, state.Snapshot.Index
	}
	rng := newRand(cfg.ID)
	n.r = raft.New(raft.Options{ID: cfg.ID, ElectionTicks: cfg.ElectionTick, HeartbeatTicks: 2, MaxBatch: 1024,
		PreVote: true, CheckQuorum: true, Rand: rng.Intn, Bootstrap: cfg.Bootstrap}, state)
	tr, err := NewTransport(cfg.ID, cfg.RaftAddr, func(m raft.Message) {
		select {
		case n.recvc <- m:
		default: // overloaded: drop, Raft will retry
		}
	})
	if err != nil {
		st.Close()
		return nil, err
	}
	n.tr = tr
	n.publish()
	go n.run()
	return n, nil
}

func (n *Node) logf(format string, args ...any) {
	if n.cfg.Logger != nil {
		n.cfg.Logger.Printf(format, args...)
	}
}

func (n *Node) publish() {
	c := n.r.Config()
	n.members.Store(&c.Members)
	n.tr.SetPeers(c.Members)
	n.leader.Store(n.r.Leader())
	n.role.Store(int32(n.r.Role()))
	n.term.Store(n.r.Term())
}

func (n *Node) run() {
	defer close(n.done)
	tick := n.cfg.Tick
	ticker := time.NewTicker(tick)
	defer ticker.Stop()
	prevLeader := n.r.Leader()
	for {
		select {
		case <-n.stopc:
			n.failAll()
			return
		case <-ticker.C:
			n.r.Tick()
		case f := <-n.skewc:
			// A skewed clock: this node's ticks run faster or slower than real time.
			ticker.Reset(time.Duration(float64(tick) / f))
		case m := <-n.recvc:
			n.r.Step(m)
		case p := <-n.propc:
			n.propose(p)
		case q := <-n.readc:
			n.read(q)
		case c := <-n.statc:
			s := n.r.Status(40)
			c <- s
			continue
		}
		// Take whatever else is waiting, so one fsync covers a batch.
	drain:
		for i := 0; i < 1024; i++ {
			select {
			case m := <-n.recvc:
				n.r.Step(m)
			case p := <-n.propc:
				n.propose(p)
			case q := <-n.readc:
				n.read(q)
			default:
				break drain
			}
		}
		if err := n.handleReady(); err != nil {
			n.logf("fatal: %v", err)
			n.failAll()
			return
		}
		if l := n.r.Leader(); l != prevLeader {
			if l == n.cfg.ID {
				n.Elections.Add(1)
				n.logf("leader in term %d", n.r.Term())
			}
			prevLeader = l
		}
		n.leader.Store(n.r.Leader())
		n.role.Store(int32(n.r.Role()))
		n.term.Store(n.r.Term())
	}
}

func (n *Node) propose(p proposal) {
	var idx, term uint64
	var err error
	if p.conf != nil {
		idx, term, err = n.r.ProposeConfChange(*p.conf)
	} else {
		idx, term, err = n.r.Propose(p.data)
	}
	if err != nil {
		p.done <- outcome{err: err}
		return
	}
	n.waiters[idx] = waiter{term: term, done: p.done}
}

func (n *Node) read(q readRequest) {
	n.readCtx++
	n.reads[n.readCtx] = q
	n.r.ReadIndex(n.readCtx)
}

func (n *Node) handleReady() error {
	rd := n.r.Ready()
	if rd.Empty() {
		return nil
	}
	if err := n.st.Save(rd); err != nil {
		return err
	}
	if rd.Snapshot != nil {
		if err := n.KV.Restore(rd.Snapshot.Data); err != nil {
			return err
		}
		n.applied, n.lastSnap = rd.Snapshot.Index, rd.Snapshot.Index
		for idx, w := range n.waiters {
			if idx <= rd.Snapshot.Index {
				w.done <- outcome{err: ErrUnknown}
				delete(n.waiters, idx)
			}
		}
		n.publish()
	}
	n.tr.Send(rd.Messages)
	for _, e := range rd.Committed {
		var res Result
		switch e.Type {
		case raft.EntryNormal:
			if len(e.Data) > 0 {
				var c Command
				if err := json.Unmarshal(e.Data, &c); err == nil {
					res = n.KV.Apply(c)
				}
			}
		case raft.EntryConfChange:
			n.publish()
			res = Result{OK: true}
		}
		n.applied = e.Index
		n.Applied.Add(1)
		if w, ok := n.waiters[e.Index]; ok {
			if w.term == e.Term {
				w.done <- outcome{res: res}
			} else {
				w.done <- outcome{err: ErrLost}
			}
			delete(n.waiters, e.Index)
		}
	}
	for _, rs := range rd.Reads {
		if q, ok := n.reads[rs.Ctx]; ok {
			delete(n.reads, rs.Ctx)
			n.ready = append(n.ready, pendingRead{index: rs.Index, done: q.done})
		}
	}
	for _, ctx := range rd.FailedReads {
		if q, ok := n.reads[ctx]; ok {
			delete(n.reads, ctx)
			q.done <- ErrNotLeader
		}
	}
	keep := n.ready[:0]
	for _, p := range n.ready {
		if p.index <= n.applied {
			p.done <- nil
		} else {
			keep = append(keep, p)
		}
	}
	n.ready = keep
	// Lost leadership: proposals waiting here may never be decided by this node.
	if n.r.Role() != raft.Leader {
		for idx, w := range n.waiters {
			if idx > n.applied {
				w.done <- outcome{err: ErrUnknown}
				delete(n.waiters, idx)
			}
		}
	}
	if n.applied-n.lastSnap >= n.cfg.SnapshotEvery {
		if s := n.r.Compact(n.applied, n.KV.Snapshot()); s != nil {
			if err := n.st.SaveSnapshot(s); err != nil {
				return err
			}
			n.lastSnap = n.applied
		}
	}
	if len(rd.Committed) > 0 {
		n.publish()
	}
	return nil
}

func (n *Node) failAll() {
	for idx, w := range n.waiters {
		w.done <- outcome{err: ErrStopped}
		delete(n.waiters, idx)
	}
	for ctx, q := range n.reads {
		q.done <- ErrStopped
		delete(n.reads, ctx)
	}
	for _, p := range n.ready {
		p.done <- ErrStopped
	}
	n.ready = nil
}

// ---- for the HTTP side ----------------------------------------------------------------------

// Propose replicates a command and waits for it to be applied here.
func (n *Node) Propose(c Command, timeout time.Duration) (Result, error) {
	data, _ := json.Marshal(c)
	p := proposal{data: data, done: make(chan outcome, 1)}
	return n.submit(p, timeout)
}

func (n *Node) ChangeMembers(cc raft.ConfChange, timeout time.Duration) error {
	p := proposal{conf: &cc, done: make(chan outcome, 1)}
	_, err := n.submit(p, timeout)
	return err
}

func (n *Node) submit(p proposal, timeout time.Duration) (Result, error) {
	select {
	case n.propc <- p:
	case <-n.done:
		return Result{}, ErrStopped
	}
	select {
	case o := <-p.done:
		if errors.Is(o.err, raft.ErrNotLeader) {
			return o.res, ErrNotLeader
		}
		return o.res, o.err
	case <-time.After(timeout):
		return Result{}, ErrUnknown
	}
}

// LinearizableRead returns once this node's state machine reflects every write that completed
// before the call (ReadIndex).
func (n *Node) LinearizableRead(timeout time.Duration) error {
	q := readRequest{done: make(chan error, 1)}
	select {
	case n.readc <- q:
	case <-n.done:
		return ErrStopped
	}
	select {
	case err := <-q.done:
		return err
	case <-time.After(timeout):
		return ErrUnknown
	}
}

func (n *Node) Status() raft.Status {
	c := make(chan raft.Status, 1)
	select {
	case n.statc <- c:
		return <-c
	case <-n.done:
		return raft.Status{ID: n.cfg.ID, Role: "stopped"}
	}
}

func (n *Node) IsLeader() bool { return n.role.Load() == int32(raft.Leader) }

func (n *Node) LeaderMember() (raft.Member, bool) {
	l := n.leader.Load()
	for _, m := range *n.members.Load() {
		if m.ID == l {
			return m, true
		}
	}
	return raft.Member{}, false
}

func (n *Node) SetFaults(f Faults) { n.tr.SetFaults(f) }
func (n *Node) Faults() Faults     { return n.tr.Faults() }

// SetClockRate makes this node's timers run at rate times real time.
func (n *Node) SetClockRate(rate float64) {
	if rate <= 0 {
		rate = 1
	}
	select {
	case n.skewc <- rate:
	default:
	}
}

func (n *Node) Syncs() int64 { return n.st.Syncs }

func (n *Node) Stop() {
	close(n.stopc)
	<-n.done
	n.tr.Close()
	n.st.Close()
}
