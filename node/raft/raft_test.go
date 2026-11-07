package raft

import (
	"fmt"
	"math/rand"
	"os"
	"slices"
	"strconv"
	"testing"
)

// simNode is a server in a simulated cluster: the Raft state machine plus the "disk" a Ready
// writes to, so that a crash can be simulated by rebuilding Raft from the disk alone.
type simNode struct {
	id      uint64
	r       *Raft
	opts    Options
	hs      HardState
	snap    *Snapshot
	entries []Entry
	applied []string // the commands applied, in order (index:data)
	last    uint64   // index of the last entry applied
	down    bool
	reads   []uint64 // read contexts confirmed
	failed  []uint64 // read contexts failed
}

func (n *simNode) persist(rd Ready) {
	if !rd.HardState.IsEmpty() {
		n.hs = rd.HardState
	}
	if rd.Snapshot != nil {
		n.snap = rd.Snapshot
		n.entries = nil
		n.applied = []string{fmt.Sprintf("snapshot@%d", rd.Snapshot.Index)}
		n.last = rd.Snapshot.Index
	}
	if len(rd.Entries) > 0 {
		first := rd.Entries[0].Index
		keep := n.entries[:0]
		for _, e := range n.entries {
			if e.Index < first {
				keep = append(keep, e)
			}
		}
		n.entries = append(keep, rd.Entries...)
	}
}

func (n *simNode) restart() {
	var entries []Entry
	for _, e := range n.entries {
		if n.snap == nil || e.Index > n.snap.Index {
			entries = append(entries, e)
		}
	}
	n.r = New(n.opts, State{HardState: n.hs, Snapshot: n.snap, Entries: entries})
	// The state machine is rebuilt from the snapshot, then committed entries are re-applied.
	n.applied = nil
	n.last = 0
	if n.snap != nil {
		n.applied = []string{fmt.Sprintf("snapshot@%d", n.snap.Index)}
		n.last = n.snap.Index
	}
	n.down = false
}

type sim struct {
	t       *testing.T
	rng     *rand.Rand
	nodes   map[uint64]*simNode
	queue   []Message
	cut     map[[2]uint64]bool // dropped links
	loss    float64
	leaders map[uint64]uint64 // term -> leader seen
	// committed[index] = data, as first applied anywhere: every node must agree.
	committed map[uint64]string
	terms     map[uint64]uint64 // term of the entry committed at each index, any type
	applies   int
	noPreVote bool
}

func newSim(t *testing.T, seed int64, n int) *sim {
	return newSimWith(t, seed, n, false)
}

func newSimWith(t *testing.T, seed int64, n int, noPreVote bool) *sim {
	s := &sim{t: t, rng: rand.New(rand.NewSource(seed)), nodes: map[uint64]*simNode{}, cut: map[[2]uint64]bool{},
		leaders: map[uint64]uint64{}, committed: map[uint64]string{}, terms: map[uint64]uint64{}, noPreVote: noPreVote}
	var members []Member
	for i := 1; i <= n; i++ {
		members = append(members, Member{ID: uint64(i), Raft: fmt.Sprintf("r%d", i), HTTP: fmt.Sprintf("h%d", i)})
	}
	for _, m := range members {
		s.addNode(m.ID, members)
	}
	return s
}

// ids and ordered visit servers by id: map order is random in Go, and the simulation must be
// reproducible from its seed.
func (s *sim) ids() []uint64 {
	var out []uint64
	for id := range s.nodes {
		out = append(out, id)
	}
	slices.Sort(out)
	return out
}

func (s *sim) ordered() []*simNode {
	var out []*simNode
	for _, id := range s.ids() {
		out = append(out, s.nodes[id])
	}
	return out
}

func (s *sim) addNode(id uint64, bootstrap []Member) *simNode {
	opts := Options{ID: id, ElectionTicks: 10, HeartbeatTicks: 1, MaxBatch: 8, PreVote: !s.noPreVote, CheckQuorum: !s.noPreVote,
		Rand: s.rng.Intn, Bootstrap: bootstrap}
	n := &simNode{id: id, opts: opts}
	n.restart()
	s.nodes[id] = n
	return n
}

func (s *sim) process(n *simNode) {
	rd := n.r.Ready()
	n.persist(rd)
	for _, m := range rd.Messages {
		if s.rng.Float64() < 0.02 {
			s.queue = append(s.queue, m) // duplicated
		}
		s.queue = append(s.queue, m)
	}
	for _, r := range rd.Reads {
		n.reads = append(n.reads, r.Ctx)
	}
	n.failed = append(n.failed, rd.FailedReads...)
	for _, e := range rd.Committed {
		// Entries are applied in order, without gaps.
		if e.Index != n.last+1 {
			s.t.Fatalf("node %d applied index %d after %d", n.id, e.Index, n.last)
		}
		n.last = e.Index
		if t, ok := s.terms[e.Index]; ok && t != e.Term {
			s.t.Fatalf("node %d committed index %d with term %d; another node with term %d", n.id, e.Index, e.Term, t)
		}
		s.terms[e.Index] = e.Term
		if e.Type != EntryNormal || len(e.Data) == 0 {
			continue
		}
		got := string(e.Data)
		if prev, ok := s.committed[e.Index]; ok && prev != got {
			s.t.Fatalf("node %d applied %q at %d; another node applied %q", n.id, got, e.Index, prev)
		}
		s.committed[e.Index] = got
		n.applied = append(n.applied, fmt.Sprintf("%d:%s", e.Index, got))
		s.applies++
	}
	// Election safety: one leader per term.
	if n.r.role == Leader {
		if other, ok := s.leaders[n.r.term]; ok && other != n.id {
			s.t.Fatalf("two leaders in term %d: %d and %d", n.r.term, other, n.id)
		}
		s.leaders[n.r.term] = n.id
	}
}

// step runs one unit of simulated time: deliver (some of) the queued messages, tick everyone.
func (s *sim) step() {
	q := s.queue
	s.queue = nil
	s.rng.Shuffle(len(q), func(i, j int) { q[i], q[j] = q[j], q[i] }) // reordering
	for _, m := range q {
		to := s.nodes[m.To]
		if to == nil || to.down || s.nodes[m.From] == nil || s.nodes[m.From].down {
			continue
		}
		if s.cut[[2]uint64{m.From, m.To}] || s.rng.Float64() < s.loss {
			continue
		}
		if s.rng.Float64() < 0.05 {
			s.queue = append(s.queue, m) // delayed to a later step
			continue
		}
		to.r.Step(m)
		s.process(to)
	}
	// Servers tick in an order drawn from the seed: reproducible, but varied.
	order := s.ordered()
	s.rng.Shuffle(len(order), func(i, j int) { order[i], order[j] = order[j], order[i] })
	for _, n := range order {
		if !n.down {
			n.r.Tick()
			s.process(n)
		}
	}
}

func (s *sim) leader() *simNode {
	var best *simNode
	for _, n := range s.ordered() {
		if !n.down && n.r.role == Leader && (best == nil || n.r.term > best.r.term) {
			best = n
		}
	}
	return best
}

func (s *sim) propose(data string) {
	if l := s.leader(); l != nil {
		l.r.Propose([]byte(data))
		s.process(l)
	}
}

func (s *sim) partition(groups ...[]uint64) {
	s.cut = map[[2]uint64]bool{}
	group := map[uint64]int{}
	for g, ids := range groups {
		for _, id := range ids {
			group[id] = g
		}
	}
	for _, a := range s.ids() {
		for _, b := range s.ids() {
			if group[a] != group[b] {
				s.cut[[2]uint64{a, b}] = true
			}
		}
	}
}

func (s *sim) heal() { s.cut = map[[2]uint64]bool{} }

// checkPrefixes: what each node applied agrees, index by index, with what every other node
// applied (State Machine Safety; s.committed holds the first value seen at each index).
func (s *sim) checkPrefixes() {
	for _, n := range s.ordered() {
		for _, a := range n.applied {
			var idx uint64
			var data string
			if _, err := fmt.Sscanf(a, "%d:%s", &idx, &data); err != nil {
				continue // a snapshot marker
			}
			if s.committed[idx] != data {
				s.t.Fatalf("node %d applied %q at %d, others %q", n.id, data, idx, s.committed[idx])
			}
		}
	}
}

func TestElectsOneLeaderAndReplicates(t *testing.T) {
	s := newSim(t, 1, 5)
	for i := 0; i < 50 && s.leader() == nil; i++ {
		s.step()
	}
	if s.leader() == nil {
		t.Fatal("no leader elected")
	}
	for i := 0; i < 20; i++ {
		s.propose(fmt.Sprintf("v%d", i))
		s.step()
	}
	for i := 0; i < 30; i++ {
		s.step()
	}
	for _, n := range s.ordered() {
		if got := len(n.applied); got != 20 {
			t.Fatalf("node %d applied %d commands, want 20", n.id, got)
		}
	}
	s.checkPrefixes()
}

// Random partitions, message loss, reordering, duplication and crashes with restarts from
// persisted state, for many seeds. Safety is checked on every step; at the end the cluster must
// heal, elect a leader and commit new commands everywhere.
func TestSafetyUnderChaos(t *testing.T) {
	// 200 runs: 3- and 5-server clusters, with and without pre-vote and check-quorum (which
	// would otherwise hide some bugs in plain voting).
	runs := int64(200)
	if v, err := strconv.ParseInt(os.Getenv("CONCORD_SIM_RUNS"), 10, 64); err == nil {
		runs = v
	}
	for run := int64(0); run < runs; run++ {
		seed, size, noPreVote := run, 3+2*int(run%2), run%4 >= 2
		t.Run(fmt.Sprintf("seed%d-n%d-prevote%v", seed, size, !noPreVote), func(t *testing.T) {
			s := newSimWith(t, seed, size, noPreVote)
			s.loss = 0.1
			var ids []uint64
			for i := 1; i <= size; i++ {
				ids = append(ids, uint64(i))
			}
			for step := 0; step < 1500; step++ {
				switch r := s.rng.Intn(100); {
				case r < 3:
					s.rng.Shuffle(len(ids), func(i, j int) { ids[i], ids[j] = ids[j], ids[i] })
					k := 1 + s.rng.Intn(size-1)
					s.partition(append([]uint64(nil), ids[:k]...), append([]uint64(nil), ids[k:]...))
				case r < 5:
					s.heal()
				case r < 6:
					// Crash the leader right after it accepted a command: the classic way to end
					// up with entries of old terms on a minority (Figure 8 of the Raft paper).
					if l := s.leader(); l != nil {
						s.propose(fmt.Sprintf("s%d-%d-last", seed, step))
						l.down = true
					}
				case r < 7:
					n := s.nodes[uint64(1+s.rng.Intn(size))]
					n.down = true
				case r < 10:
					for _, n := range s.ordered() {
						if n.down {
							n.restart()
							break
						}
					}
				case r < 13:
					if l := s.leader(); l != nil && l.r.applied > 30 && s.rng.Intn(3) == 0 {
						data := []byte(fmt.Sprint(l.applied))
						if snap := l.r.Compact(l.r.applied, data); snap != nil {
							l.snap = snap
						}
					}
				}
				if step%3 == 0 {
					s.propose(fmt.Sprintf("s%d-%d", seed, step))
				}
				s.step()
				s.checkPrefixes()
			}
			// Heal everything and require progress.
			s.heal()
			s.loss = 0
			for _, n := range s.ordered() {
				if n.down {
					n.restart()
				}
			}
			for i := 0; i < 200; i++ {
				s.step()
			}
			before := s.applies
			for i := 0; i < 10; i++ {
				s.propose(fmt.Sprintf("final-%d", i))
				s.step()
			}
			for i := 0; i < 200; i++ {
				s.step()
			}
			if s.applies <= before {
				t.Fatalf("no progress after healing (leader %v)", s.leader() != nil)
			}
			s.checkPrefixes()
			// Everyone converges to the same commit index.
			var commit uint64
			for _, n := range s.ordered() {
				if commit == 0 {
					commit = n.r.commit
				} else if n.r.commit != commit {
					t.Fatalf("commit indexes differ after healing: %d vs %d", commit, n.r.commit)
				}
			}
		})
	}
}

// A minority partition cannot elect a leader or commit; on rejoining, pre-vote keeps it from
// disrupting the majority's leader.
func TestMinorityCannotProgressOrDisrupt(t *testing.T) {
	s := newSim(t, 7, 5)
	for i := 0; i < 50; i++ {
		s.step()
	}
	l := s.leader()
	var minority []uint64
	for _, id := range s.ids() {
		if id != l.id && len(minority) < 2 {
			minority = append(minority, id)
		}
	}
	var majority []uint64
	for _, id := range s.ids() {
		if !slices.Contains(minority, id) {
			majority = append(majority, id)
		}
	}
	s.partition(minority, majority)
	term := l.r.term
	for i := 0; i < 300; i++ {
		s.step()
	}
	for _, id := range minority {
		if s.nodes[id].r.role == Leader {
			t.Fatal("a minority elected a leader")
		}
		if s.nodes[id].r.term != term {
			t.Fatalf("pre-vote should keep the minority's term at %d, got %d", term, s.nodes[id].r.term)
		}
	}
	s.heal()
	for i := 0; i < 50; i++ {
		s.step()
	}
	if s.leader() == nil || s.leader().r.term != term {
		t.Fatal("the rejoining minority disrupted the leader")
	}
}

func TestReadIndexNeedsAMajority(t *testing.T) {
	s := newSim(t, 3, 3)
	for i := 0; i < 50; i++ {
		s.step()
	}
	l := s.leader()
	// Isolate the leader, then ask it for a read: it must never be confirmed, and must fail
	// once check-quorum makes the leader step down.
	var others []uint64
	for _, id := range s.ids() {
		if id != l.id {
			others = append(others, id)
		}
	}
	s.partition([]uint64{l.id}, others)
	l.r.ReadIndex(42)
	s.process(l)
	for i := 0; i < 100; i++ {
		s.step()
	}
	if slices.Contains(l.reads, 42) {
		t.Fatal("an isolated leader served a read")
	}
	if !slices.Contains(l.failed, 42) {
		t.Fatal("the read should fail once the isolated leader steps down")
	}
	// Healthy: confirmed by a round of heartbeats.
	s.heal()
	for i := 0; i < 80; i++ {
		s.step()
	}
	l = s.leader()
	l.r.ReadIndex(7)
	s.process(l)
	for i := 0; i < 20; i++ {
		s.step()
	}
	if !slices.Contains(l.reads, 7) {
		t.Fatal("read not confirmed by a healthy majority")
	}
}

func TestSnapshotCatchesUpALaggingFollower(t *testing.T) {
	s := newSim(t, 5, 3)
	for i := 0; i < 50; i++ {
		s.step()
	}
	l := s.leader()
	var lag uint64
	for _, id := range s.ids() {
		if id != l.id {
			lag = id
			break
		}
	}
	s.nodes[lag].down = true
	for i := 0; i < 40; i++ {
		s.propose(fmt.Sprintf("x%d", i))
		s.step()
	}
	for i := 0; i < 20; i++ {
		s.step()
	}
	snap := l.r.Compact(l.r.applied, []byte("state"))
	if snap == nil {
		t.Fatal("no snapshot")
	}
	l.snap = snap
	s.nodes[lag].restart()
	for i := 0; i < 100; i++ {
		s.step()
	}
	f := s.nodes[lag]
	if f.snap == nil || f.r.commit != l.r.commit {
		t.Fatalf("follower not caught up: snapshot %v, commit %d vs %d", f.snap != nil, f.r.commit, l.r.commit)
	}
}

func TestMembershipChanges(t *testing.T) {
	s := newSim(t, 9, 3)
	for i := 0; i < 50; i++ {
		s.step()
	}
	// Add server 4 (it starts empty, knowing nothing).
	n4 := s.addNode(4, nil)
	_ = n4
	l := s.leader()
	if _, _, err := l.r.ProposeConfChange(ConfChange{Op: AddMember, Member: Member{ID: 4, Raft: "r4"}}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := l.r.ProposeConfChange(ConfChange{Op: AddMember, Member: Member{ID: 5, Raft: "r5"}}); err != ErrConfChangeAhead {
		t.Fatalf("a second change before the first commits must wait, got %v", err)
	}
	s.process(l)
	for i := 0; i < 100; i++ {
		s.step()
	}
	for _, n := range s.ordered() {
		if len(n.r.config.Members) != 4 {
			t.Fatalf("node %d sees %d members", n.id, len(n.r.config.Members))
		}
	}
	// Remove the leader itself: it must step down once the change commits, and the rest go on.
	l = s.leader()
	old := l.id
	if _, _, err := l.r.ProposeConfChange(ConfChange{Op: RemoveMember, Member: Member{ID: old}}); err != nil {
		t.Fatal(err)
	}
	s.process(l)
	for i := 0; i < 200; i++ {
		s.step()
	}
	nl := s.leader()
	if nl == nil || nl.id == old {
		t.Fatalf("no new leader after the old one removed itself")
	}
	s.propose("after-removal")
	for i := 0; i < 50; i++ {
		s.step()
	}
	for _, id := range s.ids() {
		n := s.nodes[id]
		if id != old && !slices.ContainsFunc(n.applied, func(a string) bool { return len(a) > 13 && a[len(a)-13:] == "after-removal" }) {
			t.Fatalf("node %d did not apply the command after the removal", id)
		}
	}
}

func TestConfChangeEncoding(t *testing.T) {
	cc := ConfChange{Op: AddMember, Member: Member{ID: 300, Raft: "127.0.0.1:7001", HTTP: "127.0.0.1:8001"}}
	got, err := DecodeConfChange(cc.Encode())
	if err != nil || got != cc {
		t.Fatalf("got %+v %v", got, err)
	}
	if _, err := DecodeConfChange([]byte{1}); err == nil {
		t.Fatal("short input accepted")
	}
}

// deliver runs messages among the given servers until none are left, dropping those `drop`
// rejects (and everything to or from servers not listed).
func (s *sim) deliver(alive []uint64, drop func(Message) bool) {
	for len(s.queue) > 0 {
		q := s.queue
		s.queue = nil
		for _, m := range q {
			if !slices.Contains(alive, m.From) || !slices.Contains(alive, m.To) || (drop != nil && drop(m)) {
				continue
			}
			s.nodes[m.To].r.Step(m)
			s.process(s.nodes[m.To])
		}
	}
}

func (s *sim) elect(id uint64, alive []uint64) {
	n := s.nodes[id]
	// A term may already be taken by another candidate's votes: campaign again, as a timeout would.
	for try := 0; try < 3 && n.r.role != Leader; try++ {
		n.r.campaign()
		s.process(n)
		s.deliver(alive, func(m Message) bool { return m.Type == MsgApp || m.Type == MsgHeartbeat })
	}
	if n.r.role != Leader {
		s.t.Fatalf("server %d did not win: %s", id, n.r.role)
	}
}

// Figure 8 of the Raft paper, scripted: an entry from an old term that reaches a majority
// must not be counted as committed, or a later leader can overwrite it.
func TestFigure8(t *testing.T) {
	s := newSimWith(t, 1, 5, true)
	for _, n := range s.ordered() {
		n.opts.MaxBatch = 1 // one entry per message, so delivery can stop between two entries
		n.restart()
	}
	all := []uint64{1, 2, 3, 4, 5}
	s.elect(1, all)
	s.deliver(all, nil)
	// (a) S1 (term 2) gets "a" onto S2 only.
	s.nodes[1].r.Propose([]byte("a"))
	s.process(s.nodes[1])
	s.deliver([]uint64{1, 2}, nil)
	idx := s.nodes[1].r.log.last()
	s.nodes[1].down = true
	// (b) S5 is elected in term 3 by S3, S4, S5, takes "b" at the same index, replicates nothing.
	s.elect(5, []uint64{3, 4, 5})
	s.queue = nil
	s.nodes[5].r.Propose([]byte("b"))
	s.process(s.nodes[5])
	s.queue = nil
	s.nodes[5].down = true
	// (c) S1 restarts and is elected in term 4; it gets "a" (term 2) onto S3 -- a majority --
	// and crashes before its own term-4 entry gets there.
	s.nodes[1].restart()
	s.elect(1, []uint64{1, 2, 3})
	s.nodes[1].r.Tick() // a heartbeat restarts replication
	s.process(s.nodes[1])
	sentA := false
	s.deliver([]uint64{1, 2, 3}, func(m Message) bool {
		if m.Type == MsgApp && m.To == 3 && len(m.Entries) == 1 && m.Entries[0].Index > idx {
			return true // the term-4 entry never arrives
		}
		if m.Type == MsgApp && m.To == 3 && len(m.Entries) == 1 && m.Entries[0].Index == idx {
			sentA = true
		}
		return false
	})
	if !sentA {
		t.Fatal("scenario did not replicate the old entry")
	}
	s.nodes[1].down = true
	// (d) S5 restarts and wins term 5 with S2, S3, S4, then replicates its log.
	s.nodes[5].restart()
	s.elect(5, []uint64{2, 3, 4, 5})
	s.deliver([]uint64{2, 3, 4, 5}, nil)
	for i := 0; i < 30; i++ {
		s.step()
	}
	// s.process fails the test if two servers ever committed different terms at one index.
}
