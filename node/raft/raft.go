package raft

import (
	"sort"
)

type Role uint8

const (
	Follower Role = iota
	PreCandidate
	Candidate
	Leader
)

func (r Role) String() string {
	return [...]string{"follower", "pre-candidate", "candidate", "leader"}[r]
}

type Options struct {
	ID             uint64
	ElectionTicks  int // a follower campaigns after a random timeout in [ElectionTicks, 2*ElectionTicks)
	HeartbeatTicks int
	MaxBatch       int // entries per append message
	PreVote        bool
	CheckQuorum    bool
	// Rand returns a number in [0, n); tests pass a seeded source.
	Rand func(n int) int
	// Bootstrap is the initial membership of a new cluster (empty when joining one).
	Bootstrap []Member
}

// State is what a node finds on its disk when it starts.
type State struct {
	HardState HardState
	Snapshot  *Snapshot
	Entries   []Entry
}

type progress struct {
	match, next uint64
}

type readReq struct {
	ctx   uint64
	index uint64
	acks  map[uint64]bool
}

type Raft struct {
	opts Options
	id   uint64

	term uint64
	vote uint64
	role Role
	lead uint64

	log      raftLog
	snapshot *Snapshot // the latest, to send to followers that are too far behind
	commit   uint64
	applied  uint64 // committed entries handed out so far

	config           Config
	pendingConfIndex uint64

	electionElapsed  int
	heartbeatElapsed int
	timeout          int

	votes        map[uint64]bool
	prs          map[uint64]*progress
	recentActive map[uint64]bool

	msgs        []Message
	readQ       []readReq
	reads       []ReadState
	failedReads []uint64
	prevHS      HardState
	newSnapshot *Snapshot
}

func New(opts Options, st State) *Raft {
	if opts.ElectionTicks == 0 {
		opts.ElectionTicks = 10
	}
	if opts.HeartbeatTicks == 0 {
		opts.HeartbeatTicks = 1
	}
	if opts.MaxBatch == 0 {
		opts.MaxBatch = 512
	}
	r := &Raft{opts: opts, id: opts.ID}
	if st.Snapshot != nil {
		s := *st.Snapshot
		r.snapshot = &s
		r.log.reset(s.Index, s.Term)
		r.applied = s.Index
		r.commit = s.Index
	}
	r.log.entries = append(r.log.entries, st.Entries...)
	r.log.stable = r.log.last()
	r.term, r.vote = st.HardState.Term, st.HardState.Vote
	r.commit = max(r.commit, min(st.HardState.Commit, r.log.last()))
	if r.log.last() == 0 && len(opts.Bootstrap) > 0 {
		// A new cluster: every founding member starts with the same log, one entry per member,
		// all committed, so a server that joins later learns the membership by replication.
		for i, m := range opts.Bootstrap {
			cc := ConfChange{Op: AddMember, Member: m}
			r.log.entries = append(r.log.entries, Entry{Term: 1, Index: uint64(i + 1), Type: EntryConfChange, Data: cc.Encode()})
		}
		r.term = 1
		r.commit = r.log.last()
	}
	r.prevHS = HardState{Term: r.term, Vote: r.vote, Commit: r.commit}
	if st.HardState.IsEmpty() {
		r.prevHS = HardState{}
	}
	r.recomputeConfig()
	r.becomeFollower(r.term, 0)
	return r
}

// ---- membership -------------------------------------------------------------------------------

// recomputeConfig derives the membership from the snapshot and the conf changes in the log: a
// change takes effect as soon as it is in the log (and is undone if the entry is truncated).
func (r *Raft) recomputeConfig() {
	var c Config
	if r.snapshot != nil {
		c = r.snapshot.Config.clone()
	}
	r.pendingConfIndex = 0
	for _, e := range r.log.entries {
		if e.Type == EntryConfChange {
			if cc, err := DecodeConfChange(e.Data); err == nil {
				c = c.apply(cc)
				r.pendingConfIndex = e.Index
			}
		}
	}
	r.config = c
	if r.role == Leader {
		for _, m := range c.Members {
			if _, ok := r.prs[m.ID]; !ok {
				r.prs[m.ID] = &progress{next: r.log.last() + 1}
			}
		}
		for id := range r.prs {
			if !c.Has(id) && id != r.id {
				delete(r.prs, id)
			}
		}
	}
}

// configAt is the membership in force at index i (for snapshots).
func (r *Raft) configAt(i uint64) Config {
	var c Config
	if r.snapshot != nil {
		c = r.snapshot.Config.clone()
	}
	for _, e := range r.log.entries {
		if e.Index > i {
			break
		}
		if e.Type == EntryConfChange {
			if cc, err := DecodeConfChange(e.Data); err == nil {
				c = c.apply(cc)
			}
		}
	}
	return c
}

func (r *Raft) promotable() bool { return r.config.Has(r.id) }

// ---- roles --------------------------------------------------------------------------------------

func (r *Raft) resetTimeout() {
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.timeout = r.opts.ElectionTicks
	if r.opts.Rand != nil {
		r.timeout += r.opts.Rand(r.opts.ElectionTicks)
	}
}

func (r *Raft) failReads() {
	for _, q := range r.readQ {
		r.failedReads = append(r.failedReads, q.ctx)
	}
	r.readQ = nil
}

func (r *Raft) becomeFollower(term, lead uint64) {
	if term != r.term {
		r.term = term
		r.vote = 0
	}
	if r.role == Leader {
		r.failReads()
	}
	r.role = Follower
	r.lead = lead
	r.resetTimeout()
}

func (r *Raft) becomePreCandidate() {
	r.role = PreCandidate
	r.lead = 0
	r.votes = map[uint64]bool{r.id: true}
	r.resetTimeout()
	if r.won() {
		r.becomeCandidate()
		return
	}
	for _, m := range r.config.Members {
		if m.ID != r.id {
			r.send(Message{Type: MsgPreVote, To: m.ID, Term: r.term + 1, Index: r.log.last(), LogTerm: r.log.lastTerm()})
		}
	}
}

func (r *Raft) becomeCandidate() {
	r.role = Candidate
	r.term++
	r.vote = r.id
	r.lead = 0
	r.votes = map[uint64]bool{r.id: true}
	r.resetTimeout()
	if r.won() {
		r.becomeLeader()
		return
	}
	for _, m := range r.config.Members {
		if m.ID != r.id {
			r.send(Message{Type: MsgVote, To: m.ID, Term: r.term, Index: r.log.last(), LogTerm: r.log.lastTerm()})
		}
	}
}

func (r *Raft) won() bool {
	n := 0
	for _, m := range r.config.Members {
		if r.votes[m.ID] {
			n++
		}
	}
	return n >= r.config.Quorum()
}

func (r *Raft) becomeLeader() {
	r.role = Leader
	r.lead = r.id
	r.prs = map[uint64]*progress{}
	r.recentActive = map[uint64]bool{}
	for _, m := range r.config.Members {
		r.prs[m.ID] = &progress{next: r.log.last() + 1}
	}
	r.prs[r.id] = &progress{match: r.log.last(), next: r.log.last() + 1}
	r.resetTimeout()
	// An empty entry of the new term: once it commits, everything before it is committed too,
	// and reads can be served.
	r.appendEntries(Entry{Type: EntryNormal})
	r.broadcastAppend()
}

func (r *Raft) campaign() {
	if !r.promotable() {
		return
	}
	if r.opts.PreVote {
		r.becomePreCandidate()
	} else {
		r.becomeCandidate()
	}
}

// ---- input ----------------------------------------------------------------------------------------

func (r *Raft) send(m Message) {
	m.From = r.id
	if m.Term == 0 {
		m.Term = r.term
	}
	r.msgs = append(r.msgs, m)
}

func (r *Raft) Tick() {
	r.electionElapsed++
	if r.role == Leader {
		r.heartbeatElapsed++
		if r.electionElapsed >= r.opts.ElectionTicks {
			r.electionElapsed = 0
			if r.opts.CheckQuorum {
				// Step down if a majority has not been heard from in an election timeout: clients
				// then look for the real leader instead of waiting on this one.
				n := 0
				for _, m := range r.config.Members {
					if m.ID == r.id || r.recentActive[m.ID] {
						n++
					}
				}
				r.recentActive = map[uint64]bool{}
				if n < r.config.Quorum() {
					r.becomeFollower(r.term, 0)
					return
				}
			}
		}
		if r.heartbeatElapsed >= r.opts.HeartbeatTicks {
			r.heartbeatElapsed = 0
			r.broadcastHeartbeat(0)
		}
		return
	}
	if r.electionElapsed >= r.timeout {
		r.campaign()
	}
}

func (r *Raft) upToDate(index, term uint64) bool {
	return term > r.log.lastTerm() || (term == r.log.lastTerm() && index >= r.log.last())
}

// inLease: a leader has been heard from within the election timeout.
func (r *Raft) inLease() bool {
	return r.opts.CheckQuorum && r.lead != 0 && r.electionElapsed < r.opts.ElectionTicks
}

func (r *Raft) Step(m Message) {
	switch {
	case m.Term > r.term:
		if m.Type == MsgPreVote || (m.Type == MsgPreVoteResp && !m.Reject) {
			break // pre-votes do not change terms
		}
		if (m.Type == MsgVote) && r.inLease() {
			return // a leader is alive: ignore a disruptive candidate
		}
		lead := uint64(0)
		if m.Type == MsgApp || m.Type == MsgHeartbeat || m.Type == MsgSnap {
			lead = m.From
		}
		r.becomeFollower(m.Term, lead)
	case m.Term < r.term:
		if (r.opts.CheckQuorum || r.opts.PreVote) && (m.Type == MsgApp || m.Type == MsgHeartbeat) {
			// Tell a stale leader about the newer term so it steps down.
			r.send(Message{Type: MsgAppResp, To: m.From})
		} else if m.Type == MsgPreVote {
			r.send(Message{Type: MsgPreVoteResp, To: m.From, Reject: true})
		}
		return
	}

	switch m.Type {
	case MsgPreVote, MsgVote:
		var grant bool
		if m.Type == MsgVote {
			grant = (r.vote == m.From || (r.vote == 0 && r.lead == 0)) && r.upToDate(m.Index, m.LogTerm)
		} else {
			grant = m.Term > r.term && !r.inLease() && r.upToDate(m.Index, m.LogTerm)
		}
		resp := MsgVoteResp
		if m.Type == MsgPreVote {
			resp = MsgPreVoteResp
		}
		if grant {
			term := r.term
			if m.Type == MsgVote {
				r.vote = m.From
				r.electionElapsed = 0
			} else {
				term = m.Term
			}
			r.send(Message{Type: resp, To: m.From, Term: term})
		} else {
			r.send(Message{Type: resp, To: m.From, Reject: true})
		}
		return
	}

	switch r.role {
	case Leader:
		r.stepLeader(m)
	case Candidate, PreCandidate:
		r.stepCandidate(m)
	default:
		r.stepFollower(m)
	}
}

func (r *Raft) stepCandidate(m Message) {
	switch m.Type {
	case MsgApp, MsgHeartbeat, MsgSnap:
		r.becomeFollower(m.Term, m.From)
		r.stepFollower(m)
	case MsgPreVoteResp, MsgVoteResp:
		if (m.Type == MsgPreVoteResp) != (r.role == PreCandidate) {
			return
		}
		if !m.Reject {
			r.votes[m.From] = true
		} else if _, seen := r.votes[m.From]; !seen {
			r.votes[m.From] = false
		}
		if r.won() {
			if r.role == PreCandidate {
				r.becomeCandidate()
			} else {
				r.becomeLeader()
			}
			return
		}
		rejects := 0
		for _, mm := range r.config.Members {
			if v, ok := r.votes[mm.ID]; ok && !v {
				rejects++
			}
		}
		if rejects >= r.config.Quorum() {
			r.becomeFollower(r.term, 0)
		}
	}
}

func (r *Raft) stepFollower(m Message) {
	switch m.Type {
	case MsgApp:
		r.electionElapsed = 0
		r.lead = m.From
		r.handleAppend(m)
	case MsgHeartbeat:
		r.electionElapsed = 0
		r.lead = m.From
		if m.Commit > r.commit {
			r.commit = min(m.Commit, r.log.last())
		}
		r.send(Message{Type: MsgHeartbeatResp, To: m.From, Ctx: m.Ctx})
	case MsgSnap:
		r.electionElapsed = 0
		r.lead = m.From
		r.handleSnapshot(m)
	}
}

func (r *Raft) handleAppend(m Message) {
	if m.Index < r.commit {
		r.send(Message{Type: MsgAppResp, To: m.From, Index: r.commit})
		return
	}
	last, ok, truncated := r.log.maybeAppend(m.Index, m.LogTerm, m.Entries)
	if ok {
		if truncated || hasConf(m.Entries) {
			r.recomputeConfig()
		}
		if c := min(m.Commit, last); c > r.commit {
			r.commit = c
		}
		r.send(Message{Type: MsgAppResp, To: m.From, Index: last})
		return
	}
	// Mismatch: suggest where to retry. Skip back over the whole conflicting term at once.
	hint := min(m.Index, r.log.last())
	for hint > r.commit {
		t, ok := r.log.term(hint)
		if !ok || t <= m.LogTerm {
			break
		}
		hint--
	}
	ht, _ := r.log.term(hint)
	r.send(Message{Type: MsgAppResp, To: m.From, Index: m.Index, Reject: true, Hint: hint, LogTerm: ht})
}

func hasConf(es []Entry) bool {
	for _, e := range es {
		if e.Type == EntryConfChange {
			return true
		}
	}
	return false
}

func (r *Raft) handleSnapshot(m Message) {
	s := m.Snap
	if s == nil || s.Index <= r.commit {
		r.send(Message{Type: MsgAppResp, To: m.From, Index: r.commit})
		return
	}
	if r.log.matchTerm(s.Index, s.Term) {
		// We already have everything it covers.
		r.commit = s.Index
		r.send(Message{Type: MsgAppResp, To: m.From, Index: r.log.last()})
		return
	}
	cp := *s
	r.snapshot = &cp
	r.newSnapshot = &cp
	r.log.reset(s.Index, s.Term)
	r.commit = s.Index
	r.applied = s.Index
	r.recomputeConfig()
	r.send(Message{Type: MsgAppResp, To: m.From, Index: s.Index})
}

func (r *Raft) stepLeader(m Message) {
	pr, ok := r.prs[m.From]
	if !ok {
		return
	}
	switch m.Type {
	case MsgAppResp:
		r.recentActive[m.From] = true
		if m.Reject {
			// Find the last index at or before the hint whose term is at most the follower's.
			next := m.Hint
			for next > pr.match {
				t, ok := r.log.term(next)
				if !ok || t <= m.LogTerm {
					break
				}
				next--
			}
			pr.next = max(next+1, pr.match+1)
			r.sendAppend(m.From)
			return
		}
		if m.Index > pr.match {
			pr.match = m.Index
		}
		if pr.next <= pr.match {
			pr.next = pr.match + 1
		}
		if r.maybeCommit() {
			r.broadcastAppend()
		} else if pr.next <= r.log.last() {
			r.sendAppend(m.From)
		}
	case MsgHeartbeatResp:
		r.recentActive[m.From] = true
		if pr.match < r.log.last() {
			// Something was lost on the way: resend from what is known to match.
			pr.next = pr.match + 1
			r.sendAppend(m.From)
		}
		if m.Ctx != 0 {
			r.ackRead(m.From, m.Ctx)
		}
	}
}

// maybeCommit advances the commit index to the highest index stored on a majority, if that
// entry is from the current term (older terms commit only indirectly).
func (r *Raft) maybeCommit() bool {
	var matches []uint64
	for _, m := range r.config.Members {
		if pr, ok := r.prs[m.ID]; ok {
			matches = append(matches, pr.match)
		} else {
			matches = append(matches, 0)
		}
	}
	if len(matches) == 0 {
		return false
	}
	sort.Slice(matches, func(i, j int) bool { return matches[i] > matches[j] })
	idx := matches[r.config.Quorum()-1]
	if idx <= r.commit {
		return false
	}
	if t, _ := r.log.term(idx); t != r.term {
		return false
	}
	r.commit = idx
	// A leader that removed itself steps down once the change is committed.
	if !r.config.Has(r.id) && r.pendingConfIndex <= r.commit {
		r.becomeFollower(r.term, 0)
	}
	return true
}

func (r *Raft) sendAppend(to uint64) {
	pr := r.prs[to]
	if pr == nil || to == r.id {
		return
	}
	if pr.next < r.log.first() {
		if r.snapshot == nil {
			return
		}
		r.send(Message{Type: MsgSnap, To: to, Snap: r.snapshot})
		pr.next = r.snapshot.Index + 1
		return
	}
	prev := pr.next - 1
	prevTerm, ok := r.log.term(prev)
	if !ok {
		return
	}
	hi := min(r.log.last(), prev+uint64(r.opts.MaxBatch))
	ents := r.log.slice(pr.next, hi)
	r.send(Message{Type: MsgApp, To: to, Index: prev, LogTerm: prevTerm, Entries: ents, Commit: r.commit})
	if len(ents) > 0 {
		pr.next = hi + 1 // optimistic: keep streaming without waiting for the reply
	}
}

func (r *Raft) broadcastAppend() {
	for _, m := range r.config.Members {
		if m.ID != r.id {
			r.sendAppend(m.ID)
		}
	}
}

func (r *Raft) broadcastHeartbeat(ctx uint64) {
	if ctx == 0 && len(r.readQ) > 0 {
		ctx = r.readQ[len(r.readQ)-1].ctx
	}
	for _, m := range r.config.Members {
		if m.ID == r.id {
			continue
		}
		commit := r.commit
		if pr := r.prs[m.ID]; pr != nil {
			commit = min(pr.match, r.commit)
		}
		r.send(Message{Type: MsgHeartbeat, To: m.ID, Commit: commit, Ctx: ctx})
	}
}

func (r *Raft) appendEntries(es ...Entry) {
	for i := range es {
		es[i].Term = r.term
		es[i].Index = r.log.last() + 1
		r.log.entries = append(r.log.entries, es[i])
		if es[i].Type == EntryConfChange {
			r.recomputeConfig()
		}
	}
	if pr := r.prs[r.id]; pr != nil {
		pr.match = r.log.last()
		pr.next = pr.match + 1
	}
	r.maybeCommit()
}

// ---- requests from the server -------------------------------------------------------------------

// Propose appends a command; it returns the index and term it got. It is committed (and will
// appear in Ready.Committed) only if an entry with that index and term commits.
func (r *Raft) Propose(data []byte) (uint64, uint64, error) {
	if r.role != Leader {
		return 0, 0, ErrNotLeader
	}
	r.appendEntries(Entry{Type: EntryNormal, Data: data})
	r.broadcastAppend()
	return r.log.last(), r.term, nil
}

func (r *Raft) ProposeConfChange(cc ConfChange) (uint64, uint64, error) {
	if r.role != Leader {
		return 0, 0, ErrNotLeader
	}
	if r.pendingConfIndex > r.commit {
		return 0, 0, ErrConfChangeAhead
	}
	r.appendEntries(Entry{Type: EntryConfChange, Data: cc.Encode()})
	r.broadcastAppend()
	return r.log.last(), r.term, nil
}

// ReadIndex asks for a linearizable read: once a majority confirms this node is still leader,
// Ready.Reads says from which index the read may be served.
func (r *Raft) ReadIndex(ctx uint64) {
	if r.role != Leader {
		r.failedReads = append(r.failedReads, ctx)
		return
	}
	if t, _ := r.log.term(r.commit); t != r.term {
		// Nothing committed in this term yet: the commit index may be stale.
		r.failedReads = append(r.failedReads, ctx)
		return
	}
	if r.config.Quorum() <= 1 && r.config.Has(r.id) {
		r.reads = append(r.reads, ReadState{Ctx: ctx, Index: r.commit})
		return
	}
	r.readQ = append(r.readQ, readReq{ctx: ctx, index: r.commit, acks: map[uint64]bool{r.id: true}})
	r.broadcastHeartbeat(ctx)
}

func (r *Raft) ackRead(from, ctx uint64) {
	for i, q := range r.readQ {
		if q.ctx != ctx {
			continue
		}
		q.acks[from] = true
		n := 0
		for _, m := range r.config.Members {
			if q.acks[m.ID] {
				n++
			}
		}
		if n >= r.config.Quorum() {
			// Confirmed: this one and every earlier request.
			for _, done := range r.readQ[:i+1] {
				r.reads = append(r.reads, ReadState{Ctx: done.ctx, Index: done.index})
			}
			r.readQ = r.readQ[i+1:]
		}
		return
	}
}

// Compact replaces the log up to index (already applied) by a snapshot of the state machine.
func (r *Raft) Compact(index uint64, data []byte) *Snapshot {
	if index <= r.log.snapIndex || index > r.applied {
		return nil
	}
	t, _ := r.log.term(index)
	s := &Snapshot{Index: index, Term: t, Config: r.configAt(index), Data: data}
	r.snapshot = s
	r.log.compact(index, t)
	return s
}

// Ready collects what the server must do now, and assumes it will be done.
func (r *Raft) Ready() Ready {
	rd := Ready{Messages: r.msgs, Reads: r.reads, FailedReads: r.failedReads, Snapshot: r.newSnapshot}
	hs := HardState{Term: r.term, Vote: r.vote, Commit: r.commit}
	if hs != r.prevHS {
		rd.HardState = hs
		r.prevHS = hs
	}
	rd.Entries = r.log.unstable()
	r.log.stable = r.log.last()
	if r.commit > r.applied {
		rd.Committed = append([]Entry(nil), r.log.slice(max(r.applied+1, r.log.first()), r.commit)...)
		r.applied = r.commit
	}
	r.msgs, r.reads, r.failedReads, r.newSnapshot = nil, nil, nil, nil
	return rd
}

// ---- inspection ---------------------------------------------------------------------------------

type PeerStatus struct {
	ID    uint64 `json:"id"`
	Match uint64 `json:"match"`
	Next  uint64 `json:"next"`
}

type EntryInfo struct {
	Index uint64 `json:"index"`
	Term  uint64 `json:"term"`
	Conf  bool   `json:"conf,omitempty"`
}

type Status struct {
	ID        uint64       `json:"id"`
	Role      string       `json:"role"`
	Term      uint64       `json:"term"`
	Vote      uint64       `json:"vote"`
	Lead      uint64       `json:"lead"`
	Commit    uint64       `json:"commit"`
	Applied   uint64       `json:"applied"`
	LastIndex uint64       `json:"last_index"`
	SnapIndex uint64       `json:"snap_index"`
	Members   []Member     `json:"members"`
	Peers     []PeerStatus `json:"peers,omitempty"`
	Tail      []EntryInfo  `json:"tail"`
}

func (r *Raft) Status(tail int) Status {
	s := Status{ID: r.id, Role: r.role.String(), Term: r.term, Vote: r.vote, Lead: r.lead, Commit: r.commit,
		Applied: r.applied, LastIndex: r.log.last(), SnapIndex: r.log.snapIndex, Members: r.config.Members}
	if r.role == Leader {
		for _, m := range r.config.Members {
			if pr := r.prs[m.ID]; pr != nil {
				s.Peers = append(s.Peers, PeerStatus{ID: m.ID, Match: pr.match, Next: pr.next})
			}
		}
	}
	from := r.log.first()
	if r.log.last() >= uint64(tail) && r.log.last()-uint64(tail)+1 > from {
		from = r.log.last() - uint64(tail) + 1
	}
	for _, e := range r.log.slice(from, r.log.last()) {
		s.Tail = append(s.Tail, EntryInfo{Index: e.Index, Term: e.Term, Conf: e.Type == EntryConfChange})
	}
	return s
}

func (r *Raft) Role() Role        { return r.role }
func (r *Raft) Leader() uint64    { return r.lead }
func (r *Raft) Term() uint64      { return r.term }
func (r *Raft) Config() Config    { return r.config.clone() }
func (r *Raft) Applied() uint64   { return r.applied }
func (r *Raft) SnapIndex() uint64 { return r.log.snapIndex }
