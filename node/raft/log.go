package raft

// raftLog is the in-memory log: entries after the last snapshot, and how far they are on disk.
type raftLog struct {
	snapIndex uint64
	snapTerm  uint64
	entries   []Entry // entries[i].Index == snapIndex+1+i
	stable    uint64  // entries up to here have been handed out for persisting
}

func (l *raftLog) first() uint64 { return l.snapIndex + 1 }

func (l *raftLog) last() uint64 { return l.snapIndex + uint64(len(l.entries)) }

func (l *raftLog) lastTerm() uint64 {
	t, _ := l.term(l.last())
	return t
}

// term of the entry at i; false when i is compacted away or beyond the end.
func (l *raftLog) term(i uint64) (uint64, bool) {
	switch {
	case i == l.snapIndex:
		return l.snapTerm, true
	case i < l.snapIndex || i > l.last():
		return 0, false
	}
	return l.entries[i-l.snapIndex-1].Term, true
}

func (l *raftLog) matchTerm(i, t uint64) bool {
	got, ok := l.term(i)
	return ok && got == t
}

// slice returns entries [lo, hi].
func (l *raftLog) slice(lo, hi uint64) []Entry {
	if lo > hi || lo <= l.snapIndex || hi > l.last() {
		return nil
	}
	return l.entries[lo-l.snapIndex-1 : hi-l.snapIndex]
}

// truncateFrom drops entries at i and after.
func (l *raftLog) truncateFrom(i uint64) {
	l.entries = l.entries[:i-l.snapIndex-1]
	if l.stable >= i {
		l.stable = i - 1
	}
}

// maybeAppend appends entries that follow (prevIndex, prevTerm), replacing any that conflict.
// It returns the index of the last new entry and whether the log matched at prevIndex.
func (l *raftLog) maybeAppend(prevIndex, prevTerm uint64, ents []Entry) (uint64, bool, bool) {
	if !l.matchTerm(prevIndex, prevTerm) {
		return 0, false, false
	}
	truncated := false
	for i, e := range ents {
		if e.Index <= l.snapIndex {
			continue // already in the snapshot
		}
		t, ok := l.term(e.Index)
		if ok && t == e.Term {
			continue
		}
		if ok {
			l.truncateFrom(e.Index)
			truncated = true
		}
		l.entries = append(l.entries, ents[i:]...)
		break
	}
	return prevIndex + uint64(len(ents)), true, truncated
}

func (l *raftLog) unstable() []Entry {
	if l.stable >= l.last() {
		return nil
	}
	from := max(l.stable+1, l.first())
	return append([]Entry(nil), l.slice(from, l.last())...)
}

// compact drops entries up to and including i (now covered by a snapshot).
func (l *raftLog) compact(i, term uint64) {
	if i <= l.snapIndex {
		return
	}
	if i >= l.last() {
		l.entries = nil
	} else {
		l.entries = append([]Entry(nil), l.entries[i-l.snapIndex:]...)
	}
	l.snapIndex, l.snapTerm = i, term
	if l.stable < i {
		l.stable = i
	}
}

// reset replaces the whole log by a snapshot.
func (l *raftLog) reset(i, term uint64) {
	l.entries = nil
	l.snapIndex, l.snapTerm = i, term
	l.stable = i
}
