package server

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"

	"github.com/YUStulinu/Concord/node/raft"
)

// Storage keeps a node's Raft state on disk:
//
//	log       records [crc32 u32][length u32][kind u8][payload]: entries, and hard states
//	          (term, vote, commit) written into the same file so one fsync covers both
//	snapshot  the latest snapshot, replaced atomically (written aside, synced, renamed)
//
// On start the log is read up to the first record whose checksum fails (a torn write) and cut
// there. Replacing entries after a conflict truncates the file at the first replaced entry and
// writes the current hard state again, so a vote recorded after that entry is not lost.
type Storage struct {
	dir     string
	f       *os.File
	size    int64
	offsets map[uint64]int64 // entry index -> offset of its record
	hs      raft.HardState
	Syncs   int64
	NoSync  bool // for tests that do not care about durability
}

const (
	recEntry     = 1
	recHardState = 2
)

func (s *Storage) logPath() string  { return filepath.Join(s.dir, "log") }
func (s *Storage) snapPath() string { return filepath.Join(s.dir, "snapshot") }

func encodeEntry(e raft.Entry) []byte {
	b := make([]byte, 0, 17+len(e.Data))
	b = binary.BigEndian.AppendUint64(b, e.Term)
	b = binary.BigEndian.AppendUint64(b, e.Index)
	b = append(b, byte(e.Type))
	return append(b, e.Data...)
}

func encodeHS(h raft.HardState) []byte {
	b := binary.BigEndian.AppendUint64(nil, h.Term)
	b = binary.BigEndian.AppendUint64(b, h.Vote)
	return binary.BigEndian.AppendUint64(b, h.Commit)
}

func record(kind byte, payload []byte) []byte {
	body := append([]byte{kind}, payload...)
	out := make([]byte, 8, 8+len(body))
	binary.BigEndian.PutUint32(out[0:4], crc32.ChecksumIEEE(body))
	binary.BigEndian.PutUint32(out[4:8], uint32(len(body)))
	return append(out, body...)
}

// OpenStorage reads what a previous run left and returns it with the storage, ready to append.
func OpenStorage(dir string) (*Storage, raft.State, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, raft.State{}, err
	}
	s := &Storage{dir: dir, offsets: map[uint64]int64{}}
	var st raft.State
	snap, err := readSnapshot(s.snapPath())
	if err != nil {
		return nil, st, err
	}
	st.Snapshot = snap
	f, err := os.OpenFile(s.logPath(), os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, st, err
	}
	s.f = f
	r := bufio.NewReader(f)
	var off int64
	var entries []raft.Entry
	for {
		var hdr [8]byte
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			break
		}
		n := binary.BigEndian.Uint32(hdr[4:8])
		if n == 0 || n > 64<<20 {
			break
		}
		body := make([]byte, n)
		if _, err := io.ReadFull(r, body); err != nil {
			break
		}
		if crc32.ChecksumIEEE(body) != binary.BigEndian.Uint32(hdr[0:4]) {
			break
		}
		switch body[0] {
		case recEntry:
			if len(body) < 18 {
				break
			}
			e := raft.Entry{Term: binary.BigEndian.Uint64(body[1:9]), Index: binary.BigEndian.Uint64(body[9:17]),
				Type: raft.EntryType(body[17]), Data: append([]byte(nil), body[18:]...)}
			// A later record for an index replaces the tail from there.
			for len(entries) > 0 && entries[len(entries)-1].Index >= e.Index {
				entries = entries[:len(entries)-1]
			}
			entries = append(entries, e)
			s.offsets[e.Index] = off
		case recHardState:
			if len(body) == 25 {
				s.hs = raft.HardState{Term: binary.BigEndian.Uint64(body[1:9]), Vote: binary.BigEndian.Uint64(body[9:17]),
					Commit: binary.BigEndian.Uint64(body[17:25])}
			}
		}
		off += int64(8 + n)
	}
	// Cut a torn tail so new records follow the last good one.
	if err := f.Truncate(off); err != nil {
		return nil, st, err
	}
	if _, err := f.Seek(off, io.SeekStart); err != nil {
		return nil, st, err
	}
	s.size = off
	// Keep the entries after the snapshot, and only a contiguous run.
	var keep []raft.Entry
	for _, e := range entries {
		if snap != nil && e.Index <= snap.Index {
			continue
		}
		if len(keep) > 0 && e.Index != keep[len(keep)-1].Index+1 {
			break
		}
		if len(keep) == 0 && snap != nil && e.Index != snap.Index+1 {
			break
		}
		keep = append(keep, e)
	}
	st.Entries = keep
	st.HardState = s.hs
	return s, st, nil
}

func (s *Storage) write(b []byte) error {
	if _, err := s.f.WriteAt(b, s.size); err != nil {
		return err
	}
	s.size += int64(len(b))
	return nil
}

// Save persists a Ready's hard state, snapshot and entries, then syncs once.
func (s *Storage) Save(rd raft.Ready) error {
	dirty := false
	if rd.Snapshot != nil {
		if err := s.SaveSnapshot(rd.Snapshot); err != nil {
			return err
		}
	}
	if len(rd.Entries) > 0 {
		first := rd.Entries[0].Index
		if off, ok := s.offsets[first]; ok {
			// Conflicting entries are replaced: cut the file there and restate the hard state.
			if err := s.f.Truncate(off); err != nil {
				return err
			}
			s.size = off
			for i := range s.offsets {
				if i >= first {
					delete(s.offsets, i)
				}
			}
			if err := s.write(record(recHardState, encodeHS(s.hs))); err != nil {
				return err
			}
		}
		buf := make([]byte, 0, 64*len(rd.Entries))
		for _, e := range rd.Entries {
			s.offsets[e.Index] = s.size + int64(len(buf))
			buf = append(buf, record(recEntry, encodeEntry(e))...)
		}
		if err := s.write(buf); err != nil {
			return err
		}
		dirty = true
	}
	if !rd.HardState.IsEmpty() && rd.HardState != s.hs {
		s.hs = rd.HardState
		if err := s.write(record(recHardState, encodeHS(s.hs))); err != nil {
			return err
		}
		dirty = true
	}
	if dirty && !s.NoSync {
		s.Syncs++
		return s.f.Sync()
	}
	return nil
}

type snapFile struct {
	Index  uint64
	Term   uint64
	Config raft.Config
	Data   []byte
}

func readSnapshot(path string) (*raft.Snapshot, error) {
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if len(b) < 4 || crc32.ChecksumIEEE(b[4:]) != binary.BigEndian.Uint32(b[:4]) {
		return nil, errors.New("snapshot file is corrupt")
	}
	var sf snapFile
	if err := json.Unmarshal(b[4:], &sf); err != nil {
		return nil, err
	}
	return &raft.Snapshot{Index: sf.Index, Term: sf.Term, Config: sf.Config, Data: sf.Data}, nil
}

// SaveSnapshot writes a snapshot atomically, then rewrites the log without the entries it
// covers (keeping the hard state).
func (s *Storage) SaveSnapshot(snap *raft.Snapshot) error {
	body, err := json.Marshal(snapFile{Index: snap.Index, Term: snap.Term, Config: snap.Config, Data: snap.Data})
	if err != nil {
		return err
	}
	out := binary.BigEndian.AppendUint32(nil, crc32.ChecksumIEEE(body))
	out = append(out, body...)
	tmp := s.snapPath() + ".tmp"
	if err := writeSynced(tmp, out); err != nil {
		return err
	}
	if err := os.Rename(tmp, s.snapPath()); err != nil {
		return err
	}
	return s.rewriteLog(snap.Index)
}

func writeSynced(path string, b []byte) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// rewriteLog keeps the entries after `after` and the hard state, in a new file put in place
// atomically.
func (s *Storage) rewriteLog(after uint64) error {
	var buf []byte
	buf = append(buf, record(recHardState, encodeHS(s.hs))...)
	offsets := map[uint64]int64{}
	var idx []uint64
	for i := range s.offsets {
		if i > after {
			idx = append(idx, i)
		}
	}
	sortU64(idx)
	for _, i := range idx {
		off := s.offsets[i]
		var hdr [8]byte
		if _, err := s.f.ReadAt(hdr[:], off); err != nil {
			return err
		}
		n := binary.BigEndian.Uint32(hdr[4:8])
		rec := make([]byte, 8+n)
		if _, err := s.f.ReadAt(rec, off); err != nil {
			return err
		}
		offsets[i] = int64(len(buf))
		buf = append(buf, rec...)
	}
	tmp := s.logPath() + ".tmp"
	if err := writeSynced(tmp, buf); err != nil {
		return err
	}
	s.f.Close()
	if err := os.Rename(tmp, s.logPath()); err != nil {
		return err
	}
	f, err := os.OpenFile(s.logPath(), os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	s.f, s.size, s.offsets = f, int64(len(buf)), offsets
	return nil
}

func sortU64(a []uint64) {
	for i := 1; i < len(a); i++ {
		for j := i; j > 0 && a[j] < a[j-1]; j-- {
			a[j], a[j-1] = a[j-1], a[j]
		}
	}
}

func (s *Storage) Close() error { return s.f.Close() }
