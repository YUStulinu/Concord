package server

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/YUStulinu/Concord/node/raft"
)

func TestStorageRecoversAndCutsATornTail(t *testing.T) {
	dir := t.TempDir()
	s, st, err := OpenStorage(dir)
	if err != nil || len(st.Entries) != 0 {
		t.Fatal(err)
	}
	var ents []raft.Entry
	for i := uint64(1); i <= 5; i++ {
		ents = append(ents, raft.Entry{Term: 1, Index: i, Data: []byte(fmt.Sprint("e", i))})
	}
	if err := s.Save(raft.Ready{HardState: raft.HardState{Term: 1, Vote: 2, Commit: 3}, Entries: ents}); err != nil {
		t.Fatal(err)
	}
	// A conflicting tail from a new leader: entries 4.. replaced in term 2, vote changes.
	if err := s.Save(raft.Ready{HardState: raft.HardState{Term: 2, Vote: 3, Commit: 3}, Entries: []raft.Entry{{Term: 2, Index: 4, Data: []byte("x")}}}); err != nil {
		t.Fatal(err)
	}
	s.Close()
	// Tear the file: a half-written record at the end.
	f, _ := os.OpenFile(filepath.Join(dir, "log"), os.O_APPEND|os.O_WRONLY, 0)
	f.Write([]byte{0, 0, 0, 1, 0, 0, 0, 40, 1, 2, 3})
	f.Close()
	s, st, err = OpenStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(st.Entries) != 4 || st.Entries[3].Term != 2 || string(st.Entries[3].Data) != "x" {
		t.Fatalf("entries after recovery: %+v", st.Entries)
	}
	if st.HardState != (raft.HardState{Term: 2, Vote: 3, Commit: 3}) {
		t.Fatalf("hard state: %+v", st.HardState)
	}
	// Truncating must not lose a vote recorded after the truncated entry.
	if err := s.Save(raft.Ready{HardState: raft.HardState{Term: 3, Vote: 1, Commit: 3}}); err != nil {
		t.Fatal(err)
	}
	if err := s.Save(raft.Ready{Entries: []raft.Entry{{Term: 3, Index: 4, Data: []byte("y")}}}); err != nil {
		t.Fatal(err)
	}
	s.Close()
	s, st, _ = OpenStorage(dir)
	if st.HardState.Vote != 1 || st.HardState.Term != 3 || string(st.Entries[3].Data) != "y" {
		t.Fatalf("after truncation: %+v %+v", st.HardState, st.Entries)
	}
	// A snapshot replaces the entries it covers.
	snap := &raft.Snapshot{Index: 3, Term: 1, Config: raft.Config{Members: []raft.Member{{ID: 1}}}, Data: []byte("state")}
	if err := s.SaveSnapshot(snap); err != nil {
		t.Fatal(err)
	}
	s.Close()
	s, st, _ = OpenStorage(dir)
	defer s.Close()
	if st.Snapshot == nil || st.Snapshot.Index != 3 || string(st.Snapshot.Data) != "state" || len(st.Entries) != 1 || st.Entries[0].Index != 4 {
		t.Fatalf("after snapshot: %+v %+v", st.Snapshot, st.Entries)
	}
}

func TestKVSessionsMakeRetriesHarmless(t *testing.T) {
	kv := NewKV()
	if r := kv.Apply(Command{Op: "cas", Key: "a", Value: "1", Client: 7, Seq: 1}); !r.OK {
		t.Fatal("cas on an absent key with no expectation should succeed")
	}
	kv.Apply(Command{Op: "put", Key: "a", Value: "2", Client: 8, Seq: 1})
	// The retry of client 7's request returns the original result and changes nothing.
	if r := kv.Apply(Command{Op: "cas", Key: "a", Value: "1", Client: 7, Seq: 1}); !r.OK {
		t.Fatal("a retry should return the first result")
	}
	if v, _ := kv.Get("a"); v != "2" {
		t.Fatalf("retry was applied again: %q", v)
	}
	one := "2"
	if r := kv.Apply(Command{Op: "cas", Key: "a", Value: "3", Expect: &one, Client: 7, Seq: 2}); !r.OK {
		t.Fatal(r)
	}
	restored := NewKV()
	restored.Restore(kv.Snapshot())
	if v, _ := restored.Get("a"); v != "3" {
		t.Fatalf("snapshot lost data: %q", v)
	}
	if r := restored.Apply(Command{Op: "put", Key: "a", Value: "z", Client: 7, Seq: 2}); !r.OK || restored.data["a"] != "3" {
		t.Fatal("the session table must survive a snapshot")
	}
}

func freeAddr(t *testing.T) string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

// Three real nodes over TCP in one process: replication, leader failover, restart from disk.
func TestClusterFailsOverAndRecovers(t *testing.T) {
	dir := t.TempDir()
	var members []raft.Member
	for i := 1; i <= 3; i++ {
		members = append(members, raft.Member{ID: uint64(i), Raft: freeAddr(t), HTTP: "unused"})
	}
	start := func(i int) *Node {
		n, err := Start(Config{ID: uint64(i), RaftAddr: members[i-1].Raft, DataDir: filepath.Join(dir, fmt.Sprint(i)),
			Bootstrap: members, Tick: 5 * time.Millisecond, SnapshotEvery: 50})
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	nodes := map[int]*Node{}
	for i := 1; i <= 3; i++ {
		nodes[i] = start(i)
	}
	defer func() {
		for _, n := range nodes {
			if n != nil {
				n.Stop()
			}
		}
	}()
	leader := func() (int, *Node) {
		for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
			for i, n := range nodes {
				if n != nil && n.IsLeader() {
					return i, n
				}
			}
		}
		t.Fatal("no leader")
		return 0, nil
	}
	put := func(k, v string) {
		for try := 0; try < 50; try++ {
			_, l := leader()
			if _, err := l.Propose(Command{Op: "put", Key: k, Value: v}, time.Second); err == nil {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Fatalf("put %s failed", k)
	}
	for i := 0; i < 120; i++ { // enough to trigger snapshots
		put(fmt.Sprint("k", i%10), fmt.Sprint(i))
	}
	li, l := leader()
	if err := l.LinearizableRead(time.Second); err != nil {
		t.Fatal(err)
	}
	if v, _ := l.KV.Get("k9"); v != "119" {
		t.Fatalf("k9 = %q", v)
	}
	// Kill the leader; the others elect a new one and keep going.
	l.Stop()
	nodes[li] = nil
	put("after", "failover")
	// Restart it from disk: it catches up (by log or snapshot) with the writes it missed.
	nodes[li] = start(li)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if v, _ := nodes[li].KV.Get("after"); v == "failover" {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	for i, n := range nodes {
		st := n.Status()
		t.Logf("node %d: %s term %d lead %d commit %d applied %d last %d snap %d members %d", i, st.Role, st.Term, st.Lead, st.Commit, st.Applied, st.LastIndex, st.SnapIndex, len(st.Members))
	}
	t.Fatal("restarted node did not catch up")
}
