package server

import (
	"encoding/json"
	"errors"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/YUStulinu/Concord/node/raft"
)

func newRand(seed uint64) *rand.Rand {
	return rand.New(rand.NewSource(time.Now().UnixNano() ^ int64(seed)))
}

// API serves clients and the chaos harness:
//
//	POST /v1/kv                 {"op": "put"|"get"|"delete"|"cas", "key", "value", "expect", "client", "seq"}
//	GET  /v1/status             role, term, log, commit, membership, network faults
//	POST /v1/admin/faults       {"blocked": [ids], "drop": 0.2, "delay_ms": 50, "jitter_ms": 20}
//	POST /v1/admin/clock        {"rate": 2.0}
//	POST /v1/admin/members      {"op": "add"|"remove", "id", "raft", "http"}
//	POST /v1/admin/crash        exits the process at once
//
// Requests to a follower are answered 421 with the leader's address. With UnsafeReads, reads
// are served from whatever this node has applied, skipping ReadIndex -- a deliberate bug the
// linearizability checker should catch.
type API struct {
	Node        *Node
	UnsafeReads bool
	Timeout     time.Duration
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

type kvReply struct {
	Result
	Error  string `json:"error,omitempty"`
	Leader string `json:"leader,omitempty"`
}

func (a *API) notLeader(w http.ResponseWriter) {
	reply := kvReply{Error: "not_leader"}
	if m, ok := a.Node.LeaderMember(); ok {
		reply.Leader = m.HTTP
	}
	writeJSON(w, http.StatusMisdirectedRequest, reply)
}

func (a *API) kv(w http.ResponseWriter, r *http.Request) {
	var c Command
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&c); err != nil || c.Key == "" {
		writeJSON(w, http.StatusBadRequest, kvReply{Error: "bad request"})
		return
	}
	switch c.Op {
	case "get":
		if !a.UnsafeReads {
			if !a.Node.IsLeader() {
				a.notLeader(w)
				return
			}
			if err := a.Node.LinearizableRead(a.Timeout); err != nil {
				a.fail(w, err)
				return
			}
		}
		v, found := a.Node.KV.Get(c.Key)
		writeJSON(w, http.StatusOK, kvReply{Result: Result{OK: true, Value: v, Found: found}})
	case "put", "delete", "cas":
		if !a.Node.IsLeader() {
			a.notLeader(w)
			return
		}
		res, err := a.Node.Propose(c, a.Timeout)
		if err != nil {
			a.fail(w, err)
			return
		}
		writeJSON(w, http.StatusOK, kvReply{Result: res})
	default:
		writeJSON(w, http.StatusBadRequest, kvReply{Error: "unknown op"})
	}
}

// fail tells the client whether the operation certainly did not happen (it may retry) or
// might have (an unknown outcome).
func (a *API) fail(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrNotLeader):
		a.notLeader(w)
	case errors.Is(err, ErrLost):
		writeJSON(w, http.StatusConflict, kvReply{Error: "lost"})
	default:
		writeJSON(w, http.StatusServiceUnavailable, kvReply{Error: "unknown: " + err.Error()})
	}
}

type statusReply struct {
	raft.Status
	Faults    Faults `json:"faults"`
	Keys      int    `json:"keys"`
	Syncs     int64  `json:"syncs"`
	Elections int64  `json:"elections_won"`
	Time      int64  `json:"time_ms"`
}

func (a *API) status(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, statusReply{Status: a.Node.Status(), Faults: a.Node.Faults(), Keys: a.Node.KV.Len(),
		Syncs: a.Node.Syncs(), Elections: a.Node.Elections.Load(), Time: time.Now().UnixMilli()})
}

func (a *API) faults(w http.ResponseWriter, r *http.Request) {
	var f Faults
	if err := json.NewDecoder(r.Body).Decode(&f); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	a.Node.SetFaults(f)
	writeJSON(w, http.StatusOK, f)
}

func (a *API) clock(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Rate float64 `json:"rate"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	a.Node.SetClockRate(req.Rate)
	writeJSON(w, http.StatusOK, req)
}

func (a *API) members(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Op   string `json:"op"`
		ID   uint64 `json:"id"`
		Raft string `json:"raft"`
		HTTP string `json:"http"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.ID == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "send {op, id, raft, http}"})
		return
	}
	if !a.Node.IsLeader() {
		a.notLeader(w)
		return
	}
	cc := raft.ConfChange{Op: raft.AddMember, Member: raft.Member{ID: req.ID, Raft: req.Raft, HTTP: req.HTTP}}
	if req.Op == "remove" {
		cc.Op = raft.RemoveMember
	}
	if err := a.Node.ChangeMembers(cc, a.Timeout); err != nil {
		writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (a *API) Routes() http.Handler {
	if a.Timeout == 0 {
		a.Timeout = 2 * time.Second
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/kv", a.kv)
	mux.HandleFunc("GET /v1/status", a.status)
	mux.HandleFunc("POST /v1/admin/faults", a.faults)
	mux.HandleFunc("POST /v1/admin/clock", a.clock)
	mux.HandleFunc("POST /v1/admin/members", a.members)
	mux.HandleFunc("POST /v1/admin/crash", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]bool{"crashing": true})
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		go func() { time.Sleep(20 * time.Millisecond); os.Exit(1) }()
	})
	// The dashboard is served from another origin.
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if strings.HasPrefix(r.URL.Path, "/v1/") {
			mux.ServeHTTP(w, r)
			return
		}
		http.NotFound(w, r)
	})
}
