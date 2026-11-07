// concord runs one server of a Concord cluster.
//
//	concord --id 1 --raft 127.0.0.1:7001 --http 127.0.0.1:8001 --data data/n1 \
//	        --cluster 1=127.0.0.1:7001@127.0.0.1:8001,2=...,3=...
//
// --cluster names the founding members (the same list on each); a server added later starts
// with --join and is then added through POST /v1/admin/members on the leader.
package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/YUStulinu/Concord/node/raft"
	"github.com/YUStulinu/Concord/node/server"
)

func parseCluster(s string) ([]raft.Member, error) {
	var out []raft.Member
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		id, rest, ok := strings.Cut(part, "=")
		raftAddr, httpAddr, ok2 := strings.Cut(rest, "@")
		n, err := strconv.ParseUint(id, 10, 64)
		if !ok || !ok2 || err != nil || n == 0 {
			return nil, fmt.Errorf("bad member %q: want id=raft@http", part)
		}
		out = append(out, raft.Member{ID: n, Raft: raftAddr, HTTP: httpAddr})
	}
	return out, nil
}

func main() {
	id := flag.Uint64("id", 1, "server id (>0)")
	raftAddr := flag.String("raft", "127.0.0.1:7001", "address for Raft traffic")
	httpAddr := flag.String("http", "127.0.0.1:8001", "address for clients")
	dataDir := flag.String("data", "", "data directory (default data/n<id>)")
	cluster := flag.String("cluster", "", "founding members: id=raft@http,...")
	join := flag.Bool("join", false, "join an existing cluster instead of founding one")
	tick := flag.Duration("tick", 10*time.Millisecond, "Raft tick")
	election := flag.Int("election-ticks", 15, "election timeout in ticks (randomised up to twice that)")
	snapEvery := flag.Uint64("snapshot-every", 10000, "compact the log after this many entries")
	unsafeReads := flag.Bool("unsafe-reads", false, "serve reads from local state without ReadIndex (a deliberate bug)")
	flag.Parse()
	logger := log.New(os.Stderr, fmt.Sprintf("concord[%d] ", *id), log.LstdFlags|log.Lmicroseconds)
	if *dataDir == "" {
		*dataDir = fmt.Sprintf("data/n%d", *id)
	}
	var boot []raft.Member
	if !*join {
		var err error
		if boot, err = parseCluster(*cluster); err != nil {
			logger.Fatal(err)
		}
		if len(boot) == 0 {
			boot = []raft.Member{{ID: *id, Raft: *raftAddr, HTTP: *httpAddr}}
		}
	}
	n, err := server.Start(server.Config{ID: *id, RaftAddr: *raftAddr, DataDir: *dataDir, Bootstrap: boot, Tick: *tick,
		ElectionTick: *election, SnapshotEvery: *snapEvery, Logger: logger})
	if err != nil {
		logger.Fatal(err)
	}
	api := &server.API{Node: n, UnsafeReads: *unsafeReads}
	logger.Printf("raft on %s, clients on http://%s, data in %s", *raftAddr, *httpAddr, *dataDir)
	srv := &http.Server{Addr: *httpAddr, Handler: api.Routes(), ReadHeaderTimeout: 5 * time.Second}
	logger.Fatal(srv.ListenAndServe())
}
