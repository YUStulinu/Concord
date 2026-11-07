// concord-bench drives Concord or etcd with the same closed-loop workload and reports
// throughput and latency percentiles.
//
//	concord-bench --target concord --endpoints 127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003 \
//	              --workload write --clients 32 --duration 10s --json out.json
//	concord-bench --target etcd --endpoints 127.0.0.1:2379,... (same flags)
//
// Writes are puts of --value-size bytes to random keys out of --keys; reads are linearizable
// gets (etcd's default; Concord's ReadIndex). Each client sends one request at a time to the
// leader (Concord) or through the official client (etcd), so latency is per request.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type doer interface {
	put(ctx context.Context, key, value string) error
	get(ctx context.Context, key string) error
}

type concord struct {
	http     *http.Client
	mu       sync.Mutex
	leader   string
	followed int // leader changes followed during the run
}

func newConcord(endpoints []string) (*concord, error) {
	c := &concord{http: &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{MaxIdleConnsPerHost: 512}}}
	for _, e := range endpoints {
		r, err := c.http.Get("http://" + e + "/v1/status")
		if err != nil {
			continue
		}
		var st struct {
			Role string `json:"role"`
		}
		json.NewDecoder(r.Body).Decode(&st)
		r.Body.Close()
		if st.Role == "leader" {
			c.leader = e
			return c, nil
		}
	}
	return nil, fmt.Errorf("no leader among %v", endpoints)
}

// do sends to the leader, following a redirect when leadership has moved (as etcd's client
// does on its own).
func (c *concord) do(body map[string]any) error {
	b, _ := json.Marshal(body)
	for try := 0; try < 5; try++ {
		c.mu.Lock()
		leader := c.leader
		c.mu.Unlock()
		r, err := c.http.Post("http://"+leader+"/v1/kv", "application/json", bytes.NewReader(b))
		if err != nil {
			return err
		}
		var reply struct {
			Leader string `json:"leader"`
		}
		json.NewDecoder(r.Body).Decode(&reply)
		io.Copy(io.Discard, r.Body)
		r.Body.Close()
		if r.StatusCode == 200 {
			return nil
		}
		if r.StatusCode == http.StatusMisdirectedRequest && reply.Leader != "" {
			c.mu.Lock()
			if c.leader != reply.Leader {
				c.leader = reply.Leader
				c.followed++
			}
			c.mu.Unlock()
			continue
		}
		if r.StatusCode == http.StatusMisdirectedRequest {
			time.Sleep(20 * time.Millisecond) // an election is under way
			continue
		}
		return fmt.Errorf("status %d", r.StatusCode)
	}
	return fmt.Errorf("no leader")
}

func (c *concord) put(_ context.Context, k, v string) error {
	return c.do(map[string]any{"op": "put", "key": k, "value": v})
}

func (c *concord) get(_ context.Context, k string) error {
	return c.do(map[string]any{"op": "get", "key": k})
}

type etcd struct{ c *clientv3.Client }

func (e *etcd) put(ctx context.Context, k, v string) error {
	_, err := e.c.Put(ctx, k, v)
	return err
}

func (e *etcd) get(ctx context.Context, k string) error {
	_, err := e.c.Get(ctx, k)
	return err
}

func pct(sorted []time.Duration, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	i := int(p * float64(len(sorted)-1))
	return float64(sorted[i].Microseconds()) / 1000
}

func main() {
	target := flag.String("target", "concord", "concord or etcd")
	endpoints := flag.String("endpoints", "127.0.0.1:8001", "comma-separated")
	workload := flag.String("workload", "write", "write, read or mixed (half each)")
	clients := flag.Int("clients", 32, "concurrent clients")
	duration := flag.Duration("duration", 10*time.Second, "measured time")
	warmup := flag.Duration("warmup", 2*time.Second, "unmeasured time first")
	valueSize := flag.Int("value-size", 100, "bytes per value")
	keys := flag.Int("keys", 10000, "key space")
	out := flag.String("json", "", "append a JSON line with the result to this file")
	flag.Parse()
	eps := strings.Split(*endpoints, ",")

	var d doer
	var cc *concord
	switch *target {
	case "concord":
		c, err := newConcord(eps)
		if err != nil {
			log.Fatal(err)
		}
		d, cc = c, c
	case "etcd":
		urls := make([]string, len(eps))
		for i, e := range eps {
			urls[i] = "http://" + e
		}
		c, err := clientv3.New(clientv3.Config{Endpoints: urls, DialTimeout: 5 * time.Second})
		if err != nil {
			log.Fatal(err)
		}
		defer c.Close()
		d = &etcd{c}
	default:
		log.Fatalf("unknown target %q", *target)
	}

	value := strings.Repeat("x", *valueSize)
	// Load the key space so reads find values.
	if *workload != "write" {
		var wg sync.WaitGroup
		for w := 0; w < 32; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				for k := w; k < *keys; k += 32 {
					d.put(context.Background(), fmt.Sprintf("key%06d", k), value)
				}
			}(w)
		}
		wg.Wait()
	}

	var mu sync.Mutex
	var lats []time.Duration
	errs := 0
	start := time.Now()
	measureFrom := start.Add(*warmup)
	end := measureFrom.Add(*duration)
	var wg sync.WaitGroup
	for i := 0; i < *clients; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(i)))
			var mine []time.Duration
			myErrs := 0
			for time.Now().Before(end) {
				key := fmt.Sprintf("key%06d", rng.Intn(*keys))
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				t0 := time.Now()
				var err error
				write := *workload == "write" || (*workload == "mixed" && rng.Intn(2) == 0)
				if write {
					err = d.put(ctx, key, value)
				} else {
					err = d.get(ctx, key)
				}
				cancel()
				if t0.After(measureFrom) {
					if err != nil {
						myErrs++
					} else {
						mine = append(mine, time.Since(t0))
					}
				}
			}
			mu.Lock()
			lats = append(lats, mine...)
			errs += myErrs
			mu.Unlock()
		}(i)
	}
	wg.Wait()
	sort.Slice(lats, func(a, b int) bool { return lats[a] < lats[b] })
	res := map[string]any{
		"target": *target, "workload": *workload, "clients": *clients, "servers": len(eps), "value_size": *valueSize,
		"seconds": duration.Seconds(), "ops": len(lats), "errors": errs,
		"ops_per_s": float64(len(lats)) / duration.Seconds(),
		"p50_ms":    pct(lats, 0.50), "p99_ms": pct(lats, 0.99), "p999_ms": pct(lats, 0.999),
	}
	if cc != nil {
		res["leader_changes_followed"] = cc.followed
	}
	b, _ := json.Marshal(res)
	fmt.Println(string(b))
	if *out != "" {
		f, err := os.OpenFile(*out, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		if err != nil {
			log.Fatal(err)
		}
		f.Write(append(b, '\n'))
		f.Close()
	}
}
