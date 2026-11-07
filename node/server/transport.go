package server

import (
	"encoding/gob"
	"errors"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/YUStulinu/Concord/node/raft"
)

// Faults are injected into this node's network: messages to and from blocked peers are
// dropped (a partition, when every node blocks the other side), others are dropped with
// probability Drop, and all are delayed by Delay plus up to Jitter.
type Faults struct {
	Blocked  []uint64 `json:"blocked"`
	Drop     float64  `json:"drop"`
	DelayMs  int      `json:"delay_ms"`
	JitterMs int      `json:"jitter_ms"`
}

func (f *Faults) blocks(id uint64) bool {
	for _, b := range f.Blocked {
		if b == id {
			return true
		}
	}
	return false
}

type envelope struct {
	m  raft.Message
	at time.Time
}

type peer struct {
	id   uint64
	addr string
	q    chan envelope
	stop chan struct{}
}

// Transport carries Raft messages over TCP: one outgoing connection per peer, messages encoded
// with gob, a bounded queue per peer (messages are dropped when it is full -- Raft retries).
type Transport struct {
	id     uint64
	recv   func(raft.Message)
	ln     net.Listener
	mu     sync.Mutex
	peers  map[uint64]*peer
	conns  map[net.Conn]bool // accepted connections, closed with the transport
	faults atomic.Pointer[Faults]
	rng    *rand.Rand
	rngMu  sync.Mutex
	closed atomic.Bool

	Sent, Dropped atomic.Int64
}

func NewTransport(id uint64, addr string, recv func(raft.Message)) (*Transport, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	t := &Transport{id: id, recv: recv, ln: ln, peers: map[uint64]*peer{}, conns: map[net.Conn]bool{}, rng: rand.New(rand.NewSource(time.Now().UnixNano()))}
	t.faults.Store(&Faults{})
	go t.accept()
	return t, nil
}

func (t *Transport) SetFaults(f Faults) { t.faults.Store(&f) }
func (t *Transport) Faults() Faults     { return *t.faults.Load() }

func (t *Transport) chance() float64 {
	t.rngMu.Lock()
	defer t.rngMu.Unlock()
	return t.rng.Float64()
}

func (t *Transport) delay(f *Faults) time.Duration {
	d := time.Duration(f.DelayMs) * time.Millisecond
	if f.JitterMs > 0 {
		t.rngMu.Lock()
		d += time.Duration(t.rng.Intn(f.JitterMs+1)) * time.Millisecond
		t.rngMu.Unlock()
	}
	return d
}

// SetPeers updates the members messages can go to.
func (t *Transport) SetPeers(members []raft.Member) {
	t.mu.Lock()
	defer t.mu.Unlock()
	want := map[uint64]string{}
	for _, m := range members {
		if m.ID != t.id {
			want[m.ID] = m.Raft
		}
	}
	for id, p := range t.peers {
		if addr, ok := want[id]; !ok || addr != p.addr {
			close(p.stop)
			delete(t.peers, id)
		}
	}
	for id, addr := range want {
		if _, ok := t.peers[id]; !ok {
			p := &peer{id: id, addr: addr, q: make(chan envelope, 4096), stop: make(chan struct{})}
			t.peers[id] = p
			go t.sender(p)
		}
	}
}

func (t *Transport) Send(msgs []raft.Message) {
	f := t.faults.Load()
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, m := range msgs {
		p := t.peers[m.To]
		if p == nil || f.blocks(m.To) || (f.Drop > 0 && t.chance() < f.Drop) {
			t.Dropped.Add(1)
			continue
		}
		select {
		case p.q <- envelope{m: m, at: time.Now().Add(t.delay(f))}:
			t.Sent.Add(1)
		default:
			t.Dropped.Add(1)
		}
	}
}

func (t *Transport) sender(p *peer) {
	var conn net.Conn
	var enc *gob.Encoder
	backoff := 10 * time.Millisecond
	for {
		var env envelope
		select {
		case <-p.stop:
			if conn != nil {
				conn.Close()
			}
			return
		case env = <-p.q:
		}
		if d := time.Until(env.at); d > 0 {
			time.Sleep(d)
		}
		if conn == nil {
			c, err := net.DialTimeout("tcp", p.addr, 200*time.Millisecond)
			if err != nil {
				// The peer is down: drop this message (Raft will resend) and back off.
				t.Dropped.Add(1)
				time.Sleep(backoff)
				backoff = min(backoff*2, 500*time.Millisecond)
				continue
			}
			backoff = 10 * time.Millisecond
			if tc, ok := c.(*net.TCPConn); ok {
				tc.SetNoDelay(true)
			}
			conn, enc = c, gob.NewEncoder(c)
			if err := enc.Encode(t.id); err != nil {
				conn.Close()
				conn = nil
				continue
			}
		}
		conn.SetWriteDeadline(time.Now().Add(time.Second))
		if err := enc.Encode(&env.m); err != nil {
			conn.Close()
			conn = nil
		}
	}
}

func (t *Transport) accept() {
	for {
		c, err := t.ln.Accept()
		if err != nil {
			if t.closed.Load() || errors.Is(err, net.ErrClosed) {
				return
			}
			continue
		}
		go t.read(c)
	}
}

func (t *Transport) read(c net.Conn) {
	t.mu.Lock()
	if t.closed.Load() {
		t.mu.Unlock()
		c.Close()
		return
	}
	t.conns[c] = true
	t.mu.Unlock()
	defer func() {
		t.mu.Lock()
		delete(t.conns, c)
		t.mu.Unlock()
		c.Close()
	}()
	dec := gob.NewDecoder(c)
	var from uint64
	if err := dec.Decode(&from); err != nil {
		return
	}
	for {
		var m raft.Message
		if err := dec.Decode(&m); err != nil {
			return
		}
		f := t.faults.Load()
		if f.blocks(m.From) || t.closed.Load() {
			continue
		}
		t.recv(m)
	}
}

func (t *Transport) Close() {
	t.closed.Store(true)
	t.ln.Close()
	t.mu.Lock()
	for id, p := range t.peers {
		close(p.stop)
		delete(t.peers, id)
	}
	// Close incoming connections too, so senders notice and reconnect to whoever listens next.
	for c := range t.conns {
		c.Close()
	}
	t.mu.Unlock()
}
