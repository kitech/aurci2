// libp2p-lite-goc: C API over the QUIC-free fork of go-libp2p
// (envsh/peerscale myvendor/go-libp2p).  Built as a c-shared library; the C
// contract lives in libp2p-lite-goc.h and mirrors libp2p-rsc's 42 functions.
package main

/*
#cgo CFLAGS: -I${SRCDIR}
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include "libp2p-lite-goc.h"
*/
import "C"

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/p2p/discovery/mdns"
	"github.com/libp2p/go-libp2p/p2p/net/connmgr"
	"github.com/libp2p/go-libp2p/p2p/net/swarm"
	"github.com/libp2p/go-libp2p/p2p/protocol/ping"
	ma "github.com/multiformats/go-multiaddr"
	mh "github.com/multiformats/go-multihash"
)

// ---------------------------------------------------------------------------
// constants / state
// ---------------------------------------------------------------------------

const (
	lp2pOK       = 0
	lp2pErr      = -1
	lp2pEINVAL   = -2
	lp2pEPANIC   = -3
	lp2pETIMEOUT = -4
	eventCap     = 1024
)

type config struct {
	Listen                    []string `json:"listen"`
	EnableMDNS                *bool    `json:"enable_mdns"`
	EnableKad                 *bool    `json:"enable_kad"`
	KadMode                   string   `json:"kad_mode"`
	EnableIdentify            *bool    `json:"enable_identify"`
	EnablePing                *bool    `json:"enable_ping"`
	EnableGossipsub           *bool    `json:"enable_gossipsub"`
	EnableRequestResponse     *bool    `json:"enable_request_response"`
	RequestResponseProtocol   string   `json:"request_response_protocol"`
	EnableRelayClient         *bool    `json:"enable_relay_client"`
	EnableRelayServer         *bool    `json:"enable_relay_server"`
	EnableDCUTR               *bool    `json:"enable_dcutr"`
	EnableAutonat             *bool    `json:"enable_autonat"`
	EnableUPNP                *bool    `json:"enable_upnp"`
	AgentVersion              string   `json:"agent_version"`
	IdleConnectionTimeoutSecs int64    `json:"idle_connection_timeout_secs"`
	PingIntervalSecs          int64    `json:"ping_interval_secs"`
	GossipsubHeartbeatMS      int64    `json:"gossipsub_heartbeat_ms"`
}

type rrSlot struct {
	st   network.Stream
	resp chan []byte // nil = decline
}

type swarmState struct {
	id     int64
	h      host.Host
	ctx    context.Context
	cancel context.CancelFunc
	cfg    config

	ps    *pubsub.PubSub
	dht   *dht.IpfsDHT
	dhtMu sync.Mutex
	ping  *ping.PingService
	mdns  mdns.Service

	events chan string
	cmdSeq int64

	topicsMu sync.Mutex
	topics   map[string]*pubsub.Topic
	subs     map[string]*pubsub.Subscription

	rrProto string
	rrMu    sync.Mutex
	rrSeq   int64
	rrSlots map[int64]*rrSlot

	closeOnce sync.Once
}

var (
	registryMu sync.RWMutex
	registry   = map[int64]*swarmState{}
	swarmSeq   int64

	lastErrMu sync.Mutex
	lastErrC  *C.char
	emptyErrC *C.char
)

// ---------------------------------------------------------------------------
// small helpers
// ---------------------------------------------------------------------------

func enabled(v *bool, def bool) bool {
	if v == nil {
		return def
	}
	return *v
}

func ctxFor(parent context.Context, ms int64) (context.Context, context.CancelFunc) {
	if ms <= 0 { // LP2P_TIMEOUT_FOREVER (and 0 for non-event calls)
		return context.WithCancel(parent)
	}
	return context.WithTimeout(parent, time.Duration(ms)*time.Millisecond)
}

func argStr(p *C.char) (string, bool) {
	if p == nil {
		return "", false
	}
	return C.GoString(p), true
}

func argBytes(p *C.uchar, ln C.size_t) ([]byte, bool) {
	if ln == 0 {
		return []byte{}, true
	}
	if p == nil {
		return nil, false
	}
	return C.GoBytes(unsafe.Pointer(p), C.int(ln)), true
}

func mkRes(code int64, errmsg *C.char, data unsafe.Pointer, ln int) C.l2p_result {
	var r C.l2p_result
	r.code = C.int64_t(code)
	r.errmsg = errmsg
	r.data = data
	r.len = C.size_t(ln)
	return r
}

func setLastError(msg string) {
	lastErrMu.Lock()
	defer lastErrMu.Unlock()
	if lastErrC != nil {
		C.free(unsafe.Pointer(lastErrC))
		lastErrC = nil
	}
	if msg != "" {
		lastErrC = C.CString(msg)
	}
}

func okNull() C.l2p_result {
	setLastError("")
	return mkRes(lp2pOK, nil, nil, 0)
}

func okStr(s string) C.l2p_result {
	setLastError("")
	p := C.CString(s)
	return mkRes(lp2pOK, nil, unsafe.Pointer(p), len(s)+1)
}

func okBytes(b []byte) C.l2p_result {
	setLastError("")
	if len(b) == 0 {
		return mkRes(lp2pOK, nil, nil, 0)
	}
	return mkRes(lp2pOK, nil, C.CBytes(b), len(b))
}

func failCode(code int64, msg string) C.l2p_result {
	setLastError(msg)
	return mkRes(code, C.CString(msg), nil, 0)
}

func failErr(fn string, err error) C.l2p_result {
	return failCode(lp2pErr, fmt.Sprintf("l2p_%s: %v", fn, err))
}

func einval() C.l2p_result {
	setLastError("")
	return mkRes(lp2pEINVAL, nil, nil, 0)
}

func timeoutErr(fn string) C.l2p_result {
	return failCode(lp2pETIMEOUT, fmt.Sprintf("l2p_%s: timeout", fn))
}

func recoverRes(r *C.l2p_result) {
	if p := recover(); p != nil {
		*r = failCode(lp2pEPANIC, fmt.Sprintf("panic: %v (bug, please report)", p))
	}
}

func getSwarm(s *C.l2p_swarm) (*swarmState, bool) {
	if s == nil {
		return nil, false
	}
	id := *(*int64)(unsafe.Pointer(s))
	registryMu.RLock()
	st, ok := registry[id]
	registryMu.RUnlock()
	return st, ok
}

func newHandle(id int64) *C.l2p_swarm {
	p := C.malloc(C.size_t(8))
	*(*int64)(p) = id
	return (*C.l2p_swarm)(p)
}

func (s *swarmState) push(v any) {
	b, err := json.Marshal(v)
	if err != nil {
		return
	}
	select {
	case s.events <- string(b):
	default:
		select {
		case s.events <- `{"type":"overflow"}`:
		default:
		}
	}
}

func (s *swarmState) async(kind string, fn func() (any, error)) string {
	id := atomic.AddInt64(&s.cmdSeq, 1)
	cid := strconv.FormatInt(id, 10)
	go func() {
		v, err := fn()
		ev := map[string]any{"type": "command_result", "command_id": cid, "command": kind}
		if err != nil {
			ev["ok"] = false
			ev["error"] = err.Error()
		} else {
			ev["ok"] = true
			if b, isBytes := v.([]byte); isBytes {
				ev["data_base64"] = base64.StdEncoding.EncodeToString(b)
				ev["len"] = len(b)
			} else if v != nil {
				ev["result"] = v
			}
		}
		s.push(ev)
	}()
	return cid
}

// ---------------------------------------------------------------------------
// keys / host construction
// ---------------------------------------------------------------------------

func parseKey(hexKey string) (crypto.PrivKey, error) {
	b, err := hex.DecodeString(hexKey)
	if err != nil {
		return nil, fmt.Errorf("invalid key hex: %w", err)
	}
	return crypto.UnmarshalPrivateKey(b)
}

func canonicalKey(priv crypto.PrivKey) (string, error) {
	b, err := crypto.MarshalPrivateKey(priv)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

type anyValidator struct{}

func (anyValidator) Validate(key string, value []byte) error { return nil }
func (anyValidator) Select(key string, values [][]byte) (int, error) {
	best := 0
	for i, v := range values {
		if len(v) > len(values[best]) {
			best = i
		}
	}
	return best, nil
}

type mdnsNotifee struct{ s *swarmState }

func (m mdnsNotifee) HandlePeerFound(pi peer.AddrInfo) {
	if pi.ID == "" || m.s.h == nil {
		return
	}
	m.s.push(map[string]any{"type": "mdns_peer_found", "peer_id": pi.ID.String()})
	go func() {
		ctx, cancel := context.WithTimeout(m.s.ctx, 5*time.Second)
		defer cancel()
		_ = m.s.h.Connect(ctx, pi)
	}()
}

func makeDHT(s *swarmState, mode dht.ModeOpt) (*dht.IpfsDHT, error) {
	return dht.New(s.h, dht.Mode(mode), dht.Validator(anyValidator{}))
}

func newSwarm(keyHex, cfgJSON string) (*swarmState, error) {
	var cfg config
	if strings.TrimSpace(cfgJSON) != "" {
		if err := json.Unmarshal([]byte(cfgJSON), &cfg); err != nil {
			return nil, fmt.Errorf("config json: %w", err)
		}
	}
	priv, err := parseKey(keyHex)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	s := &swarmState{
		ctx: ctx, cancel: cancel, cfg: cfg,
		events:  make(chan string, eventCap),
		topics:  map[string]*pubsub.Topic{},
		subs:    map[string]*pubsub.Subscription{},
		rrProto: cfg.RequestResponseProtocol,
		rrSlots: map[int64]*rrSlot{},
	}
	if s.rrProto == "" {
		s.rrProto = "/l2p/req/1"
	}

	listen := cfg.Listen
	if len(listen) == 0 {
		listen = []string{"/ip4/0.0.0.0/tcp/0"}
	}
	opts := []libp2p.Option{
		libp2p.Identity(priv),
		libp2p.ListenAddrStrings(listen...),
	}
	if enabled(cfg.EnableUPNP, true) {
		opts = append(opts, libp2p.NATPortMap())
	}
	if enabled(cfg.EnableRelayClient, false) {
		opts = append(opts, libp2p.EnableRelay())
	}
	if enabled(cfg.EnableRelayServer, false) {
		opts = append(opts, libp2p.EnableRelayService())
	}
	if enabled(cfg.EnableDCUTR, true) {
		opts = append(opts, libp2p.EnableHolePunching())
	}
	if enabled(cfg.EnableAutonat, true) {
		opts = append(opts, libp2p.EnableNATService())
	}
	if cfg.AgentVersion != "" {
		opts = append(opts, libp2p.UserAgent(cfg.AgentVersion))
	}
	if cfg.IdleConnectionTimeoutSecs > 0 {
		grace := time.Duration(cfg.IdleConnectionTimeoutSecs) * time.Second
		opts = append(opts, libp2p.ConnectionManager(
			connmgr.NewConnManager(64, 128, connmgr.WithGracePeriod(grace))))
	}

	h, err := libp2p.New(opts...)
	if err != nil {
		cancel()
		return nil, err
	}
	s.h = h
	h.Network().Notify(&network.NotifyBundle{
		ListenF: func(_ network.Network, a ma.Multiaddr) {
			s.push(map[string]any{"type": "new_listen_addr", "address": a.String()})
		},
		ListenCloseF: func(_ network.Network, a ma.Multiaddr) {
			s.push(map[string]any{"type": "expired_listen_addr", "address": a.String()})
		},
		ConnectedF: func(_ network.Network, c network.Conn) {
			s.push(map[string]any{
				"type": "connection_established", "peer_id": c.RemotePeer().String(),
				"addr": c.RemoteMultiaddr().String(),
			})
		},
		DisconnectedF: func(_ network.Network, c network.Conn) {
			s.push(map[string]any{"type": "connection_closed", "peer_id": c.RemotePeer().String()})
		},
	})

	if enabled(cfg.EnableGossipsub, true) {
		var popts []pubsub.Option
		if cfg.GossipsubHeartbeatMS > 0 {
			params := pubsub.DefaultGossipSubParams()
			params.HeartbeatInterval = time.Duration(cfg.GossipsubHeartbeatMS) * time.Millisecond
			popts = append(popts, pubsub.WithGossipSubParams(params))
		}
		ps, err := pubsub.NewGossipSub(ctx, h, popts...)
		if err != nil {
			cancel()
			_ = h.Close()
			return nil, err
		}
		s.ps = ps
	}

	if enabled(cfg.EnableKad, true) {
		mode := dht.ModeClient
		if cfg.KadMode == "server" {
			mode = dht.ModeServer
		}
		d, err := makeDHT(s, mode)
		if err != nil {
			cancel()
			_ = h.Close()
			return nil, err
		}
		s.dht = d
	}

	if enabled(cfg.EnableMDNS, false) {
		svc := mdns.NewMdnsService(h, "", mdnsNotifee{s: s})
		if err := svc.Start(); err != nil {
			cancel()
			_ = h.Close()
			return nil, err
		}
		s.mdns = svc
	}

	if enabled(cfg.EnableIdentify, true) || enabled(cfg.EnablePing, true) {
		s.ping = ping.NewPingService(h)
	}

	if enabled(cfg.EnableRequestResponse, false) {
		h.SetStreamHandler(protocol.ID(s.rrProto), func(st network.Stream) {
			go s.handleInboundRR(st)
		})
	}

	id := atomic.AddInt64(&swarmSeq, 1)
	s.id = id
	registryMu.Lock()
	registry[id] = s
	registryMu.Unlock()
	return s, nil
}

// ---------------------------------------------------------------------------
// request-response framing
// ---------------------------------------------------------------------------

func readFrame(r io.Reader) ([]byte, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(hdr[:])
	buf := make([]byte, int(n))
	if n > 0 {
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
	}
	return buf, nil
}

func writeFrame(w io.Writer, b []byte) error {
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(b)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	if len(b) > 0 {
		_, err := w.Write(b)
		return err
	}
	return nil
}

func (s *swarmState) handleInboundRR(st network.Stream) {
	_ = st.SetDeadline(time.Now().Add(5 * time.Minute))
	req, err := readFrame(st)
	if err != nil {
		_ = st.Reset()
		return
	}
	chID := atomic.AddInt64(&s.rrSeq, 1)
	slot := &rrSlot{st: st, resp: make(chan []byte, 1)}
	s.rrMu.Lock()
	s.rrSlots[chID] = slot
	s.rrMu.Unlock()
	s.push(map[string]any{
		"type": "request_response_inbound", "channel_id": chID,
		"data_base64": base64.StdEncoding.EncodeToString(req),
	})
	select {
	case resp := <-slot.resp:
		if resp == nil {
			_ = st.Reset()
		} else {
			if err := writeFrame(st, resp); err != nil {
				_ = st.Reset()
			} else {
				_ = st.Close()
			}
		}
	case <-s.ctx.Done():
		_ = st.Reset()
	}
	s.rrMu.Lock()
	delete(s.rrSlots, chID)
	s.rrMu.Unlock()
}

func (s *swarmState) doRequest(ctx context.Context, peerID string, req []byte) ([]byte, error) {
	p, err := peer.Decode(peerID)
	if err != nil {
		return nil, err
	}
	st, err := s.h.NewStream(ctx, p, protocol.ID(s.rrProto))
	if err != nil {
		return nil, err
	}
	if dl, ok := ctx.Deadline(); ok {
		_ = st.SetDeadline(dl)
	}
	if err := writeFrame(st, req); err != nil {
		_ = st.Reset()
		return nil, err
	}
	resp, err := readFrame(st)
	if err != nil {
		_ = st.Reset()
		return nil, err
	}
	_ = st.Close()
	return resp, nil
}

// ---------------------------------------------------------------------------
// per-call cores
// ---------------------------------------------------------------------------

func (s *swarmState) doListen(ctx context.Context, addr string) (string, error) {
	a, err := ma.NewMultiaddr(addr)
	if err != nil {
		return "", err
	}
	if err := s.h.Network().Listen(a); err != nil {
		return "", err
	}
	// With /tcp/0 the assigned port is only known from ListenAddresses().
	for _, la := range s.h.Network().ListenAddresses() {
		if la.String() == addr {
			return addr, nil
		}
		pa := a.Protocols()
		lb := la.Protocols()
		if len(pa) >= 2 && len(lb) >= 2 && pa[0].Code == lb[0].Code &&
			pa[1].Code == lb[1].Code &&
			strings.HasPrefix(la.String(), strings.Split(addr, "/tcp/")[0]) {
			return la.String(), nil
		}
	}
	return addr, nil
}

func (s *swarmState) doDial(ctx context.Context, addr string) error {
	ai, err := peer.AddrInfoFromP2pAddr(mustAddr(addr))
	if err != nil {
		return err
	}
	return s.h.Connect(ctx, *ai)
}

func mustAddr(s string) ma.Multiaddr {
	a, err := ma.NewMultiaddr(s)
	if err != nil {
		return nil
	}
	return a
}

func peerInfo(h host.Host, p peer.ID) (map[string]any, error) {
	ps := h.Peerstore()
	if ps == nil {
		return nil, fmt.Errorf("no peerstore")
	}
	var agent, protoVer string
	if v, err := ps.Get(p, "AgentVersion"); err == nil {
		agent, _ = v.(string)
	}
	if v, err := ps.Get(p, "ProtocolVersion"); err == nil {
		protoVer, _ = v.(string)
	}
	addrs := []string{}
	for _, a := range ps.Addrs(p) {
		addrs = append(addrs, a.String())
	}
	prots := []string{}
	for _, pr := range ps.GetProtocols(p) {
		prots = append(prots, string(pr))
	}
	pub := ""
	if k := ps.PubKey(p); k != nil {
		if b, err := crypto.MarshalPublicKey(k); err == nil {
			pub = hex.EncodeToString(b)
		}
	}
	return map[string]any{
		"peer_id": p.String(), "addrs": addrs, "protocols": prots,
		"agent_version": agent, "protocol_version": protoVer, "public_key": pub,
	}, nil
}

func (s *swarmState) doIdentify(ctx context.Context, pid string) (map[string]any, error) {
	p, err := peer.Decode(pid)
	if err != nil {
		return nil, err
	}
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		info, err := peerInfo(s.h, p)
		if err != nil {
			return nil, err
		}
		if agent, _ := info["agent_version"].(string); agent != "" {
			return info, nil
		}
		select {
		case <-ctx.Done():
			return info, ctx.Err()
		case <-ticker.C:
		}
	}
}

func (s *swarmState) doPing(ctx context.Context, pid string) (string, error) {
	p, err := peer.Decode(pid)
	if err != nil {
		return "", err
	}
	if s.ping == nil {
		return "", fmt.Errorf("ping behavior disabled")
	}
	select {
	case res := <-s.ping.Ping(ctx, p):
		if res.Error != nil {
			return "", res.Error
		}
		return strconv.FormatInt(res.RTT.Milliseconds(), 10), nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func keyString(hexStr string) (string, error) {
	b, err := hex.DecodeString(hexStr)
	if err != nil {
		return "", fmt.Errorf("invalid key hex: %w", err)
	}
	return string(b), nil
}

func keyCID(hexStr string) (cid.Cid, error) {
	b, err := hex.DecodeString(hexStr)
	if err != nil {
		return cid.Undef, fmt.Errorf("invalid key hex: %w", err)
	}
	sum := sha256.Sum256(b)
	m, err := mh.Encode(sum[:], mh.SHA2_256)
	if err != nil {
		return cid.Undef, err
	}
	return cid.NewCidV1(cid.Raw, m), nil
}

// ---------------------------------------------------------------------------
// exported C API - identity
// ---------------------------------------------------------------------------

//export l2p_key_new
func l2p_key_new() (r C.l2p_result) {
	defer recoverRes(&r)
	priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		return failErr("key_new", err)
	}
	k, err := canonicalKey(priv)
	if err != nil {
		return failErr("key_new", err)
	}
	return okStr(k)
}

//export l2p_key_from_bytes
func l2p_key_from_bytes(hexKey *C.char) (r C.l2p_result) {
	defer recoverRes(&r)
	in, ok := argStr(hexKey)
	if !ok {
		return einval()
	}
	priv, err := parseKey(in)
	if err != nil {
		return failErr("key_from_bytes", err)
	}
	k, err := canonicalKey(priv)
	if err != nil {
		return failErr("key_from_bytes", err)
	}
	return okStr(k)
}

//export l2p_key_peer_id
func l2p_key_peer_id(hexKey *C.char) (r C.l2p_result) {
	defer recoverRes(&r)
	in, ok := argStr(hexKey)
	if !ok {
		return einval()
	}
	priv, err := parseKey(in)
	if err != nil {
		return failErr("key_peer_id", err)
	}
	id, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		return failErr("key_peer_id", err)
	}
	return okStr(id.String())
}

// ---------------------------------------------------------------------------
// exported C API - lifecycle & events
// ---------------------------------------------------------------------------

//export l2p_swarm_new
func l2p_swarm_new(hexKey *C.char, cfgJSON *C.char, out **C.l2p_swarm) (r C.l2p_result) {
	defer recoverRes(&r)
	if out == nil {
		return einval()
	}
	*out = nil
	k, ok := argStr(hexKey)
	if !ok {
		return einval()
	}
	cfg := ""
	if cfgJSON != nil {
		cfg = C.GoString(cfgJSON)
	}
	s, err := newSwarm(k, cfg)
	if err != nil {
		return failErr("swarm_new", err)
	}
	*out = newHandle(s.id)
	return okNull()
}

//export l2p_swarm_local_peer_id
func l2p_swarm_local_peer_id(s *C.l2p_swarm) (r C.l2p_result) {
	defer recoverRes(&r)
	st, ok := getSwarm(s)
	if !ok {
		return einval()
	}
	return okStr(st.h.ID().String())
}

//export l2p_swarm_free
func l2p_swarm_free(s *C.l2p_swarm) {
	if s == nil {
		return
	}
	id := *(*int64)(unsafe.Pointer(s))
	registryMu.Lock()
	st := registry[id]
	delete(registry, id)
	registryMu.Unlock()
	C.free(unsafe.Pointer(s))
	if st == nil {
		return
	}
	st.closeOnce.Do(func() {
		st.cancel()
		if st.mdns != nil {
			_ = st.mdns.Close()
		}
		st.dhtMu.Lock()
		if st.dht != nil {
			_ = st.dht.Close()
		}
		st.dhtMu.Unlock()
		if st.h != nil {
			_ = st.h.Close()
		}
	})
}

//export l2p_next_event
func l2p_next_event(s *C.l2p_swarm, timeoutMs C.int64_t) (r C.l2p_result) {
	defer recoverRes(&r)
	st, ok := getSwarm(s)
	if !ok {
		return einval()
	}
	ms := int64(timeoutMs)
	if ms == 0 {
		select {
		case ev := <-st.events:
			return okStr(ev)
		default:
			return okNull()
		}
	}
	var ch <-chan time.Time
	if ms > 0 {
		t := time.NewTimer(time.Duration(ms) * time.Millisecond)
		defer t.Stop()
		ch = t.C
	}
	select {
	case ev := <-st.events:
		return okStr(ev)
	case <-ch:
		return okNull()
	case <-st.ctx.Done():
		return okNull()
	}
}

// ---------------------------------------------------------------------------
// exported C API - network commands
// ---------------------------------------------------------------------------

//export l2p_listen
func l2p_listen(s *C.l2p_swarm, addr *C.char, timeoutMs C.int64_t) (r C.l2p_result) {
	defer recoverRes(&r)
	st, ok := getSwarm(s)
	if !ok {
		return einval()
	}
	a, ok := argStr(addr)
	if !ok {
		return einval()
	}
	ctx, cancel := ctxFor(st.ctx, int64(timeoutMs))
	defer cancel()
	v, err := st.doListen(ctx, a)
	if err != nil {
		return failErr("listen", err)
	}
	return okStr(v)
}

//export l2p_listen_async
func l2p_listen_async(s *C.l2p_swarm, addr *C.char) (r C.l2p_result) {
	defer recoverRes(&r)
	st, ok := getSwarm(s)
	if !ok {
		return einval()
	}
	a, ok := argStr(addr)
	if !ok {
		return einval()
	}
	return okStr(st.async("listen", func() (any, error) {
		return st.doListen(st.ctx, a)
	}))
}

//export l2p_stop_listen
func l2p_stop_listen(s *C.l2p_swarm, addr *C.char) (r C.l2p_result) {
	defer recoverRes(&r)
	st, ok := getSwarm(s)
	if !ok {
		return einval()
	}
	a, ok := argStr(addr)
	if !ok {
		return einval()
	}
	m, err := ma.NewMultiaddr(a)
	if err != nil {
		return failErr("stop_listen", err)
	}
	// This libp2p fork's Network interface has no Listeners(); use the
	// concrete Swarm's ListenClose (closes all listeners covering addr).
	sw, ok := st.h.Network().(*swarm.Swarm)
	if !ok {
		return failCode(lp2pErr, "l2p_stop_listen: unsupported network type")
	}
	found := false
	for _, la := range st.h.Network().ListenAddresses() {
		if la.String() == m.String() {
			found = true
			break
		}
	}
	if !found {
		return failCode(lp2pErr, "l2p_stop_listen: listener not found")
	}
	sw.ListenClose(m)
	return okNull()
}

//export l2p_dial
func l2p_dial(s *C.l2p_swarm, addr *C.char, timeoutMs C.int64_t) (r C.l2p_result) {
	defer recoverRes(&r)
	st, ok := getSwarm(s)
	if !ok {
		return einval()
	}
	a, ok := argStr(addr)
	if !ok {
		return einval()
	}
	ctx, cancel := ctxFor(st.ctx, int64(timeoutMs))
	defer cancel()
	if err := st.doDial(ctx, a); err != nil {
		if ctx.Err() != nil {
			return timeoutErr("dial")
		}
		return failErr("dial", err)
	}
	return okNull()
}

//export l2p_dial_async
func l2p_dial_async(s *C.l2p_swarm, addr *C.char) (r C.l2p_result) {
	defer recoverRes(&r)
	st, ok := getSwarm(s)
	if !ok {
		return einval()
	}
	a, ok := argStr(addr)
	if !ok {
		return einval()
	}
	return okStr(st.async("dial", func() (any, error) {
		return nil, st.doDial(st.ctx, a)
	}))
}

//export l2p_disconnect
func l2p_disconnect(s *C.l2p_swarm, peerID *C.char) (r C.l2p_result) {
	defer recoverRes(&r)
	st, ok := getSwarm(s)
	if !ok {
		return einval()
	}
	in, ok := argStr(peerID)
	if !ok {
		return einval()
	}
	p, err := peer.Decode(in)
	if err != nil {
		return failErr("disconnect", err)
	}
	if err := st.h.Network().ClosePeer(p); err != nil {
		return failErr("disconnect", err)
	}
	return okNull()
}

//export l2p_ping
func l2p_ping(s *C.l2p_swarm, peerID *C.char, timeoutMs C.int64_t) (r C.l2p_result) {
	defer recoverRes(&r)
	st, ok := getSwarm(s)
	if !ok {
		return einval()
	}
	in, ok := argStr(peerID)
	if !ok {
		return einval()
	}
	ctx, cancel := ctxFor(st.ctx, int64(timeoutMs))
	defer cancel()
	v, err := st.doPing(ctx, in)
	if err != nil {
		if ctx.Err() != nil {
			return timeoutErr("ping")
		}
		return failErr("ping", err)
	}
	return okStr(v)
}

//export l2p_ping_async
func l2p_ping_async(s *C.l2p_swarm, peerID *C.char) (r C.l2p_result) {
	defer recoverRes(&r)
	st, ok := getSwarm(s)
	if !ok {
		return einval()
	}
	in, ok := argStr(peerID)
	if !ok {
		return einval()
	}
	return okStr(st.async("ping", func() (any, error) {
		return st.doPing(st.ctx, in)
	}))
}

//export l2p_identify_peer
func l2p_identify_peer(s *C.l2p_swarm, peerID *C.char, timeoutMs C.int64_t) (r C.l2p_result) {
	defer recoverRes(&r)
	st, ok := getSwarm(s)
	if !ok {
		return einval()
	}
	in, ok := argStr(peerID)
	if !ok {
		return einval()
	}
	ctx, cancel := ctxFor(st.ctx, int64(timeoutMs))
	defer cancel()
	v, err := st.doIdentify(ctx, in)
	if err != nil {
		if ctx.Err() != nil {
			return timeoutErr("identify_peer")
		}
		return failErr("identify_peer", err)
	}
	b, _ := json.Marshal(v)
	return okStr(string(b))
}

//export l2p_identify_peer_async
func l2p_identify_peer_async(s *C.l2p_swarm, peerID *C.char) (r C.l2p_result) {
	defer recoverRes(&r)
	st, ok := getSwarm(s)
	if !ok {
		return einval()
	}
	in, ok := argStr(peerID)
	if !ok {
		return einval()
	}
	return okStr(st.async("identify_peer", func() (any, error) {
		v, err := st.doIdentify(st.ctx, in)
		if err != nil {
			return nil, err
		}
		b, _ := json.Marshal(v)
		return string(b), nil
	}))
}

//export l2p_peer_info
func l2p_peer_info(s *C.l2p_swarm, peerID *C.char) (r C.l2p_result) {
	defer recoverRes(&r)
	st, ok := getSwarm(s)
	if !ok {
		return einval()
	}
	in, ok := argStr(peerID)
	if !ok {
		return einval()
	}
	p, err := peer.Decode(in)
	if err != nil {
		return failErr("peer_info", err)
	}
	v, err := peerInfo(st.h, p)
	if err != nil {
		return failErr("peer_info", err)
	}
	b, _ := json.Marshal(v)
	return okStr(string(b))
}

// ---------------------------------------------------------------------------
// exported C API - gossipsub
// ---------------------------------------------------------------------------

//export l2p_gossipsub_subscribe
func l2p_gossipsub_subscribe(s *C.l2p_swarm, topic *C.char) (r C.l2p_result) {
	defer recoverRes(&r)
	st, ok := getSwarm(s)
	if !ok {
		return einval()
	}
	t, ok := argStr(topic)
	if !ok {
		return einval()
	}
	if st.ps == nil {
		return failCode(lp2pErr, "l2p_gossipsub_subscribe: gossipsub disabled")
	}
	st.topicsMu.Lock()
	defer st.topicsMu.Unlock()
	if _, exists := st.subs[t]; exists {
		return okNull()
	}
	tp, err := st.ps.Join(t)
	if err != nil {
		return failErr("gossipsub_subscribe", err)
	}
	sub, err := tp.Subscribe()
	if err != nil {
		return failErr("gossipsub_subscribe", err)
	}
	st.topics[t] = tp
	st.subs[t] = sub
	go func() {
		for {
			msg, err := sub.Next(st.ctx)
			if err != nil {
				return
			}
			st.push(map[string]any{
				"type": "gossipsub_message", "topic": t,
				"from":        msg.GetFrom().String(),
				"data_base64": base64.StdEncoding.EncodeToString(msg.GetData()),
			})
		}
	}()
	return okNull()
}

//export l2p_gossipsub_unsubscribe
func l2p_gossipsub_unsubscribe(s *C.l2p_swarm, topic *C.char) (r C.l2p_result) {
	defer recoverRes(&r)
	st, ok := getSwarm(s)
	if !ok {
		return einval()
	}
	t, ok := argStr(topic)
	if !ok {
		return einval()
	}
	st.topicsMu.Lock()
	defer st.topicsMu.Unlock()
	tp, exists := st.topics[t]
	if !exists {
		return okNull()
	}
	delete(st.topics, t)
	delete(st.subs, t)
	if err := tp.Close(); err != nil {
		return failErr("gossipsub_unsubscribe", err)
	}
	return okNull()
}

//export l2p_gossipsub_publish
func l2p_gossipsub_publish(s *C.l2p_swarm, topic *C.char, data *C.uchar, ln C.size_t) (r C.l2p_result) {
	defer recoverRes(&r)
	st, ok := getSwarm(s)
	if !ok {
		return einval()
	}
	t, ok := argStr(topic)
	if !ok {
		return einval()
	}
	b, ok := argBytes(data, ln)
	if !ok {
		return einval()
	}
	if st.ps == nil {
		return failCode(lp2pErr, "l2p_gossipsub_publish: gossipsub disabled")
	}
	if err := st.ps.Publish(t, b); err != nil {
		return failErr("gossipsub_publish", err)
	}
	return okNull()
}

// ---------------------------------------------------------------------------
// exported C API - request-response
// ---------------------------------------------------------------------------

//export l2p_request
func l2p_request(s *C.l2p_swarm, peerID *C.char, req *C.uchar, ln C.size_t, timeoutMs C.int64_t) (r C.l2p_result) {
	defer recoverRes(&r)
	st, ok := getSwarm(s)
	if !ok {
		return einval()
	}
	pid, ok := argStr(peerID)
	if !ok {
		return einval()
	}
	b, ok := argBytes(req, ln)
	if !ok {
		return einval()
	}
	ctx, cancel := ctxFor(st.ctx, int64(timeoutMs))
	defer cancel()
	v, err := st.doRequest(ctx, pid, b)
	if err != nil {
		if ctx.Err() != nil {
			return timeoutErr("request")
		}
		return failErr("request", err)
	}
	return okBytes(v)
}

//export l2p_request_async
func l2p_request_async(s *C.l2p_swarm, peerID *C.char, req *C.uchar, ln C.size_t) (r C.l2p_result) {
	defer recoverRes(&r)
	st, ok := getSwarm(s)
	if !ok {
		return einval()
	}
	pid, ok := argStr(peerID)
	if !ok {
		return einval()
	}
	b, ok := argBytes(req, ln)
	if !ok {
		return einval()
	}
	cp := append([]byte(nil), b...)
	return okStr(st.async("request", func() (any, error) {
		return st.doRequest(st.ctx, pid, cp)
	}))
}

//export l2p_response
func l2p_response(s *C.l2p_swarm, channelID C.int64_t, resp *C.uchar, ln C.size_t) (r C.l2p_result) {
	defer recoverRes(&r)
	st, ok := getSwarm(s)
	if !ok {
		return einval()
	}
	b, ok := argBytes(resp, ln)
	if !ok {
		return einval()
	}
	st.rrMu.Lock()
	slot, exists := st.rrSlots[int64(channelID)]
	st.rrMu.Unlock()
	if !exists {
		return failCode(lp2pErr, "l2p_response: unknown channel_id")
	}
	cp := append([]byte(nil), b...)
	if len(cp) == 0 { // decline
		select {
		case slot.resp <- nil:
		default:
		}
		return okNull()
	}
	select {
	case slot.resp <- cp:
	default:
		return failCode(lp2pErr, "l2p_response: channel already answered")
	}
	return okNull()
}

// ---------------------------------------------------------------------------
// exported C API - kad DHT
// ---------------------------------------------------------------------------

//export l2p_kad_add_address
func l2p_kad_add_address(s *C.l2p_swarm, peerID *C.char, addr *C.char) (r C.l2p_result) {
	defer recoverRes(&r)
	st, ok := getSwarm(s)
	if !ok {
		return einval()
	}
	pid, ok := argStr(peerID)
	if !ok {
		return einval()
	}
	a, ok := argStr(addr)
	if !ok {
		return einval()
	}
	p, err := peer.Decode(pid)
	if err != nil {
		return failErr("kad_add_address", err)
	}
	m, err := ma.NewMultiaddr(a)
	if err != nil {
		return failErr("kad_add_address", err)
	}
	st.h.Peerstore().AddAddrs(p, []ma.Multiaddr{m}, peerstore.PermanentAddrTTL)
	return okNull()
}

//export l2p_kad_bootstrap
func l2p_kad_bootstrap(s *C.l2p_swarm, timeoutMs C.int64_t) (r C.l2p_result) {
	defer recoverRes(&r)
	st, ok := getSwarm(s)
	if !ok {
		return einval()
	}
	if st.dht == nil {
		return failCode(lp2pErr, "l2p_kad_bootstrap: kad disabled")
	}
	ctx, cancel := ctxFor(st.ctx, int64(timeoutMs))
	defer cancel()
	if err := st.dht.Bootstrap(ctx); err != nil {
		if ctx.Err() != nil {
			return timeoutErr("kad_bootstrap")
		}
		return failErr("kad_bootstrap", err)
	}
	return okNull()
}

//export l2p_kad_bootstrap_async
func l2p_kad_bootstrap_async(s *C.l2p_swarm) (r C.l2p_result) {
	defer recoverRes(&r)
	st, ok := getSwarm(s)
	if !ok {
		return einval()
	}
	if st.dht == nil {
		return failCode(lp2pErr, "l2p_kad_bootstrap_async: kad disabled")
	}
	return okStr(st.async("kad_bootstrap", func() (any, error) {
		return nil, st.dht.Bootstrap(st.ctx)
	}))
}

func (s *swarmState) doGetRecord(ctx context.Context, hexKey string) ([]byte, error) {
	k, err := keyString(hexKey)
	if err != nil {
		return nil, err
	}
	if s.dht == nil {
		return nil, fmt.Errorf("kad disabled")
	}
	return s.dht.GetValue(ctx, k)
}

//export l2p_kad_get_record
func l2p_kad_get_record(s *C.l2p_swarm, hexKey *C.char, timeoutMs C.int64_t) (r C.l2p_result) {
	defer recoverRes(&r)
	st, ok := getSwarm(s)
	if !ok {
		return einval()
	}
	k, ok := argStr(hexKey)
	if !ok {
		return einval()
	}
	ctx, cancel := ctxFor(st.ctx, int64(timeoutMs))
	defer cancel()
	v, err := st.doGetRecord(ctx, k)
	if err != nil {
		if ctx.Err() != nil {
			return timeoutErr("kad_get_record")
		}
		return failErr("kad_get_record", err)
	}
	return okBytes(v)
}

//export l2p_kad_get_record_async
func l2p_kad_get_record_async(s *C.l2p_swarm, hexKey *C.char) (r C.l2p_result) {
	defer recoverRes(&r)
	st, ok := getSwarm(s)
	if !ok {
		return einval()
	}
	k, ok := argStr(hexKey)
	if !ok {
		return einval()
	}
	return okStr(st.async("kad_get_record", func() (any, error) {
		return st.doGetRecord(st.ctx, k)
	}))
}

func (s *swarmState) doPutRecord(ctx context.Context, hexKey string, val []byte) error {
	k, err := keyString(hexKey)
	if err != nil {
		return err
	}
	if s.dht == nil {
		return fmt.Errorf("kad disabled")
	}
	return s.dht.PutValue(ctx, k, val)
}

//export l2p_kad_put_record
func l2p_kad_put_record(s *C.l2p_swarm, hexKey *C.char, val *C.uchar, ln C.size_t, timeoutMs C.int64_t) (r C.l2p_result) {
	defer recoverRes(&r)
	st, ok := getSwarm(s)
	if !ok {
		return einval()
	}
	k, ok := argStr(hexKey)
	if !ok {
		return einval()
	}
	b, ok := argBytes(val, ln)
	if !ok {
		return einval()
	}
	ctx, cancel := ctxFor(st.ctx, int64(timeoutMs))
	defer cancel()
	if err := st.doPutRecord(ctx, k, b); err != nil {
		if ctx.Err() != nil {
			return timeoutErr("kad_put_record")
		}
		return failErr("kad_put_record", err)
	}
	return okNull()
}

//export l2p_kad_put_record_async
func l2p_kad_put_record_async(s *C.l2p_swarm, hexKey *C.char, val *C.uchar, ln C.size_t) (r C.l2p_result) {
	defer recoverRes(&r)
	st, ok := getSwarm(s)
	if !ok {
		return einval()
	}
	k, ok := argStr(hexKey)
	if !ok {
		return einval()
	}
	b, ok := argBytes(val, ln)
	if !ok {
		return einval()
	}
	cp := append([]byte(nil), b...)
	return okStr(st.async("kad_put_record", func() (any, error) {
		return nil, st.doPutRecord(st.ctx, k, cp)
	}))
}

func (s *swarmState) doGetClosest(ctx context.Context, target string) (string, error) {
	if s.dht == nil {
		return "", fmt.Errorf("kad disabled")
	}
	ps, err := s.dht.GetClosestPeers(ctx, target)
	if err != nil {
		return "", err
	}
	ids := make([]string, 0, len(ps))
	for _, p := range ps {
		ids = append(ids, p.String())
	}
	b, err := json.Marshal(ids)
	return string(b), err
}

//export l2p_kad_get_closest_peers
func l2p_kad_get_closest_peers(s *C.l2p_swarm, peerID *C.char, timeoutMs C.int64_t) (r C.l2p_result) {
	defer recoverRes(&r)
	st, ok := getSwarm(s)
	if !ok {
		return einval()
	}
	in, ok := argStr(peerID)
	if !ok {
		return einval()
	}
	ctx, cancel := ctxFor(st.ctx, int64(timeoutMs))
	defer cancel()
	v, err := st.doGetClosest(ctx, in)
	if err != nil {
		if ctx.Err() != nil {
			return timeoutErr("kad_get_closest_peers")
		}
		return failErr("kad_get_closest_peers", err)
	}
	return okStr(v)
}

//export l2p_kad_get_closest_peers_async
func l2p_kad_get_closest_peers_async(s *C.l2p_swarm, peerID *C.char) (r C.l2p_result) {
	defer recoverRes(&r)
	st, ok := getSwarm(s)
	if !ok {
		return einval()
	}
	in, ok := argStr(peerID)
	if !ok {
		return einval()
	}
	return okStr(st.async("kad_get_closest_peers", func() (any, error) {
		return st.doGetClosest(st.ctx, in)
	}))
}

//export l2p_kad_start_providing
func l2p_kad_start_providing(s *C.l2p_swarm, hexKey *C.char) (r C.l2p_result) {
	defer recoverRes(&r)
	st, ok := getSwarm(s)
	if !ok {
		return einval()
	}
	k, ok := argStr(hexKey)
	if !ok {
		return einval()
	}
	if st.dht == nil {
		return failCode(lp2pErr, "l2p_kad_start_providing: kad disabled")
	}
	c, err := keyCID(k)
	if err != nil {
		return failErr("kad_start_providing", err)
	}
	ctx, cancel := ctxFor(st.ctx, 30_000)
	defer cancel()
	if err := st.dht.Provide(ctx, c, true); err != nil {
		return failErr("kad_start_providing", err)
	}
	return okNull()
}

//export l2p_kad_stop_providing
func l2p_kad_stop_providing(s *C.l2p_swarm, hexKey *C.char) (r C.l2p_result) {
	defer recoverRes(&r)
	st, ok := getSwarm(s)
	if !ok {
		return einval()
	}
	k, ok := argStr(hexKey)
	if !ok {
		return einval()
	}
	if _, err := keyCID(k); err != nil { // validate hex for API parity
		return failErr("kad_stop_providing", err)
	}
	// go-libp2p-kad-dht has no "stop providing" API: provider records expire
	// from the store by TTL.  Accept and report success (no-op).
	return okNull()
}

//export l2p_kad_set_mode
func l2p_kad_set_mode(s *C.l2p_swarm, mode *C.char) (r C.l2p_result) {
	defer recoverRes(&r)
	st, ok := getSwarm(s)
	if !ok {
		return einval()
	}
	m, ok := argStr(mode)
	if !ok {
		return einval()
	}
	var want dht.ModeOpt
	switch m {
	case "server":
		want = dht.ModeServer
	case "client":
		want = dht.ModeClient
	default:
		return failCode(lp2pEINVAL, "l2p_kad_set_mode: invalid mode")
	}
	st.dhtMu.Lock()
	defer st.dhtMu.Unlock()
	if st.dht == nil {
		return failCode(lp2pErr, "l2p_kad_set_mode: kad disabled")
	}
	if st.dht.Mode() == want {
		return okNull()
	}
	if err := st.dht.Close(); err != nil {
		return failErr("kad_set_mode", err)
	}
	d, err := makeDHT(st, want)
	if err != nil {
		return failErr("kad_set_mode", err)
	}
	st.dht = d
	return okNull()
}

// ---------------------------------------------------------------------------
// exported C API - relay
// ---------------------------------------------------------------------------

//export l2p_relay_reserve
func l2p_relay_reserve(s *C.l2p_swarm, relayPeerID *C.char, timeoutMs C.int64_t) (r C.l2p_result) {
	defer recoverRes(&r)
	st, ok := getSwarm(s)
	if !ok {
		return einval()
	}
	in, ok := argStr(relayPeerID)
	if !ok {
		return einval()
	}
	p, err := peer.Decode(in)
	if err != nil {
		return failErr("relay_reserve", err)
	}
	ctx, cancel := ctxFor(st.ctx, int64(timeoutMs))
	defer cancel()
	// Dial the relay so its addresses are known and a direct connection exists.
	addrs := st.h.Peerstore().Addrs(p)
	if len(addrs) == 0 {
		return failCode(lp2pErr, "l2p_relay_reserve: no relay addresses; add them with l2p_kad_add_address")
	}
	if conns := st.h.Network().ConnsToPeer(p); len(conns) == 0 {
		if err := st.h.Connect(ctx, peer.AddrInfo{ID: p, Addrs: addrs}); err != nil {
			return failErr("relay_reserve", err)
		}
	}
	base := addrs[0].String()
	circuit := base
	if !strings.HasSuffix(circuit, "/p2p/"+p.String()) {
		circuit += "/p2p/" + p.String()
	}
	circuit += "/p2p-circuit"
	a, err := ma.NewMultiaddr(circuit)
	if err != nil {
		return failErr("relay_reserve", err)
	}
	if err := st.h.Network().Listen(a); err != nil {
		return failErr("relay_reserve", err)
	}
	return okNull()
}

//export l2p_relay_cancel
func l2p_relay_cancel(s *C.l2p_swarm, relayPeerID *C.char) (r C.l2p_result) {
	defer recoverRes(&r)
	st, ok := getSwarm(s)
	if !ok {
		return einval()
	}
	in, ok := argStr(relayPeerID)
	if !ok {
		return einval()
	}
	p, err := peer.Decode(in)
	if err != nil {
		return failErr("relay_cancel", err)
	}
	if err := st.h.Network().ClosePeer(p); err != nil {
		return failErr("relay_cancel", err)
	}
	return okNull()
}

// ---------------------------------------------------------------------------
// exported C API - memory / errors
// ---------------------------------------------------------------------------

//export l2p_free_result
func l2p_free_result(r *C.l2p_result) {
	if r == nil {
		return
	}
	if r.errmsg != nil {
		C.free(unsafe.Pointer(r.errmsg))
		r.errmsg = nil
	}
	if r.data != nil {
		C.free(r.data)
		r.data = nil
	}
	r.code = 0
	r.len = 0
}

//export l2p_free_string
func l2p_free_string(s *C.char) {
	if s != nil {
		C.free(unsafe.Pointer(s))
	}
}

//export l2p_free_bytes
func l2p_free_bytes(p unsafe.Pointer, ln C.size_t) {
	if p != nil {
		C.free(p)
	}
}

//export l2p_last_error
func l2p_last_error() *C.char {
	lastErrMu.Lock()
	defer lastErrMu.Unlock()
	if lastErrC != nil {
		return lastErrC
	}
	if emptyErrC == nil {
		emptyErrC = C.CString("")
	}
	return emptyErrC
}

func main() {}
