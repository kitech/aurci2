module github.com/envsh/libp2p-lite-goc

go 1.25.9

require (
	github.com/envsh/toxera/fedkey v0.0.0-20260524011924-6488539daf77
	github.com/gorilla/websocket v1.5.3
	github.com/ipfs/go-cid v0.6.2
	github.com/klauspost/compress v1.19.1
	github.com/libp2p/go-msgio v0.3.0
	github.com/multiformats/go-multibase v0.3.0
	github.com/multiformats/go-multihash v0.2.3
	github.com/pion/logging v0.2.4
	github.com/pion/transport/v2 v2.2.10
	github.com/pion/turn/v2 v2.1.6
	golang.zx2c4.com/wireguard v0.0.0-20231211153847-12269c276173
	lukechampine.com/blake3 v1.4.1
)

require (
	github.com/bep/debounce v1.2.1
	github.com/vishvananda/netlink v1.3.1
)



// require github.com/envsh/toxera/fedkey v0.0.0

// replace github.com/envsh/toxera/fedkey => ../fedkey

replace github.com/libp2p/go-libp2p => ../peerscale/myvendor/go-libp2p

replace github.com/libp2p/go-libp2p-asn-util => ../peerscale/myvendor/go-libp2p-asn-util

//	fiatjaf.com/nostr v0.0.0-20260508234157-a4c590d923ee
require github.com/decred/dcrd/dcrec/secp256k1/v4 v4.4.1 // indirect

require (
	// github.com/envsh/toxera/p2put v0.0.0-20260524151103-ae1cdc3fdc5e
	github.com/libp2p/go-libp2p v0.49.0
	github.com/libp2p/go-libp2p-kad-dht v0.42.2
	github.com/libp2p/go-libp2p-pubsub v0.18.0
	github.com/multiformats/go-multiaddr v0.16.1
	github.com/multiformats/go-multiaddr-dns v0.6.0
)

require (
	github.com/benbjohnson/clock v1.3.5 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/davidlazar/go-crypto v0.0.0-20200604182044-b73af7476f6c // indirect
	github.com/decred/dcrd/crypto/blake256 v1.1.0 // indirect
	github.com/ebitengine/purego v0.10.0 // indirect
	github.com/filecoin-project/go-clock v0.1.0 // indirect
	github.com/flynn/noise v1.1.0 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/gobwas/httphead v0.1.0 // indirect
	github.com/gobwas/pool v0.2.1 // indirect
	github.com/gobwas/ws v1.2.0 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/google/gopacket v1.1.19 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/hashicorp/golang-lru v1.0.2 // indirect
	github.com/hashicorp/golang-lru/v2 v2.0.7 // indirect
	github.com/huin/goupnp v1.3.0 // indirect
	github.com/ipfs/boxo v0.41.0 // indirect
	github.com/ipfs/go-datastore v0.9.2 // indirect
	github.com/ipfs/go-log/v2 v2.9.2 // indirect
	github.com/ipld/go-ipld-prime v0.24.0 // indirect
	github.com/jackpal/go-nat-pmp v1.0.2 // indirect
	github.com/jbenet/go-temp-err-catcher v0.1.0 // indirect
	github.com/josharian/intern v1.0.0 // indirect
	github.com/klauspost/cpuid/v2 v2.4.0 // indirect
	github.com/koron/go-ssdp v0.9.1 // indirect
	github.com/libp2p/go-buffer-pool v0.1.0 // indirect
	github.com/libp2p/go-cidranger v1.1.0 // indirect
	github.com/libp2p/go-flow-metrics v0.3.0 // indirect
	github.com/libp2p/go-libp2p-asn-util v0.4.1 // indirect
	github.com/libp2p/go-libp2p-kbucket v0.9.0 // indirect
	github.com/libp2p/go-libp2p-record v0.3.1 // indirect
	github.com/libp2p/go-libp2p-routing-helpers v0.7.5 // indirect
	github.com/libp2p/go-netroute v0.4.0 // indirect
	github.com/libp2p/go-reuseport v0.4.0 // indirect
	github.com/libp2p/go-yamux/v5 v5.1.0 // indirect
	github.com/mailru/easyjson v0.7.7 // indirect
	github.com/marten-seemann/tcp v0.0.0-20210406111302-dfbc87cc63fd // indirect
	github.com/mattn/go-isatty v0.0.22 // indirect
	github.com/mikioh/tcpinfo v0.0.0-20190314235526-30a79bb1804b // indirect
	github.com/mikioh/tcpopt v0.0.0-20190314235656-172688c1accc // indirect
	github.com/minio/sha256-simd v1.0.1 // indirect
	github.com/mr-tron/base58 v1.3.0 // indirect
	github.com/multiformats/go-base32 v0.1.0 // indirect
	github.com/multiformats/go-base36 v0.2.0 // indirect
	github.com/multiformats/go-multiaddr-fmt v0.1.0 // indirect
	github.com/multiformats/go-multicodec v0.10.0 // indirect
	github.com/multiformats/go-multistream v0.6.1 // indirect
	github.com/multiformats/go-varint v0.1.0 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/nbd-wtf/go-nostr v0.34.0 // indirect
	github.com/pbnjay/memory v0.0.0-20210728143218-7b4eea64cf58 // indirect
	github.com/pion/dtls/v2 v2.2.12 // indirect
	github.com/pion/randutil v0.1.0 // indirect
	github.com/pion/stun v0.6.1 // indirect
	github.com/pion/transport/v3 v3.1.1 // indirect
	github.com/polydawn/refmt v0.90.0 // indirect
	github.com/prometheus/client_golang v1.24.1 // indirect
	github.com/prometheus/client_model v0.6.2 // indirect
	github.com/prometheus/common v0.70.1 // indirect
	github.com/prometheus/procfs v0.21.1 // indirect
	github.com/puzpuzpuz/xsync/v3 v3.0.2 // indirect
	github.com/spaolacci/murmur3 v1.1.0 // indirect
	github.com/tidwall/gjson v1.14.4 // indirect
	github.com/tidwall/match v1.1.1 // indirect
	github.com/tidwall/pretty v1.2.0 // indirect
	github.com/vishvananda/netns v0.0.5 // indirect
	github.com/whyrusleeping/go-keyspace v0.0.0-20160322163242-5b898ac5add1 // indirect
	github.com/wlynxg/anet v0.0.5 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/otel v1.44.0 // indirect
	go.opentelemetry.io/otel/metric v1.44.0 // indirect
	go.opentelemetry.io/otel/trace v1.44.0 // indirect
	go.uber.org/dig v1.19.0 // indirect
	go.uber.org/fx v1.24.0 // indirect
	go.uber.org/mock v0.6.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.28.0 // indirect
	golang.org/x/crypto v0.54.0 // indirect
	golang.org/x/exp v0.0.0-20260718201538-764159d718ef // indirect
	golang.org/x/mod v0.38.0 // indirect
	golang.org/x/net v0.57.0 // indirect
	golang.org/x/sync v0.22.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/telemetry v0.0.0-20260717140457-bdb89881bb75 // indirect
	golang.org/x/time v0.15.0 // indirect
	golang.org/x/tools v0.48.0 // indirect
	golang.zx2c4.com/wintun v0.0.0-20230126152724-0fa3db229ce2 // indirect
	gonum.org/v1/gonum v0.17.0 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)

require (
	// github.com/ProjectZKM/Ziren/crates/go-runtime/zkvm_runtime v0.0.0-20251001021608-1fe7b43fc4d6 // indirect
	github.com/btcsuite/btcd/btcec/v2 v2.3.4 // indirect
	github.com/btcsuite/btcd/chaincfg/chainhash v1.1.0 // indirect
	github.com/kilic/bls12-381 v0.1.0 // indirect
// golang.org/x/crypto v0.44.0
// golang.org/x/exp v0.0.0-20250305212735-054e65f0b394 // indirect
// golang.org/x/sync v0.18.0 // indirect
// golang.org/x/sys v0.40.0 // indirect
)
