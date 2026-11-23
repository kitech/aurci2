/*
 * libp2p-rsc.h - C API for the Rust libp2p crate (0.58), via the
 * libp2p-rsc package.  Full contract: /usr/share/doc/libp2p-rsc/detail.md
 * (source package ships detail.md next to the crate).
 *
 * Model: every swarm runs a background task; commands go in through the
 * API (blocking variants await the final result, `_async` variants return
 * a command_id string), and all completions are additionally delivered as
 * JSON events via l2p_next_event().
 *
 * Encodings: function binary args are raw bytes + len; event payloads are
 * base64 ("*_base64"); keys and kad keys are hex ("*_hex"); PeerId,
 * Multiaddr, topic and mode are UTF-8 strings.
 *
 * Naming: Cargo-internal library name is p2p_rsc; the installed files are
 * libp2p-rsc.so / libp2p-rsc.a (link with -lp2p-rsc).
 */
#ifndef LIBP2P_RSC_H
#define LIBP2P_RSC_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ---- error codes (l2p_result.code) -------------------------------------- */

#define LP2P_OK         0   /* success */
#define LP2P_ERR       -1   /* failed (network/protocol/parse); errmsg set */
#define LP2P_EINVAL    -2   /* NULL handle/ptr or bad enum; errmsg NOT set */
#define LP2P_EPANIC    -3   /* Rust panic caught at FFI boundary; errmsg set */
#define LP2P_ETIMEOUT  -4   /* blocking wait timed out; errmsg set;
                            * the result is NOT lost: it still arrives as a
                            * {"type":"command_result"} event */

/* l2p_* timeout_ms argument: < 0 = wait forever (LP2P_TIMEOUT_FOREVER),
 * > 0 = wait that many milliseconds; 0 is only valid for l2p_next_event
 * (non-blocking poll). */
#define LP2P_TIMEOUT_FOREVER (-1)

/* ---- opaque handles ------------------------------------------------------ */

typedef struct l2p_swarm l2p_swarm;

/* ---- result struct (returned by value) ----------------------------------- */

typedef struct l2p_result {
    int64_t  code;    /* LP2P_* */
    char    *errmsg;  /* NULL on LP2P_OK / LP2P_EINVAL */
    void    *data;    /* see per-function table below; NULL unless noted */
    size_t   len;
} l2p_result;

/*
 * data/len semantics by function:
 *
 *   l2p_key_new / l2p_key_from_bytes   canonical hex private key (NUL-term.) len=strlen+1
 *   l2p_key_peer_id                    PeerId string (NUL-terminated)      len=strlen+1
 *   l2p_swarm_local_peer_id            PeerId string (NUL-terminated)      len=strlen+1
 *   l2p_peer_info                      identify info JSON (NUL-terminated) len=strlen+1
 *   l2p_*_async                        command_id decimal string           len=strlen+1
 *   l2p_listen                         confirmed listen multiaddr          len=strlen+1
 *   l2p_ping                           RTT milliseconds decimal string     len=strlen+1
 *   l2p_identify_peer                  identify info JSON                  len=strlen+1
 *   l2p_kad_get_record                 record value raw bytes              len=bytes
 *   l2p_kad_get_closest_peers          JSON array of peer ids              len=strlen+1
 *   l2p_next_event                     JSON event; no event = data NULL    len=strlen+1
 *   everything else on success, and
 *   all functions on failure           data NULL                            len=0
 *
 * Ownership: l2p_free_result() releases errmsg and data and zeroes the
 * struct (reusable).  It must not be mixed with l2p_free_string /
 * l2p_free_bytes (double free).
 */

/* ---- dual-track error reporting ------------------------------------------
 * Besides l2p_result.errmsg (a copy you must free), the library keeps a
 * thread-local message readable with l2p_last_error() (no free needed;
 * invalidated by the next call on the same thread).  Format:
 *   "l2p_<fn>(...): <original error>"
 *   "l2p_<fn>: panic (bug, please report)"
 */

/* ---- identity keys (ed25519 only; hex = libp2p-identity protobuf
 *       encoding of the private key) --------------------------------------- */

l2p_result l2p_key_new(void);
l2p_result l2p_key_from_bytes(const char *hex);
l2p_result l2p_key_peer_id(const char *key_hex);

/* ---- swarm lifecycle ------------------------------------------------------
 * config_json: all fields optional ({"listen":[...], "enable_mdns":false,
 * "enable_kad":true, "kad_mode":"client", "enable_identify":true,
 * "enable_ping":true, "enable_gossipsub":true,
 * "enable_request_response":false, "request_response_protocol":"/app/req/1",
 * "enable_relay_client":false, "enable_relay_server":false,
 * "enable_dcutr":true, "enable_autonat":true, "enable_upnp":true,
 * "agent_version":"l2p-app/1.0", "idle_connection_timeout_secs":0,
 * "ping_interval_secs":15, "gossipsub_heartbeat_ms":1000}).
 * idle_connection_timeout_secs: 0 = no idle timeout.
 */

l2p_result l2p_swarm_new(const char *key_hex, const char *config_json,
                         l2p_swarm **out);
l2p_result l2p_swarm_local_peer_id(l2p_swarm *s);
void       l2p_swarm_free(l2p_swarm *s);            /* NULL-safe */
l2p_result l2p_next_event(l2p_swarm *s, int64_t timeout_ms);

/* ---- network commands -----------------------------------------------------
 * identify has no active request API (exchanged automatically on connect):
 * l2p_identify_peer returns the cached info or registers a pending waiter
 * for the next identify exchange.  l2p_ping waits for the next RTT sample
 * from the periodic ping behaviour.
 */

l2p_result l2p_listen(l2p_swarm *s, const char *addr, int64_t timeout_ms);
l2p_result l2p_listen_async(l2p_swarm *s, const char *addr);
l2p_result l2p_stop_listen(l2p_swarm *s, const char *addr);
l2p_result l2p_dial(l2p_swarm *s, const char *addr, int64_t timeout_ms);
l2p_result l2p_dial_async(l2p_swarm *s, const char *addr);
l2p_result l2p_disconnect(l2p_swarm *s, const char *peer_id);
l2p_result l2p_ping(l2p_swarm *s, const char *peer_id, int64_t timeout_ms);
l2p_result l2p_ping_async(l2p_swarm *s, const char *peer_id);
l2p_result l2p_identify_peer(l2p_swarm *s, const char *peer_id, int64_t timeout_ms);
l2p_result l2p_identify_peer_async(l2p_swarm *s, const char *peer_id);
l2p_result l2p_peer_info(l2p_swarm *s, const char *peer_id);

/* ---- gossipsub (local, immediate) ---------------------------------------- */

l2p_result l2p_gossipsub_subscribe(l2p_swarm *s, const char *topic);
l2p_result l2p_gossipsub_unsubscribe(l2p_swarm *s, const char *topic);
l2p_result l2p_gossipsub_publish(l2p_swarm *s, const char *topic,
                                 const unsigned char *data, size_t len);

/* ---- request-response ------------------------------------------------------
 * Inbound requests arrive as {"type":"request_response_inbound",
 * "channel_id":N,"data_base64":...} events.  l2p_response() with
 * resp=NULL, len=0 declines (the channel is dropped; the requester sees a
 * broken stream).  Unanswered channels are dropped at l2p_swarm_free().
 */

l2p_result l2p_request(l2p_swarm *s, const char *peer_id,
                       const unsigned char *req, size_t len, int64_t timeout_ms);
l2p_result l2p_request_async(l2p_swarm *s, const char *peer_id,
                             const unsigned char *req, size_t len);
l2p_result l2p_response(l2p_swarm *s, int64_t channel_id,
                        const unsigned char *resp, size_t len);

/* ---- kad DHT ---------------------------------------------------------------
 * key_hex = hex of the raw record key bytes.
 */

l2p_result l2p_kad_add_address(l2p_swarm *s, const char *peer_id, const char *addr);
l2p_result l2p_kad_bootstrap(l2p_swarm *s, int64_t timeout_ms);
l2p_result l2p_kad_bootstrap_async(l2p_swarm *s);
l2p_result l2p_kad_get_record(l2p_swarm *s, const char *key_hex, int64_t timeout_ms);
l2p_result l2p_kad_get_record_async(l2p_swarm *s, const char *key_hex);
l2p_result l2p_kad_put_record(l2p_swarm *s, const char *key_hex,
                              const unsigned char *val, size_t len, int64_t timeout_ms);
l2p_result l2p_kad_put_record_async(l2p_swarm *s, const char *key_hex,
                                    const unsigned char *val, size_t len);
l2p_result l2p_kad_get_closest_peers(l2p_swarm *s, const char *peer_id, int64_t timeout_ms);
l2p_result l2p_kad_get_closest_peers_async(l2p_swarm *s, const char *peer_id);
l2p_result l2p_kad_start_providing(l2p_swarm *s, const char *key_hex);
l2p_result l2p_kad_stop_providing(l2p_swarm *s, const char *key_hex);
l2p_result l2p_kad_set_mode(l2p_swarm *s, const char *mode); /* "server"/"client" */

/* ---- relay ------------------------------------------------------------------
 * l2p_relay_reserve dials the relay and completes when the reservation is
 * accepted (relay client event).  l2p_relay_cancel disconnects from the
 * relay peer (the client API has no explicit cancel).
 */

l2p_result l2p_relay_reserve(l2p_swarm *s, const char *relay_peer_id, int64_t timeout_ms);
l2p_result l2p_relay_cancel(l2p_swarm *s, const char *relay_peer_id);

/* ---- freeing & errors (all NULL-safe) ------------------------------------- */

void       l2p_free_result(l2p_result *r);
void       l2p_free_string(char *s);
void       l2p_free_bytes(void *p, size_t len);
const char *l2p_last_error(void);

#ifdef __cplusplus
}
#endif

#endif /* LIBP2P_RSC_H */
