# libp2p-rsc 需求与设计记录

基于 crate [`libp2p` 0.58.0](https://github.com/libp2p/rust-libp2p)（MIT，MSRV 1.88，
edition 2024）自写 C API 绑定，产出 `libp2p-rsc.so`（cdylib）与 `libp2p-rsc.a`
（staticlib）。上游 API（builder 链、各 Behaviour 构造、Swarm 方法、SwarmEvent
变体、DialOpts::connection_id）均已对照 0.58 源码逐条核实。

## 1. 与 reqwest-dav-ffi 的差异

| 维度 | reqwest_dav | libp2p |
|---|---|---|
| 模式 | 同步请求/响应 | 事件驱动引擎（常驻 swarm task） |
| 调用模型 | 每调用一次操作 | command channel + event channel |
| 事件 | 无 | SwarmEvent 无 Serialize，手写 JSON 映射 |
| 组合性 | 单一 Client | Behaviour 编译期组合（derive 宏） |

采用上游 `examples/file-sharing/src/network.rs` 的 **command/event-loop 架构**。
回调注册方式否决（回调内再调库死锁、panic 跨线程、阻塞回调卡死 swarm）。

## 2. 命名约定

- 包名：`libp2p-rsc`
- 库文件：`libp2p-rsc.so` / `libp2p-rsc.a`（链接 `-lp2p-rsc`；Cargo 内部名
  `p2p_rsc`，安装时重命名）
- 头文件：`libp2p-rsc.h`（用户指定），guard `LIBP2P_RSC_H`，保留 `l2p_`/`L2P_`
- pkg-config：`libp2p-rsc.pc`（`Libs: -lp2p-rsc`）
- 符号前缀：`l2p_`
- 前缀惯例依据：neon `ne_`、libsignal `signal_`、wasmi `wasmi_`（压缩式短前缀，
  同 `rwdav_` 决策风格）

## 3. 常量

```c
#define LP2P_OK        0   /* success */
#define LP2P_ERR      -1   /* failed (network/protocol/parse); errmsg set */
#define LP2P_EINVAL   -2   /* NULL handle/ptr or bad enum; errmsg NOT set */
#define LP2P_EPANIC   -3   /* Rust panic caught at FFI boundary; errmsg set */
#define LP2P_ETIMEOUT -4   /* blocking wait timed out; errmsg set; 结果不丢 */
#define LP2P_TIMEOUT_FOREVER (-1)
```

输入解析失败（multiaddr/key hex/peer id/topic/mode/config JSON）归 `LP2P_ERR`；
`LP2P_EINVAL` 仅指 NULL 句柄/指针与非法枚举值。

## 4. 返回值结构体（与 rwdav_result 同构）

```c
typedef struct l2p_result {
    int64_t  code;    /* LP2P_* */
    char    *errmsg;  /* NULL on LP2P_OK / LP2P_EINVAL */
    void    *data;
    size_t   len;
} l2p_result;
```

按值返回；`#[repr(C)]`（`i64 / *mut c_char / *mut c_void / usize`）；
x86_64 SysV 经 sret 隐藏指针，两侧约定一致（已核实可行）。

data/len 语义：

| 函数 | data | len |
|---|---|---|
| key_new / key_from_bytes | 规范化 hex 私钥（NUL 结尾） | strlen+1 |
| key_peer_id / swarm_local_peer_id | PeerId 字符串 | strlen+1 |
| peer_info | identify info JSON | strlen+1 |
| *_async | command_id 十进制字符串 | strlen+1 |
| ping | RTT 毫秒十进制字符串 | strlen+1 |
| kad_get_record | 记录值原始字节 | 字节数 |
| kad_get_closest_peers | JSON 数组 | strlen+1 |
| next_event | JSON 事件；无事件超时 = data NULL | strlen+1 |
| 其余成功 / 全部失败 | NULL | 0 |

所有权：`l2p_free_result(&r)` 释放 errmsg+data 并清零（可复用）；三个 free
函数 NULL-safe；不得与 free_string/free_bytes 混用造成双重释放。
双轨错误：结构体 errmsg（拷贝）+ `l2p_last_error()`（线程本地、库持有、
下次同线程调用失效、免释放）。消息格式
`"l2p_<fn>(...): <错误原文>"`、panic 格式 `"l2p_<fn>: panic (bug, please report)"`。

## 5. 双轨调用模型

```c
/* 默认：阻塞到最终结果；timeout_ms < 0 = 永久等待 */
l2p_result l2p_dial(l2p_swarm *s, const char *addr, int64_t timeout_ms);
/* _async：提交即返回，data = command_id；
 * 完成时事件 {"type":"command_result","command_id":N,"ok":true/false,...} */
l2p_result l2p_dial_async(l2p_swarm *s, const char *addr);
```

- command_id 由 FFI 侧原子计数器分配，随命令传入 task。
- 完成 = `ConnectionEstablished`/`OutgoingConnectionError`（dial，按
  connection_id 配对）、ping 事件（按 peer）、identify 事件（按 peer）、
  kad 查询（按 QueryId）、request-response 回复（按 OutboundRequestId）。
- 完成时**先发 oneshot 回执，再发 command_result 事件**：阻塞等待超时
  （ETIMEOUT）后结果仍会以事件形式送达（"结果不丢"）。
- `_async` 变体：listen、dial、ping、identify_peer、kad_bootstrap、
  kad_get_record、kad_put_record、kad_get_closest_peers、request。
- 本地即时操作（stop_listen、disconnect、gossipsub 三件套、response、
  kad_add_address/start_providing/stop_providing/set_mode、peer_info、
  relay_cancel）无 `_async`。
- dial 两段式：命令回执只覆盖 `swarm.dial()` 的同步错误（地址解析/立即失败），
  终态由 connection_id 配对事件给出。

## 6. v1.0 函数清单（42 个导出符号）

身份密钥（3）：

```c
l2p_result l2p_key_new(void);
l2p_result l2p_key_from_bytes(const char *hex);
l2p_result l2p_key_peer_id(const char *key_hex);
```

（仅 ed25519；hex = libp2p-identity 的 protobuf 编码私钥。）

Swarm 生命周期（4）：

```c
l2p_result l2p_swarm_new(const char *key_hex, const char *config_json,
                         l2p_swarm **out);
l2p_result l2p_swarm_local_peer_id(l2p_swarm *s);
l2p_result l2p_swarm_free(l2p_swarm *s);   /* Shutdown + join task + 清理 */
l2p_result l2p_next_event(l2p_swarm *s, int64_t timeout_ms);
```

config_json 全字段可选（未写字段按缺省）：

```jsonc
{ "listen": ["/ip4/0.0.0.0/tcp/0","/ip4/0.0.0.0/udp/0/quic-v1"],
  "enable_mdns":false, "enable_kad":true, "kad_mode":"client",
  "enable_identify":true, "enable_ping":true, "enable_gossipsub":true,
  "enable_request_response":false, "request_response_protocol":"/app/req/1",
  "enable_relay_client":false, "enable_relay_server":false,
  "enable_dcutr":true, "enable_autonat":true, "enable_upnp":true,
  "agent_version":"l2p-app/1.0", "idle_connection_timeout_secs":0,
  "ping_interval_secs":15, "gossipsub_heartbeat_ms":1000 }
```

传输栈固定 tcp+noise+yamux+quic+dns（不设开关；websocket/webtransport 不进
v1 二进制）。`idle_connection_timeout_secs: 0` = u64::MAX。

网络命令（11）：

```c
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
```

- identify 无主动请求 API（连接时自动交换）：identify_peer = 缓存命中即返回，
  否则登记 pending 等下一次 identify 交换（新连接触发）。
- ping 由 behaviour 自动周期执行：l2p_ping = 等待该 peer 的下一个 RTT 样本。

gossipsub（3，本地即时）：

```c
l2p_result l2p_gossipsub_subscribe(l2p_swarm *s, const char *topic);
l2p_result l2p_gossipsub_unsubscribe(l2p_swarm *s, const char *topic);
l2p_result l2p_gossipsub_publish(l2p_swarm *s, const char *topic,
                                 const unsigned char *data, size_t len);
```

request-response（3）：

```c
l2p_result l2p_request(l2p_swarm *s, const char *peer_id,
                       const unsigned char *req, size_t len, int64_t timeout_ms);
l2p_result l2p_request_async(l2p_swarm *s, const char *peer_id,
                             const unsigned char *req, size_t len);
l2p_result l2p_response(l2p_swarm *s, int64_t channel_id,
                        const unsigned char *resp, size_t len);
```

- 入站请求 → 事件 `request_response_inbound`（channel_id 递增、data base64）；
- `l2p_response(..., resp=NULL, 0)` = 拒绝（drop channel，对方断流）；
- 未回复的 channel 在 swarm_free 时统一 drop。
- 编解码用上游 `cbor` feature 的 `Codec<Vec<u8>, Vec<u8>>`（字节透传）。

kad DHT（12）：

```c
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
```

relay（2）：

```c
l2p_result l2p_relay_reserve(l2p_swarm *s, const char *relay_peer_id, int64_t timeout_ms);
l2p_result l2p_relay_cancel(l2p_swarm *s, const char *relay_peer_id);
```

释放/错误（4）：`l2p_free_result`、`l2p_free_string`、`l2p_free_bytes`、
`l2p_last_error`（void/const char*，NULL-safe）。

## 7. 事件 JSON

统一 `{"type":"...","peer_id":"...",...}`；未映射分支 →
`{"type":"other","debug":"<Debug 输出>"}`（前向兼容）。事件名对照上游
SwarmEvent 变体（已按源码核实）：

| type | 上游变体 |
|---|---|
| new_listen_addr / expired_listen_addr | NewListenAddr / ExpiredListenAddr（带 listener_id） |
| listener_error / listener_closed | ListenerError / ListenerClosed |
| connection_established / connection_closed | ConnectionEstablished / ConnectionClosed（connection_id） |
| incoming_connection / incoming_connection_error | IncomingConnection / IncomingConnectionError |
| outgoing_connection_error | OutgoingConnectionError（修正：上游无 ..._failed） |
| dialing / new_external_addr_candidate / external_addr_confirmed / external_addr_expired / new_external_addr_of_peer | 同名变体 |
| ping / command_result | ping::Event::Ping（peer_id+connection_id+result） |
| identify | identify::Event::Received（info + rtt） |
| gossipsub_message / gossipsub_subscribed / gossipsub_unsubscribed / gossipsub_not_supported / gossipsub_slow_peer | gossipsub::Event |
| request_response_inbound / request_response_outbound | request_response::Event::Message（Request/Response 分支） |
| kad_record / kad_routing_updated / kad_query_progressed | kad::Event（OutboundQueryProgressed 按 QueryId） |
| mdns_discovered / mdns_expired | mdns::Event::Discovered / Expired |
| upnp_new_external_addr / upnp_gateway_not_found / upnp_non_routable_gateway | upnp::Event |
| relay_* / dcutr_* / autonat_* | debug 透传 |
| command_result（command_id 配对，_async 命令的终态） | pending 表 |
| overflow / other | 队列满（上限 1024，丢弃溢出事件）/ 未映射 |

## 8. 二进制数据编码约定

| 场景 | 编码 |
|---|---|
| 函数入参 data（publish/request/put_record） | 原始字节 + len |
| 事件出参 data（message/inbound/kad value） | base64 |
| kad key、私钥 | hex |
| PeerId / Multiaddr / topic / mode | UTF-8 字符串 |

## 9. Rust 实现要点（全部对照 0.58 源码核实）

- builder 链：

```rust
SwarmBuilder::with_existing_identity(keypair)      // 注意：不是 with_identity
  .with_tokio()
  .with_tcp(tcp::Config::default(), noise::Config::new, yamux::Config::default)?
  .with_quic()
  .with_dns()?
  // enable_relay_client 时追加：
  .with_relay_client(noise::Config::new, yamux::Config::default)?
  .with_behaviour(|key /*, relay*/| Behaviour { ... })?
  .with_swarm_config(|cfg| cfg.with_idle_connection_timeout(...))
  .build()
```

- Behaviour 构造（实测签名）：

```rust
ping::Behaviour::new(ping::Config::new().with_interval(...))
identify::Behaviour::new(identify::Config::new("l2p/1".into(), key.public())
                         .with_agent_version(...))
gossipsub::Behaviour::new(MessageAuthenticity::Signed(key), gossipsub::Config)? // Result<&'static str>
kad::Behaviour::new(local_peer_id, kad::store::MemoryStore::new(local_peer_id))
request_response::Behaviour::new([StreamProtocol::new(rr_proto)], Config::default())
mdns::Behaviour::new(mdns::Config::default(), local_peer_id)?     // io::Result
relay::Behaviour::new(local_peer_id, relay::Config::default())    // server
dcutr::Behaviour::new(local_peer_id)
autonat::v1::Behaviour::new(local_peer_id, autonat::v1::Config::default())
upnp::tokio::Behaviour::default()                                 // tokio 专属类型
```

- `#[derive(NetworkBehaviour)]`，字段全 `Option<...>`（derive 支持空 Option 跳过）。
- dial：`DialOpts::unknown_peer_id().address(addr).build()` /
  `DialOpts::peer_id(pid).addresses(vec![addr]).build()`，先 `opts.connection_id()`
  登记 pending 再 dial；addr 含 `/p2p/<peer>` 时用后者并启用
  `extend_addresses_through_behaviour()`。
- 全局 `LazyLock<Runtime>`；每 swarm 一个常驻 task（`select!`：command
  channel + `swarm.select_next_some()`）。
- pending：`HashMap<u64, Pending{id, Option<oneshot>}>` + 二级索引
  （conn/query/req/ping/ident → cmd_id）。
- 事件通道：`std::sync::mpsc::sync_channel(1024)`，满则丢弃并尝试补发
  `overflow` 事件；`next_event` 用 `recv_timeout`（Receiver 由 Mutex 包住，
  多线程调用被串行化，建议单线程消费）。
- 阻塞等待：`RT.handle().block_on(tokio::time::timeout(dur, rx))`（C 线程
  非 runtime 上下文，合法）；超时回执通道不销毁，任务端完成时
  oneshot 与事件双发，保证结果不丢。
- 二进制编码：`base64`、`hex` crate；JSON 用 `serde_json::json!`。

## 10. 打包

- `depends=('glibc' 'gcc-libs')`（tcp/quic 栈无 openssl；websocket 才需要）。
- `makedepends=('rust' 'cargo')`；rust ≥1.88（edition 2024）。
- license=('MIT')（libp2p 0.58 meta crate 仅 MIT）。
- features：`tcp dns quic yamux noise ed25519 ping identify gossipsub cbor
  request-response kad mdns relay dcutr autonat upnp tokio macros`。
- 安装：`/usr/lib/libp2p-rsc.so`、`/usr/lib/libp2p-rsc.a`、
  `/usr/include/libp2p-rsc.h`、`/usr/lib/pkgconfig/libp2p-rsc.pc`。
- 不加版本化 SONAME（第一版裸 .so）。暂不加 CI 列表，编译通过后加。

## 11. 验收标准

1. `bash -n` + `makepkg --printsrcinfo` 通过。
2. 编译产出 `libp2p-rsc.so/.a`；`nm -D | grep l2p_` ≈42 符号。
3. C 冒烟测试：双节点 dial → connection_established → identify/ping →
   gossipsub 互通 → kad put/get record。
4. readelf 无 openssl 依赖。
