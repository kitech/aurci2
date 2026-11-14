# reqwest-dav-rsc 需求与设计记录

基于 crate [`reqwest_dav`](https://github.com/niuhuan/reqwest_dav)（MIT OR Apache-2.0，
crates.io 下载量 1.3M+，活跃维护）自写 C API 绑定，产出 `librwdav.so`（cdylib）与
`librwdav.a`（staticlib）。

## 1. 背景与选型

- 需求：Rust 实现的、带 C API 的 WebDAV 客户端库。
- `opendal-c`（官方 C binding）标注 WIP / unreleased，API 会漂移，**不采用**。
- 其余 Rust WebDAV crate（`hyperdav`、`rustydav`、`webdavc`、`io-webdav`）均已停滞或用量极低。
- 选定：**自己包一层 `reqwest_dav` 0.3**。
  - 优点：唯一活跃的专用 WebDAV 客户端；协议语义直接（`list/get/put/mkcol/unzip/
    delete/mv/cp`）；错误类型信息完整（HTTP 状态码、XML 解析错误）。
  - 代价：async + tokio，FFI 层需内置全局 runtime 并 `block_on`。

## 2. 命名约定

- 包名：`reqwest-dav-rsc`（包层面保留完整出处，便于检索）。
- 库文件名：`librwdav.so` / `librwdav.a`（`-lrwdav`）。
- 头文件：`rwdav.h`，include guard `RWDAV_FFI_H`。
- pkg-config：`rwdav.pc`（`Libs: -lrwdav`，`Cflags: -I${includedir}`）。
- 符号前缀：**`rwdav_`**，宏前缀 `RWDAV_`；头文件注释中声明保留这两个命名空间。
- 前缀取舍依据（业界惯例，均已核实）：
  - neon（同为 WebDAV 库）：项目名 `neon` → 前缀 `ne_` / `NE_`，历史上由 `http_`/
    `dav_`/`neon_` 统一缩为 `ne_`。
  - libsignal：crate `libsignal-ffi` → 符号前缀 `signal_`。
  - rustls-ffi → `rustls_`；wasmi → `wasmi_`；opendal → `opendal_`。
  - 结论：前缀取库名缩写、小写加下划线是通行做法；`rdav_` 被否决，最终采用 `rwdav_`。

## 3. 常量（写进 rwdav.h）

```c
/* rwdav_result.code */
#define RWDAV_OK        0   /* success */
#define RWDAV_ERR      -1   /* operation failed (network/HTTP/WebDAV); errmsg set */
#define RWDAV_EINVAL   -2   /* invalid argument; errmsg NOT set */
#define RWDAV_EPANIC   -3   /* Rust panic caught at FFI boundary; errmsg set */

/* depth argument of rwdav_list() */
#define RWDAV_DEPTH_INFINITY (-1)   /* PROPFIND Depth: infinity */
                                  /* >= 0: PROPFIND Depth: <n> */

/* auth argument of rwdav_new() */
#define RWDAV_AUTH_BASIC      0
#define RWDAV_AUTH_DIGEST     1
#define RWDAV_AUTH_ANONYMOUS  2
```

## 4. 返回值结构体

```c
#include <stdint.h>
typedef struct rwdav_result {
  int64_t  code;    /* RWDAV_* constant */
  char    *errmsg;  /* NULL on success; owned by the library/result */
  void    *data;    /* list: JSON string (NUL-terminated); read: raw bytes; else NULL */
  size_t   len;     /* list: strlen(data)+1; read: byte count; else 0 */
} rwdav_result;
```

- 所有函数**按值返回** `rwdav_result`。
- Rust 侧 `#[repr(C)]`，字段 `i64 / *mut c_char / *mut c_void / usize`，与 C 侧一一对应。
- 可行性已核实（Rustonomicon《Other reprs》+ Mainmatter C→Rust 迁移教程）：
  `repr(C)` 结构体跨 `extern "C"` 的传参与返回与 C 完全一致；32 字节结构在
  x86_64 SysV 上通过隐藏指针（sret）返回，两侧约定相同。
- 采用原因：调用侧只需一个局部变量、不会忘记初始化 out-param、`data` 与 `len`
  绑定在同一结构体内、单一释放函数 `rwdav_free_result()`。
- 已知代价：返回值总是带所有权语义（即使 `RWDAV_EINVAL` 也要释放），依靠
  `rwdav_free_result()` 的 NULL-safe + 清零行为降低误用风险；异平台（非 x86_64
  主流 ABI）需重新确认结构体返回约定。

## 5. 函数清单（12 个）

```c
rwdav_result rwdav_new(const char *endpoint, int auth,
                       const char *user, const char *pass,
                       rwdav_client **out);
rwdav_result rwdav_list (rwdav_client *c, const char *path, int depth);
rwdav_result rwdav_read (rwdav_client *c, const char *path);
rwdav_result rwdav_write(rwdav_client *c, const char *path,
                         const unsigned char *buf, size_t len);
rwdav_result rwdav_mkcol(rwdav_client *c, const char *path);
rwdav_result rwdav_unzip(rwdav_client *c, const char *path);
rwdav_result rwdav_delete(rwdav_client *c, const char *path);
rwdav_result rwdav_mv(rwdav_client *c, const char *from, const char *to);
rwdav_result rwdav_cp(rwdav_client *c, const char *from, const char *to, int overwrite);

void         rwdav_free_client(rwdav_client *c);
void         rwdav_free_string(char *s);
void         rwdav_free_bytes(unsigned char *p, size_t len);
void         rwdav_free_result(rwdav_result *r);
const char  *rwdav_last_error(void);
```

### 与上游 `reqwest_dav::Client` 的覆盖关系

| 上游 | 本库 | 说明 |
|---|---|---|
| `ClientBuilder::{new,set_host,set_auth,build}` | `rwdav_new` | `auth` 参数选择 Basic/Digest/Anonymous |
| `get()` / `get_raw()` | `rwdav_read` | 取 `.bytes()`；`*_raw` 不单独暴露 |
| `put()` / `put_raw()` | `rwdav_write` | `Vec<u8>` 作为 body |
| `mkcol()` / `mkcol_raw()` | `rwdav_mkcol` | |
| `unzip()` / `unzip_raw()` | `rwdav_unzip` | 服务端 `UNZIP`（Nextcloud 等支持） |
| `delete()` / `delete_raw()` | `rwdav_delete` | |
| `mv()` / `mv_raw()` | `rwdav_mv` | 目标为完整目标路径 |
| `cp()` / `cp_raw(from,to,overwrite)` | `rwdav_cp(..., overwrite)` | **暴露 overwrite**（上游 `cp()` 写死 true） |
| `list(path, depth)` | `rwdav_list(c, path, depth)` | `depth` 暴露，`-1` = infinity |
| `list_rsp` / `list_raw` | —— | 不暴露；`list` 直接给 JSON |
| `ClientBuilder::set_agent` | —— | FFI 场景无合理用途 |

## 6. 契约

```
Return value
  - code == RWDAV_OK 表示成功；其余见常量。
  - RWDAV_ERR / RWDAV_EPANIC 一定设置 errmsg；RWDAV_EINVAL 不设置 errmsg。

data/len
  - list() 成功时 data 指向 NUL 结尾的 JSON 字符串，len = strlen+1；
    内容是 Vec<ListEntity> 的 serde_json 序列化（外部标签枚举：
    {"File": {...}} / {"Folder": {...}}）。
  - read() 成功时 data 指向原始字节，长度 len，**不保证 NUL 结尾**；
    空文件时 data == NULL, len == 0。
  - 其余函数成功时 data == NULL, len == 0。
  - 失败时 data == NULL, len == 0。

内存所有权
  - errmsg 与 data 均由库分配，必须用 rwdav_free_result(&r) 统一释放；
    该函数释放两者、清零整个结构体，可安全复用结构体。
  - rwdav_free_string() 仅用于把指针从结果中剥离后单独释放的场景
    （先 r.errmsg = NULL 再 free，避免与 free_result 重复释放）。
  - rwdav_free_bytes() 与 free_result 对 data 的释放路径一致（boxed slice）。
  - 三个 free 函数均 NULL-safe。

线程
  - rwdav_client 线程安全，可跨线程共享同一个句柄。
  - 库内部使用全局 tokio runtime，所有调用同步阻塞；不要在已持有该 runtime 的
    Rust async 上下文中回调本库。
  - rwdav_last_error() 使用线程本地存储，按线程隔离。

错误信息（双轨）
  - 结构体 errmsg：显式捕获，可跨线程传递、生命周期由调用方控制。
  - rwdav_last_error()：库持有的线程本地指针，下次同线程调用即失效，不许 free；
    与 strerror() 惯例一致。
  - 二者每次失败都会同时更新。
  - 消息格式：`rwdav_<fn>('<path>'): <reqwest_dav::Error 的 Display 输出>`；
    panic 格式：`rwdav_<fn>: panic (bug, please report)`。

参数校验
  - NULL 句柄、NULL 必需字符串、非法 auth/depth 取值 → RWDAV_EINVAL。
  - 路径需调用方自行做 percent-encoding（与 neon 的约定一致）。
```

## 7. Rust 实现要点

```rust
#[repr(C)]
pub struct RwdavResult { code: i64, errmsg: *mut c_char, data: *mut c_void, len: usize }

static RT: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| Runtime::new().unwrap());
thread_local! { static LAST_ERROR: RefCell<Option<CString>> = ... }

fn guard(fname: &str, f: impl FnOnce() -> RwdavResult) -> RwdavResult {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(r) => r,
        Err(_) => err_result(RWDAV_EPANIC, format!("{fname}: panic (bug, please report)")),
    }
}
```

- 每个导出函数都经 `guard()`，**禁止 panic 跨 FFI**。
- 成功路径清空线程本地 last_error。
- `data` 统一用 `Vec<u8> → into_boxed_slice()` 分配，释放时
  `Box::from_raw(slice_from_raw_parts_mut(p, len))`，list 的 JSON 末尾附加 NUL。
- `errmsg` 用 `CString::into_raw()` 分配，`rwdav_free_string` 用 `CString::from_raw`。
- 依赖：`reqwest_dav = "0.3"`、`tokio`（rt-multi-thread）、`serde_json`、`libc`。

## 8. 打包

- `PKGBUILD`：源码内嵌在 `crate/`（避免与 makepkg 的 $srcdir 冲突），`source=()`。
- `depends=('glibc' 'gcc-libs' 'openssl')`——`reqwest_dav` 默认 feature 启用
  `reqwest/default` → native-tls → OpenSSL。
- `makedepends=('rust' 'cargo')`。
- 安装：`/usr/lib/librwdav.so`、`/usr/lib/librwdav.a`、`/usr/include/rwdav.h`、
  `/usr/lib/pkgconfig/rwdav.pc`。
- 不生成版本化 SONAME（第一版为裸 `librwdav.so`）；需要时再加
  `-Wl,-soname,librwdav.so.0` 与符号链接。
- 暂不加入 CI 包列表，确认可编译后再加。

## 9. 验收标准

1. `bash -n PKGBUILD` 通过，`makepkg --printsrcinfo` 能生成 `.SRCINFO`。
2. `cargo build --release` 产出 `lib rwdav.so` 与 `librwdav.a`。
3. `readelf -d librwdav.so` 显示 OpenSSL / glibc 依赖正确。
4. C 程序以 `-lrwdav` 链接，完成 new → list → read/write → free 全流程。
5. `nm -D --defined-only librwdav.so | grep rwdav_` 显示全部 12 个导出符号。
