#ifndef RWDAV_FFI_H
#define RWDAV_FFI_H
/*
 * rwdav - synchronous C API over the Rust crate reqwest_dav
 *         (https://github.com/niuhuan/reqwest_dav, MIT OR Apache-2.0)
 *
 * This library reserves the namespaces rwdav_ and RWDAV_.
 *
 * ---------------------------------------------------------------------------
 * Return value
 *   Every function returns struct rwdav_result by value.
 *   code == RWDAV_OK means success; see the RWDAV_* constants below.
 *   RWDAV_ERR and RWDAV_EPANIC always set errmsg; RWDAV_EINVAL never does
 *   (and clears rwdav_last_error()).
 *
 * data / len
 *   rwdav_list()  success: data = NUL-terminated JSON string, len = strlen+1.
 *                 JSON is the serialization of reqwest_dav's Vec<ListEntity>:
 *                 [{"File":{"href":...,"last_modified":...,"content_length":...,
 *                          "content_type":...,"tag":...}},
 *                  {"Folder":{"href":...,"last_modified":...,"tag":...,
 *                            "quota_used_bytes":...,"quota_available_bytes":...,
 *                            "address_book":...}}]
 *   rwdav_read()  success: data = raw bytes, len = byte count, NOT
 *                 NUL-terminated. Empty file: data = NULL, len = 0.
 *   other calls   success: data = NULL, len = 0. Failure: same.
 *
 * Ownership
 *   errmsg and data are allocated by the library. Release them with
 *   rwdav_free_result(&r); it frees both, zeroes the struct, and the same
 *   variable can be reused for the next call. The three free helpers are
 *   NULL-safe. Do not free errmsg/data twice: use either rwdav_free_result()
 *   or the individual free function, never both.
 *
 * Threading
 *   rwdav_client is thread-safe; one handle may be shared across threads.
 *   All calls are synchronous (an internal global tokio runtime is used);
 *   they may block. Do not call into this library from an async context
 *   that already owns that runtime.
 *   rwdav_last_error() is thread-local.
 *
 * Errors
 *   Two channels, both updated on failure:
 *     - result.errmsg: a copy, lifetime controlled by the caller.
 *     - rwdav_last_error(): library-owned pointer, valid until the next
 *       rwdav_* call on the same thread; never free it (strerror() style).
 *   Message format: "rwdav_<fn>('<path>'): <reqwest_dav error text>" or
 *   "rwdav_<fn>: panic (bug, please report)".
 *
 * Paths
 *   Paths are absolute WebDAV paths and must be percent-encoded by the
 *   caller (same rule as neon).
 * ---------------------------------------------------------------------------
 */
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* rwdav_result.code values */
#define RWDAV_OK        0   /* success */
#define RWDAV_ERR      -1   /* operation failed (network/HTTP/WebDAV) */
#define RWDAV_EINVAL   -2   /* invalid argument; errmsg is not set */
#define RWDAV_EPANIC   -3   /* Rust panic caught at the FFI boundary (bug) */

/* depth argument of rwdav_list() */
#define RWDAV_DEPTH_INFINITY (-1)  /* PROPFIND Depth: infinity */
                                   /* 0, 1, 2, ...: PROPFIND Depth: <n> */

/* auth argument of rwdav_new() */
#define RWDAV_AUTH_BASIC      0    /* HTTP Basic auth */
#define RWDAV_AUTH_DIGEST     1    /* HTTP Digest auth */
#define RWDAV_AUTH_ANONYMOUS  2    /* no auth; user/pass are ignored */

typedef struct rwdav_client rwdav_client;

typedef struct rwdav_result {
    int64_t  code;    /* RWDAV_* */
    char    *errmsg;  /* NULL on success; free with rwdav_free_result() */
    void    *data;    /* see "data / len" above */
    size_t   len;     /* see "data / len" above */
} rwdav_result;

/*
 * Creates a client.
 *   endpoint   base URL, e.g. "https://dav.example.com/remote.php/dav"
 *   auth       RWDAV_AUTH_*
 *   user,pass  may be NULL for RWDAV_AUTH_ANONYMOUS (NULL else -> "")
 *   out        receives the new handle on success
 * On failure *out is not modified.
 */
rwdav_result rwdav_new(const char *endpoint, int auth,
                       const char *user, const char *pass,
                       rwdav_client **out);

/*
 * Lists a collection (PROPFIND).
 *   depth  RWDAV_DEPTH_INFINITY, or a non-negative Depth value.
 * Returns JSON in data (NUL-terminated), see "data / len".
 */
rwdav_result rwdav_list(rwdav_client *c, const char *path, int depth);

/* Downloads a file into data (raw bytes, not NUL-terminated). */
rwdav_result rwdav_read(rwdav_client *c, const char *path);

/* Uploads buf/len to path. len == 0 uploads an empty file (buf may be NULL). */
rwdav_result rwdav_write(rwdav_client *c, const char *path,
                         const unsigned char *buf, size_t len);

/* Creates a collection (MKCOL). */
rwdav_result rwdav_mkcol(rwdav_client *c, const char *path);

/* Asks the server to unzip an archive in place (server-side UNZIP, e.g.
 * Nextcloud). */
rwdav_result rwdav_unzip(rwdav_client *c, const char *path);

/* Deletes a file or collection. */
rwdav_result rwdav_delete(rwdav_client *c, const char *path);

/* Moves/renames. "to" is the full destination path (file name included). */
rwdav_result rwdav_mv(rwdav_client *c, const char *from, const char *to);

/* Copies. "to" is the full destination path; overwrite != 0 allows
 * overwriting the destination. */
rwdav_result rwdav_cp(rwdav_client *c, const char *from, const char *to,
                      int overwrite);

/* Releases a client handle (NULL-safe). */
void rwdav_free_client(rwdav_client *c);

/* Releases a library-allocated C string (NULL-safe). */
void rwdav_free_string(char *s);

/* Releases a library-allocated byte buffer; len must be the value returned
 * in rwdav_result.len (NULL-safe). */
void rwdav_free_bytes(unsigned char *p, size_t len);

/* Releases errmsg and data of a result and zeroes the struct (NULL-safe). */
void rwdav_free_result(rwdav_result *r);

/* Error message of the last failed call on this thread, or NULL. The pointer
 * is library-owned and invalidated by the next rwdav_* call on this thread. */
const char *rwdav_last_error(void);

#ifdef __cplusplus
}
#endif

#endif /* RWDAV_FFI_H */
