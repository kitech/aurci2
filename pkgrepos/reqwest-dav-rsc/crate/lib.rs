//! Synchronous C API over the Rust crate `reqwest_dav`.
//!
//! See `detail.md` in the package directory for the full contract.

use std::cell::RefCell;
use std::ffi::{c_char, c_void, CStr, CString};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::ptr;
use std::sync::LazyLock;

use reqwest_dav::{Auth, Client, ClientBuilder, Depth};

pub const RWDAV_OK: i64 = 0;
pub const RWDAV_ERR: i64 = -1;
pub const RWDAV_EINVAL: i64 = -2;
pub const RWDAV_EPANIC: i64 = -3;

pub const RWDAV_DEPTH_INFINITY: i32 = -1;

pub const RWDAV_AUTH_BASIC: i32 = 0;
pub const RWDAV_AUTH_DIGEST: i32 = 1;
pub const RWDAV_AUTH_ANONYMOUS: i32 = 2;

static RT: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    tokio::runtime::Runtime::new().expect("rwdav: failed to create tokio runtime")
});

thread_local! {
    static LAST_ERROR: RefCell<Option<CString>> = const { RefCell::new(None) };
}

/// Must stay layout-compatible with `struct rwdav_result` in `rwdav.h`.
#[repr(C)]
pub struct RwdavResult {
    pub code: i64,
    pub errmsg: *mut c_char,
    pub data: *mut c_void,
    pub len: usize,
}

pub struct RwdavClient {
    client: Client,
}

fn cstring_lossy(msg: &str) -> CString {
    CString::new(msg.replace('\0', "?"))
        .unwrap_or_else(|_| CString::new("rwdav: error").expect("static string"))
}

fn set_last_error(msg: Option<&str>) {
    LAST_ERROR.with(|slot| {
        *slot.borrow_mut() = msg.map(cstring_lossy);
    });
}

fn ok_empty() -> RwdavResult {
    set_last_error(None);
    RwdavResult {
        code: RWDAV_OK,
        errmsg: ptr::null_mut(),
        data: ptr::null_mut(),
        len: 0,
    }
}

/// `bytes` is moved into a boxed slice; `nul_terminate` is used for the
/// `list()` JSON payload so that C can treat it as a C string.
fn ok_bytes(mut bytes: Vec<u8>, nul_terminate: bool) -> RwdavResult {
    set_last_error(None);
    if bytes.is_empty() && !nul_terminate {
        return RwdavResult {
            code: RWDAV_OK,
            errmsg: ptr::null_mut(),
            data: ptr::null_mut(),
            len: 0,
        };
    }
    if nul_terminate {
        bytes.push(0);
    }
    let len = bytes.len();
    let data = Box::into_raw(bytes.into_boxed_slice()) as *mut [u8] as *mut u8 as *mut c_void;
    RwdavResult {
        code: RWDAV_OK,
        errmsg: ptr::null_mut(),
        data,
        len,
    }
}

fn fail(code: i64, msg: String) -> RwdavResult {
    set_last_error(Some(&msg));
    RwdavResult {
        code,
        errmsg: cstring_lossy(&msg).into_raw(),
        data: ptr::null_mut(),
        len: 0,
    }
}

/// Invalid argument: no message, and any stale thread-local error is cleared.
fn einval() -> RwdavResult {
    set_last_error(None);
    RwdavResult {
        code: RWDAV_EINVAL,
        errmsg: ptr::null_mut(),
        data: ptr::null_mut(),
        len: 0,
    }
}

fn guard<F: FnOnce() -> RwdavResult>(fname: &'static str, f: F) -> RwdavResult {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(result) => result,
        Err(_) => fail(
            RWDAV_EPANIC,
            format!("{}: panic (bug, please report)", fname),
        ),
    }
}

/// Reads a required C string. NULL is an error; invalid UTF-8 is replaced lossily.
unsafe fn arg_str(p: *const c_char) -> Result<String, RwdavResult> {
    if p.is_null() {
        return Err(einval());
    }
    Ok(CStr::from_ptr(p).to_string_lossy().into_owned())
}

/// Reads an optional C string; NULL becomes the empty string.
unsafe fn arg_str_opt(p: *const c_char) -> String {
    if p.is_null() {
        String::new()
    } else {
        CStr::from_ptr(p).to_string_lossy().into_owned()
    }
}

unsafe fn client_ref<'a>(c: *mut RwdavClient) -> Result<&'a RwdavClient, RwdavResult> {
    match c.as_ref() {
        Some(client) => Ok(client),
        None => Err(einval()),
    }
}

fn depth_from_i32(depth: i32) -> Option<Depth> {
    match depth {
        RWDAV_DEPTH_INFINITY => Some(Depth::Infinity),
        n if n >= 0 => Some(Depth::Number(i64::from(n))),
        _ => None,
    }
}

fn auth_from_i32(auth: i32, user: String, pass: String) -> Option<Auth> {
    match auth {
        RWDAV_AUTH_BASIC => Some(Auth::Basic(user, pass)),
        RWDAV_AUTH_DIGEST => Some(Auth::Digest(user, pass)),
        RWDAV_AUTH_ANONYMOUS => Some(Auth::Anonymous),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Exported API
// ---------------------------------------------------------------------------

#[no_mangle]
pub extern "C" fn rwdav_new(
    endpoint: *const c_char,
    auth: i32,
    user: *const c_char,
    pass: *const c_char,
    out: *mut *mut RwdavClient,
) -> RwdavResult {
    guard("rwdav_new", || unsafe {
        if out.is_null() {
            return einval();
        }
        let endpoint = match arg_str(endpoint) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let auth = match auth_from_i32(auth, arg_str_opt(user), arg_str_opt(pass)) {
            Some(a) => a,
            None => return einval(),
        };
        let client = match ClientBuilder::new()
            .set_host(endpoint.clone())
            .set_auth(auth)
            .build()
        {
            Ok(client) => client,
            Err(e) => return fail(RWDAV_ERR, format!("rwdav_new('{}'): {}", endpoint, e)),
        };
        *out = Box::into_raw(Box::new(RwdavClient { client }));
        ok_empty()
    })
}

#[no_mangle]
pub extern "C" fn rwdav_list(
    c: *mut RwdavClient,
    path: *const c_char,
    depth: i32,
) -> RwdavResult {
    guard("rwdav_list", || unsafe {
        let client = match client_ref(c) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let path = match arg_str(path) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let depth = match depth_from_i32(depth) {
            Some(d) => d,
            None => return einval(),
        };
        let listing = match RT.block_on(client.client.list(&path, depth)) {
            Ok(v) => v,
            Err(e) => return fail(RWDAV_ERR, format!("rwdav_list('{}'): {}", path, e)),
        };
        let json = match serde_json::to_vec(&listing) {
            Ok(v) => v,
            Err(e) => {
                return fail(
                    RWDAV_ERR,
                    format!("rwdav_list('{}'): json encode: {}", path, e),
                )
            }
        };
        ok_bytes(json, true)
    })
}

#[no_mangle]
pub extern "C" fn rwdav_read(c: *mut RwdavClient, path: *const c_char) -> RwdavResult {
    guard("rwdav_read", || unsafe {
        let client = match client_ref(c) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let path = match arg_str(path) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let response = match RT.block_on(client.client.get(&path)) {
            Ok(v) => v,
            Err(e) => return fail(RWDAV_ERR, format!("rwdav_read('{}'): {}", path, e)),
        };
        let body = match RT.block_on(response.bytes()) {
            Ok(v) => v,
            Err(e) => return fail(RWDAV_ERR, format!("rwdav_read('{}'): {}", path, e)),
        };
        ok_bytes(body.to_vec(), false)
    })
}

#[no_mangle]
pub extern "C" fn rwdav_write(
    c: *mut RwdavClient,
    path: *const c_char,
    buf: *const u8,
    len: usize,
) -> RwdavResult {
    guard("rwdav_write", || unsafe {
        let client = match client_ref(c) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let path = match arg_str(path) {
            Ok(v) => v,
            Err(r) => return r,
        };
        if len > 0 && buf.is_null() {
            return einval();
        }
        let body = if len == 0 {
            Vec::new()
        } else {
            std::slice::from_raw_parts(buf, len).to_vec()
        };
        match RT.block_on(client.client.put(&path, body)) {
            Ok(()) => ok_empty(),
            Err(e) => fail(RWDAV_ERR, format!("rwdav_write('{}'): {}", path, e)),
        }
    })
}

#[no_mangle]
pub extern "C" fn rwdav_mkcol(c: *mut RwdavClient, path: *const c_char) -> RwdavResult {
    guard("rwdav_mkcol", || unsafe {
        let client = match client_ref(c) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let path = match arg_str(path) {
            Ok(v) => v,
            Err(r) => return r,
        };
        match RT.block_on(client.client.mkcol(&path)) {
            Ok(()) => ok_empty(),
            Err(e) => fail(RWDAV_ERR, format!("rwdav_mkcol('{}'): {}", path, e)),
        }
    })
}

#[no_mangle]
pub extern "C" fn rwdav_unzip(c: *mut RwdavClient, path: *const c_char) -> RwdavResult {
    guard("rwdav_unzip", || unsafe {
        let client = match client_ref(c) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let path = match arg_str(path) {
            Ok(v) => v,
            Err(r) => return r,
        };
        match RT.block_on(client.client.unzip(&path)) {
            Ok(()) => ok_empty(),
            Err(e) => fail(RWDAV_ERR, format!("rwdav_unzip('{}'): {}", path, e)),
        }
    })
}

#[no_mangle]
pub extern "C" fn rwdav_delete(c: *mut RwdavClient, path: *const c_char) -> RwdavResult {
    guard("rwdav_delete", || unsafe {
        let client = match client_ref(c) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let path = match arg_str(path) {
            Ok(v) => v,
            Err(r) => return r,
        };
        match RT.block_on(client.client.delete(&path)) {
            Ok(()) => ok_empty(),
            Err(e) => fail(RWDAV_ERR, format!("rwdav_delete('{}'): {}", path, e)),
        }
    })
}

#[no_mangle]
pub extern "C" fn rwdav_mv(
    c: *mut RwdavClient,
    from: *const c_char,
    to: *const c_char,
) -> RwdavResult {
    guard("rwdav_mv", || unsafe {
        let client = match client_ref(c) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let from = match arg_str(from) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let to = match arg_str(to) {
            Ok(v) => v,
            Err(r) => return r,
        };
        match RT.block_on(client.client.mv(&from, &to)) {
            Ok(()) => ok_empty(),
            Err(e) => fail(
                RWDAV_ERR,
                format!("rwdav_mv('{}', '{}'): {}", from, to, e),
            ),
        }
    })
}

#[no_mangle]
pub extern "C" fn rwdav_cp(
    c: *mut RwdavClient,
    from: *const c_char,
    to: *const c_char,
    overwrite: i32,
) -> RwdavResult {
    guard("rwdav_cp", || unsafe {
        let client = match client_ref(c) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let from = match arg_str(from) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let to = match arg_str(to) {
            Ok(v) => v,
            Err(r) => return r,
        };
        match RT.block_on(client.client.cp(&from, &to, overwrite != 0)) {
            Ok(()) => ok_empty(),
            Err(e) => fail(
                RWDAV_ERR,
                format!("rwdav_cp('{}', '{}'): {}", from, to, e),
            ),
        }
    })
}

#[no_mangle]
pub extern "C" fn rwdav_free_client(c: *mut RwdavClient) {
    if !c.is_null() {
        unsafe { drop(Box::from_raw(c)) };
    }
}

#[no_mangle]
pub extern "C" fn rwdav_free_string(s: *mut c_char) {
    if !s.is_null() {
        unsafe { drop(CString::from_raw(s)) };
    }
}

#[no_mangle]
pub extern "C" fn rwdav_free_bytes(p: *mut c_void, len: usize) {
    if p.is_null() {
        return;
    }
    unsafe {
        let slice = ptr::slice_from_raw_parts_mut(p as *mut u8, len);
        drop(Box::from_raw(slice));
    }
}

/// Frees `errmsg` and `data` of a result and zeroes the struct, so that the
/// same variable can be reused for the next call.
#[no_mangle]
pub extern "C" fn rwdav_free_result(r: *mut RwdavResult) {
    if r.is_null() {
        return;
    }
    unsafe {
        let result = &mut *r;
        if !result.errmsg.is_null() {
            drop(CString::from_raw(result.errmsg));
            result.errmsg = ptr::null_mut();
        }
        if !result.data.is_null() {
            let slice = ptr::slice_from_raw_parts_mut(result.data as *mut u8, result.len);
            drop(Box::from_raw(slice));
            result.data = ptr::null_mut();
        }
        result.len = 0;
        result.code = RWDAV_OK;
    }
}

#[no_mangle]
pub extern "C" fn rwdav_last_error() -> *const c_char {
    LAST_ERROR.with(|slot| match &*slot.borrow() {
        Some(msg) => msg.as_ptr(),
        None => ptr::null(),
    })
}
