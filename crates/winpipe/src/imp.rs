//! The Windows implementation. Each `unsafe` block wraps one Win32 call and
//! says what it relies on.

use std::ffi::{OsStr, c_void};
use std::io;
use std::os::windows::io::AsRawHandle;
use std::ptr;

use tokio::net::windows::named_pipe::{
    ClientOptions, NamedPipeClient, NamedPipeServer, PipeMode, ServerOptions,
};
use windows_sys::Win32::Foundation::{
    CloseHandle, ERROR_INSUFFICIENT_BUFFER, ERROR_PIPE_BUSY, HANDLE, LocalFree,
};
use windows_sys::Win32::Security::Authorization::{
    ConvertSidToStringSidW, ConvertStringSecurityDescriptorToSecurityDescriptorW, GetSecurityInfo,
    SDDL_REVISION_1, SE_KERNEL_OBJECT,
};
use windows_sys::Win32::Security::{
    GetTokenInformation, OWNER_SECURITY_INFORMATION, PSECURITY_DESCRIPTOR, PSID, RevertToSelf,
    SECURITY_ATTRIBUTES, TOKEN_QUERY, TOKEN_USER, TokenUser,
};
use windows_sys::Win32::Storage::FileSystem::{SECURITY_IDENTIFICATION, SECURITY_SQOS_PRESENT};
use windows_sys::Win32::System::Pipes::{
    GetNamedPipeClientProcessId, GetNamedPipeServerProcessId, ImpersonateNamedPipeClient,
};
use windows_sys::Win32::System::Threading::{
    GetCurrentProcess, GetCurrentThread, OpenProcessToken, OpenThreadToken,
};

/// What the kernel says about the process on the other end of a pipe.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PipePeer {
    /// The account SID in string form, `S-1-5-21-...`.
    pub sid: String,
    /// The process id, as `GetNamedPipeClientProcessId` /
    /// `GetNamedPipeServerProcessId` report it: an observation, never an
    /// identity.
    pub pid: u32,
}

/// A security descriptor granting full access to this process's user and
/// to `SYSTEM`, and nothing to anyone else.
///
/// The default descriptor of a named pipe grants "read access to Everyone
/// and the anonymous account" (`docs/research/ipc.md` §3.2), which is why
/// [0010 §4.5] requires an explicit one. This is the pipe's counterpart of a
/// `0600` socket file: the owner is named by SID rather than left to the
/// token's default owner, which for an elevated administrator is the
/// `Administrators` group rather than the account.
pub struct OwnerOnlyDacl {
    sd: PSECURITY_DESCRIPTOR,
}

// SAFETY: the descriptor is written once by `ConvertStringSecurityDescriptor
// ToSecurityDescriptorW` and only read afterwards; it is freed exactly once,
// in `Drop`.
unsafe impl Send for OwnerOnlyDacl {}
// SAFETY: as above — shared reads of immutable memory.
unsafe impl Sync for OwnerOnlyDacl {}

impl OwnerOnlyDacl {
    /// Builds the descriptor for the user this process runs as.
    pub fn for_current_user() -> io::Result<OwnerOnlyDacl> {
        let sid = Token::of_process()?.user_sid()?;
        // SDDL: owner is this account; a protected DACL (no inheritance)
        // with generic-all for SYSTEM and for this account.
        let sddl = format!("O:{sid}D:P(A;;GA;;;SY)(A;;GA;;;{sid})");
        let wide: Vec<u16> = sddl.encode_utf16().chain(Some(0)).collect();
        let mut sd: PSECURITY_DESCRIPTOR = ptr::null_mut();
        // SAFETY: `wide` is a NUL-terminated UTF-16 string that outlives the
        // call; `sd` is a valid out-pointer and receives a `LocalAlloc`ed
        // self-relative descriptor that `Drop` frees.
        let ok = unsafe {
            ConvertStringSecurityDescriptorToSecurityDescriptorW(
                wide.as_ptr(),
                SDDL_REVISION_1,
                &mut sd,
                ptr::null_mut(),
            )
        };
        if ok == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(OwnerOnlyDacl { sd })
    }
}

impl Drop for OwnerOnlyDacl {
    fn drop(&mut self) {
        // SAFETY: `sd` came from `ConvertStringSecurityDescriptorToSecurity
        // DescriptorW`, which documents `LocalFree` as its release.
        unsafe { LocalFree(self.sd) };
    }
}

impl std::fmt::Debug for OwnerOnlyDacl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("OwnerOnlyDacl")
    }
}

/// Creates one server instance of the pipe at `path` (`\\.\pipe\<name>`).
///
/// Byte mode, local clients only (`PIPE_REJECT_REMOTE_CLIENTS`), and the
/// caller's descriptor rather than the default one. `first` sets
/// `FILE_FLAG_FIRST_PIPE_INSTANCE`, which makes the call fail if the name
/// already exists — the answer to pipe squatting, where another process
/// creates the name first and receives the clients meant for this one.
pub fn create_instance(
    path: &OsStr,
    dacl: &OwnerOnlyDacl,
    first: bool,
) -> io::Result<NamedPipeServer> {
    let mut attrs = SECURITY_ATTRIBUTES {
        nLength: size_of::<SECURITY_ATTRIBUTES>() as u32,
        lpSecurityDescriptor: dacl.sd,
        bInheritHandle: 0,
    };
    // SAFETY: `attrs` is a fully initialised `SECURITY_ATTRIBUTES` that lives
    // for the duration of the call, and the descriptor it points to is owned
    // by `dacl`, which outlives the call; `CreateNamedPipeW` copies what it
    // keeps.
    unsafe {
        ServerOptions::new()
            .pipe_mode(PipeMode::Byte)
            .reject_remote_clients(true)
            .first_pipe_instance(first)
            .create_with_security_attributes_raw(path, ptr::from_mut(&mut attrs).cast::<c_void>())
    }
}

/// Opens the client end of the pipe at `path`.
///
/// Identification-level impersonation only: the server may learn who the
/// client is, and may not act as the client. `ERROR_PIPE_BUSY` — every
/// instance connected, none listening — is returned as is; see
/// [`is_pipe_busy`].
pub fn open_client(path: &OsStr) -> io::Result<NamedPipeClient> {
    ClientOptions::new()
        .read(true)
        .write(true)
        .security_qos_flags(SECURITY_IDENTIFICATION | SECURITY_SQOS_PRESENT)
        .open(path)
}

/// Whether `error` is `ERROR_PIPE_BUSY`: the pipe exists and every instance
/// is taken, so the open should be retried.
pub fn is_pipe_busy(error: &io::Error) -> bool {
    error.raw_os_error() == Some(ERROR_PIPE_BUSY as i32)
}

/// Who connected to `server`, by the kernel's account.
///
/// Reads the client's token SID through `ImpersonateNamedPipeClient`,
/// which requires that the client has written at least one byte, and
/// reverts before returning on every path. The SID is the identity; the pid
/// is an observation.
pub fn client_peer(server: &NamedPipeServer) -> io::Result<PipePeer> {
    let handle: HANDLE = server.as_raw_handle();
    let pid = pipe_pid(handle, GetNamedPipeClientProcessId)?;
    let sid = {
        let _impersonating = Impersonation::begin(handle)?;
        Token::of_thread()?.user_sid()?
    };
    Ok(PipePeer { sid, pid })
}

/// Who created the pipe `client` connected to, by the kernel's account.
///
/// The owner SID of the pipe object, which the creating process's token
/// set; the pid is `GetNamedPipeServerProcessId`'s.
pub fn server_peer(client: &NamedPipeClient) -> io::Result<PipePeer> {
    let handle: HANDLE = client.as_raw_handle();
    let pid = pipe_pid(handle, GetNamedPipeServerProcessId)?;
    let sid = owner_sid(handle)?;
    Ok(PipePeer { sid, pid })
}

fn pipe_pid(
    handle: HANDLE,
    query: unsafe extern "system" fn(HANDLE, *mut u32) -> i32,
) -> io::Result<u32> {
    let mut pid = 0u32;
    // SAFETY: `handle` is an open pipe handle borrowed for the call, `pid` a
    // valid out-pointer.
    if unsafe { query(handle, &mut pid) } == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(pid)
}

fn owner_sid(handle: HANDLE) -> io::Result<String> {
    let mut owner: PSID = ptr::null_mut();
    let mut sd: PSECURITY_DESCRIPTOR = ptr::null_mut();
    // SAFETY: `handle` is an open handle borrowed for the call; the out
    // pointers are valid; `sd` receives a `LocalAlloc`ed descriptor that
    // owns the memory `owner` points into, freed below after the SID is
    // copied out.
    let err = unsafe {
        GetSecurityInfo(
            handle,
            SE_KERNEL_OBJECT,
            OWNER_SECURITY_INFORMATION,
            &mut owner,
            ptr::null_mut(),
            ptr::null_mut(),
            ptr::null_mut(),
            &mut sd,
        )
    };
    if err != 0 {
        return Err(io::Error::from_raw_os_error(err as i32));
    }
    let sid = sid_to_string(owner);
    // SAFETY: `sd` came from `GetSecurityInfo`, which documents `LocalFree`
    // as its release; nothing reads `owner` after this.
    unsafe { LocalFree(sd) };
    sid
}

/// Client impersonation on the current thread, reverted on drop.
struct Impersonation(());

impl Impersonation {
    fn begin(pipe: HANDLE) -> io::Result<Impersonation> {
        // SAFETY: `pipe` is an open server-end handle borrowed for the call.
        // The impersonation is thread-local and `Drop` reverts it before any
        // await can move this task to another thread.
        if unsafe { ImpersonateNamedPipeClient(pipe) } == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Impersonation(()))
    }
}

impl Drop for Impersonation {
    fn drop(&mut self) {
        // SAFETY: no preconditions.
        if unsafe { RevertToSelf() } == 0 {
            // A thread that cannot drop the client's identity must not go on
            // to run anything else as that identity. A panic would not stop
            // it: whatever runs the task catches the unwinding and the thread
            // — a runtime worker — carries on with the client's token still
            // attached. Only ending the process does.
            eprintln!(
                "RevertToSelf failed after ImpersonateNamedPipeClient: {}",
                io::Error::last_os_error()
            );
            std::process::abort();
        }
    }
}

/// An access token handle, closed on drop.
struct Token(HANDLE);

impl Token {
    /// The impersonation token of the current thread.
    fn of_thread() -> io::Result<Token> {
        let mut handle: HANDLE = ptr::null_mut();
        // SAFETY: `GetCurrentThread` is a pseudo-handle that needs no
        // closing; `handle` is a valid out-pointer. `OpenAsSelf` is set so
        // the check runs against the process's own identity rather than the
        // client's, which may not be allowed to open its own token.
        if unsafe { OpenThreadToken(GetCurrentThread(), TOKEN_QUERY, 1, &mut handle) } == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Token(handle))
    }

    /// The primary token of this process.
    fn of_process() -> io::Result<Token> {
        let mut handle: HANDLE = ptr::null_mut();
        // SAFETY: `GetCurrentProcess` is a pseudo-handle that needs no
        // closing; `handle` is a valid out-pointer.
        if unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut handle) } == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Token(handle))
    }

    /// The token's user SID, as a string.
    fn user_sid(&self) -> io::Result<String> {
        let mut len = 0u32;
        // SAFETY: a null buffer of length zero asks for the required size;
        // the contract is that the call fails with
        // `ERROR_INSUFFICIENT_BUFFER` and sets `len`.
        unsafe { GetTokenInformation(self.0, TokenUser, ptr::null_mut(), 0, &mut len) };
        let sizing = io::Error::last_os_error();
        if sizing.raw_os_error() != Some(ERROR_INSUFFICIENT_BUFFER as i32) {
            return Err(sizing);
        }
        // `u64` storage so the buffer satisfies `TOKEN_USER`'s pointer
        // alignment.
        let mut buf = vec![0u64; (len as usize).div_ceil(size_of::<u64>())];
        // SAFETY: `buf` is at least `len` bytes of writable, 8-byte-aligned
        // memory, which is what `TOKEN_USER` requires; `len` is a valid
        // out-pointer.
        let ok = unsafe {
            GetTokenInformation(
                self.0,
                TokenUser,
                buf.as_mut_ptr().cast::<c_void>(),
                len,
                &mut len,
            )
        };
        if ok == 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: the call filled `buf` with a `TOKEN_USER` whose SID pointer
        // points inside `buf`, which lives until this function returns; the
        // string is copied out before that.
        let sid: PSID = unsafe { (*buf.as_ptr().cast::<TOKEN_USER>()).User.Sid };
        sid_to_string(sid)
    }
}

impl Drop for Token {
    fn drop(&mut self) {
        // SAFETY: `self.0` is a token handle this type opened and owns.
        unsafe { CloseHandle(self.0) };
    }
}

fn sid_to_string(sid: PSID) -> io::Result<String> {
    let mut wide: *mut u16 = ptr::null_mut();
    // SAFETY: `sid` is a valid SID for the duration of the call; `wide` is a
    // valid out-pointer and receives a `LocalAlloc`ed NUL-terminated UTF-16
    // string freed below.
    if unsafe { ConvertSidToStringSidW(sid, &mut wide) } == 0 {
        return Err(io::Error::last_os_error());
    }
    let mut len = 0usize;
    // SAFETY: the string is NUL-terminated, so every index read before the
    // terminator is in bounds.
    while unsafe { *wide.add(len) } != 0 {
        len += 1;
    }
    // SAFETY: `len` code units starting at `wide` were just read as
    // non-NUL and are valid for the lifetime of the allocation.
    let text = String::from_utf16_lossy(unsafe { std::slice::from_raw_parts(wide, len) });
    // SAFETY: `wide` came from `ConvertSidToStringSidW`, which documents
    // `LocalFree` as its release.
    unsafe { LocalFree(wide.cast::<c_void>()) };
    Ok(text)
}
