//! Port of `facade/socket_utils.cc` plus the TCP half of helio's
//! `io/proc_reader.{h,cc}`: `GetSocketInfo` describes a socket's kernel state
//! by reading `/proc/net/tcp` / `/proc/net/tcp6` (Linux), mirroring the
//! reference's per-platform strings on non-Linux systems.

use std::io;
use std::net::{Ipv4Addr, Ipv6Addr};
use std::os::fd::RawFd;

/// A parsed `/proc/net/tcp{,6}` entry (`io::TcpInfo`, proc_reader.h).
#[derive(Debug, PartialEq, Eq)]
pub struct TcpInfo {
    pub is_ipv6: bool,
    /// TCP state code; see `tcp_state_to_string`.
    pub state: u32,
    pub local_port: u16,
    pub remote_port: u16,
    pub inode: u64,
    pub local_addr: [u8; 4],
    pub remote_addr: [u8; 4],
    pub local_addr6: [u8; 16],
    pub remote_addr6: [u8; 16],
}

/// `io::TcpStateToString` (proc_reader.cc:338): maps a numeric TCP state to the
/// reference's names.
#[must_use]
pub fn tcp_state_to_string(state: u32) -> &'static str {
    match state {
        0x01 => "ESTABLISHED",
        0x02 => "SYN_SENT",
        0x03 => "SYN_RECV",
        0x04 => "FIN_WAIT1",
        0x05 => "FIN_WAIT2",
        0x06 => "TIME_WAIT",
        0x07 => "CLOSE",
        0x08 => "CLOSE_WAIT",
        0x09 => "LAST_ACK",
        0x0A => "LISTEN",
        0x0B => "CLOSING",
        _ => "UNKNOWN",
    }
}

/// `GetSocketInfo` (socket_utils.cc:37). Linux reads the socket's kernel state
/// through `/proc/net/tcp{,6}`; other platforms return the reference's fixed
/// string. Returns a human-readable description of the TCP socket state.
#[must_use]
pub fn get_socket_info(fd: RawFd) -> String {
    if fd < 0 {
        return "invalid socket".into();
    }
    get_socket_info_impl(fd)
}

#[cfg(target_os = "linux")]
fn get_socket_info_impl(fd: RawFd) -> String {
    let inode = match socket_inode(fd) {
        Ok(inode) => inode,
        Err(_) => return "could not stat socket".into(),
    };
    let (is_ipv6, path) = match socket_family(fd) {
        Some(false) => (false, "/proc/net/tcp"),
        Some(true) => (true, "/proc/net/tcp6"),
        None => return "unsupported socket family".into(),
    };
    match read_tcp_info_from_file(path, inode, is_ipv6) {
        Ok(info) => format_socket_info(&info),
        Err(_) => "socket not found in /proc/net/tcp or /proc/net/tcp6".into(),
    }
}

#[cfg(not(target_os = "linux"))]
fn get_socket_info_impl(_fd: RawFd) -> String {
    "socket info not available on this platform".into()
}

/// `ReadTcpInfoFromFile` (proc_reader.cc:158): scan one proc file for the entry
/// whose inode matches `sock_inode`. The first line is the header; each data
/// line is split at its first `:` (stripping the `sl` column), then parsed.
/// Reachable only on Linux; exercised by unit tests everywhere.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn read_tcp_info_from_file(proc_path: &str, sock_inode: u64, is_ipv6: bool) -> io::Result<TcpInfo> {
    let contents = std::fs::read_to_string(proc_path)?;
    for line in contents.lines().skip(1) {
        let Some((_, value)) = line.split_once(':') else {
            continue;
        };
        if let Some(info) = parse_socket_line(value.trim_start(), sock_inode, is_ipv6) {
            return Ok(info);
        }
    }
    Err(io::Error::from_raw_os_error(libc::ENOENT))
}

/// `ParseSocketLine` (proc_reader.cc:103). Field layout of a data line after
/// the `sl` column is stripped:
/// `local_address rem_address st tx_queue rx_queue tr tm->when retrnsmt uid timeout inode`.
/// A line whose inode does not match `target_inode` is skipped.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn parse_socket_line(line: &str, target_inode: u64, is_ipv6: bool) -> Option<TcpInfo> {
    let parts: Vec<&str> = line.split_whitespace().collect();
    if parts.len() < 9 {
        return None;
    }
    let (local_addr_hex, local_port_hex) = parts[0].split_once(':')?;
    let (remote_addr_hex, remote_port_hex) = parts[1].split_once(':')?;
    let state = u32::from_str_radix(parts[2], 16).ok()?;
    let inode = parts[8].parse::<u64>().ok()?;
    if inode != target_inode {
        return None;
    }
    let mut info = TcpInfo {
        is_ipv6,
        state,
        local_port: 0,
        remote_port: 0,
        inode,
        local_addr: [0; 4],
        remote_addr: [0; 4],
        local_addr6: [0; 16],
        remote_addr6: [0; 16],
    };
    if let Ok(port) = u16::from_str_radix(local_port_hex, 16) {
        info.local_port = port;
    }
    if let Ok(port) = u16::from_str_radix(remote_port_hex, 16) {
        info.remote_port = port;
    }
    if is_ipv6 {
        hex_to_bytes(local_addr_hex, &mut info.local_addr6);
        hex_to_bytes(remote_addr_hex, &mut info.remote_addr6);
    } else {
        hex_to_ipv4(local_addr_hex, &mut info.local_addr);
        hex_to_ipv4(remote_addr_hex, &mut info.remote_addr);
    }
    Some(info)
}

/// IPv4 half of the address parse (proc_reader.cc:150). `/proc/net/tcp` prints
/// the little-endian in-memory bytes of the socket address, so the reference's
/// `SimpleHexAtoi` → `big_endian::Load32` → `htonl` → `inet_ntop` chain reduces
/// to the parsed value's little-endian bytes (`0100007F` → 127.0.0.1).
fn hex_to_ipv4(hex_str: &str, out: &mut [u8; 4]) {
    if let Ok(v) = u32::from_str_radix(hex_str, 16) {
        *out = v.to_le_bytes();
    }
}

/// `HexToIPv6` (proc_reader.cc:92): fills `out` with the hex string's byte
/// pairs from the start, leaving trailing bytes zero. IPv6 addresses are stored
/// and printed in network byte order, so the pairs are `inet_ntop`'s octets as
///-is (unlike IPv4's little-endian memory dump, handled by `hex_to_ipv4`).
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn hex_to_bytes(hex_str: &str, out: &mut [u8]) {
    for (i, byte) in out.iter_mut().enumerate() {
        let Some(pair) = hex_str.get(i * 2..i * 2 + 2) else {
            break;
        };
        if let Ok(b) = u8::from_str_radix(pair, 16) {
            *byte = b;
        }
    }
}

/// The tail of `GetSocketInfo` (socket_utils.cc:63): formats the reference's
/// `State: ..., Local: ..., Remote: ..., Inode: ...` line. IPv6 addresses are
/// bracketed; `Ipv6Addr`'s RFC 5952 rendering matches `inet_ntop`'s.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn format_socket_info(info: &TcpInfo) -> String {
    let state = tcp_state_to_string(info.state);
    if info.is_ipv6 {
        format!(
            "State: {state}, Local: [{}]:{}, Remote: [{}]:{}, Inode: {}",
            Ipv6Addr::from(info.local_addr6),
            info.local_port,
            Ipv6Addr::from(info.remote_addr6),
            info.remote_port,
            info.inode
        )
    } else {
        format!(
            "State: {state}, Local: {}:{}, Remote: {}:{}, Inode: {}",
            Ipv4Addr::from(info.local_addr),
            info.local_port,
            Ipv4Addr::from(info.remote_addr),
            info.remote_port,
            info.inode
        )
    }
}

#[cfg(target_os = "linux")]
fn socket_inode(fd: RawFd) -> io::Result<u64> {
    let mut st: libc::stat = unsafe { std::mem::zeroed() };
    if unsafe { libc::fstat(fd, &mut st) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(st.st_ino)
}

#[cfg(target_os = "linux")]
fn socket_family(fd: RawFd) -> Option<bool> {
    let mut ss: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
    let mut len = std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
    let rc = unsafe {
        libc::getsockname(
            fd,
            (&mut ss as *mut libc::sockaddr_storage).cast::<libc::sockaddr>(),
            &mut len,
        )
    };
    if rc != 0 {
        return None;
    }
    match ss.ss_family as i32 {
        libc::AF_INET => Some(false),
        libc::AF_INET6 => Some(true),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(value: &str, inode: u64, is_ipv6: bool) -> Option<TcpInfo> {
        parse_socket_line(value.trim_start(), inode, is_ipv6)
    }

    /// A realistic `/proc/net/tcp` data line (after the `sl` column) for a
    /// LISTEN socket on 127.0.0.1:53 (kernel hex `0100007F:0035`).
    const TCP4_LINE: &str = "0100007F:0035 00000000:0000 0A 00000000:00000000 00:00000000 00000000 114 0 19611 1 0000000000000000 100 0 0 10 0";

    #[test]
    fn state_strings() {
        let expect = [
            (0x01, "ESTABLISHED"),
            (0x02, "SYN_SENT"),
            (0x03, "SYN_RECV"),
            (0x04, "FIN_WAIT1"),
            (0x05, "FIN_WAIT2"),
            (0x06, "TIME_WAIT"),
            (0x07, "CLOSE"),
            (0x08, "CLOSE_WAIT"),
            (0x09, "LAST_ACK"),
            (0x0A, "LISTEN"),
            (0x0B, "CLOSING"),
            (0x10, "UNKNOWN"),
        ];
        for (state, name) in expect {
            assert_eq!(tcp_state_to_string(state), name);
        }
    }

    #[test]
    fn parse_ipv4_line() {
        let info = parse(TCP4_LINE, 19611, false).unwrap();
        assert!(!info.is_ipv6);
        assert_eq!(info.state, 0x0A);
        assert_eq!(info.local_port, 53);
        assert_eq!(info.remote_port, 0);
        assert_eq!(info.inode, 19611);
        assert_eq!(info.local_addr, [127, 0, 0, 1]);
        assert_eq!(info.remote_addr, [0, 0, 0, 0]);
        assert_eq!(
            format_socket_info(&info),
            "State: LISTEN, Local: 127.0.0.1:53, Remote: 0.0.0.0:0, Inode: 19611"
        );
    }

    #[test]
    fn parse_ipv6_line() {
        let line = "00000000000000000000000000000000:0065 00000000000000000000000000000000:0000 0A 00000000:00000000 00:00000000 00000000 0 0 44000 1 0000000000000000 100 0 0 10 0";
        let info = parse(line, 44000, true).unwrap();
        assert!(info.is_ipv6);
        assert_eq!(info.local_port, 101);
        assert_eq!(info.remote_port, 0);
        assert_eq!(info.local_addr6, [0; 16]);
        assert_eq!(
            format_socket_info(&info),
            "State: LISTEN, Local: [::]:101, Remote: [::]:0, Inode: 44000"
        );
    }

    #[test]
    fn ipv6_addr_renders_canonical() {
        let mut info = parse(TCP4_LINE, 19611, true).unwrap();
        info.local_addr6 = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];
        let s = format_socket_info(&info);
        assert!(s.contains("Local: [::1]:53"), "{s}");
    }

    #[test]
    fn mismatched_inode_is_skipped() {
        assert!(parse(TCP4_LINE, 9999, false).is_none());
    }

    #[test]
    fn malformed_lines_are_skipped() {
        assert!(parse("", 19611, false).is_none());
        assert!(parse("0100007F:0035 00000000:0000 0A", 19611, false).is_none());
        assert!(
            parse(
                "no-colon here 0A 00000000:00000000 00:00000000 00000000 0 0 19611",
                19611,
                false
            )
            .is_none()
        );
        assert!(
            parse(
                "0100007F:0035 00000000:0000 ZZ 00000000:00000000 00:00000000 00000000 0 0 19611",
                19611,
                false
            )
            .is_none()
        );
        assert!(parse("0100007F:0035 00000000:0000 0A 00000000:00000000 00:00000000 00000000 0 0 notanumber", 19611, false).is_none());
    }

    #[test]
    fn unknown_state_formats() {
        let mut info = parse(TCP4_LINE, 19611, false).unwrap();
        info.state = 0xFF;
        let s = format_socket_info(&info);
        assert!(s.starts_with("State: UNKNOWN, "), "{s}");
    }

    #[test]
    fn invalid_fd_is_rejected() {
        assert_eq!(get_socket_info(-1), "invalid socket");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_reads_proc_net_tcp() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let fd = std::os::fd::AsRawFd::as_raw_fd(&listener);
        let info = get_socket_info(fd);
        assert!(info.contains("State: LISTEN"), "{info}");
        assert!(info.contains("Local: 127.0.0.1:"), "{info}");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_reads_proc_net_tcp6() {
        let Ok(listener) = std::net::TcpListener::bind("[::1]:0") else {
            return;
        };
        let fd = std::os::fd::AsRawFd::as_raw_fd(&listener);
        let info = get_socket_info(fd);
        assert!(info.contains("State: LISTEN"), "{info}");
        assert!(info.contains("Local: [::1]:"), "{info}");
    }

    #[cfg(not(target_os = "linux"))]
    #[test]
    fn non_linux_platform_string() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let fd = std::os::fd::AsRawFd::as_raw_fd(&listener);
        assert_eq!(
            get_socket_info(fd),
            "socket info not available on this platform"
        );
    }
}
