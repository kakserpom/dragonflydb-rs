use crate::error::RespValue;

const MAX_MULTIBULK: usize = 1_048_576;
const MAX_BULK: usize = 512 * 1024 * 1024;
const MAX_INLINE: usize = 64 * 1024;

/// Incremental RESP2 request parser used by connection handling.
pub struct RespParser {
    buf: Vec<u8>,
    protocol_error: bool,
}

impl Default for RespParser {
    fn default() -> Self {
        Self::new()
    }
}

impl RespParser {
    #[must_use]
    pub fn new() -> Self {
        RespParser {
            buf: Vec::with_capacity(1024),
            protocol_error: false,
        }
    }

    pub fn feed(&mut self, data: &[u8]) {
        if self.protocol_error {
            return;
        }
        self.buf.extend_from_slice(data);
    }

    #[must_use]
    pub fn has_protocol_error(&self) -> bool {
        self.protocol_error
    }

    /// Try to parse a complete request. Returns Ok(None) when more data is needed.
    pub fn next_request(&mut self) -> Result<Option<Vec<Vec<u8>>>, String> {
        if self.protocol_error {
            return Err("Protocol error: connection reset by protocol error".to_string());
        }
        if self.buf.is_empty() {
            return Ok(None);
        }
        let result = match self.buf[0] {
            b'*' => self.parse_multibulk(),
            other => {
                if other == b'$' || other == b'+' || other == b'-' || other == b':' {
                    return Err(format!(
                        "Protocol error: invalid multibulk length, expected '*', got '{}'",
                        other as char
                    ));
                }
                self.parse_inline()
            }
        };
        match result {
            Ok(Some((consumed, args))) => {
                self.buf.drain(..consumed);
                Ok(Some(args))
            }
            Ok(None) => Ok(None),
            Err(e) => {
                self.protocol_error = true;
                Err(e)
            }
        }
    }

    fn parse_multibulk(&mut self) -> Result<Option<(usize, Vec<Vec<u8>>)>, String> {
        let (line_end, line_len) = match find_crlf(&self.buf) {
            Some(p) => (p, p),
            None => return Ok(None),
        };
        let line = &self.buf[1..line_len];
        let count: i64 = parse_decimal(line)
            .ok_or_else(|| "Protocol error: invalid multibulk length".to_string())?;
        if count < 0 || count as usize > MAX_MULTIBULK {
            return Err("Protocol error: invalid multibulk length".to_string());
        }
        let count = count as usize;
        let mut pos = line_end + 2;
        let mut args = Vec::with_capacity(count);
        for _ in 0..count {
            if self.buf.len() <= pos {
                return Ok(None);
            }
            if self.buf[pos] != b'$' {
                return Err(format!(
                    "Protocol error: expected '$', got '{}'",
                    self.buf[pos] as char
                ));
            }
            let (blen, bp) = match find_crlf(&self.buf[pos + 1..]) {
                Some(p) => (p, pos + 1),
                None => return Ok(None),
            };
            let len: i64 = parse_decimal(&self.buf[bp..bp + blen])
                .ok_or_else(|| "Protocol error: invalid bulk length".to_string())?;
            if len < 0 || len as usize > MAX_BULK {
                return Err("Protocol error: invalid bulk length".to_string());
            }
            let len = len as usize;
            let start = bp + blen + 2;
            let end = start + len;
            if self.buf.len() < end + 2 {
                return Ok(None);
            }
            if &self.buf[end..end + 2] != b"\r\n" {
                return Err("Protocol error: invalid bulk terminator".to_string());
            }
            args.push(self.buf[start..end].to_vec());
            pos = end + 2;
        }
        Ok(Some((pos, args)))
    }

    fn parse_inline(&mut self) -> Result<Option<(usize, Vec<Vec<u8>>)>, String> {
        loop {
            let mut end = None;
            let mut crlf = false;
            let mut i = 0;
            while i < self.buf.len() {
                if self.buf[i] == b'\n' {
                    end = Some(i);
                    if i > 0 && self.buf[i - 1] == b'\r' {
                        crlf = true;
                    }
                    break;
                }
                i += 1;
            }
            let Some(end) = end else {
                if self.buf.len() > MAX_INLINE {
                    return Err("Protocol error: too big inline request".to_string());
                }
                return Ok(None);
            };
            let line_end = if crlf { end - 1 } else { end };
            let line = &self.buf[..line_end];
            let consumed = end + 1;
            if line.is_empty() {
                // empty line: skip and continue with the next one
                self.buf.drain(..consumed);
                continue;
            }
            let args = split_inline(line)?;
            return Ok(Some((consumed, args)));
        }
    }
}

/// Split an inline command line respecting double quotes (Redis semantics).
fn split_inline(line: &[u8]) -> Result<Vec<Vec<u8>>, String> {
    let mut args = Vec::new();
    let mut cur = Vec::new();
    let mut in_quotes = false;
    let mut escaped = false;
    for &b in line {
        if escaped {
            cur.push(b);
            escaped = false;
            continue;
        }
        if b == b'\\' && in_quotes {
            escaped = true;
            continue;
        }
        if b == b'"' {
            in_quotes = !in_quotes;
            continue;
        }
        if b.is_ascii_whitespace() && !in_quotes {
            if !cur.is_empty() {
                args.push(std::mem::take(&mut cur));
            }
            continue;
        }
        cur.push(b);
    }
    if in_quotes {
        return Err("Protocol error: unbalanced quotes in request".to_string());
    }
    if escaped {
        cur.push(b'\\');
    }
    if !cur.is_empty() {
        args.push(cur);
    }
    Ok(args)
}

fn find_crlf(buf: &[u8]) -> Option<usize> {
    buf.windows(2).position(|w| w == b"\r\n")
}

fn parse_decimal(s: &[u8]) -> Option<i64> {
    crate::util::parse_i64(s)
}

// ---------------------------------------------------------------------------
// Reply encoding (RESP2)
// ---------------------------------------------------------------------------

pub fn encode_reply(value: &RespValue, out: &mut Vec<u8>) {
    match value {
        RespValue::Simple(s) => {
            out.extend_from_slice(b"+");
            out.extend_from_slice(s.as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        RespValue::Error(e) => {
            // Error strings carry their prefix inline ("ERR ...", "NOSCRIPT ...",
            // or a leading "-" added by the script error-table formatter); never
            // emit a double dash (helio `SendError` only prefixes when missing).
            if !e.starts_with('-') {
                out.extend_from_slice(b"-");
            }
            out.extend_from_slice(e.as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        RespValue::Integer(i) => {
            out.extend_from_slice(b":");
            out.extend_from_slice(&crate::util::itoa(*i));
            out.extend_from_slice(b"\r\n");
        }
        RespValue::Bulk(b) => {
            out.extend_from_slice(b"$");
            out.extend_from_slice(&crate::util::itoa(b.len() as i64));
            out.extend_from_slice(b"\r\n");
            out.extend_from_slice(b);
            out.extend_from_slice(b"\r\n");
        }
        RespValue::Nil => out.extend_from_slice(b"$-1\r\n"),
        RespValue::Array(items) => {
            out.extend_from_slice(b"*");
            out.extend_from_slice(&crate::util::itoa(items.len() as i64));
            out.extend_from_slice(b"\r\n");
            for item in items {
                encode_reply(item, out);
            }
        }
        RespValue::Double(f) => {
            // RESP2 has no double type; render as bulk string.
            let s = crate::util::format_double(*f);
            encode_reply(&RespValue::Bulk(s.into_bytes()), out);
        }
        RespValue::Bool(b) => {
            encode_reply(&RespValue::Integer(i64::from(*b)), out);
        }
        RespValue::Map(pairs) => {
            // RESP2: emit a flat array [k1,v1,k2,v2,...]
            let mut items = Vec::with_capacity(pairs.len() * 2);
            for (k, v) in pairs {
                items.push(k.clone());
                items.push(v.clone());
            }
            encode_reply(&RespValue::Array(items), out);
        }
    }
}

/// A null multi-bulk reply (`*-1\r\n`), distinct from a null bulk string
/// (`$-1\r\n`). Blocking commands time out with this, as the reference's
/// `NIL_ARRAY` (Redis `addReplyNullArray`).
#[must_use]
pub fn encode_nil_array() -> Vec<u8> {
    b"*-1\r\n".to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn feed_req(p: &mut RespParser, s: &str) -> Result<Option<Vec<Vec<u8>>>, String> {
        p.feed(s.as_bytes());
        p.next_request()
    }

    #[test]
    fn parse_basic_request() {
        let mut p = RespParser::new();
        assert_eq!(
            feed_req(&mut p, "*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n").unwrap(),
            Some(vec![b"ECHO".to_vec(), b"hello".to_vec()])
        );
        assert!(p.buf.is_empty());
    }

    #[test]
    fn partial_request_waits() {
        let mut p = RespParser::new();
        assert!(
            feed_req(&mut p, "*2\r\n$4\r\nECHO\r\n$5\r\nhe")
                .unwrap()
                .is_none()
        );
        p.feed(b"llo\r\n");
        assert_eq!(
            p.next_request().unwrap(),
            Some(vec![b"ECHO".to_vec(), b"hello".to_vec()])
        );
    }

    #[test]
    fn inline_command() {
        let mut p = RespParser::new();
        assert_eq!(
            feed_req(&mut p, "PING\r\n").unwrap(),
            Some(vec![b"PING".to_vec()])
        );
        let mut p = RespParser::new();
        assert_eq!(
            feed_req(&mut p, "SET \"my key\" \"my value\"\r\n").unwrap(),
            Some(vec![
                b"SET".to_vec(),
                b"my key".to_vec(),
                b"my value".to_vec()
            ])
        );
    }

    #[test]
    fn protocol_error() {
        let mut p = RespParser::new();
        let r = feed_req(&mut p, "+OK\r\n");
        assert!(r.is_err());
    }

    #[test]
    fn encode_works() {
        let mut out = Vec::new();
        encode_reply(&RespValue::Simple("OK".into()), &mut out);
        assert_eq!(out, b"+OK\r\n");
        let mut out = Vec::new();
        encode_reply(&RespValue::Bulk(b"hello".to_vec()), &mut out);
        assert_eq!(out, b"$5\r\nhello\r\n");
        let mut out = Vec::new();
        encode_reply(&RespValue::Nil, &mut out);
        assert_eq!(out, b"$-1\r\n");
        let mut out = Vec::new();
        encode_reply(
            &RespValue::Array(vec![RespValue::Integer(1), RespValue::Bulk(b"a".to_vec())]),
            &mut out,
        );
        assert_eq!(out, b"*2\r\n:1\r\n$1\r\na\r\n");
    }

    #[test]
    fn encode_error_never_double_dashes() {
        // Inline prefix ("ERR ...") -> one dash from the encoder.
        let mut out = Vec::new();
        encode_reply(&RespValue::Error("ERR syntax error".into()), &mut out);
        assert_eq!(out, b"-ERR syntax error\r\n");
        // Prefixless message -> encoder dash.
        let mut out = Vec::new();
        encode_reply(
            &RespValue::Error("NOSCRIPT No matching script. Please use EVAL.".into()),
            &mut out,
        );
        assert_eq!(out, b"-NOSCRIPT No matching script. Please use EVAL.\r\n");
        // Leading dash already present (script error-table formatter) -> verbatim.
        let mut out = Vec::new();
        encode_reply(&RespValue::Error("-oops".into()), &mut out);
        assert_eq!(out, b"-oops\r\n");
    }
}
