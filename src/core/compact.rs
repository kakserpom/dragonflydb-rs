use std::borrow::Borrow;
use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::ops::Deref;

/// Number of bytes stored inline, mirroring Dragonfly's `CompactObject` (15 bytes).
pub const INLINE_CAP: usize = 15;

/// A binary-safe string that stores up to 15 bytes inline, otherwise on the heap.
/// This is the Rust analogue of Dragonfly's `CompactObject` used for STRING values
/// and map keys.
#[derive(Clone, Default)]
pub struct CompactString {
    inline: [u8; INLINE_CAP],
    len: u8,
    heap: Option<Box<[u8]>>,
}

impl CompactString {
    pub fn new() -> Self {
        Self { inline: [0u8; INLINE_CAP], len: 0, heap: None }
    }

    pub fn from_bytes(b: &[u8]) -> Self {
        if b.len() <= INLINE_CAP {
            let mut inline = [0u8; INLINE_CAP];
            inline[..b.len()].copy_from_slice(b);
            Self { inline, len: b.len() as u8, heap: None }
        } else {
            Self {
                inline: [0u8; INLINE_CAP],
                len: b.len() as u8,
                heap: Some(b.to_vec().into_boxed_slice()),
            }
        }
    }

    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        if self.len as usize <= INLINE_CAP {
            &self.inline[..self.len as usize]
        } else {
            self.heap.as_deref().unwrap()
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len as usize
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.as_bytes().to_vec()
    }

    /// Append at the end (creates a new compact string).
    pub fn push_str(&self, suffix: &[u8]) -> Self {
        let mut v = Vec::with_capacity(self.len() + suffix.len());
        v.extend_from_slice(self.as_bytes());
        v.extend_from_slice(suffix);
        CompactString::from_bytes(&v)
    }
}

impl From<&str> for CompactString {
    fn from(s: &str) -> Self {
        CompactString::from_bytes(s.as_bytes())
    }
}

impl From<String> for CompactString {
    fn from(s: String) -> Self {
        CompactString::from_bytes(s.as_bytes())
    }
}

impl AsRef<[u8]> for CompactString {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl Borrow<[u8]> for CompactString {
    fn borrow(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl Deref for CompactString {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl PartialEq for CompactString {
    fn eq(&self, other: &Self) -> bool {
        self.as_bytes() == other.as_bytes()
    }
}
impl Eq for CompactString {}

impl PartialOrd for CompactString {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for CompactString {
    fn cmp(&self, other: &Self) -> Ordering {
        self.as_bytes().cmp(other.as_bytes())
    }
}

impl Hash for CompactString {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_bytes().hash(state);
    }
}

impl fmt::Debug for CompactString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", String::from_utf8_lossy(self.as_bytes()))
    }
}

impl fmt::Display for CompactString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", String::from_utf8_lossy(self.as_bytes()))
    }
}

impl PartialEq<[u8]> for CompactString {
    fn eq(&self, other: &[u8]) -> bool {
        self.as_bytes() == other
    }
}
impl PartialEq<&str> for CompactString {
    fn eq(&self, other: &&str) -> bool {
        self.as_bytes() == other.as_bytes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inline_and_heap() {
        let short = CompactString::from("hello");
        assert_eq!(short.as_bytes(), b"hello");
        assert_eq!(short.heap, None);

        let long = CompactString::from("this is a very long string beyond 15 bytes");
        assert!(long.heap.is_some());
        assert_eq!(long.as_bytes(), b"this is a very long string beyond 15 bytes");
    }

    #[test]
    fn binary_safe() {
        let s = CompactString::from_bytes(&[0, 1, 2, 255, 10]);
        assert_eq!(s.as_bytes(), &[0, 1, 2, 255, 10]);
    }

    #[test]
    fn ordering() {
        let a = CompactString::from("abc");
        let b = CompactString::from("abd");
        assert!(a < b);
        assert!(a == CompactString::from("abc"));
        assert!(a != CompactString::from("ab"));
    }
}
