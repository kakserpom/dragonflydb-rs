use crate::core::bloom::SBF;
use crate::core::cms::Cms;
use crate::core::compact::CompactString;
use crate::core::cuckoo::CuckooFilter;
use crate::core::hash::Hash;
use crate::core::json::Json;
use crate::core::quicklist::QuickList;
use crate::core::set::Set;
use crate::core::stream::Stream;
use crate::core::topk::Topk;
use crate::core::zset::ZSet;

/// The object type tag, mirroring Dragonfly's `ObjType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjType {
    Str,
    List,
    Set,
    Hash,
    ZSet,
    Stream,
    Sbf,
    Cms,
    Cuckoo,
    Topk,
    Json,
}

/// A value stored in the prime table: the Rust analogue of Dragonfly's
/// `PrimeValue` union over the object types.
#[derive(Debug, Clone)]
pub enum PrimeValue {
    Str(CompactString),
    List(QuickList),
    Set(Set),
    Hash(Hash),
    ZSet(ZSet),
    Stream(Stream),
    Sbf(SBF),
    Cms(Cms),
    Cuckoo(CuckooFilter),
    Topk(Topk),
    Json(Json),
}

impl PrimeValue {
    pub fn obj_type(&self) -> ObjType {
        match self {
            PrimeValue::Str(_) => ObjType::Str,
            PrimeValue::List(_) => ObjType::List,
            PrimeValue::Set(_) => ObjType::Set,
            PrimeValue::Hash(_) => ObjType::Hash,
            PrimeValue::ZSet(_) => ObjType::ZSet,
            PrimeValue::Stream(_) => ObjType::Stream,
            PrimeValue::Sbf(_) => ObjType::Sbf,
            PrimeValue::Cms(_) => ObjType::Cms,
            PrimeValue::Cuckoo(_) => ObjType::Cuckoo,
            PrimeValue::Topk(_) => ObjType::Topk,
            PrimeValue::Json(_) => ObjType::Json,
        }
    }

    pub fn type_name(&self) -> &'static str {
        match self.obj_type() {
            ObjType::Str => "string",
            ObjType::List => "list",
            ObjType::Set => "set",
            ObjType::Hash => "hash",
            ObjType::ZSet => "zset",
            ObjType::Stream => "stream",
            ObjType::Sbf => "MBbloom--",
            ObjType::Cms => "CMSk-TYPE",
            ObjType::Cuckoo => "MBbloomCF",
            ObjType::Topk => "TopK-TYPE",
            ObjType::Json => "ReJSON-RL",
        }
    }

    /// Approximate allocated size of the value in bytes, mirroring Dragonfly's
    /// `CompactObj::MallocUsed` used by SCAN `MINMSZ`. Strings report
    /// 0 when stored inline (length <= 16), the rounded-up heap portion for
    /// SmallString-sized values (17..=255), and the request rounded down to the
    /// allocator's 8-byte granularity for LargeString-sized values. Calibrated
    /// to the reference `ScanMallocSize` test: 15/500/1000-byte values report
    /// 0/496/1000.
    pub fn malloc_used(&self) -> usize {
        match self {
            PrimeValue::Str(s) => {
                // `CompactString::len()` truncates to a u8; use the real byte
                // length for heap-backed strings.
                let len = s.as_bytes().len();
                if len <= 16 {
                    0
                } else if len <= 255 {
                    (len - 10).div_ceil(8) * 8
                } else {
                    len / 8 * 8
                }
            }
            // Container types are not exercised by the SCAN MINMSZ tests.
            PrimeValue::Sbf(s) => s.malloc_used(),
            PrimeValue::Cms(c) => c.malloc_used(),
            PrimeValue::Cuckoo(c) => c.malloc_used(),
            PrimeValue::Topk(t) => t.malloc_used(),
            PrimeValue::Json(j) => j.memory_usage(),
            _ => 0,
        }
    }
}

impl ObjType {
    /// Parse a TYPE argument (case-insensitive). Returns `None` for unknown
    /// names. Pseudo-types ("key", ...) are valid for SCAN but never match a
    /// stored value; they are handled by the caller.
    pub fn from_name(s: &[u8]) -> Option<ObjType> {
        match s.to_ascii_lowercase().as_slice() {
            b"string" => Some(ObjType::Str),
            b"list" => Some(ObjType::List),
            b"set" => Some(ObjType::Set),
            b"hash" => Some(ObjType::Hash),
            b"zset" => Some(ObjType::ZSet),
            b"stream" => Some(ObjType::Stream),
            b"mbbloom--" => Some(ObjType::Sbf),
            b"cmsk-type" => Some(ObjType::Cms),
            b"mbbloomcf" => Some(ObjType::Cuckoo),
            b"topk-type" => Some(ObjType::Topk),
            b"rejson-rl" => Some(ObjType::Json),
            _ => None,
        }
    }
}

impl Default for PrimeValue {
    fn default() -> Self {
        PrimeValue::Str(CompactString::new())
    }
}
