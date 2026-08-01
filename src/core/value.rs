use crate::core::compact::CompactString;
use crate::core::hash::Hash;
use crate::core::quicklist::QuickList;
use crate::core::set::Set;
use crate::core::stream::Stream;
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
        }
    }
}

impl Default for PrimeValue {
    fn default() -> Self {
        PrimeValue::Str(CompactString::new())
    }
}
