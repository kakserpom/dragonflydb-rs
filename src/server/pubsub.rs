//! Pub/sub subscriber index and reply construction, mirroring
//! `ChannelStore` (`channel_store.cc`) and the reply builders in
//! `conn_context.cc` / `dragonfly_connection.cc`.
//!
//! All connections are owned by the single-threaded IO loop, so the store
//! needs no locking; connection ids are the only subscriber identity.

use std::collections::{HashMap, HashSet};

use crate::commands::exec::keys::glob_match;
use crate::error::RespValue;

/// Channels/patterns each connection subscribes to (deduped), mirroring
/// `ConnectionState::SubscribeInfo`.
#[derive(Default, Debug)]
pub struct SubscribeInfo {
    pub channels: HashSet<Vec<u8>>,
    pub patterns: HashSet<Vec<u8>>,
    pub sharded: HashSet<Vec<u8>>,
}

impl SubscribeInfo {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.channels.is_empty() && self.patterns.is_empty() && self.sharded.is_empty()
    }

    /// Total subscription count reported in subscribe/unsubscribe replies
    /// (`SubscribeInfo::SubscriptionCount`).
    #[must_use]
    pub fn count(&self) -> usize {
        self.channels.len() + self.patterns.len() + self.sharded.len()
    }
}

/// channel/pattern -> set of subscribed connection ids. Separate maps for
/// regular channels, patterns and shard channels (SPUBLISH/SSUBSCRIBE).
#[derive(Default, Debug)]
pub struct ChannelStore {
    channels: HashMap<Vec<u8>, HashSet<u64>>,
    patterns: HashMap<Vec<u8>, HashSet<u64>>,
    sharded: HashMap<Vec<u8>, HashSet<u64>>,
}

impl ChannelStore {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn subscribe(&mut self, channel: &[u8], conn: u64) {
        self.channels
            .entry(channel.to_vec())
            .or_default()
            .insert(conn);
    }

    pub fn unsubscribe(&mut self, channel: &[u8], conn: u64) {
        if let Some(set) = self.channels.get_mut(channel) {
            set.remove(&conn);
            if set.is_empty() {
                self.channels.remove(channel);
            }
        }
    }

    pub fn psubscribe(&mut self, pattern: &[u8], conn: u64) {
        self.patterns
            .entry(pattern.to_vec())
            .or_default()
            .insert(conn);
    }

    pub fn punsubscribe(&mut self, pattern: &[u8], conn: u64) {
        if let Some(set) = self.patterns.get_mut(pattern) {
            set.remove(&conn);
            if set.is_empty() {
                self.patterns.remove(pattern);
            }
        }
    }

    pub fn ssubscribe(&mut self, channel: &[u8], conn: u64) {
        self.sharded
            .entry(channel.to_vec())
            .or_default()
            .insert(conn);
    }

    pub fn sunsubscribe(&mut self, channel: &[u8], conn: u64) {
        if let Some(set) = self.sharded.get_mut(channel) {
            set.remove(&conn);
            if set.is_empty() {
                self.sharded.remove(channel);
            }
        }
    }

    /// Drop a connection from every subscription (disconnect / RESET).
    pub fn remove_conn(&mut self, conn: u64) {
        self.channels.retain(|_, subs| {
            subs.remove(&conn);
            !subs.is_empty()
        });
        self.patterns.retain(|_, subs| {
            subs.remove(&conn);
            !subs.is_empty()
        });
        self.sharded.retain(|_, subs| {
            subs.remove(&conn);
            !subs.is_empty()
        });
    }

    /// Everyone that should receive a message on `channel`: exact subscribers
    /// first, then pattern subscribers whose pattern matches. A connection
    /// subscribed to both a matching pattern and the channel receives both
    /// pushes (one "message", one "pmessage"), like upstream.
    #[must_use]
    pub fn subscribers(&self, channel: &[u8]) -> Vec<(u64, Option<Vec<u8>>)> {
        let mut out = Vec::new();
        if let Some(subs) = self.channels.get(channel) {
            for &c in subs {
                out.push((c, None));
            }
        }
        for (pat, subs) in &self.patterns {
            if glob_match(pat, channel) {
                for &c in subs {
                    out.push((c, Some(pat.clone())));
                }
            }
        }
        out
    }

    #[must_use]
    pub fn sharded_subscribers(&self, channel: &[u8]) -> Vec<u64> {
        self.sharded
            .get(channel)
            .map(|s| s.iter().copied().collect())
            .unwrap_or_default()
    }

    /// Active channel names sorted, optionally filtered by `pattern` (empty
    /// means all) — `ChannelStore::ListChannels`.
    #[must_use]
    pub fn list_channels(&self, pattern: &[u8]) -> Vec<Vec<u8>> {
        let mut v: Vec<Vec<u8>> = self
            .channels
            .keys()
            .filter(|c| pattern.is_empty() || glob_match(pattern, c))
            .cloned()
            .collect();
        v.sort();
        v
    }

    #[must_use]
    pub fn list_sharded_channels(&self, pattern: &[u8]) -> Vec<Vec<u8>> {
        let mut v: Vec<Vec<u8>> = self
            .sharded
            .keys()
            .filter(|c| pattern.is_empty() || glob_match(pattern, c))
            .cloned()
            .collect();
        v.sort();
        v
    }

    /// Number of unique active patterns (`ChannelStore::PatternCount`).
    #[must_use]
    pub fn pattern_count(&self) -> usize {
        self.patterns.len()
    }

    pub fn numsub(&self, channel: &[u8]) -> usize {
        self.channels.get(channel).map_or(0, HashSet::len)
    }

    pub fn shard_numsub(&self, channel: &[u8]) -> usize {
        self.sharded.get(channel).map_or(0, HashSet::len)
    }
}

/// `[action, channel | nil, count]` — one top-level reply per channel of
/// SUBSCRIBE/UNSUBSCRIBE/PSUBSCRIBE/PUNSUBSCRIBE (`SendSubscriptionChangedResponse`).
#[must_use]
pub fn sub_change(action: &str, channel: Option<&[u8]>, count: usize) -> RespValue {
    RespValue::Array(vec![
        RespValue::bulk(action),
        match channel {
            Some(c) => RespValue::bulk(c),
            None => RespValue::Nil,
        },
        RespValue::Integer(count as i64),
    ])
}

/// A pushed message. Exact subscribers get `["message", channel, msg]`; pattern
/// subscribers `["pmessage", pattern, channel, msg]`; shard subscribers
/// `["smessage", channel, msg]` (`Connection::SendPubMessageAsync`).
#[must_use]
pub fn push_message(
    pattern: Option<&[u8]>,
    channel: &[u8],
    msg: &[u8],
    sharded: bool,
) -> RespValue {
    let mut v = Vec::with_capacity(4);
    if sharded {
        v.push(RespValue::bulk("smessage"));
    } else if let Some(pat) = pattern {
        v.push(RespValue::bulk("pmessage"));
        v.push(RespValue::bulk(pat));
    } else {
        v.push(RespValue::bulk("message"));
    }
    v.push(RespValue::bulk(channel));
    v.push(RespValue::bulk(msg));
    RespValue::Array(v)
}

/// PING while subscribed in RESP2 replies with `["pong", msg]`
/// (`GenericFamily::Ping`).
#[must_use]
pub fn ping_pubsub(msg: &[u8]) -> RespValue {
    RespValue::Array(vec![RespValue::bulk("pong"), RespValue::bulk(msg)])
}

/// Handle the PUBSUB introspection subcommands, returning either a reply or an
/// error string (`Service::Pubsub`).
pub fn pubsub_command(args: &[Vec<u8>], store: &ChannelStore) -> Result<RespValue, String> {
    if args.len() < 2 {
        return Err("ERR wrong number of arguments for 'pubsub' command".to_string());
    }
    let sub = args[1].to_ascii_uppercase();
    match sub.as_slice() {
        b"HELP" => Ok(RespValue::Array(vec![
            RespValue::Simple("PUBSUB <subcommand> [<arg> [value] [opt] ...]. Subcommands are:".into()),
            RespValue::Simple("CHANNELS [<pattern>]".into()),
            RespValue::Simple("\tReturn the currently active channels matching a <pattern> (default: '*').".into()),
            RespValue::Simple("NUMPAT".into()),
            RespValue::Simple("\tReturn number of subscriptions to patterns.".into()),
            RespValue::Simple("NUMSUB [<channel> <channel...>]".into()),
            RespValue::Simple("\tReturns the number of subscribers for the specified channels, excluding".into()),
            RespValue::Simple("\tpattern subscriptions.".into()),
            RespValue::Simple("SHARDCHANNELS [pattern]".into()),
            RespValue::Simple("\tReturns a list of active shard channels, optionally matching the specified pattern ".into()),
            RespValue::Simple("(default: '*').".into()),
            RespValue::Simple("SHARDNUMSUB [<channel> <channel...>]".into()),
            RespValue::Simple("\tReturns the number of subscribers for the specified shard channels, excluding".into()),
            RespValue::Simple("\tpattern subscriptions.".into()),
            RespValue::Simple("HELP".into()),
            RespValue::Simple("\tPrints this help.".into()),
        ])),
        b"CHANNELS" => {
            let pattern = args.get(2).map_or(&b""[..], |p| p.as_slice());
            Ok(RespValue::Array(
                store.list_channels(pattern).into_iter().map(RespValue::bulk).collect(),
            ))
        }
        b"NUMPAT" => Ok(RespValue::Integer(store.pattern_count() as i64)),
        b"NUMSUB" => {
            let mut v = Vec::with_capacity((args.len() - 2) * 2);
            for ch in &args[2..] {
                v.push(RespValue::bulk(ch.clone()));
                v.push(RespValue::Integer(store.numsub(ch) as i64));
            }
            Ok(RespValue::Array(v))
        }
        b"SHARDCHANNELS" | b"SHARDNUMSUB" => Err(format!(
            "ERR PUBSUB {} is not supported in non cluster mode",
            String::from_utf8_lossy(&sub)
        )),
        other => Err(format!(
            "ERR Unknown subcommand or wrong number of arguments for '{}'. Try PUBSUB HELP.",
            String::from_utf8_lossy(other)
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn b(a: &[&str]) -> Vec<Vec<u8>> {
        a.iter().map(|s| s.as_bytes().to_vec()).collect()
    }

    fn render(v: &RespValue) -> String {
        match v {
            RespValue::Bulk(b) => String::from_utf8_lossy(b).into_owned(),
            RespValue::Simple(s) => s.clone(),
            RespValue::Integer(i) => i.to_string(),
            RespValue::Nil | RespValue::NilArray => "(nil)".into(),
            RespValue::Error(e) => e.clone(),
            RespValue::Array(a) => {
                format!("[{}]", a.iter().map(render).collect::<Vec<_>>().join(", "))
            }
            RespValue::Map(m) => format!("MAP{}", m.len()),
            RespValue::Bool(b) => b.to_string(),
            RespValue::Double(f) => crate::util::format_double(*f),
            RespValue::Push(_) => "(push)".into(),
        }
    }

    #[test]
    fn store_subscribe_unsubscribe() {
        let mut s = ChannelStore::new();
        s.subscribe(b"news", 1);
        s.subscribe(b"news", 2);
        s.subscribe(b"sports", 1);
        assert_eq!(s.numsub(b"news"), 2);
        assert_eq!(s.numsub(b"sports"), 1);
        assert_eq!(s.numsub(b"nope"), 0);

        s.unsubscribe(b"news", 1);
        assert_eq!(s.numsub(b"news"), 1);
        s.unsubscribe(b"news", 2);
        assert_eq!(s.numsub(b"news"), 0);
        // Unsubscribing a stale channel is a no-op.
        s.unsubscribe(b"news", 99);
    }

    #[test]
    fn store_pattern_matching() {
        let mut s = ChannelStore::new();
        s.subscribe(b"news", 1);
        s.psubscribe(b"news.*", 2);
        s.psubscribe(b"sports.*", 3);
        s.psubscribe(b"n*", 4);

        let subs = s.subscribers(b"news.tech");
        // news.tech matches news.* and n*, but not the exact channel news.
        assert_eq!(subs.len(), 2);
        assert!(subs.contains(&(2, Some(b"news.*".to_vec()))));
        assert!(subs.contains(&(4, Some(b"n*".to_vec()))));

        // A connection subscribed to both the exact channel and a matching
        // pattern receives one delivery per subscription: (1, None) for the
        // channel and (1, Some("n*")) for the pattern. Conn 4 (also on "n*")
        // receives a third delivery.
        s.psubscribe(b"n*", 1);
        let subs = s.subscribers(b"news");
        assert_eq!(subs.len(), 3);
        assert_eq!(subs[0], (1, None));
        assert!(subs.contains(&(1, Some(b"n*".to_vec()))));
        assert!(subs.contains(&(4, Some(b"n*".to_vec()))));
    }

    #[test]
    fn store_sharded_and_cleanup() {
        let mut s = ChannelStore::new();
        s.subscribe(b"a", 1);
        s.psubscribe(b"b*", 2);
        s.ssubscribe(b"c", 3);
        assert_eq!(s.pattern_count(), 1);
        assert_eq!(s.list_sharded_channels(b""), vec![b"c".to_vec()]);

        s.remove_conn(1);
        assert_eq!(s.numsub(b"a"), 0);
        assert_eq!(s.list_channels(b""), Vec::<Vec<u8>>::new());

        s.remove_conn(2);
        assert_eq!(s.pattern_count(), 0);

        s.remove_conn(3);
        assert_eq!(s.shard_numsub(b"c"), 0);
    }

    #[test]
    fn store_list_channels_sorted_and_pattern() {
        let mut s = ChannelStore::new();
        for ch in ["z", "a", "m"] {
            s.subscribe(ch.as_bytes(), 1);
        }
        assert_eq!(
            s.list_channels(b""),
            vec![b"a".to_vec(), b"m".to_vec(), b"z".to_vec()]
        );
        assert_eq!(s.list_channels(b"?*"), s.list_channels(b""));
        s.psubscribe(b"zz*", 2);
        // Patterns are not active channels.
        assert_eq!(s.list_channels(b"").len(), 3);
    }

    #[test]
    fn reply_formats() {
        assert_eq!(
            render(&sub_change("subscribe", Some(b"news"), 1)),
            "[subscribe, news, 1]"
        );
        assert_eq!(
            render(&sub_change("unsubscribe", None, 0)),
            "[unsubscribe, (nil), 0]"
        );
        assert_eq!(
            render(&push_message(None, b"news", b"hi", false)),
            "[message, news, hi]"
        );
        assert_eq!(
            render(&push_message(Some(b"n*"), b"news", b"hi", false)),
            "[pmessage, n*, news, hi]"
        );
        assert_eq!(
            render(&push_message(None, b"c", b"hi", true)),
            "[smessage, c, hi]"
        );
        assert_eq!(render(&ping_pubsub(b"")), "[pong, ]");
        assert_eq!(render(&ping_pubsub(b"hi")), "[pong, hi]");
    }

    #[test]
    fn pubsub_subcommands() {
        let mut store = ChannelStore::new();
        store.subscribe(b"news", 1);
        store.psubscribe(b"n*", 2);

        let help = pubsub_command(&b(&["PUBSUB", "HELP"]), &store).unwrap();
        assert!(render(&help).contains("PUBSUB <subcommand>"));

        let channels = pubsub_command(&b(&["PUBSUB", "CHANNELS"]), &store).unwrap();
        assert_eq!(render(&channels), "[news]");
        let channels = pubsub_command(&b(&["PUBSUB", "CHANNELS", "sp*"]), &store).unwrap();
        assert_eq!(render(&channels), "[]");
        assert_eq!(
            render(&pubsub_command(&b(&["PUBSUB", "NUMPAT"]), &store).unwrap()),
            "1"
        );
        let numsub = pubsub_command(&b(&["PUBSUB", "NUMSUB", "news", "x"]), &store).unwrap();
        assert_eq!(render(&numsub), "[news, 1, x, 0]");

        assert_eq!(
            pubsub_command(&b(&["PUBSUB"]), &store),
            Err("ERR wrong number of arguments for 'pubsub' command".to_string())
        );
        assert_eq!(
            pubsub_command(&b(&["PUBSUB", "SHARDCHANNELS"]), &store),
            Err("ERR PUBSUB SHARDCHANNELS is not supported in non cluster mode".to_string())
        );
        assert_eq!(
            pubsub_command(&b(&["PUBSUB", "BOGUS"]), &store),
            Err(
                "ERR Unknown subcommand or wrong number of arguments for 'BOGUS'. Try PUBSUB HELP."
                    .to_string()
            )
        );
    }
}
