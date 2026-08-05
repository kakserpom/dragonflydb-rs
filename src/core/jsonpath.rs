//! `JSONPath` v2 evaluation engine, ported from `dragonfly/src/core/json/`.
//!
//! This mirrors three pieces of the reference implementation:
//!
//! * The `gram_jsonpath_lexer.lex` / `gram_jsonpath_grammar.y` parser (a
//!   hand-rolled recursive descent parser here), producing a `Path` - a list of
//!   [`Segment`]s.
//! * The `jsoncons_dfs.h` / `detail_jsoncons_dfs.cc` iterative DFS engine that
//!   walks a `Json` document against a path, invoking a callback for every
//!   matched value.
//! * The `path_expression::functions` aggregates (`max`/`min`/`avg`).
//!
//! The traversal semantics are subtle and are reproduced faithfully:
//!
//! * `DESCENT` consumes zero segments on its first step: it re-visits the
//!   current node with the *next* path segment applied (`$..a` matches `a`
//!   itself, not just descendants). The very first descent visit keeps the
//!   `segment_step_` toggle on so the following `init` on the same node applies
//!   the next segment.
//! * `IDENTIFIER`/`INDEX` exhaust after producing a single child: once a named
//!   branch is entered, no sibling branch is ever considered again, so
//!   `$.a.b` matches exactly one value. Only `WILDCARD`/`DESCENT` iterate all
//!   children.
//! * Every matched value is reported through the DFS; `$` (an empty path) is a
//!   valid path matching the root document.

use crate::core::json::Json;

/// The maximum supported path length; longer paths are rejected with
/// `"Path too long"` (mirrors the reference `kMaxJsonPathLen`).
pub const MAX_PATH_LEN: usize = 128;

/// Error produced when a `JSONPath` cannot be parsed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JsonPathError {
    msg: String,
}

impl JsonPathError {
    fn new(msg: impl Into<String>) -> JsonPathError {
        JsonPathError { msg: msg.into() }
    }
}

impl std::fmt::Display for JsonPathError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.msg)
    }
}

impl std::error::Error for JsonPathError {}

/// The `INDEX` component of a path segment, mirroring `jsoncons::jsonpath`
/// `IndexExpr`: a closed range `[first, second]` (both ends inclusive).
/// `[a:b]` in the path syntax is stored as `HalfOpen(a, b)` = `(a, b-1)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexExpr {
    /// Range start. Negative values are counted from the end of the array.
    pub first: i64,
    /// Range end (inclusive). Negative values are counted from the end.
    pub second: i64,
}

impl IndexExpr {
    /// A single-index expression (`first == second`).
    #[must_use]
    pub fn single(index: i64) -> IndexExpr {
        IndexExpr {
            first: index,
            second: index,
        }
    }

    /// A range `[a:b)` from the path syntax, stored as the closed range
    /// `[a, b-1]` (mirrors `IndexExpr::HalfOpen`).
    #[must_use]
    pub fn range(first: i64, second: i64) -> IndexExpr {
        IndexExpr {
            first,
            second: second - 1,
        }
    }

    /// The `[*]` wildcard expression (mirrors `IndexExpr::All`).
    #[must_use]
    pub fn all() -> IndexExpr {
        IndexExpr {
            first: 0,
            second: i64::MAX,
        }
    }

    /// Normalize this expression against an array of the given length,
    /// mirroring `IndexExpr::Normalize`. Returns `None` for an empty array or
    /// when the range is empty after normalization.
    ///
    /// The result is a closed, inclusive range clamped into the array bounds,
    /// with negative indices wrapping from the end.
    #[must_use]
    pub fn normalize(&self, array_len: usize) -> Option<(usize, usize)> {
        if array_len == 0 {
            return None;
        }

        let wrap = |negative: i64| -> usize {
            let positive = negative.unsigned_abs();
            if positive > array_len as u64 {
                0
            } else {
                array_len - positive as usize
            }
        };

        let second = if self.second >= array_len as i64 {
            array_len - 1
        } else if self.second < 0 {
            wrap(self.second)
        } else {
            self.second as usize
        };

        let first = if self.first < 0 {
            wrap(self.first)
        } else {
            self.first as usize
        };

        if first > second {
            return None;
        }

        Some((first, second))
    }
}

/// The aggregate functions accepted in function form: `max(EXPR)`,
/// `min(EXPR)`, `avg(EXPR)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Agg {
    Max,
    Min,
    Avg,
}

/// A single path segment, mirroring `jsoncons::jsonpath::SegmentType`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Segment {
    /// `.name` or `['name']`.
    Identifier(String),
    /// `[index]`, `[start:end]` or `[*]`.
    Index(IndexExpr),
    /// `.*` or `[*]` - matches every child.
    Wildcard,
    /// `..` - recursive descent.
    Descent,
    /// `max(EXPR)`, `min(EXPR)`, `avg(EXPR)`.
    Function(Agg),
}

/// A parsed `JSONPath` - the root `$` plus a sequence of [`Segment`]s. The empty
/// path matches the root document.
pub type Path = Vec<Segment>;

/// Parse a `JSONPath` (`$...`) or function expression (`max($.a[*])`) into a
/// [`Path`]. Returns `Err` on a syntax error or when the path is too long.
pub fn parse_path(path: &str) -> Result<Path, JsonPathError> {
    let chars: Vec<char> = path.chars().collect();
    let mut pos = 0;
    let mut result = Path::new();

    match chars.first() {
        Some('$') => {
            pos += 1;
            parse_path_impl(&chars, &mut pos, &mut result)?;
        }
        Some(&c) if is_name_char(c) => {
            let name_start = pos;
            while pos < chars.len() && is_name_char(chars[pos]) {
                pos += 1;
            }
            let name: String = chars[name_start..pos].iter().collect();
            let agg = match name.as_str() {
                "max" => Agg::Max,
                "min" => Agg::Min,
                "avg" => Agg::Avg,
                _ => return Err(JsonPathError::new("Unknown function")),
            };
            push_segment(&mut result, Segment::Function(agg))?;
            expect(&chars, &mut pos, '(')?;
            if chars.get(pos) != Some(&'$') {
                return Err(JsonPathError::new("Expected '$'"));
            }
            pos += 1;
            parse_path_impl(&chars, &mut pos, &mut result)?;
            expect(&chars, &mut pos, ')')?;
            parse_path_impl(&chars, &mut pos, &mut result)?;
        }
        _ => return Err(JsonPathError::new("Invalid Json path")),
    }

    if pos != chars.len() {
        return Err(JsonPathError::new(format!("Syntax error near {pos}")));
    }

    Ok(result)
}

fn parse_path_impl(
    chars: &[char],
    pos: &mut usize,
    result: &mut Path,
) -> Result<(), JsonPathError> {
    while *pos < chars.len() {
        match chars[*pos] {
            '.' => {
                *pos += 1;
                if *pos >= chars.len() {
                    return Err(JsonPathError::new("Trailing dot"));
                }
                if chars[*pos] == '.' {
                    *pos += 1;
                    push_segment(result, Segment::Descent)?;
                }
                parse_relative_path(chars, pos, result)?;
            }
            '[' => {
                *pos += 1;
                if *pos >= chars.len() {
                    return Err(JsonPathError::new("Unterminated index"));
                }
                parse_bracket(chars, pos, result)?;
            }
            ')' => break,
            _ => {
                return Err(JsonPathError::new(format!("Syntax error near {}", *pos)));
            }
        }
    }
    Ok(())
}

/// Parse a `relative_path`: an unquoted identifier, a wildcard `*`, or a
/// bracket expression.
fn parse_relative_path(
    chars: &[char],
    pos: &mut usize,
    result: &mut Path,
) -> Result<(), JsonPathError> {
    match chars.get(*pos) {
        Some('*') => {
            *pos += 1;
            push_segment(result, Segment::Wildcard)
        }
        Some('[') => {
            *pos += 1;
            parse_bracket(chars, pos, result)
        }
        Some(&c) if is_name_char(c) => parse_name(chars, pos, result),
        _ => Err(JsonPathError::new("Expected identifier")),
    }
}

fn push_segment(result: &mut Path, segment: Segment) -> Result<(), JsonPathError> {
    if result.len() >= MAX_PATH_LEN {
        return Err(JsonPathError::new("Path too long"));
    }
    result.push(segment);
    Ok(())
}

/// Parse a name after `.` (an unquoted identifier).
fn parse_name(chars: &[char], pos: &mut usize, result: &mut Path) -> Result<(), JsonPathError> {
    let start = *pos;
    while *pos < chars.len() && is_name_char(chars[*pos]) {
        *pos += 1;
    }
    if *pos == start {
        return Err(JsonPathError::new("Expected identifier"));
    }
    let name: String = chars[start..*pos].iter().collect();
    push_segment(result, Segment::Identifier(name))
}

fn is_name_char(c: char) -> bool {
    c.is_alphanumeric() || c == '_' || c == '-'
}

/// Parse a bracketed expression: `['name']`, `["name"]`, `[index]`,
/// `[start:end]`, `[:end]`, `[start:]`, `[*]`, or a function call
/// `min($.a[*])`.
fn parse_bracket(chars: &[char], pos: &mut usize, result: &mut Path) -> Result<(), JsonPathError> {
    match chars.get(*pos) {
        Some('\'' | '"') => {
            let name = parse_quoted_string(chars, pos)?;
            expect(chars, pos, ']')?;
            push_segment(result, Segment::Identifier(name))
        }
        Some('*') => {
            *pos += 1;
            expect(chars, pos, ']')?;
            push_segment(result, Segment::Index(IndexExpr::all()))
        }
        Some('(') => {
            *pos += 1;
            let agg = match chars.get(*pos) {
                Some('m') => {
                    let i = *pos;
                    if chars[i..].starts_with(&['m', 'a', 'x']) {
                        *pos += 3;
                        Some(Agg::Max)
                    } else if chars[i..].starts_with(&['m', 'i', 'n']) {
                        *pos += 3;
                        Some(Agg::Min)
                    } else {
                        None
                    }
                }
                Some('a') if chars[*pos..].starts_with(&['a', 'v', 'g']) => {
                    *pos += 3;
                    Some(Agg::Avg)
                }
                _ => None,
            };
            let agg = agg.ok_or_else(|| JsonPathError::new("Unknown function"))?;
            push_segment(result, Segment::Function(agg))?;
            expect(chars, pos, '(')?;
            if chars.get(*pos) != Some(&'$') {
                return Err(JsonPathError::new("Expected '$'"));
            }
            *pos += 1;
            parse_path_impl(chars, pos, result)?;
            expect(chars, pos, ')')?;
            expect(chars, pos, ']')?;
            Ok(())
        }
        Some(':') => {
            *pos += 1;
            let second = parse_int(chars, pos)?;
            expect(chars, pos, ']')?;
            push_segment(result, Segment::Index(IndexExpr::range(0, second)))
        }
        Some(c) if *c == '-' || c.is_ascii_digit() => {
            let first = parse_int(chars, pos)?;
            if chars.get(*pos) == Some(&':') {
                *pos += 1;
                if chars.get(*pos) == Some(&']') {
                    expect(chars, pos, ']')?;
                    push_segment(
                        result,
                        Segment::Index(IndexExpr {
                            first,
                            second: i64::MAX,
                        }),
                    )
                } else {
                    let second = parse_int(chars, pos)?;
                    expect(chars, pos, ']')?;
                    push_segment(result, Segment::Index(IndexExpr::range(first, second)))
                }
            } else {
                expect(chars, pos, ']')?;
                push_segment(result, Segment::Index(IndexExpr::single(first)))
            }
        }
        _ => Err(JsonPathError::new("Invalid index")),
    }
}

/// Parse `['...']` or `["..."]`.
fn parse_quoted_string(chars: &[char], pos: &mut usize) -> Result<String, JsonPathError> {
    let quote = chars[*pos];
    *pos += 1;
    let mut out = String::new();
    loop {
        match chars.get(*pos) {
            None => return Err(JsonPathError::new("Unterminated string")),
            Some(&c) if c == quote => {
                *pos += 1;
                return Ok(out);
            }
            Some(&c) => {
                out.push(c);
                *pos += 1;
            }
        }
    }
}

fn parse_int(chars: &[char], pos: &mut usize) -> Result<i64, JsonPathError> {
    let start = *pos;
    if chars.get(*pos) == Some(&'-') {
        *pos += 1;
    }
    while *pos < chars.len() && chars[*pos].is_ascii_digit() {
        *pos += 1;
    }
    if *pos == start || (*pos == start + 1 && chars[start] == '-') {
        return Err(JsonPathError::new("Invalid index"));
    }
    let s: String = chars[start..*pos].iter().collect();
    s.parse::<i64>()
        .map_err(|_| JsonPathError::new("Index out of range"))
}

fn expect(chars: &[char], pos: &mut usize, c: char) -> Result<(), JsonPathError> {
    if chars.get(*pos) == Some(&c) {
        *pos += 1;
        Ok(())
    } else {
        Err(JsonPathError::new(format!("Expected '{c}'")))
    }
}

// ---------------------------------------------------------------------------
// DFS engine
// ---------------------------------------------------------------------------

/// State of the DFS position while iterating the children of an object or
/// array. Mirrors `JsonconsDfsItem::state_`.
#[derive(Debug, Default)]
enum ItemState {
    /// No children produced yet - the next step is an `init`.
    #[default]
    Mono,
    /// Object member iteration - the index of the member to yield next.
    Obj(usize),
    /// Array element iteration - the index of the next element to yield and
    /// the (inclusive) last index.
    Arr { next: usize, last: usize },
}

/// A step in a mutation/delete location path: a single key or index that
/// uniquely locates a node within its parent.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Step {
    Key(String),
    Index(usize),
}

/// The result of one advance of the DFS iterator over a shared document.
enum Advance<'a> {
    /// A child node, the step used to reach it from its parent, and the index
    /// of the next path segment to apply to it.
    Node {
        child: &'a Json,
        step: Option<Step>,
        seg_idx: usize,
    },
    /// The node does not match the current segment.
    Mismatch,
    /// No more children.
    Exhausted,
}

/// The result of one advance of the DFS iterator over a mutable document.
enum AdvanceMut<'a> {
    Node {
        child: &'a mut Json,
        step: Option<Step>,
        seg_idx: usize,
    },
    Mismatch,
    Exhausted,
}

/// A DFS frame: a node to visit together with the location of its parent
/// within the root document (used for mutation/delete).
#[derive(Debug)]
struct Frame<'a> {
    node: &'a Json,
    loc: Vec<Step>,
    state: ItemState,
    segment_step: u8,
    seg_idx: usize,
}

/// A DFS frame over a mutable document.
#[derive(Debug, Default)]
struct FrameMut {
    loc: Vec<Step>,
    state: ItemState,
    segment_step: u8,
    seg_idx: usize,
}

fn should_iterate_all(segment: &Segment) -> bool {
    matches!(segment, Segment::Wildcard | Segment::Descent)
}

/// Apply the `init` step of the DFS for `segment` to `node`, updating
/// `state`/`segment_step` and producing the first child (if any).
fn init<'a>(
    node: &'a Json,
    state: &mut ItemState,
    segment_step: &mut u8,
    seg_idx: usize,
    segment: &Segment,
) -> Advance<'a> {
    match segment {
        Segment::Identifier(key) => match node.object_get(key) {
            Some(child) => {
                *state = ItemState::Obj(0);
                Advance::Node {
                    child,
                    step: Some(Step::Key(key.clone())),
                    seg_idx: seg_idx + 1,
                }
            }
            None => Advance::Mismatch,
        },
        Segment::Index(expr) => {
            let items = node.array_items();
            match expr.normalize(items.len()) {
                Some((first, second)) => {
                    *state = ItemState::Arr {
                        next: first,
                        last: second,
                    };
                    Advance::Node {
                        child: &items[first],
                        step: Some(Step::Index(first)),
                        seg_idx: seg_idx + 1,
                    }
                }
                None => Advance::Mismatch,
            }
        }
        Segment::Wildcard => init_wildcard(node, state, *segment_step, seg_idx),
        Segment::Descent => {
            if *segment_step == 1 {
                *segment_step = 0;
                return Advance::Node {
                    child: node,
                    step: None,
                    seg_idx: seg_idx + 1,
                };
            }
            init_wildcard(node, state, *segment_step, seg_idx)
        }
        Segment::Function(_) => Advance::Mismatch,
    }
}

fn init_wildcard<'a>(
    node: &'a Json,
    state: &mut ItemState,
    segment_step: u8,
    seg_idx: usize,
) -> Advance<'a> {
    if node.is_object() {
        let members = node.object_members();
        if members.is_empty() {
            return Advance::Exhausted;
        }
        *state = ItemState::Obj(0);
        Advance::Node {
            child: &members[0].1,
            step: Some(Step::Key(members[0].0.clone())),
            seg_idx: seg_idx + segment_step as usize,
        }
    } else if node.is_array() {
        let items = node.array_items();
        if items.is_empty() {
            return Advance::Exhausted;
        }
        let last = items.len() - 1;
        *state = ItemState::Arr { next: 0, last };
        Advance::Node {
            child: &items[0],
            step: Some(Step::Index(0)),
            seg_idx: seg_idx + segment_step as usize,
        }
    } else {
        Advance::Mismatch
    }
}

/// Advance the iterator one step, applying `segment` to `node` (shared
/// variant).
fn advance_impl<'a>(
    node: &'a Json,
    state: &mut ItemState,
    segment_step: &mut u8,
    seg_idx: usize,
    segment: &Segment,
) -> Advance<'a> {
    match state {
        ItemState::Mono => init(node, state, segment_step, seg_idx, segment),
        ItemState::Obj(idx) => {
            if !should_iterate_all(segment) {
                return Advance::Exhausted;
            }
            *idx += 1;
            match node.object_members().get(*idx) {
                Some((key, child)) => Advance::Node {
                    child,
                    step: Some(Step::Key(key.clone())),
                    seg_idx: seg_idx + *segment_step as usize,
                },
                None => Advance::Exhausted,
            }
        }
        ItemState::Arr { next, last } => {
            if *next == *last {
                return Advance::Exhausted;
            }
            *next += 1;
            match node.array_items().get(*next) {
                Some(child) => Advance::Node {
                    child,
                    step: Some(Step::Index(*next)),
                    seg_idx: seg_idx + *segment_step as usize,
                },
                None => Advance::Exhausted,
            }
        }
    }
}

/// A read-only traversal callback.
pub type PathCallback<'a> = dyn FnMut(Option<&str>, &'a Json) + 'a;

/// Walk `path` against `root`, invoking `callback` for every matched value
/// (in DFS order). Returns the number of matches.
///
/// Mirrors `jsoncons::jsonpath::Dfs::Traverse`. The path must be non-empty;
/// an empty path matches the root document itself.
pub fn traverse<F>(path: &[Segment], root: &Json, mut callback: F) -> usize
where
    F: FnMut(Option<&str>, &Json),
{
    if path.is_empty() {
        callback(None, root);
        return 1;
    }

    let terminal = &path[path.len() - 1];

    walk_dfs(path, root, |node| {
        perform_step(terminal, node, &mut callback)
    })
}

/// Iterative DFS over `root` applying `path`, calling `on_terminal` with the
/// node of every path match. Returns the number of matches.
fn walk_dfs<'a, F>(path: &[Segment], root: &'a Json, mut on_terminal: F) -> usize
where
    F: FnMut(&'a Json) -> usize,
{
    let mut matches = 0;
    let mut stack = Vec::new();
    stack.push(Frame {
        node: root,
        loc: Vec::new(),
        state: ItemState::Mono,
        segment_step: 1,
        seg_idx: 0,
    });

    while let Some(frame) = stack.last_mut() {
        if path.len() == 1 {
            matches += on_terminal(frame.node);
            stack.pop();
            continue;
        }

        let segment = &path[frame.seg_idx];
        match advance_impl(
            frame.node,
            &mut frame.state,
            &mut frame.segment_step,
            frame.seg_idx,
            segment,
        ) {
            Advance::Node {
                child,
                step,
                seg_idx,
            } => {
                if child.is_object() || child.is_array() {
                    let next_seg_id = seg_idx;
                    if next_seg_id + 1 < path.len() {
                        let mut child_loc = frame.loc.clone();
                        if let Some(step) = &step {
                            child_loc.push(step.clone());
                        }
                        stack.push(Frame {
                            node: child,
                            loc: child_loc,
                            state: ItemState::Mono,
                            segment_step: 1,
                            seg_idx: next_seg_id,
                        });
                        continue;
                    }
                    matches += on_terminal(child);
                }
            }
            Advance::Mismatch | Advance::Exhausted => {
                stack.pop();
            }
        }
    }

    matches
}

/// Apply `segment` to `node` and report every matching value to `callback`.
/// Returns the number of matches.
fn perform_step<F>(segment: &Segment, node: &Json, callback: &mut F) -> usize
where
    F: FnMut(Option<&str>, &Json),
{
    match segment {
        Segment::Identifier(key) => match node.object_get(key) {
            Some(child) => {
                callback(Some(key), child);
                1
            }
            None => 0,
        },
        Segment::Index(expr) => {
            let items = node.array_items();
            if items.is_empty() {
                return 0;
            }
            match expr.normalize(items.len()) {
                Some((first, second)) => {
                    let mut matches = 0;
                    for child in &items[first..=second] {
                        callback(None, child);
                        matches += 1;
                    }
                    matches
                }
                None => 0,
            }
        }
        Segment::Wildcard => {
            let mut matches = 0;
            if node.is_object() {
                for (key, child) in node.object_members() {
                    callback(Some(key), child);
                    matches += 1;
                }
            } else if node.is_array() {
                for child in node.array_items() {
                    callback(None, child);
                    matches += 1;
                }
            }
            matches
        }
        Segment::Descent => {
            let mut matches = 0;
            match node {
                Json::Object(members) => {
                    for (key, child) in members {
                        callback(Some(key), child);
                        matches += 1;
                    }
                }
                Json::Array(items) => {
                    for child in items {
                        callback(None, child);
                        matches += 1;
                    }
                }
                _ => {}
            }
            matches
        }
        Segment::Function(_) => 0,
    }
}

/// Aggregate state for the `max`/`min`/`avg` functions.
///
/// Mirrors the reference `AggFunction`: applying a non-numeric value
/// invalidates the aggregate permanently (`valid_` becomes 0), so `$..*` over
/// a tree containing a non-number yields null.
#[derive(Debug, Clone)]
enum AggState {
    Max { current: Option<Json>, valid: bool },
    Min { current: Option<Json>, valid: bool },
    Avg { sum: f64, count: usize, valid: bool },
}

impl AggState {
    fn init(agg: Agg) -> AggState {
        match agg {
            Agg::Max => AggState::Max {
                current: None,
                valid: true,
            },
            Agg::Min => AggState::Min {
                current: None,
                valid: true,
            },
            Agg::Avg => AggState::Avg {
                sum: 0.0,
                count: 0,
                valid: true,
            },
        }
    }

    fn apply(&mut self, val: &Json) {
        match self {
            AggState::Max { current, valid } => {
                if !*valid {
                    return;
                }
                if val.is_number() {
                    match current {
                        Some(c) => {
                            if value_greater(val, c) {
                                *current = Some(val.clone());
                            }
                        }
                        None => *current = Some(val.clone()),
                    }
                } else {
                    *valid = false;
                }
            }
            AggState::Min { current, valid } => {
                if !*valid {
                    return;
                }
                if val.is_number() {
                    match current {
                        Some(c) => {
                            if value_less(val, c) {
                                *current = Some(val.clone());
                            }
                        }
                        None => *current = Some(val.clone()),
                    }
                } else {
                    *valid = false;
                }
            }
            AggState::Avg { sum, count, valid } => {
                if !*valid {
                    return;
                }
                if let Some(num) = as_f64_number(val) {
                    *sum += num;
                    *count += 1;
                } else {
                    *valid = false;
                }
            }
        }
    }

    /// Produce the result value, or `None` if the aggregate is empty or was
    /// invalidated by a non-numeric value.
    fn result(&self) -> Option<Json> {
        match self {
            AggState::Max { current, valid } | AggState::Min { current, valid } => {
                if *valid {
                    current.clone()
                } else {
                    None
                }
            }
            AggState::Avg { sum, count, valid } => {
                if *valid && *count > 0 {
                    Some(Json::Double(*sum / *count as f64))
                } else {
                    None
                }
            }
        }
    }
}

fn as_f64_number(value: &Json) -> Option<f64> {
    match value {
        Json::Int(v) => Some(*v as f64),
        Json::Uint(v) => Some(*v as f64),
        Json::Double(v) => Some(*v),
        _ => None,
    }
}

fn value_greater(a: &Json, b: &Json) -> bool {
    match (a, b) {
        (Json::Int(x), Json::Int(y)) => x > y,
        (Json::Uint(x), Json::Uint(y)) => x > y,
        (Json::Int(x), Json::Uint(y)) => i128::from(*x) > i128::from(*y),
        (Json::Uint(x), Json::Int(y)) => i128::from(*x) > i128::from(*y),
        _ => match (as_f64_number(a), as_f64_number(b)) {
            (Some(x), Some(y)) => x > y,
            _ => false,
        },
    }
}

fn value_less(a: &Json, b: &Json) -> bool {
    match (a, b) {
        (Json::Int(x), Json::Int(y)) => x < y,
        (Json::Uint(x), Json::Uint(y)) => x < y,
        (Json::Int(x), Json::Uint(y)) => i128::from(*x) < i128::from(*y),
        (Json::Uint(x), Json::Int(y)) => i128::from(*x) < i128::from(*y),
        _ => match (as_f64_number(a), as_f64_number(b)) {
            (Some(x), Some(y)) => x < y,
            _ => false,
        },
    }
}

/// Evaluate `path` against `root` (read-only), invoking `callback` for every
/// matched value. Returns the number of matches.
///
/// Mirrors `jsoncons::jsonpath::EvaluatePath`. The empty path matches the root
/// document; a leading `Function` segment evaluates the aggregate over the rest
/// of the path and reports the single result.
pub fn eval_path<F>(path: &[Segment], root: &Json, mut callback: F) -> usize
where
    F: FnMut(Option<&str>, &Json),
{
    if path.is_empty() {
        callback(None, root);
        return 1;
    }

    match &path[0] {
        Segment::Function(agg) => {
            let mut state = AggState::init(*agg);
            if path.len() > 1 {
                traverse(&path[1..], root, |_, value| state.apply(value));
            }
            let value = state.result().unwrap_or(Json::Null);
            callback(None, &value);
            1
        }
        _ => traverse(path, root, &mut callback),
    }
}

/// Locate a mutable node within `root` by a location path.
fn node_mut<'a>(root: &'a mut Json, loc: &[Step]) -> &'a mut Json {
    let mut cur = root;
    for step in loc {
        cur = match step {
            Step::Key(key) => match cur {
                Json::Object(members) => {
                    let i = members
                        .binary_search_by(|(k, _)| k.as_str().cmp(key))
                        .expect("stale mutation location");
                    &mut members[i].1
                }
                _ => unreachable!("location traverses a non-object"),
            },
            Step::Index(index) => match cur {
                Json::Array(items) => &mut items[*index],
                _ => unreachable!("location traverses a non-array"),
            },
        };
    }
    cur
}

/// The mutable variant of [`init`].
fn init_mut<'a>(
    node: &'a mut Json,
    state: &mut ItemState,
    segment_step: &mut u8,
    seg_idx: usize,
    segment: &Segment,
) -> AdvanceMut<'a> {
    match segment {
        Segment::Identifier(key) => match node {
            Json::Object(members) => match members.binary_search_by(|(k, _)| k.as_str().cmp(key)) {
                Ok(i) => {
                    *state = ItemState::Obj(i);
                    AdvanceMut::Node {
                        child: &mut members[i].1,
                        step: Some(Step::Key(key.clone())),
                        seg_idx: seg_idx + 1,
                    }
                }
                Err(_) => AdvanceMut::Mismatch,
            },
            _ => AdvanceMut::Mismatch,
        },
        Segment::Index(expr) => match node {
            Json::Array(items) if !items.is_empty() => match expr.normalize(items.len()) {
                Some((first, second)) => {
                    *state = ItemState::Arr {
                        next: first,
                        last: second,
                    };
                    AdvanceMut::Node {
                        child: &mut items[first],
                        step: Some(Step::Index(first)),
                        seg_idx: seg_idx + 1,
                    }
                }
                None => AdvanceMut::Mismatch,
            },
            _ => AdvanceMut::Mismatch,
        },
        Segment::Wildcard => init_wildcard_mut(node, state, *segment_step, seg_idx),
        Segment::Descent => {
            if *segment_step == 1 {
                *segment_step = 0;
                return AdvanceMut::Node {
                    child: node,
                    step: None,
                    seg_idx: seg_idx + 1,
                };
            }
            init_wildcard_mut(node, state, *segment_step, seg_idx)
        }
        Segment::Function(_) => AdvanceMut::Mismatch,
    }
}

/// The mutable variant of [`init_wildcard`].
fn init_wildcard_mut<'a>(
    node: &'a mut Json,
    state: &mut ItemState,
    segment_step: u8,
    seg_idx: usize,
) -> AdvanceMut<'a> {
    match node {
        Json::Object(members) => {
            if members.is_empty() {
                return AdvanceMut::Exhausted;
            }
            *state = ItemState::Obj(0);
            let (key, child) = &mut members[0];
            AdvanceMut::Node {
                child,
                step: Some(Step::Key(key.clone())),
                seg_idx: seg_idx + segment_step as usize,
            }
        }
        Json::Array(items) => {
            if items.is_empty() {
                return AdvanceMut::Exhausted;
            }
            let last = items.len() - 1;
            *state = ItemState::Arr { next: 0, last };
            AdvanceMut::Node {
                child: &mut items[0],
                step: Some(Step::Index(0)),
                seg_idx: seg_idx + segment_step as usize,
            }
        }
        _ => AdvanceMut::Mismatch,
    }
}

/// The mutable variant of [`advance_impl`].
fn advance_impl_mut<'a>(
    root: &'a mut Json,
    loc: &[Step],
    state: &mut ItemState,
    segment_step: &mut u8,
    seg_idx: usize,
    segment: &Segment,
) -> AdvanceMut<'a> {
    match state {
        ItemState::Mono => {
            let node = node_mut(root, loc);
            init_mut(node, state, segment_step, seg_idx, segment)
        }
        ItemState::Obj(idx) => {
            if !should_iterate_all(segment) {
                return AdvanceMut::Exhausted;
            }
            *idx += 1;
            let node = node_mut(root, loc);
            match node {
                Json::Object(members) => match members.get_mut(*idx) {
                    Some((key, child)) => AdvanceMut::Node {
                        child,
                        step: Some(Step::Key(key.clone())),
                        seg_idx: seg_idx + *segment_step as usize,
                    },
                    None => AdvanceMut::Exhausted,
                },
                _ => AdvanceMut::Exhausted,
            }
        }
        ItemState::Arr { next, last } => {
            if *next == *last {
                return AdvanceMut::Exhausted;
            }
            *next += 1;
            let node = node_mut(root, loc);
            match node {
                Json::Array(items) => match items.get_mut(*next) {
                    Some(child) => AdvanceMut::Node {
                        child,
                        step: Some(Step::Index(*next)),
                        seg_idx: seg_idx + *segment_step as usize,
                    },
                    None => AdvanceMut::Exhausted,
                },
                _ => AdvanceMut::Exhausted,
            }
        }
    }
}

/// A mutation callback: `key` is the member name (or `None` for array
/// elements), `value` the matched value, and the returned value replaces it.
pub type MutateCallback<'a> = dyn FnMut(Option<&str>, &mut Json) -> Json + 'a;

/// Mutate every value matched by `path` in `root`, replacing each with the
/// value returned by `callback`. Returns the number of matches.
///
/// Mirrors `jsoncons::jsonpath::Dfs::Mutate`. Matched nodes are collected
/// first (the DFS must not run to completion while nodes are borrowed
/// mutably), then each is replaced in a second pass. The reference also deletes
/// nodes matched via a recursive descent during this pass; here such nodes are
/// simply replaced.
pub fn mutate_path<F>(path: &[Segment], root: &mut Json, mut callback: F) -> usize
where
    F: FnMut(Option<&str>, &mut Json) -> Json,
{
    if path.is_empty() {
        let value = callback(None, root);
        *root = value;
        return 1;
    }

    let mut nodes_to_mutate = Vec::new();
    collect_terminals(path, root, &mut nodes_to_mutate);

    for loc in &nodes_to_mutate {
        let node = node_mut(root, loc);
        mutate_step(&path[path.len() - 1], node, &mut callback);
    }

    nodes_to_mutate.len()
}

fn collect_terminals(path: &[Segment], root: &Json, out: &mut Vec<Vec<Step>>) {
    fn walk(path: &[Segment], root: &Json, out: &mut Vec<Vec<Step>>) {
        let mut stack = Vec::new();
        stack.push(Frame {
            node: root,
            loc: Vec::new(),
            state: ItemState::Mono,
            segment_step: 1,
            seg_idx: 0,
        });

        while let Some(frame) = stack.last_mut() {
            if path.len() == 1 {
                out.push(frame.loc.clone());
                stack.pop();
                continue;
            }

            let segment = &path[frame.seg_idx];
            match advance_impl(
                frame.node,
                &mut frame.state,
                &mut frame.segment_step,
                frame.seg_idx,
                segment,
            ) {
                Advance::Node {
                    child,
                    step,
                    seg_idx,
                } => {
                    if child.is_object() || child.is_array() {
                        let next_seg_id = seg_idx;
                        let mut child_loc = frame.loc.clone();
                        if let Some(step) = &step {
                            child_loc.push(step.clone());
                        }
                        if next_seg_id + 1 < path.len() {
                            stack.push(Frame {
                                node: child,
                                loc: child_loc,
                                state: ItemState::Mono,
                                segment_step: 1,
                                seg_idx: next_seg_id,
                            });
                            continue;
                        }
                        out.push(child_loc);
                    }
                }
                Advance::Mismatch | Advance::Exhausted => {
                    stack.pop();
                }
            }
        }
    }

    if path.is_empty() {
        return;
    }
    walk(path, root, out);
}

/// Apply `segment` to `node` and replace every matching value with the result
/// of `callback`.
fn mutate_step<F>(segment: &Segment, node: &mut Json, callback: &mut F)
where
    F: FnMut(Option<&str>, &mut Json) -> Json,
{
    match segment {
        Segment::Identifier(key) => {
            if let Json::Object(members) = node
                && let Ok(i) = members.binary_search_by(|(k, _)| k.as_str().cmp(key))
            {
                let value = callback(Some(key), &mut members[i].1);
                members[i].1 = value;
            }
        }
        Segment::Index(expr) => {
            if let Json::Array(items) = node
                && let Some((first, second)) = expr.normalize(items.len())
            {
                for value in &mut items[first..=second] {
                    let new_value = callback(None, value);
                    *value = new_value;
                }
            }
        }
        Segment::Wildcard | Segment::Descent => match node {
            Json::Object(members) => {
                for (key, value) in members.iter_mut() {
                    let new_value = callback(Some(key), value);
                    *value = new_value;
                }
            }
            Json::Array(items) => {
                for value in items.iter_mut() {
                    let new_value = callback(None, value);
                    *value = new_value;
                }
            }
            _ => {}
        },
        Segment::Function(_) => {}
    }
}

/// Delete every value matched by `path` from `root` and return the number of
/// deletions. Mirrors `jsoncons::jsonpath::Dfs::Delete`: deleting a value
/// matched through a recursive descent deletes the nearest matching ancestor
/// and stops (so `$..a` on `{"a":{"a":1}}` deletes the root `a` once).
pub fn delete_path(path: &[Segment], root: &mut Json) -> usize {
    fn walk(path: &[Segment], root: &mut Json, deleted: &mut usize) {
        let mut stack = Vec::new();
        stack.push(FrameMut {
            loc: Vec::new(),
            state: ItemState::Mono,
            segment_step: 1,
            seg_idx: 0,
        });

        while let Some(frame) = stack.last_mut() {
            if path.len() == 1 {
                *deleted += delete_step(&path[0], node_mut(root, &frame.loc));
                stack.pop();
                continue;
            }

            let segment = &path[frame.seg_idx];
            match advance_impl_mut(
                root,
                &frame.loc,
                &mut frame.state,
                &mut frame.segment_step,
                frame.seg_idx,
                segment,
            ) {
                AdvanceMut::Node {
                    child,
                    step,
                    seg_idx,
                } => {
                    if child.is_object() || child.is_array() {
                        let next_seg_id = seg_idx;
                        if next_seg_id + 1 < path.len() {
                            let mut child_loc = frame.loc.clone();
                            if let Some(step) = &step {
                                child_loc.push(step.clone());
                            }
                            stack.push(FrameMut {
                                loc: child_loc,
                                state: ItemState::Mono,
                                segment_step: 1,
                                seg_idx: next_seg_id,
                            });
                            continue;
                        }
                        *deleted += delete_step(&path[next_seg_id], child);
                    }
                }
                AdvanceMut::Mismatch | AdvanceMut::Exhausted => {
                    stack.pop();
                }
            }
        }
    }

    if path.is_empty() {
        return 0;
    }
    let mut deleted = 0;
    walk(path, root, &mut deleted);

    deleted
}

/// Delete every value matched by `segment` within `node`, returning the number
/// of deletions.
fn delete_step(segment: &Segment, node: &mut Json) -> usize {
    match segment {
        Segment::Identifier(key) => match node {
            Json::Object(members) => {
                if let Ok(i) = members.binary_search_by(|(k, _)| k.as_str().cmp(key)) {
                    members.remove(i);
                    1
                } else {
                    0
                }
            }
            _ => 0,
        },
        Segment::Index(expr) => match node {
            Json::Array(items) => {
                let len = items.len();
                if len == 0 {
                    return 0;
                }
                match expr.normalize(len) {
                    Some((first, second)) => {
                        let mut count = 0;
                        for index in (first..=second).rev() {
                            items.remove(index);
                            count += 1;
                        }
                        count
                    }
                    None => 0,
                }
            }
            _ => 0,
        },
        Segment::Wildcard | Segment::Descent => match node {
            Json::Object(members) => {
                let count = members.len();
                members.clear();
                count
            }
            Json::Array(items) => {
                let count = items.len();
                items.clear();
                count
            }
            _ => 0,
        },
        Segment::Function(_) => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn doc(s: &str) -> Json {
        Json::parse(s.as_bytes()).unwrap()
    }

    fn collect(path: &[Segment], root: &Json) -> Vec<String> {
        let mut out = Vec::new();
        eval_path(path, root, |_, value| out.push(value.dump()));
        out
    }

    fn delete(path: &str, s: &str) -> (usize, String) {
        let path = parse_path(path).unwrap();
        let mut root = doc(s);
        let n = delete_path(&path, &mut root);
        (n, root.dump())
    }

    fn assert_delete(path: &str, s: &str, expected_count: usize, expected: &str) {
        let (n, out) = delete(path, s);
        assert_eq!(n, expected_count, "delete count for {path}");
        assert_eq!(out, expected, "result of delete {path}");
    }

    #[test]
    fn parse_basic() {
        assert_eq!(parse_path("$").unwrap(), vec![]);
        assert_eq!(
            parse_path("$.a").unwrap(),
            vec![Segment::Identifier("a".to_string())]
        );
        assert_eq!(
            parse_path("$.a.b").unwrap(),
            vec![
                Segment::Identifier("a".to_string()),
                Segment::Identifier("b".to_string())
            ]
        );
        assert_eq!(
            parse_path("$.a[0]").unwrap(),
            vec![
                Segment::Identifier("a".to_string()),
                Segment::Index(IndexExpr::single(0))
            ]
        );
        assert_eq!(
            parse_path("$[1]").unwrap(),
            vec![Segment::Index(IndexExpr::single(1))]
        );
        assert_eq!(
            parse_path("$..a").unwrap(),
            vec![Segment::Descent, Segment::Identifier("a".to_string())]
        );
        assert_eq!(
            parse_path("$..*").unwrap(),
            vec![Segment::Descent, Segment::Wildcard]
        );
        assert_eq!(
            parse_path("$.a.*").unwrap(),
            vec![Segment::Identifier("a".to_string()), Segment::Wildcard]
        );
        assert_eq!(parse_path("$.*").unwrap(), vec![Segment::Wildcard]);
        assert_eq!(
            parse_path("$[*]").unwrap(),
            vec![Segment::Index(IndexExpr::all())]
        );
        assert_eq!(
            parse_path("$[1:3]").unwrap(),
            vec![Segment::Index(IndexExpr::range(1, 3))]
        );
        assert_eq!(
            parse_path("$[2:]").unwrap(),
            vec![Segment::Index(IndexExpr {
                first: 2,
                second: i64::MAX
            })]
        );
        assert_eq!(
            parse_path("$[:2]").unwrap(),
            vec![Segment::Index(IndexExpr::range(0, 2))]
        );
        assert_eq!(
            parse_path("$[-1]").unwrap(),
            vec![Segment::Index(IndexExpr::single(-1))]
        );
        assert_eq!(
            parse_path("$['a']").unwrap(),
            vec![Segment::Identifier("a".to_string())]
        );
        assert_eq!(
            parse_path("$[\"a b\"]").unwrap(),
            vec![Segment::Identifier("a b".to_string())]
        );
        assert_eq!(
            parse_path("$[0][1]").unwrap(),
            vec![
                Segment::Index(IndexExpr::single(0)),
                Segment::Index(IndexExpr::single(1))
            ]
        );
        assert_eq!(
            parse_path("max($.a[*])").unwrap(),
            vec![
                Segment::Function(Agg::Max),
                Segment::Identifier("a".to_string()),
                Segment::Index(IndexExpr::all())
            ]
        );
        assert_eq!(
            parse_path("$.phoneNumbers[0].type").unwrap(),
            vec![
                Segment::Identifier("phoneNumbers".to_string()),
                Segment::Index(IndexExpr::single(0)),
                Segment::Identifier("type".to_string())
            ]
        );
    }

    #[test]
    fn parse_errors() {
        assert!(parse_path("").is_err());
        assert!(parse_path("a").is_err());
        assert!(parse_path("$.").is_err());
        assert!(parse_path("$a").is_err());
        assert!(parse_path("$[a]").is_err());
        assert!(parse_path("$[1:").is_err());
        assert!(parse_path("$['a'").is_err());
        assert!(parse_path("$['a']").is_ok());
        assert!(parse_path("$.a.").is_err());
        assert!(parse_path("min($.a[*])").is_ok());
        assert!(parse_path("max($.a[*])").is_ok());
        assert!(parse_path("avg($.a[*])").is_ok());
        assert!(parse_path("foo($.a[*])").is_err());
    }

    #[test]
    fn normalize_index() {
        let expr = IndexExpr::single(0);
        assert_eq!(expr.normalize(0), None);
        assert_eq!(expr.normalize(3), Some((0, 0)));
        assert_eq!(IndexExpr::single(-1).normalize(3), Some((2, 2)));
        assert_eq!(IndexExpr::single(-4).normalize(3), Some((0, 0)));
        assert_eq!(IndexExpr::range(1, 3).normalize(5), Some((1, 2)));
        assert_eq!(IndexExpr::range(0, -1).normalize(3), Some((0, 1)));
        assert_eq!(IndexExpr::range(0, 10).normalize(3), Some((0, 2)));
        assert_eq!(IndexExpr::range(-1, 1).normalize(3), None);
        assert_eq!(IndexExpr::all().normalize(3), Some((0, 2)));
        assert_eq!(
            IndexExpr {
                first: 2,
                second: i64::MAX
            }
            .normalize(3),
            Some((2, 2))
        );
    }

    #[test]
    fn traverse_simple() {
        let root = doc(r#"{"a":{"b":"x"}}"#);
        assert_eq!(
            collect(&parse_path("$.a").unwrap(), &root),
            vec![r#"{"b":"x"}"#]
        );
        assert_eq!(collect(&parse_path("$.a.b").unwrap(), &root), vec!["\"x\""]);
        assert_eq!(collect(&parse_path("$").unwrap(), &root), vec![root.dump()]);
        assert_eq!(
            collect(&parse_path("$.b").unwrap(), &root),
            Vec::<String>::new()
        );
    }

    #[test]
    fn traverse_identifier_exhausts_after_first_child() {
        let root = doc(r#"{"a":{"b":1},"inner":{"a":{"b":2}}}"#);
        assert_eq!(collect(&parse_path("$.a.b").unwrap(), &root), vec!["1"]);
    }

    #[test]
    fn traverse_identifier_on_missing() {
        let root = doc(r#"{"a":5}"#);
        assert_eq!(
            collect(&parse_path("$.a.b").unwrap(), &root),
            Vec::<String>::new()
        );
    }

    #[test]
    fn descent_matches_self() {
        let root = doc(r#"{"a":"foo","inner":{"a":"bye"},"inner1":{"a":7}}"#);
        assert_eq!(
            collect(&parse_path("$..a").unwrap(), &root),
            vec!["\"foo\"", "\"bye\"", "7"]
        );
    }

    #[test]
    fn descent_index() {
        let root = doc(r#"["first",["second"]]"#);
        assert_eq!(
            collect(&parse_path("$..[0]").unwrap(), &root),
            vec!["\"first\"", "\"second\""]
        );
    }

    #[test]
    fn descent_wildcard() {
        let root = doc(r#"{"a":{},"b":{"a":1},"c":{"a":1,"b":2},"d":{},"e":[]}"#);
        assert_eq!(
            collect(&parse_path("$..*").unwrap(), &root),
            vec![
                "{}",
                r#"{"a":1}"#,
                r#"{"a":1,"b":2}"#,
                "{}",
                "[]",
                "1",
                "1",
                "2"
            ]
        );
    }

    #[test]
    fn descent_wildcard_nested() {
        let root = doc(r#"{"a":{},"b":{"c":{"d":{"e":1337}}}}"#);
        assert_eq!(
            collect(&parse_path("$..*").unwrap(), &root),
            vec![
                "{}",
                r#"{"c":{"d":{"e":1337}}}"#,
                r#"{"d":{"e":1337}}"#,
                r#"{"e":1337}"#,
                "1337"
            ]
        );
    }

    #[test]
    fn descent_quoted() {
        let root = doc(r#"{"a":"first","inner":{"a":"second"}}"#);
        assert_eq!(
            collect(&parse_path("$..['a']").unwrap(), &root),
            vec!["\"first\"", "\"second\""]
        );
    }

    #[test]
    fn single_index() {
        let root = doc(r#"["first",["second"]]"#);
        assert_eq!(
            collect(&parse_path("$[0]").unwrap(), &root),
            vec!["\"first\""]
        );
    }

    #[test]
    fn index_range() {
        let root = doc("[0,1,2,3,4]");
        assert_eq!(
            collect(&parse_path("$[1:3]").unwrap(), &root),
            vec!["1", "2"]
        );
        assert_eq!(
            collect(&parse_path("$[2:]").unwrap(), &root),
            vec!["2", "3", "4"]
        );
    }

    #[test]
    fn negative_index() {
        let root = doc("[0,1,2]");
        assert_eq!(collect(&parse_path("$[-1]").unwrap(), &root), vec!["2"]);
    }

    #[test]
    fn wildcard() {
        let root = doc(r#"{"a":1,"b":2}"#);
        assert_eq!(collect(&parse_path("$.*").unwrap(), &root), vec!["1", "2"]);
    }

    #[test]
    fn no_match() {
        let root = doc(r#"{"a":5}"#);
        assert_eq!(
            collect(&parse_path("$.a.b").unwrap(), &root),
            Vec::<String>::new()
        );
        assert_eq!(
            collect(&parse_path("$[0]").unwrap(), &root),
            Vec::<String>::new()
        );
        assert_eq!(
            collect(&parse_path("$..a").unwrap(), &doc("\"x\"")),
            Vec::<String>::new()
        );
    }

    #[test]
    fn phonebook_spouse() {
        let root = doc(r#"{"firstName":"Vasily","lastName":"Zyabkin","age":27,"spouse":null}"#);
        assert_eq!(
            collect(&parse_path("$.spouse.*").unwrap(), &root),
            Vec::<String>::new()
        );
    }

    #[test]
    fn function_max() {
        let root = doc(r#"{"a":[1,5,3]}"#);
        assert_eq!(
            collect(&parse_path("max($.a[*])").unwrap(), &root),
            vec!["5"]
        );
        assert_eq!(
            collect(&parse_path("min($.a[*])").unwrap(), &root),
            vec!["1"]
        );
        assert_eq!(
            collect(&parse_path("avg($.a[*])").unwrap(), &root),
            vec!["3.0"]
        );
    }

    #[test]
    fn function_max_no_wildcard() {
        let root = doc(r#"{"a":[1,5,3]}"#);
        assert_eq!(
            collect(&parse_path("max($.a)").unwrap(), &root),
            vec!["null"]
        );
    }

    #[test]
    fn function_missing() {
        let root = doc(r#"{"a":[1,5,3]}"#);
        assert_eq!(
            collect(&parse_path("max($.a.b)").unwrap(), &root),
            vec!["null"]
        );
    }

    #[test]
    fn delete_single() {
        assert_delete("$.a", r#"{"a":1,"b":2}"#, 1, r#"{"b":2}"#);
        assert_delete("$.b", r#"{"a":1,"b":2}"#, 1, r#"{"a":1}"#);
        assert_delete("$.c", r#"{"a":1,"b":2}"#, 0, r#"{"a":1,"b":2}"#);
    }

    #[test]
    fn delete_descent_nested() {
        assert_delete("$..a", r#"{"a":{"a":1}}"#, 1, "{}");
    }

    #[test]
    fn delete_descent_multiple() {
        assert_delete(
            "$..a",
            r#"{"a":"foo","inner":{"a":"bye"}}"#,
            2,
            r#"{"inner":{}}"#,
        );
    }

    #[test]
    fn delete_index() {
        assert_delete("$[0]", r#"["first","second"]"#, 1, r#"["second"]"#);
        assert_delete("$..[0]", r#"["first",["second"]]"#, 2, "[[]]");
        assert_delete("$[*]", "[1,2,3]", 3, "[]");
        assert_delete("$[5]", "[1,2]", 0, "[1,2]");
    }

    #[test]
    fn delete_wildcard() {
        assert_delete("$..*", r#"{"a":{},"b":{"a":1}}"#, 2, "{}");
    }

    #[test]
    fn mutate_replace() {
        let mut root = doc(r#"{"a":1,"b":2}"#);
        let path = parse_path("$.a").unwrap();
        let n = mutate_path(&path, &mut root, |_, _| Json::Int(10));
        assert_eq!(n, 1);
        assert_eq!(root.dump(), r#"{"a":10,"b":2}"#);
    }

    #[test]
    fn mutate_descent_multiple() {
        let mut root = doc(r#"{"a":"foo","inner":{"a":"bye"}}"#);
        let path = parse_path("$..a").unwrap();
        let n = mutate_path(&path, &mut root, |_, _| Json::String("X".to_string()));
        assert_eq!(n, 2);
        assert_eq!(root.dump(), r#"{"a":"X","inner":{"a":"X"}}"#);
    }

    #[test]
    fn mutate_descent_nested() {
        let mut root = doc(r#"{"a":{"a":1}}"#);
        let path = parse_path("$..a").unwrap();
        let n = mutate_path(&path, &mut root, |_, _| Json::String("X".to_string()));
        assert_eq!(n, 2);
        assert_eq!(root.dump(), r#"{"a":"X"}"#);
    }

    #[test]
    fn mutate_index() {
        let mut root = doc("[0,1,2]");
        let path = parse_path("$[1]").unwrap();
        let n = mutate_path(&path, &mut root, |_, _| Json::Int(9));
        assert_eq!(n, 1);
        assert_eq!(root.dump(), "[0,9,2]");
    }

    #[test]
    fn mutate_root() {
        let mut root = doc(r#"{"a":1}"#);
        let n = mutate_path(&[], &mut root, |_, _| Json::Null);
        assert_eq!(n, 1);
        assert_eq!(root.dump(), "null");
    }
}
