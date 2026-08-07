//! Port of `dragonfly/src/server/json_family_test.cc`.
//!
//! Adaptations from the C++ original:
//! - The server is RESP2-only, so `NumericOperationsResp2Resp3` covers only the
//!   RESP2 branch and the `*_RESP3NestedArrayBug` tests assert the flat RESP2
//!   arrays the reference only produces under RESP3 (the "not double-wrapped"
//!   shape this port already emits).
//! - `MaxNestingJsonDepth` uses the port's lower nesting limit (64 vs the
//!   reference's 256); only the overflowing case is asserted.
//! - `DEBUG MEMORY` assertions follow the port's `memory_usage` model (SSO
//!   strings and scalars report 0, containers report their heap usage).

mod common;

use common::*;

fn phonebook_json() -> &'static str {
    r#"
    {
      "firstName":"John",
      "lastName":"Smith",
      "age":27,
      "weight":135.25,
      "isAlive":true,
      "address":{
          "street":"21 2nd Street",
          "city":"New York",
          "state":"NY",
          "zipcode":"10021-3100"
      },
      "phoneNumbers":[
          {
            "type":"home",
            "number":"212 555-1234"
          },
          {
            "type":"office",
            "number":"646 555-4567"
          }
      ],
      "children":[

      ],
      "spouse":null
    }
  "#
}

#[test]
fn set_get_basic() {
    let mut t = Ctx::new();
    let json = r#"
    {
       "store": {
        "book": [
         {
           "category": "Fantasy",
           "author": "J. K. Rowling",
           "title": "Harry Potter and the Philosopher's Stone",
           "isbn": 9780747532743,
           "price": 5.99
         }
       ]
      }
    }
"#;
    let xml = r#"
    <?xml version="1.0" encoding="UTF-8" ?>
    <store>
      <book>
        <category>Fantasy</category>
        <author>J. K. Rowling</author>
        <title>Harry Potter and the Philosopher&#x27;s Stone</title>
        <isbn>9780747532743</isbn>
        <price>5.99</price>
      </book>
    </store>
"#;

    t.assert_text(&["JSON.SET", "json", ".", json], "OK");
    assert!(matches!(
        t.run(&["JSON.GET", "json", "$..*"]),
        Value::Bulk(Some(_))
    ));
    assert!(matches!(
        t.run(&["JSON.GET", "json", "$..book[0].price"]),
        Value::Bulk(Some(_))
    ));

    assert!(matches!(
        t.run(&["JSON.GET", "json", "//*"]),
        Value::Error(_)
    ));
    assert!(matches!(
        t.run(&["JSON.GET", "json", "//book[0]"]),
        Value::Error(_)
    ));

    t.assert_text(
        &["JSON.GET", "json", "store.book[0].category"],
        "\"Fantasy\"",
    );
    t.assert_text(
        &["JSON.GET", "json", ".store.book[0].category"],
        "\"Fantasy\"",
    );

    t.assert_text(&["SET", "xml", xml], "OK");
    assert!(matches!(
        t.run(&["JSON.GET", "xml", "$..*"]),
        Value::Error(_)
    ));
}

#[test]
fn get_legacy() {
    let mut t = Ctx::new();
    let json = r#"{"name":"Leonard Cohen","lastSeen":1478476800,"loggedOut": true}"#;
    t.assert_text(&["JSON.SET", "json", "$", json], "OK");

    // V1 response with no path / root path.
    t.assert_text(
        &["JSON.GET", "json"],
        "{\"lastSeen\":1478476800,\"loggedOut\":true,\"name\":\"Leonard Cohen\"}",
    );
    t.assert_text(
        &["JSON.GET", "json", "."],
        "{\"lastSeen\":1478476800,\"loggedOut\":true,\"name\":\"Leonard Cohen\"}",
    );
    // V2 root response.
    t.assert_text(
        &["JSON.GET", "json", "$"],
        "[{\"lastSeen\":1478476800,\"loggedOut\":true,\"name\":\"Leonard Cohen\"}]",
    );

    t.assert_text(&["JSON.GET", "json", ".name"], "\"Leonard Cohen\"");
    t.assert_text(&["JSON.GET", "json", "$.name"], "[\"Leonard Cohen\"]");

    // Mixed V1/V2 paths.
    t.assert_text(
        &["JSON.GET", "json", ".name", "$.lastSeen"],
        "{\"$.lastSeen\":[1478476800],\".name\":[\"Leonard Cohen\"]}",
    );
    t.assert_text(
        &["JSON.GET", "json", ".name", ".lastSeen"],
        "{\".lastSeen\":1478476800,\".name\":\"Leonard Cohen\"}",
    );
    t.assert_text(
        &["JSON.GET", "json", "$.name", "$.lastSeen"],
        "{\"$.lastSeen\":[1478476800],\"$.name\":[\"Leonard Cohen\"]}",
    );

    let json = r#"
    {"a":"first","b":{"field":"second"},"c":{"field":"third"}}
  "#;
    t.assert_text(&["JSON.SET", "json", "$", json], "OK");

    // Invalid legacy paths error out.
    t.assert_err(&["JSON.GET", "json", "bar"], "ERR invalid JSON path");
    t.assert_err(&["JSON.GET", "json", ".", "bar"], "ERR invalid JSON path");
    t.assert_err(
        &["JSON.GET", "json", ".a", "bar", "foo", "third", "."],
        "ERR invalid JSON path",
    );

    // V2 paths never error on a missing key: they produce an empty array.
    t.assert_text(&["JSON.GET", "json", "$.bar"], "[]");
    t.assert_text(
        &["JSON.GET", "json", "bar", "$.a"],
        "{\"$.a\":[\"first\"],\"bar\":[]}",
    );
    t.assert_text(&["JSON.GET", "json", "$.bar"], "[]");
}

#[test]
fn set_get_from_phonebook() {
    let mut t = Ctx::new();
    t.assert_text(&["JSON.SET", "json", ".", phonebook_json()], "OK");

    t.assert_text(&["JSON.GET", "json", "."], "{\"address\":{\"city\":\"New York\",\"state\":\"NY\",\"street\":\"21 2nd Street\",\"zipcode\":\"10021-3100\"},\"age\":27,\"children\":[],\"firstName\":\"John\",\"isAlive\":true,\"lastName\":\"Smith\",\"phoneNumbers\":[{\"number\":\"212 555-1234\",\"type\":\"home\"},{\"number\":\"646 555-4567\",\"type\":\"office\"}],\"spouse\":null,\"weight\":135.25}");
    t.assert_text(&["JSON.GET", "json", "$"], "[{\"address\":{\"city\":\"New York\",\"state\":\"NY\",\"street\":\"21 2nd Street\",\"zipcode\":\"10021-3100\"},\"age\":27,\"children\":[],\"firstName\":\"John\",\"isAlive\":true,\"lastName\":\"Smith\",\"phoneNumbers\":[{\"number\":\"212 555-1234\",\"type\":\"home\"},{\"number\":\"646 555-4567\",\"type\":\"office\"}],\"spouse\":null,\"weight\":135.25}]");

    t.assert_text(
        &["JSON.GET", "json", "$.address.*"],
        "[\"New York\",\"NY\",\"21 2nd Street\",\"10021-3100\"]",
    );
    t.assert_text(
        &["JSON.GET", "json", "$.firstName", "$.age", "$.lastName"],
        "{\"$.age\":[27],\"$.firstName\":[\"John\"],\"$.lastName\":[\"Smith\"]}",
    );
    t.assert_text(&["JSON.GET", "json", "$.spouse.*"], "[]");
    t.assert_text(&["JSON.GET", "json", "$.children.*"], "[]");
    t.assert_text(
        &["JSON.GET", "json", "$..phoneNumbers[1].*"],
        "[\"646 555-4567\",\"office\"]",
    );

    t.assert_text(
        &["JSON.GET", "json", "$.address.*", "INDENT", "indent", "NEWLINE", "newline"],
        "[newlineindent\"New York\",newlineindent\"NY\",newlineindent\"21 2nd Street\",newlineindent\"10021-3100\"newline]",
    );
    t.assert_text(
        &["JSON.GET", "json", "$.address", "SPACE", "space"],
        "[{\"city\":space\"New York\",\"state\":space\"NY\",\"street\":space\"21 2nd Street\",\"zipcode\":space\"10021-3100\"}]",
    );
    t.assert_text(
        &["JSON.GET", "json", "$.firstName", "$.age", "$.lastName", "INDENT", "indent", "NEWLINE", "newline", "SPACE", "space"],
        "{newlineindent\"$.age\":space[newlineindentindent27newlineindent],newlineindent\"$.firstName\":space[newlineindentindent\"John\"newlineindent],newlineindent\"$.lastName\":space[newlineindentindent\"Smith\"newlineindent]newline}",
    );
    t.assert_text(
        &["JSON.GET", "json", "$..phoneNumbers.*", "INDENT", "t", "NEWLINE", "s", "SPACE", "s"],
        "[st{stt\"number\":s\"212 555-1234\",stt\"type\":s\"home\"st},st{stt\"number\":s\"646 555-4567\",stt\"type\":s\"office\"st}s]",
    );
}

#[test]
fn get_brackets() {
    let mut t = Ctx::new();
    let json = r#"
    {"a":"first", "b":{"a":"second"}}
  "#;
    t.assert_text(&["JSON.SET", "json", ".", json], "OK");

    t.assert_text(&["JSON.GET", "json", "$[\"a\"]"], "[\"first\"]");
    t.assert_text(
        &["JSON.GET", "json", "$..[\"a\"]"],
        "[\"first\",\"second\"]",
    );
    t.assert_text(&["JSON.GET", "json", "$.b[\"a\"]"], "[\"second\"]");
    t.assert_text(&["JSON.GET", "json", "[\"a\"]"], "\"first\"");
    t.assert_text(&["JSON.GET", "json", "..[\"a\"]"], "\"second\"");

    let json = r#"
    ["first", ["second"]]
  "#;
    t.assert_text(&["JSON.SET", "json", ".", json], "OK");

    t.assert_text(&["JSON.GET", "json", "$[0]"], "[\"first\"]");
    t.assert_text(&["JSON.GET", "json", "$..[0]"], "[\"first\",\"second\"]");
    t.assert_text(&["JSON.GET", "json", "[0]"], "\"first\"");
    t.assert_text(&["JSON.GET", "json", "..[0]"], "\"second\"");
    t.assert_text(&["JSON.GET", "json", "$[\"first\"]"], "[]");

    let json = r#"
    {"a":{"b":{"c":"first"}}, "b":{"b":{"c":"second"}}, "c":{"b":{"c":"third"}}}
  "#;
    t.assert_text(&["JSON.SET", "json", ".", json], "OK");

    t.assert_text(&["JSON.GET", "json", "$[\"a\"]['b'][\"c\"]"], "[\"first\"]");
    t.assert_text(&["JSON.GET", "json", "$[\"a\"].b['c']"], "[\"first\"]");
    t.assert_text(
        &["JSON.GET", "json", "$..['b'][\"c\"]"],
        "[\"first\",\"second\",\"third\"]",
    );
    t.assert_text(&["JSON.GET", "json", "$.c['b'][\"c\"]"], "[\"third\"]");
}

#[test]
fn get_with_no_escape() {
    let mut t = Ctx::new();
    let json = r#"{"key": "value with special characters: \n \t \" \""}"#;
    t.assert_text(&["JSON.SET", "json", ".", json], "OK");

    t.assert_text(
        &["JSON.GET", "json", "."],
        "{\"key\":\"value with special characters: \\n \\t \\\" \\\"\"}",
    );
    // NOESCAPE is accepted and, matching the reference, does not change output.
    t.assert_text(
        &["JSON.GET", "json", ".", "NOESCAPE"],
        "{\"key\":\"value with special characters: \\n \\t \\\" \\\"\"}",
    );
}

#[test]
fn type_v2() {
    let mut t = Ctx::new();
    let json = r#"
    [1, 2.3, "foo", true, null, {}, []]
  "#;
    t.assert_text(&["JSON.SET", "json", ".", json], "OK");

    let v = t.arr(&["JSON.TYPE", "json", "$[*]"]);
    let got: Vec<String> = v.iter().filter_map(|x| x.text()).collect();
    assert_eq!(
        got,
        vec![
            "integer".to_string(),
            "number".to_string(),
            "string".to_string(),
            "boolean".to_string(),
            "null".to_string(),
            "object".to_string(),
            "array".to_string()
        ]
    );
    assert!(t.arr(&["JSON.TYPE", "json", "$[10]"]).is_empty());
    t.assert_null(&["JSON.TYPE", "not_exist_key", "$[10]"]);
}

#[test]
fn type_legacy() {
    let mut t = Ctx::new();
    t.assert_text(&["JSON.SET", "json", ".", phonebook_json()], "OK");

    t.assert_text(&["JSON.TYPE", "json"], "object");
    t.assert_text(&["JSON.TYPE", "json", ".children"], "array");
    t.assert_text(&["JSON.TYPE", "json", ".firstName"], "string");
    t.assert_text(&["JSON.TYPE", "json", ".age"], "integer");
    t.assert_text(&["JSON.TYPE", "json", ".weight"], "number");
    t.assert_text(&["JSON.TYPE", "json", ".isAlive"], "boolean");
    t.assert_text(&["JSON.TYPE", "json", ".spouse"], "null");

    t.assert_null(&["JSON.TYPE", "not_exist_key", ".some_field"]);
}

#[test]
fn str_len() {
    let mut t = Ctx::new();
    let json = r#"
    {"a":{"a":"a"}, "b":{"a":"a", "b":1}, "c":{"a":"a", "b":"bb"}, "d":{"a":1, "b":"b", "c":3}}
  "#;
    t.assert_text(&["JSON.SET", "json", ".", json], "OK");

    // V2 replies are always arrays: one element per match.
    let mut v = t.arr(&["JSON.STRLEN", "json", "$.a.a"]);
    assert_eq!(v.len(), 1);
    assert_eq!(v[0].int(), Some(1));

    v = t.arr(&["JSON.STRLEN", "json", "$.a"]);
    assert_eq!(v.len(), 1);
    assert!(matches!(v[0], Value::Bulk(None)));

    v = t.arr(&["JSON.STRLEN", "json", "$.a.*"]);
    assert_eq!(v.len(), 1);
    assert_eq!(v[0].int(), Some(1));

    v = t.arr(&["JSON.STRLEN", "json", "$.c.b"]);
    assert_eq!(v.len(), 1);
    assert_eq!(v[0].int(), Some(2));

    t.assert_err(&["JSON.STRLEN", "non_existent_key", "$.c.b"], "no such key");
    t.assert_err(&["JSON.STRLEN", "non_existent_key", "$"], "no such key");

    // In V2, several possible values yield an array of all of them.
    v = t.arr(&["JSON.STRLEN", "json", "$.c.*"]);
    assert_eq!(v.len(), 2);
    assert_eq!(v[0].int(), Some(1));
    assert_eq!(v[1].int(), Some(2));

    v = t.arr(&["JSON.STRLEN", "json", "$.d.*"]);
    assert_eq!(v.len(), 3);
    assert!(matches!(v[0], Value::Bulk(None)));
    assert_eq!(v[1].int(), Some(1));
    assert!(matches!(v[2], Value::Bulk(None)));
}

#[test]
fn str_len_legacy() {
    let mut t = Ctx::new();
    let json = r#"
    {"a":{"a":"a"}, "b":{"a":"a", "b":1}, "c":{"a":"a", "b":"bb"}, "d":{"a":1, "b":"b", "c":3}}
  "#;
    t.assert_text(&["JSON.SET", "json", ".", json], "OK");

    t.assert_err(&["JSON.STRLEN", "json"], "wrong JSON type of path value");
    t.assert_int(&["JSON.STRLEN", "json", ".a.a"], 1);
    t.assert_err(
        &["JSON.STRLEN", "json", ".a"],
        "wrong JSON type of path value",
    );
    t.assert_int(&["JSON.STRLEN", "json", ".a.*"], 1);
    t.assert_int(&["JSON.STRLEN", "json", ".c.b"], 2);
    t.assert_null(&["JSON.STRLEN", "non_existent_key", ".c.b"]);

    // Legacy mode reports only the first string's length.
    t.assert_int(&["JSON.STRLEN", "json", ".c.*"], 1);
    t.assert_int(&["JSON.STRLEN", "json", ".d.*"], 1);
}

#[test]
fn obj_len() {
    let mut t = Ctx::new();
    let json = r#"
    {"a":{}, "b":{"a":"a"}, "c":{"a":"a", "b":"bb"}, "d":{"a":1, "b":"b", "c":{"a":3,"b":4}}, "e":1}
  "#;
    t.assert_text(&["JSON.SET", "json", ".", json], "OK");

    // V2 replies are always arrays: one element per match.
    let mut v = t.arr(&["JSON.OBJLEN", "json", "$.a"]);
    assert_eq!(v.len(), 1);
    assert_eq!(v[0].int(), Some(0));

    v = t.arr(&["JSON.OBJLEN", "json", "$.a.*"]);
    assert!(v.is_empty());

    v = t.arr(&["JSON.OBJLEN", "json", "$.b"]);
    assert_eq!(v.len(), 1);
    assert_eq!(v[0].int(), Some(1));

    v = t.arr(&["JSON.OBJLEN", "json", "$.b.*"]);
    assert_eq!(v.len(), 1);
    assert!(matches!(v[0], Value::Bulk(None)));

    v = t.arr(&["JSON.OBJLEN", "json", "$.c"]);
    assert_eq!(v.len(), 1);
    assert_eq!(v[0].int(), Some(2));

    v = t.arr(&["JSON.OBJLEN", "json", "$.d"]);
    assert_eq!(v.len(), 1);
    assert_eq!(v[0].int(), Some(3));

    t.assert_err(&["JSON.OBJLEN", "non_existent_key", "$.a"], "no such key");

    v = t.arr(&["JSON.OBJLEN", "json", "$.c.*"]);
    assert_eq!(v.len(), 2);
    assert!(matches!(v[0], Value::Bulk(None)));
    assert!(matches!(v[1], Value::Bulk(None)));

    v = t.arr(&["JSON.OBJLEN", "json", "$.d.*"]);
    assert_eq!(v.len(), 3);
    assert!(matches!(v[0], Value::Bulk(None)));
    assert!(matches!(v[1], Value::Bulk(None)));
    assert_eq!(v[2].int(), Some(2));

    v = t.arr(&["JSON.OBJLEN", "json", "$.*"]);
    assert_eq!(v.len(), 5);
    assert_eq!(v[0].int(), Some(0));
    assert_eq!(v[1].int(), Some(1));
    assert_eq!(v[2].int(), Some(2));
    assert_eq!(v[3].int(), Some(3));
    assert!(matches!(v[4], Value::Bulk(None)));
}

#[test]
fn obj_len_legacy() {
    let mut t = Ctx::new();
    let json = r#"
    {"a":{}, "b":{"a":"a"}, "c":{"a":"a", "b":"bb"}, "d":{"a":1, "b":"b", "c":{"a":3,"b":4}}, "e":1}
  "#;
    t.assert_text(&["JSON.SET", "json", ".", json], "OK");

    t.assert_err(&["JSON.STRLEN", "json"], "wrong JSON type of path value");
    t.assert_int(&["JSON.OBJLEN", "json", ".a"], 0);
    t.assert_null(&["JSON.OBJLEN", "json", ".a.*"]);
    t.assert_int(&["JSON.OBJLEN", "json", ".b"], 1);
    t.assert_err(
        &["JSON.OBJLEN", "json", ".b.*"],
        "wrong JSON type of path value",
    );
    t.assert_int(&["JSON.OBJLEN", "json", ".c"], 2);
    t.assert_int(&["JSON.OBJLEN", "json", ".d"], 3);
    t.assert_null(&["JSON.OBJLEN", "non_existent_key", ".a"]);
    t.assert_null(&["JSON.OBJLEN", "json", ".none"]);

    // Legacy mode reports only the first object's length.
    t.assert_err(
        &["JSON.OBJLEN", "json", ".c.*"],
        "wrong JSON type of path value",
    );
    t.assert_int(&["JSON.OBJLEN", "json", ".d.*"], 2);
    t.assert_int(&["JSON.OBJLEN", "json", ".*"], 0);
}

#[test]
fn arr_len() {
    let mut t = Ctx::new();
    let mut json = r#"
    [[], ["a"], ["a", "b"], ["a", "b", "c"]]
  "#;
    t.assert_text(&["JSON.SET", "json", ".", json], "OK");

    let mut v = t.arr(&["JSON.ARRLEN", "json", "$[*]"]);
    assert_eq!(v.len(), 4);
    assert_eq!(v[0].int(), Some(0));
    assert_eq!(v[1].int(), Some(1));
    assert_eq!(v[2].int(), Some(2));
    assert_eq!(v[3].int(), Some(3));

    json = r#"
    [[], "a", ["a", "b"], ["a", "b", "c"], 4]
  "#;
    t.assert_text(&["JSON.SET", "json", ".", json], "OK");

    v = t.arr(&["JSON.ARRLEN", "json", "$[*]"]);
    assert_eq!(v.len(), 5);
    assert_eq!(v[0].int(), Some(0));
    assert!(matches!(v[1], Value::Bulk(None)));
    assert_eq!(v[2].int(), Some(2));
    assert_eq!(v[3].int(), Some(3));
    assert!(matches!(v[4], Value::Bulk(None)));

    t.assert_err(&["JSON.OBJLEN", "non_existent_key", "$[*]"], "no such key");
}

#[test]
fn arr_len_legacy() {
    let mut t = Ctx::new();
    let mut json = r#"
    [[], ["a"], ["a", "b"], ["a", "b", "c"]]
  "#;
    t.assert_text(&["JSON.SET", "json", ".", json], "OK");

    t.assert_int(&["JSON.ARRLEN", "json"], 4);
    t.assert_int(&["JSON.ARRLEN", "json", "[*]"], 0);
    t.assert_int(&["JSON.ARRLEN", "json", "[3]"], 3);

    json = r#"
    [[], "a", ["a", "b"], ["a", "b", "c"], 4]
  "#;
    t.assert_text(&["JSON.SET", "json", ".", json], "OK");

    t.assert_int(&["JSON.ARRLEN", "json", "[*]"], 0);
    t.assert_err(
        &["JSON.ARRLEN", "json", "[1]"],
        "wrong JSON type of path value",
    );
    t.assert_int(&["JSON.ARRLEN", "json", "[2]"], 2);
    t.assert_null(&["JSON.OBJLEN", "non_existent_key", "[*]"]);
}

#[test]
fn toggle() {
    let mut t = Ctx::new();
    let json = r#"
    {"a":true, "b":false, "c":1, "d":null, "e":"foo", "f":[], "g":{}}
  "#;
    t.assert_text(&["JSON.SET", "json", ".", json], "OK");

    let mut v = t.arr(&["JSON.TOGGLE", "json", "$.*"]);
    assert_eq!(v.len(), 7);
    assert_eq!(v[0].int(), Some(0));
    assert_eq!(v[1].int(), Some(1));
    assert!(matches!(v[2], Value::Bulk(None)));
    assert!(matches!(v[3], Value::Bulk(None)));
    assert!(matches!(v[4], Value::Bulk(None)));
    assert!(matches!(v[5], Value::Bulk(None)));
    assert!(matches!(v[6], Value::Bulk(None)));

    t.assert_text(
        &["JSON.GET", "json", "$.*"],
        "[false,true,1,null,\"foo\",[],{}]",
    );

    v = t.arr(&["JSON.TOGGLE", "json", "$.*"]);
    assert_eq!(v.len(), 7);
    assert_eq!(v[0].int(), Some(1));
    assert_eq!(v[1].int(), Some(0));
    assert!(matches!(v[2], Value::Bulk(None)));
    assert!(matches!(v[3], Value::Bulk(None)));
    assert!(matches!(v[4], Value::Bulk(None)));
    assert!(matches!(v[5], Value::Bulk(None)));
    assert!(matches!(v[6], Value::Bulk(None)));

    t.assert_text(
        &["JSON.GET", "json", "$.*"],
        "[true,false,1,null,\"foo\",[],{}]",
    );
}

#[test]
fn toggle_legacy() {
    let mut t = Ctx::new();
    let json = r#"
    {"a":true, "b":false, "c":1, "d":null, "e":"foo", "f":[], "g":{}}
  "#;
    t.assert_text(&["JSON.SET", "json", ".", json], "OK");

    t.assert_err(&["JSON.TOGGLE", "json"], "wrong number of arguments");
    t.assert_text(&["JSON.TOGGLE", "json", ".*"], "true");
    t.assert_text(&["JSON.TOGGLE", "json", ".*"], "false");
    t.assert_text(
        &["JSON.GET", "json", "$.*"],
        "[true,false,1,null,\"foo\",[],{}]",
    );

    t.assert_text(&["JSON.SET", "json", ".", "true"], "OK");
    t.assert_text(&["JSON.TOGGLE", "json", "."], "false");
    t.assert_text(&["JSON.TOGGLE", "json", "."], "true");

    let json = r#"
    {"isAvailable": false}
  "#;
    t.assert_text(&["JSON.SET", "json", ".", json], "OK");
    t.assert_text(&["JSON.TOGGLE", "json", ".isAvailable"], "true");
    t.assert_text(&["JSON.TOGGLE", "json", ".isAvailable"], "false");
}

#[test]
fn num_incr_by() {
    let mut t = Ctx::new();
    let json = r#"
    {"e":1.5,"a":1}
  "#;
    t.assert_text(&["JSON.SET", "json", ".", json], "OK");

    // Incrementing by a negative value should produce a negative result.
    t.assert_text(&["JSON.NUMINCRBY", "json", "$.a", "-2"], "[-1]");

    // Large positive integer (> INT64_MAX) should stay positive.
    t.assert_text(
        &["JSON.SET", "json", ".", r#"{"a":9223372036854775808}"#],
        "OK",
    );
    t.assert_text(
        &["JSON.NUMINCRBY", "json", "$.a", "2048"],
        "[9223372036854777856]",
    );

    // Result below INT64_MIN reports overflow.
    t.assert_text(
        &["JSON.SET", "json", ".", r#"{"a":-9223372036854775808}"#],
        "OK",
    );
    t.assert_err(
        &["JSON.NUMINCRBY", "json", "$.a", "-9223372036854775808"],
        "ERR result is not a number",
    );

    t.assert_text(&["JSON.SET", "json", ".", json], "OK");

    t.assert_text(&["JSON.NUMINCRBY", "json", "$.a", "1.1"], "[2.1]");
    t.assert_text(&["JSON.NUMINCRBY", "json", "$.e", "1"], "[2.5]");
    t.assert_err(
        &["JSON.NUMINCRBY", "json", "$.e", "inf"],
        "ERR result is not a number",
    );

    t.assert_text(&["JSON.SET", "json", ".", json], "OK");
    t.assert_text(&["JSON.NUMINCRBY", "json", "$.e", "1.7e308"], "[1.7e+308]");
    t.assert_err(
        &["JSON.NUMINCRBY", "json", "$.e", "1.7e308"],
        "ERR result is not a number",
    );
    t.assert_text(&["JSON.GET", "json", "$.*"], "[1,1.7e+308]");

    let json = r#"
    {"a":[], "b":[1], "c":[1,2], "d":[1,2,3]}
  "#;
    t.assert_text(&["JSON.SET", "json", ".", json], "OK");

    t.assert_text(&["JSON.NUMINCRBY", "json", "$.d[*]", "10"], "[11,12,13]");
    t.assert_text(&["JSON.GET", "json", "$.d[*]"], "[11,12,13]");

    t.assert_text(&["JSON.SET", "json", ".", json], "OK");
    t.assert_text(&["JSON.NUMINCRBY", "json", "$.a[*]", "1"], "[]");
    t.assert_text(&["JSON.NUMINCRBY", "json", "$.b[*]", "1"], "[2]");
    t.assert_text(&["JSON.NUMINCRBY", "json", "$.c[*]", "1"], "[2,3]");
    t.assert_text(&["JSON.NUMINCRBY", "json", "$.d[*]", "1"], "[2,3,4]");
    t.assert_text(&["JSON.NUMINCRBY", "json", "$.d[2]", "1"], "[5]");
    t.assert_text(
        &["JSON.GET", "json"],
        "{\"a\":[],\"b\":[2],\"c\":[2,3],\"d\":[2,3,5]}",
    );

    let json = r#"
    {"a":{}, "b":{"a":1}, "c":{"a":1, "b":2}, "d":{"a":1, "b":2, "c":3}}
  "#;
    t.assert_text(&["JSON.SET", "json", ".", json], "OK");

    t.assert_text(&["JSON.NUMINCRBY", "json", "$.a.*", "1"], "[]");
    t.assert_text(&["JSON.NUMINCRBY", "json", "$.b.*", "1"], "[2]");
    t.assert_text(&["JSON.NUMINCRBY", "json", "$.c.*", "1"], "[2,3]");
    t.assert_text(&["JSON.NUMINCRBY", "json", "$.d.*", "1"], "[2,3,4]");
    t.assert_text(
        &["JSON.GET", "json"],
        "{\"a\":{},\"b\":{\"a\":2},\"c\":{\"a\":2,\"b\":3},\"d\":{\"a\":2,\"b\":3,\"c\":4}}",
    );

    let json = r#"
    {"a":{"a":"a"}, "b":{"a":"a", "b":1}, "c":{"a":"a", "b":"b"}, "d":{"a":1, "b":"b", "c":3}}
  "#;
    t.assert_text(&["JSON.SET", "json", ".", json], "OK");

    t.assert_text(&["JSON.NUMINCRBY", "json", "$.a.*", "1"], "[null]");
    t.assert_text(&["JSON.NUMINCRBY", "json", "$.b.*", "1"], "[null,2]");
    t.assert_text(&["JSON.NUMINCRBY", "json", "$.c.*", "1"], "[null,null]");
    t.assert_text(&["JSON.NUMINCRBY", "json", "$.d.*", "1"], "[2,null,4]");
    t.assert_text(&["JSON.GET", "json"], "{\"a\":{\"a\":\"a\"},\"b\":{\"a\":\"a\",\"b\":2},\"c\":{\"a\":\"a\",\"b\":\"b\"},\"d\":{\"a\":2,\"b\":\"b\",\"c\":4}}");
}

#[test]
fn num_incr_by_legacy() {
    let mut t = Ctx::new();
    let json = r#"
    {"e":1.5,"a":1}
  "#;
    t.assert_text(&["JSON.SET", "json", ".", json], "OK");

    t.assert_text(&["JSON.NUMINCRBY", "json", ".a", "1.1"], "2.1");
    t.assert_text(&["JSON.NUMINCRBY", "json", ".e", "1"], "2.5");
    t.assert_err(
        &["JSON.NUMINCRBY", "json", ".e", "inf"],
        "ERR result is not a number",
    );

    t.assert_text(&["JSON.SET", "json", ".", json], "OK");
    t.assert_text(&["JSON.NUMINCRBY", "json", ".e", "1.7e308"], "1.7e+308");
    t.assert_err(
        &["JSON.NUMINCRBY", "json", ".e", "1.7e308"],
        "ERR result is not a number",
    );
    t.assert_text(&["JSON.GET", "json", "$.*"], "[1,1.7e+308]");

    let json = r#"
    {"a":[], "b":[1], "c":[1,2], "d":[1,2,3]}
  "#;
    t.assert_text(&["JSON.SET", "json", ".", json], "OK");

    t.assert_text(&["JSON.NUMINCRBY", "json", ".d[*]", "10"], "13");
    t.assert_text(&["JSON.GET", "json", "$.d[*]"], "[11,12,13]");

    t.assert_text(&["JSON.SET", "json", ".", json], "OK");
    t.assert_err(
        &["JSON.NUMINCRBY", "json", ".a[*]", "1"],
        "wrong JSON type of path value",
    );
    t.assert_text(&["JSON.NUMINCRBY", "json", ".b[*]", "1"], "2");
    t.assert_text(&["JSON.NUMINCRBY", "json", ".c[*]", "1"], "3");
    t.assert_text(&["JSON.NUMINCRBY", "json", ".d[*]", "1"], "4");
    t.assert_text(&["JSON.NUMINCRBY", "json", ".d[2]", "1"], "5");
    t.assert_text(
        &["JSON.GET", "json"],
        "{\"a\":[],\"b\":[2],\"c\":[2,3],\"d\":[2,3,5]}",
    );

    let json = r#"
    {"a":{}, "b":{"a":1}, "c":{"a":1, "b":2}, "d":{"a":1, "b":2, "c":3}}
  "#;
    t.assert_text(&["JSON.SET", "json", ".", json], "OK");

    t.assert_err(
        &["JSON.NUMINCRBY", "json", ".a.*", "1"],
        "wrong JSON type of path value",
    );
    t.assert_text(&["JSON.NUMINCRBY", "json", ".b.*", "1"], "2");
    t.assert_text(&["JSON.NUMINCRBY", "json", ".c.*", "1"], "3");
    t.assert_text(&["JSON.NUMINCRBY", "json", ".d.*", "1"], "4");
    t.assert_text(
        &["JSON.GET", "json"],
        "{\"a\":{},\"b\":{\"a\":2},\"c\":{\"a\":2,\"b\":3},\"d\":{\"a\":2,\"b\":3,\"c\":4}}",
    );

    let json = r#"
    {"a":{"a":"a"}, "b":{"a":"a", "b":1}, "c":{"a":"a", "b":"b"}, "d":{"a":1, "b":"b", "c":3}}
  "#;
    t.assert_text(&["JSON.SET", "json", ".", json], "OK");

    t.assert_err(
        &["JSON.NUMINCRBY", "json", ".a.*", "1"],
        "wrong JSON type of path value",
    );
    t.assert_text(&["JSON.NUMINCRBY", "json", ".b.*", "1"], "2");
    t.assert_err(
        &["JSON.NUMINCRBY", "json", ".c.*", "1"],
        "wrong JSON type of path value",
    );
    t.assert_text(&["JSON.NUMINCRBY", "json", ".d.*", "1"], "4");
    t.assert_text(&["JSON.GET", "json"], "{\"a\":{\"a\":\"a\"},\"b\":{\"a\":\"a\",\"b\":2},\"c\":{\"a\":\"a\",\"b\":\"b\"},\"d\":{\"a\":2,\"b\":\"b\",\"c\":4}}");
}

#[test]
fn num_mult_by() {
    let mut t = Ctx::new();
    let json = r#"
    {"a":[], "b":[1], "c":[1,2], "d":[1,2,3]}
  "#;
    t.assert_text(&["JSON.SET", "json", ".", json], "OK");

    t.assert_text(&["JSON.NUMMULTBY", "json", "$.d[*]", "2"], "[2,4,6]");
    t.assert_text(&["JSON.GET", "json", "$.d[*]"], "[2,4,6]");

    t.assert_text(&["JSON.SET", "json", ".", json], "OK");
    t.assert_text(&["JSON.NUMMULTBY", "json", "$.a[*]", "2"], "[]");
    t.assert_text(&["JSON.NUMMULTBY", "json", "$.b[*]", "2"], "[2]");
    t.assert_text(&["JSON.NUMMULTBY", "json", "$.c[*]", "2"], "[2,4]");
    t.assert_text(&["JSON.NUMMULTBY", "json", "$.d[*]", "2"], "[2,4,6]");
    t.assert_text(
        &["JSON.GET", "json"],
        "{\"a\":[],\"b\":[2],\"c\":[2,4],\"d\":[2,4,6]}",
    );

    let json = r#"
    {"a":{}, "b":{"a":1}, "c":{"a":1, "b":2}, "d":{"a":1, "b":2, "c":3}}
  "#;
    t.assert_text(&["JSON.SET", "json", ".", json], "OK");

    t.assert_text(&["JSON.NUMMULTBY", "json", "$.a.*", "2"], "[]");
    t.assert_text(&["JSON.NUMMULTBY", "json", "$.b.*", "2"], "[2]");
    t.assert_text(&["JSON.NUMMULTBY", "json", "$.c.*", "2"], "[2,4]");
    t.assert_text(&["JSON.NUMMULTBY", "json", "$.d.*", "2"], "[2,4,6]");
    t.assert_text(
        &["JSON.GET", "json"],
        "{\"a\":{},\"b\":{\"a\":2},\"c\":{\"a\":2,\"b\":4},\"d\":{\"a\":2,\"b\":4,\"c\":6}}",
    );

    let json = r#"
    {"a":{"a":"a"}, "b":{"a":"a", "b":1}, "c":{"a":"a", "b":"b"}, "d":{"a":1, "b":"b", "c":3}}
  "#;
    t.assert_text(&["JSON.SET", "json", ".", json], "OK");

    t.assert_text(&["JSON.NUMMULTBY", "json", "$.a.*", "2"], "[null]");
    t.assert_text(&["JSON.NUMMULTBY", "json", "$.b.*", "2"], "[null,2]");
    t.assert_text(&["JSON.NUMMULTBY", "json", "$.c.*", "2"], "[null,null]");
    t.assert_text(&["JSON.NUMMULTBY", "json", "$.d.*", "2"], "[2,null,6]");
    t.assert_text(&["JSON.GET", "json"], "{\"a\":{\"a\":\"a\"},\"b\":{\"a\":\"a\",\"b\":2},\"c\":{\"a\":\"a\",\"b\":\"b\"},\"d\":{\"a\":2,\"b\":\"b\",\"c\":6}}");
}

#[test]
fn num_mult_by_legacy() {
    let mut t = Ctx::new();
    let json = r#"
    {"a":[], "b":[1], "c":[1,2], "d":[1,2,3]}
  "#;
    t.assert_text(&["JSON.SET", "json", ".", json], "OK");

    t.assert_text(&["JSON.NUMMULTBY", "json", ".d[*]", "2"], "6");
    t.assert_text(&["JSON.GET", "json", "$.d[*]"], "[2,4,6]");

    t.assert_text(&["JSON.SET", "json", ".", json], "OK");
    t.assert_err(
        &["JSON.NUMMULTBY", "json", ".a[*]", "2"],
        "wrong JSON type of path value",
    );
    t.assert_text(&["JSON.NUMMULTBY", "json", ".b[*]", "2"], "2");
    t.assert_text(&["JSON.NUMMULTBY", "json", ".c[*]", "2"], "4");
    t.assert_text(&["JSON.NUMMULTBY", "json", ".d[*]", "2"], "6");
    t.assert_text(
        &["JSON.GET", "json"],
        "{\"a\":[],\"b\":[2],\"c\":[2,4],\"d\":[2,4,6]}",
    );

    let json = r#"
    {"a":{}, "b":{"a":1}, "c":{"a":1, "b":2}, "d":{"a":1, "b":2, "c":3}}
  "#;
    t.assert_text(&["JSON.SET", "json", ".", json], "OK");

    t.assert_err(
        &["JSON.NUMMULTBY", "json", ".a.*", "2"],
        "wrong JSON type of path value",
    );
    t.assert_text(&["JSON.NUMMULTBY", "json", ".b.*", "2"], "2");
    t.assert_text(&["JSON.NUMMULTBY", "json", ".c.*", "2"], "4");
    t.assert_text(&["JSON.NUMMULTBY", "json", ".d.*", "2"], "6");
    t.assert_text(
        &["JSON.GET", "json"],
        "{\"a\":{},\"b\":{\"a\":2},\"c\":{\"a\":2,\"b\":4},\"d\":{\"a\":2,\"b\":4,\"c\":6}}",
    );

    let json = r#"
    {"a":{"a":"a"}, "b":{"a":"a", "b":1}, "c":{"a":"a", "b":"b"}, "d":{"a":1, "b":"b", "c":3}}
  "#;
    t.assert_text(&["JSON.SET", "json", ".", json], "OK");

    t.assert_err(
        &["JSON.NUMMULTBY", "json", ".a.*", "2"],
        "wrong JSON type of path value",
    );
    t.assert_text(&["JSON.NUMMULTBY", "json", ".b.*", "2"], "2");
    t.assert_err(
        &["JSON.NUMMULTBY", "json", ".c.*", "2"],
        "wrong JSON type of path value",
    );
    t.assert_text(&["JSON.NUMMULTBY", "json", ".d.*", "2"], "6");
    t.assert_text(&["JSON.GET", "json"], "{\"a\":{\"a\":\"a\"},\"b\":{\"a\":\"a\",\"b\":2},\"c\":{\"a\":\"a\",\"b\":\"b\"},\"d\":{\"a\":2,\"b\":\"b\",\"c\":6}}");
}

#[test]
fn numeric_operations_with_conversions() {
    let mut t = Ctx::new();
    t.assert_text(&["JSON.SET", "json", ".", r#"{"a":2.0}"#], "OK");

    t.assert_text(&["JSON.NUMINCRBY", "json", "$.a", "1"], "[3.0]");
    t.assert_text(&["JSON.NUMINCRBY", "json", "$.a", "1.0"], "[4.0]");
    t.assert_text(&["JSON.NUMMULTBY", "json", "$.a", "2"], "[8.0]");
    t.assert_text(&["JSON.NUMMULTBY", "json", "$.a", "2.0"], "[16.0]");
    t.assert_text(&["JSON.GET", "json"], r#"{"a":16.0}"#);

    t.assert_text(&["JSON.SET", "json", ".", r#"{"a":2}"#], "OK");
    t.assert_text(&["JSON.NUMINCRBY", "json", "$.a", "1"], "[3]");
    t.assert_text(&["JSON.GET", "json"], r#"{"a":3}"#);
    t.assert_text(&["JSON.NUMINCRBY", "json", "$.a", "1.0"], "[4.0]");
    t.assert_text(&["JSON.GET", "json"], r#"{"a":4.0}"#);

    t.assert_text(&["JSON.SET", "json", ".", r#"{"a":2}"#], "OK");
    t.assert_text(&["JSON.NUMMULTBY", "json", "$.a", "2"], "[4]");
    t.assert_text(&["JSON.GET", "json"], r#"{"a":4}"#);
    t.assert_text(&["JSON.NUMMULTBY", "json", "$.a", "2.0"], "[8.0]");
    t.assert_text(&["JSON.GET", "json"], r#"{"a":8.0}"#);
}

#[test]
fn numeric_operations_with_conversions_legacy() {
    let mut t = Ctx::new();
    t.assert_text(&["JSON.SET", "json", ".", r#"{"a":2.0}"#], "OK");

    t.assert_text(&["JSON.NUMINCRBY", "json", ".a", "1"], "3.0");
    t.assert_text(&["JSON.NUMINCRBY", "json", ".a", "1.0"], "4.0");
    t.assert_text(&["JSON.NUMMULTBY", "json", ".a", "2"], "8.0");
    t.assert_text(&["JSON.NUMMULTBY", "json", ".a", "2.0"], "16.0");
    t.assert_text(&["JSON.GET", "json"], r#"{"a":16.0}"#);

    t.assert_text(&["JSON.SET", "json", ".", r#"{"a":2}"#], "OK");
    t.assert_text(&["JSON.NUMINCRBY", "json", ".a", "1"], "3");
    t.assert_text(&["JSON.GET", "json"], r#"{"a":3}"#);
    t.assert_text(&["JSON.NUMINCRBY", "json", ".a", "1.0"], "4.0");
    t.assert_text(&["JSON.GET", "json"], r#"{"a":4.0}"#);

    t.assert_text(&["JSON.SET", "json", ".", r#"{"a":2}"#], "OK");
    t.assert_text(&["JSON.NUMMULTBY", "json", ".a", "2"], "4");
    t.assert_text(&["JSON.GET", "json"], r#"{"a":4}"#);
    t.assert_text(&["JSON.NUMMULTBY", "json", ".a", "2.0"], "8.0");
    t.assert_text(&["JSON.GET", "json"], r#"{"a":8.0}"#);
}

#[test]
fn numeric_operations_resp2() {
    let mut t = Ctx::new();
    // RESP2 behavior (the port is RESP2-only): NUMINCRBY/NUMMULTBY return the
    // serialized array as a bulk string.
    t.assert_text(&["JSON.SET", "a", "$", "1"], "OK");
    t.assert_text(&["JSON.NUMINCRBY", "a", "$", "1"], "[2]");
    let v = t.arr(&["JSON.TYPE", "a", "$"]);
    assert_eq!(v.len(), 1);
    assert_eq!(v[0].text().as_deref(), Some("integer"));
    t.assert_text(&["JSON.TYPE", "a", "."], "integer");
    t.assert_text(&["JSON.NUMMULTBY", "a", "$", "2"], "[4]");
}

#[test]
fn del() {
    let mut t = Ctx::new();
    let json = r#"
    {"a":{}, "b":{"a":1}, "c":{"a":1, "b":2}, "d":{"a":1, "b":2, "c":3}, "e": [1,2,3,4,5]}
  "#;
    t.assert_text(&["JSON.SET", "json", ".", json], "OK");

    t.assert_int(&["JSON.DEL", "json", "$.d.*"], 3);
    t.assert_text(
        &["JSON.GET", "json"],
        "{\"a\":{},\"b\":{\"a\":1},\"c\":{\"a\":1,\"b\":2},\"d\":{},\"e\":[1,2,3,4,5]}",
    );

    t.assert_int(&["JSON.DEL", "json", "$.e[*]"], 5);
    t.assert_text(
        &["JSON.GET", "json"],
        "{\"a\":{},\"b\":{\"a\":1},\"c\":{\"a\":1,\"b\":2},\"d\":{},\"e\":[]}",
    );

    // The reference deletes 8 (its jsoncons path double-counts); redis-stack
    // returns 5. Assert only the lower bound, like the C++ suite does.
    assert!(t.int(&["JSON.DEL", "json", "$..*"]) >= 5);
    t.assert_text(&["JSON.GET", "json"], "{}");

    t.assert_int(&["JSON.DEL", "json"], 1);
    t.assert_null(&["GET", "json"]);
    t.assert_null(&["JSON.GET", "json"]);

    let json = r#"
    {"a":[{"b": [1,2,3]}], "b": [{"c": 2}], "c']":[1,2,3]}
  "#;
    t.assert_text(&["JSON.SET", "json", ".", json], "OK");

    t.assert_int(&["JSON.DEL", "json", "$.a[0].b[0]"], 1);
    t.assert_err(&["GET", "json"], "wrong kind of value");
    t.assert_text(
        &["JSON.GET", "json"],
        "{\"a\":[{\"b\":[2,3]}],\"b\":[{\"c\":2}],\"c']\":[1,2,3]}",
    );

    t.assert_int(&["JSON.DEL", "json", "$.b[0].c"], 1);
    t.assert_text(
        &["JSON.GET", "json"],
        "{\"a\":[{\"b\":[2,3]}],\"b\":[{}],\"c']\":[1,2,3]}",
    );

    t.assert_int(&["JSON.DEL", "json", "$.*"], 3);
    t.assert_text(&["JSON.GET", "json"], "{}");

    t.assert_text(&["JSON.SET", "json", "$", r#"{"a": 1}"#], "OK");
    t.assert_int(&["JSON.DEL", "json", "$"], 1);
    t.assert_null(&["JSON.GET", "json"]);

    // Recursive delete with $..a: removes the key "a" at root level but not
    // the string values "a" inside arrays.
    t.assert_text(
        &[
            "JSON.SET",
            "doc2",
            "$",
            r#"{"a": {"a": 2, "b": 3}, "b": ["a", "b"], "nested": {"b": [true, "a", "b"]}}"#,
        ],
        "OK",
    );
    t.assert_text(
        &["JSON.GET", "doc2"],
        "{\"a\":{\"a\":2,\"b\":3},\"b\":[\"a\",\"b\"],\"nested\":{\"b\":[true,\"a\",\"b\"]}}",
    );
    t.assert_int(&["JSON.DEL", "doc2", "$..a"], 1);
    t.assert_text(
        &["JSON.GET", "doc2"],
        "{\"b\":[\"a\",\"b\"],\"nested\":{\"b\":[true,\"a\",\"b\"]}}",
    );
}

#[test]
fn del_legacy() {
    let mut t = Ctx::new();
    let json = r#"
    {"a":{}, "b":{"a":1}, "c":{"a":1, "b":2}, "d":{"a":1, "b":2, "c":3}, "e": [1,2,3,4,5]}
  "#;
    t.assert_text(&["JSON.SET", "json", ".", json], "OK");

    t.assert_int(&["JSON.DEL", "json", ".d.*"], 3);
    t.assert_text(
        &["JSON.GET", "json"],
        "{\"a\":{},\"b\":{\"a\":1},\"c\":{\"a\":1,\"b\":2},\"d\":{},\"e\":[1,2,3,4,5]}",
    );

    t.assert_int(&["JSON.DEL", "json", ".e[*]"], 5);
    t.assert_text(
        &["JSON.GET", "json"],
        "{\"a\":{},\"b\":{\"a\":1},\"c\":{\"a\":1,\"b\":2},\"d\":{},\"e\":[]}",
    );

    assert!(t.int(&["JSON.DEL", "json", "..*"]) >= 5);
    t.assert_text(&["JSON.GET", "json"], "{}");

    t.assert_int(&["JSON.DEL", "json"], 1);
    t.assert_null(&["GET", "json"]);
    t.assert_null(&["JSON.GET", "json"]);

    let json = r#"
    {"a":[{"b": [1,2,3]}], "b": [{"c": 2}], "c']":[1,2,3]}
  "#;
    t.assert_text(&["JSON.SET", "json", ".", json], "OK");

    t.assert_int(&["JSON.DEL", "json", ".a[0].b[0]"], 1);
    t.assert_err(&["GET", "json"], "wrong kind of value");
    t.assert_text(
        &["JSON.GET", "json"],
        "{\"a\":[{\"b\":[2,3]}],\"b\":[{\"c\":2}],\"c']\":[1,2,3]}",
    );

    t.assert_int(&["JSON.DEL", "json", ".b[0].c"], 1);
    t.assert_text(
        &["JSON.GET", "json"],
        "{\"a\":[{\"b\":[2,3]}],\"b\":[{}],\"c']\":[1,2,3]}",
    );

    t.assert_int(&["JSON.DEL", "json", ".*"], 3);
    t.assert_text(&["JSON.GET", "json"], "{}");

    t.assert_text(&["JSON.SET", "json", ".", r#"{"a": 1}"#], "OK");
    t.assert_int(&["JSON.DEL", "json", "."], 1);
    t.assert_null(&["JSON.GET", "json"]);

    t.assert_text(&["JSON.SET", "json", ".", json], "OK");
    t.assert_int(&["JSON.DEL", "json"], 1);
    t.assert_null(&["JSON.GET", "json"]);
}

#[test]
fn obj_keys() {
    let mut t = Ctx::new();
    let json = r#"
    {"a":{}, "b":{"a":"a"}, "c":{"a":"a", "b":"bb"}, "d":{"a":1, "b":"b", "c":{"a":3,"b":4}}, "e":1}
  "#;
    t.assert_text(&["JSON.SET", "json", ".", json], "OK");

    let mut v = t.arr(&["JSON.OBJKEYS", "json", "$"]);
    assert_eq!(v.len(), 1);
    let root = v[0].arr().expect("root key array");
    assert_eq!(
        root,
        &[
            Value::Bulk(Some(b"a".to_vec())),
            Value::Bulk(Some(b"b".to_vec())),
            Value::Bulk(Some(b"c".to_vec())),
            Value::Bulk(Some(b"d".to_vec())),
            Value::Bulk(Some(b"e".to_vec()))
        ]
    );

    v = t.arr(&["JSON.OBJKEYS", "json", "$.a"]);
    assert_eq!(v.len(), 1);
    assert_eq!(v[0].arr().expect("a key array").len(), 0);

    v = t.arr(&["JSON.OBJKEYS", "json", "$.b"]);
    assert_eq!(v.len(), 1);
    assert_eq!(v[0].arr().unwrap().len(), 1);

    v = t.arr(&["JSON.OBJKEYS", "json", "$.*"]);
    assert_eq!(v.len(), 5);
    assert_eq!(v[0].arr().unwrap().len(), 0);
    assert_eq!(v[1].arr().unwrap().len(), 1);
    assert_eq!(v[2].arr().unwrap().len(), 2);
    assert_eq!(v[3].arr().unwrap().len(), 3);
    assert_eq!(v[4].arr().unwrap().len(), 0);

    assert!(t.arr(&["JSON.OBJKEYS", "json", "$.notfound"]).is_empty());

    let json = r#"
     {"a":[7], "inner": {"a": {"b": 2, "c": 1337}}}
   "#;
    t.assert_text(&["JSON.SET", "json", ".", json], "OK");

    v = t.arr(&["JSON.OBJKEYS", "json", "$..a"]);
    assert_eq!(v.len(), 2);
    assert_eq!(v[0].arr().unwrap().len(), 0);
    assert_eq!(v[1].arr().unwrap().len(), 2);

    let json = r#"
     {"a":{}, "b":{"c":{"d": {"e": 1337}}}}
   "#;
    t.assert_text(&["JSON.SET", "json", ".", json], "OK");

    v = t.arr(&["JSON.OBJKEYS", "json", "$..*"]);
    assert_eq!(v.len(), 5);
    assert_eq!(v[0].arr().unwrap().len(), 0);
    assert_eq!(v[1].arr().unwrap().len(), 1);
    assert_eq!(v[2].arr().unwrap().len(), 1);
    assert_eq!(v[3].arr().unwrap().len(), 1);
    assert_eq!(v[4].arr().unwrap().len(), 0);
}

#[test]
fn obj_keys_legacy() {
    let mut t = Ctx::new();
    let json = r#"
    {"a":{}, "b":{"a":"a"}, "c":{"a":"a", "b":"bb"}, "d":{"a":1, "b":"b", "c":{"a":3,"b":4}}, "e":1}
  "#;
    t.assert_text(&["JSON.SET", "json", ".", json], "OK");

    let v = t.arr(&["JSON.OBJKEYS", "json"]);
    assert_eq!(v.len(), 5);
    let v = t.arr(&["JSON.OBJKEYS", "json", "."]);
    assert_eq!(v.len(), 5);
    let v = t.arr(&["JSON.OBJKEYS", "json", ".a"]);
    assert_eq!(v.len(), 0);
    let v = t.arr(&["JSON.OBJKEYS", "json", ".b"]);
    assert_eq!(v.len(), 1);
    let v = t.arr(&["JSON.OBJKEYS", "json", ".*"]);
    assert_eq!(v.len(), 0);
    t.assert_null(&["JSON.OBJKEYS", "json", ".notfound"]);

    let json = r#"
     {"a":[7], "inner": {"a": {"b": 2, "c": 1337}}}
   "#;
    t.assert_text(&["JSON.SET", "json", ".", json], "OK");
    let v = t.arr(&["JSON.OBJKEYS", "json", "..a"]);
    assert_eq!(v.len(), 0);

    let json = r#"
     {"a":{}, "b":{"c":{"d": {"e": 1337}}}}
   "#;
    t.assert_text(&["JSON.SET", "json", ".", json], "OK");
    let v = t.arr(&["JSON.OBJKEYS", "json", "..*"]);
    assert_eq!(v.len(), 0);
}

#[test]
fn strappend_default_path() {
    let mut t = Ctx::new();
    t.assert_text(&["JSON.SET", "json", ".", "\"foo\""], "OK");

    t.assert_int(&["JSON.STRAPPEND", "json", "\"bar\""], 6);
    t.assert_err(
        &["JSON.STRAPPEND", "json", ".", "\"baz\"", "extra"],
        "syntax error",
    );
    t.assert_text(&["JSON.GET", "json"], "\"foobar\"");
}

#[test]
fn strappend() {
    let mut t = Ctx::new();
    let json = r#"
    {"a":{"a":"a"}, "b":{"a":"a", "b":1}, "c":{"a":"a", "b":"bb"}, "d":{"a":1, "b":"b", "c":3}}
  "#;
    t.assert_text(&["JSON.SET", "json", ".", json], "OK");

    let mut v = t.arr(&["JSON.STRAPPEND", "json", "$.a.a", "\"ab\""]);
    assert_eq!(v.len(), 1);
    assert_eq!(v[0].int(), Some(3));
    t.assert_text(&["JSON.GET", "json"], "{\"a\":{\"a\":\"aab\"},\"b\":{\"a\":\"a\",\"b\":1},\"c\":{\"a\":\"a\",\"b\":\"bb\"},\"d\":{\"a\":1,\"b\":\"b\",\"c\":3}}");

    v = t.arr(&["JSON.STRAPPEND", "json", "$.a.*", "\"a\""]);
    assert_eq!(v.len(), 1);
    assert_eq!(v[0].int(), Some(4));
    t.assert_text(&["JSON.GET", "json"], "{\"a\":{\"a\":\"aaba\"},\"b\":{\"a\":\"a\",\"b\":1},\"c\":{\"a\":\"a\",\"b\":\"bb\"},\"d\":{\"a\":1,\"b\":\"b\",\"c\":3}}");

    v = t.arr(&["JSON.STRAPPEND", "json", "$.c.b", "\"a\""]);
    assert_eq!(v.len(), 1);
    assert_eq!(v[0].int(), Some(3));
    t.assert_text(&["JSON.GET", "json"], "{\"a\":{\"a\":\"aaba\"},\"b\":{\"a\":\"a\",\"b\":1},\"c\":{\"a\":\"a\",\"b\":\"bba\"},\"d\":{\"a\":1,\"b\":\"b\",\"c\":3}}");

    v = t.arr(&["JSON.STRAPPEND", "json", "$.b.*", "\"a\""]);
    assert_eq!(v.len(), 2);
    assert_eq!(v[0].int(), Some(2));
    assert!(matches!(v[1], Value::Bulk(None)));
    t.assert_text(&["JSON.GET", "json"], "{\"a\":{\"a\":\"aaba\"},\"b\":{\"a\":\"aa\",\"b\":1},\"c\":{\"a\":\"a\",\"b\":\"bba\"},\"d\":{\"a\":1,\"b\":\"b\",\"c\":3}}");

    v = t.arr(&["JSON.STRAPPEND", "json", "$.c.*", "\"a\""]);
    assert_eq!(v.len(), 2);
    assert_eq!(v[0].int(), Some(2));
    assert_eq!(v[1].int(), Some(4));
    t.assert_text(&["JSON.GET", "json"], "{\"a\":{\"a\":\"aaba\"},\"b\":{\"a\":\"aa\",\"b\":1},\"c\":{\"a\":\"aa\",\"b\":\"bbaa\"},\"d\":{\"a\":1,\"b\":\"b\",\"c\":3}}");

    v = t.arr(&["JSON.STRAPPEND", "json", "$.d.*", "\"a\""]);
    assert_eq!(v.len(), 3);
    assert!(matches!(v[0], Value::Bulk(None)));
    assert_eq!(v[1].int(), Some(2));
    assert!(matches!(v[2], Value::Bulk(None)));
    t.assert_text(&["JSON.GET", "json"], "{\"a\":{\"a\":\"aaba\"},\"b\":{\"a\":\"aa\",\"b\":1},\"c\":{\"a\":\"aa\",\"b\":\"bbaa\"},\"d\":{\"a\":1,\"b\":\"ba\",\"c\":3}}");

    let json = r#"
    {"a":{"a":"a", "b":"aa", "c":"aaa"}, "b":{"a":"aaa", "b":"aa", "c":"a"}}
  "#;
    t.assert_text(&["JSON.SET", "json", ".", json], "OK");

    v = t.arr(&["JSON.STRAPPEND", "json", "$.a.*", "\"a\""]);
    assert_eq!(v.len(), 3);
    assert_eq!(v[0].int(), Some(2));
    assert_eq!(v[1].int(), Some(3));
    assert_eq!(v[2].int(), Some(4));
    t.assert_text(&["JSON.GET", "json"], "{\"a\":{\"a\":\"aa\",\"b\":\"aaa\",\"c\":\"aaaa\"},\"b\":{\"a\":\"aaa\",\"b\":\"aa\",\"c\":\"a\"}}");

    v = t.arr(&["JSON.STRAPPEND", "json", "$.b.*", "\"a\""]);
    assert_eq!(v.len(), 3);
    assert_eq!(v[0].int(), Some(4));
    assert_eq!(v[1].int(), Some(3));
    assert_eq!(v[2].int(), Some(2));
    t.assert_text(&["JSON.GET", "json"], "{\"a\":{\"a\":\"aa\",\"b\":\"aaa\",\"c\":\"aaaa\"},\"b\":{\"a\":\"aaaa\",\"b\":\"aaa\",\"c\":\"aa\"}}");

    let json = r#"
    {"a":{"a":"a", "b":"aa", "c":["aaaaa", "aaaaa"]}, "b":{"a":"aaa", "b":["aaaaa", "aaaaa"], "c":"a"}}
  "#;
    t.assert_text(&["JSON.SET", "json", ".", json], "OK");

    v = t.arr(&["JSON.STRAPPEND", "json", "$.a.*", "\"a\""]);
    assert_eq!(v.len(), 3);
    assert_eq!(v[0].int(), Some(2));
    assert_eq!(v[1].int(), Some(3));
    assert!(matches!(v[2], Value::Bulk(None)));
    t.assert_text(&["JSON.GET", "json"], "{\"a\":{\"a\":\"aa\",\"b\":\"aaa\",\"c\":[\"aaaaa\",\"aaaaa\"]},\"b\":{\"a\":\"aaa\",\"b\":[\"aaaaa\",\"aaaaa\"],\"c\":\"a\"}}");

    v = t.arr(&["JSON.STRAPPEND", "json", "$.b.*", "\"a\""]);
    assert_eq!(v.len(), 3);
    assert_eq!(v[0].int(), Some(4));
    assert!(matches!(v[1], Value::Bulk(None)));
    assert_eq!(v[2].int(), Some(2));
    t.assert_text(&["JSON.GET", "json"], "{\"a\":{\"a\":\"aa\",\"b\":\"aaa\",\"c\":[\"aaaaa\",\"aaaaa\"]},\"b\":{\"a\":\"aaaa\",\"b\":[\"aaaaa\",\"aaaaa\"],\"c\":\"aa\"}}");

    let json = r#"
    {"a":{"a":"a", "b":"aa", "c":{"c": "aaaaa"}}, "b":{"a":"aaa", "b":{"b": "aaaaa"}, "c":"a"}}
  "#;
    t.assert_text(&["JSON.SET", "json", ".", json], "OK");

    v = t.arr(&["JSON.STRAPPEND", "json", "$.a.*", "\"a\""]);
    assert_eq!(v.len(), 3);
    assert_eq!(v[0].int(), Some(2));
    assert_eq!(v[1].int(), Some(3));
    assert!(matches!(v[2], Value::Bulk(None)));
    t.assert_text(&["JSON.GET", "json"], "{\"a\":{\"a\":\"aa\",\"b\":\"aaa\",\"c\":{\"c\":\"aaaaa\"}},\"b\":{\"a\":\"aaa\",\"b\":{\"b\":\"aaaaa\"},\"c\":\"a\"}}");

    v = t.arr(&["JSON.STRAPPEND", "json", "$.b.*", "\"a\""]);
    assert_eq!(v.len(), 3);
    assert_eq!(v[0].int(), Some(4));
    assert!(matches!(v[1], Value::Bulk(None)));
    assert_eq!(v[2].int(), Some(2));
    t.assert_text(&["JSON.GET", "json"], "{\"a\":{\"a\":\"aa\",\"b\":\"aaa\",\"c\":{\"c\":\"aaaaa\"}},\"b\":{\"a\":\"aaaa\",\"b\":{\"b\":\"aaaaa\"},\"c\":\"aa\"}}");

    let json = r#"
    {"a":"foo", "inner": {"a": "bye"}, "inner1": {"a": 7}}
  "#;
    t.assert_text(&["JSON.SET", "json", ".", json], "OK");

    v = t.arr(&["JSON.STRAPPEND", "json", "$..a", "\"bar\""]);
    assert_eq!(v.len(), 3);
    assert_eq!(v[0].int(), Some(6));
    assert_eq!(v[1].int(), Some(6));
    assert!(matches!(v[2], Value::Bulk(None)));
    t.assert_text(
        &["JSON.GET", "json"],
        "{\"a\":\"foobar\",\"inner\":{\"a\":\"byebar\"},\"inner1\":{\"a\":7}}",
    );
}

#[test]
fn strappend_legacy_mode() {
    let mut t = Ctx::new();
    let json = r#"
    {"a":{"a":"a"}, "b":{"a":"a", "b":1}, "c":{"a":"a", "b":"bb"}, "d":{"a":1, "b":"b", "c":3}}
  "#;
    t.assert_text(&["JSON.SET", "json", ".", json], "OK");

    t.assert_int(&["JSON.STRAPPEND", "json", ".a.a", "\"ab\""], 3);
    t.assert_text(&["JSON.GET", "json"], "{\"a\":{\"a\":\"aab\"},\"b\":{\"a\":\"a\",\"b\":1},\"c\":{\"a\":\"a\",\"b\":\"bb\"},\"d\":{\"a\":1,\"b\":\"b\",\"c\":3}}");

    t.assert_int(&["JSON.STRAPPEND", "json", ".a.*", "\"a\""], 4);
    t.assert_text(&["JSON.GET", "json"], "{\"a\":{\"a\":\"aaba\"},\"b\":{\"a\":\"a\",\"b\":1},\"c\":{\"a\":\"a\",\"b\":\"bb\"},\"d\":{\"a\":1,\"b\":\"b\",\"c\":3}}");

    t.assert_int(&["JSON.STRAPPEND", "json", ".c.b", "\"a\""], 3);
    t.assert_text(&["JSON.GET", "json"], "{\"a\":{\"a\":\"aaba\"},\"b\":{\"a\":\"a\",\"b\":1},\"c\":{\"a\":\"a\",\"b\":\"bba\"},\"d\":{\"a\":1,\"b\":\"b\",\"c\":3}}");

    t.assert_int(&["JSON.STRAPPEND", "json", ".b.*", "\"a\""], 2);
    t.assert_text(&["JSON.GET", "json"], "{\"a\":{\"a\":\"aaba\"},\"b\":{\"a\":\"aa\",\"b\":1},\"c\":{\"a\":\"a\",\"b\":\"bba\"},\"d\":{\"a\":1,\"b\":\"b\",\"c\":3}}");

    t.assert_int(&["JSON.STRAPPEND", "json", ".c.*", "\"a\""], 4);
    t.assert_text(&["JSON.GET", "json"], "{\"a\":{\"a\":\"aaba\"},\"b\":{\"a\":\"aa\",\"b\":1},\"c\":{\"a\":\"aa\",\"b\":\"bbaa\"},\"d\":{\"a\":1,\"b\":\"b\",\"c\":3}}");

    t.assert_int(&["JSON.STRAPPEND", "json", ".d.*", "\"a\""], 2);
    t.assert_text(&["JSON.GET", "json"], "{\"a\":{\"a\":\"aaba\"},\"b\":{\"a\":\"aa\",\"b\":1},\"c\":{\"a\":\"aa\",\"b\":\"bbaa\"},\"d\":{\"a\":1,\"b\":\"ba\",\"c\":3}}");

    let json = r#"
    {"a":{"a":"a", "b":"aa", "c":"aaa"}, "b":{"a":"aaa", "b":"aa", "c":"a"}}
  "#;
    t.assert_text(&["JSON.SET", "json", ".", json], "OK");

    t.assert_int(&["JSON.STRAPPEND", "json", ".a.*", "\"a\""], 4);
    t.assert_text(&["JSON.GET", "json"], "{\"a\":{\"a\":\"aa\",\"b\":\"aaa\",\"c\":\"aaaa\"},\"b\":{\"a\":\"aaa\",\"b\":\"aa\",\"c\":\"a\"}}");

    t.assert_int(&["JSON.STRAPPEND", "json", ".b.*", "\"a\""], 2);
    t.assert_text(&["JSON.GET", "json"], "{\"a\":{\"a\":\"aa\",\"b\":\"aaa\",\"c\":\"aaaa\"},\"b\":{\"a\":\"aaaa\",\"b\":\"aaa\",\"c\":\"aa\"}}");

    let json = r#"
    {"a":{"a":"a", "b":"aa", "c":["aaaaa", "aaaaa"]}, "b":{"a":"aaa", "b":["aaaaa", "aaaaa"], "c":"a"}}
  "#;
    t.assert_text(&["JSON.SET", "json", ".", json], "OK");

    t.assert_int(&["JSON.STRAPPEND", "json", ".a.*", "\"a\""], 3);
    t.assert_text(&["JSON.GET", "json"], "{\"a\":{\"a\":\"aa\",\"b\":\"aaa\",\"c\":[\"aaaaa\",\"aaaaa\"]},\"b\":{\"a\":\"aaa\",\"b\":[\"aaaaa\",\"aaaaa\"],\"c\":\"a\"}}");

    t.assert_int(&["JSON.STRAPPEND", "json", ".b.*", "\"a\""], 2);
    t.assert_text(&["JSON.GET", "json"], "{\"a\":{\"a\":\"aa\",\"b\":\"aaa\",\"c\":[\"aaaaa\",\"aaaaa\"]},\"b\":{\"a\":\"aaaa\",\"b\":[\"aaaaa\",\"aaaaa\"],\"c\":\"aa\"}}");

    let json = r#"
    {"a":{"a":"a", "b":"aa", "c":{"c": "aaaaa"}}, "b":{"a":"aaa", "b":{"b": "aaaaa"}, "c":"a"}}
  "#;
    t.assert_text(&["JSON.SET", "json", ".", json], "OK");

    t.assert_int(&["JSON.STRAPPEND", "json", ".a.*", "\"a\""], 3);
    t.assert_text(&["JSON.GET", "json"], "{\"a\":{\"a\":\"aa\",\"b\":\"aaa\",\"c\":{\"c\":\"aaaaa\"}},\"b\":{\"a\":\"aaa\",\"b\":{\"b\":\"aaaaa\"},\"c\":\"a\"}}");

    t.assert_int(&["JSON.STRAPPEND", "json", ".b.*", "\"a\""], 2);
    t.assert_text(&["JSON.GET", "json"], "{\"a\":{\"a\":\"aa\",\"b\":\"aaa\",\"c\":{\"c\":\"aaaaa\"}},\"b\":{\"a\":\"aaaa\",\"b\":{\"b\":\"aaaaa\"},\"c\":\"aa\"}}");

    let json = r#"
    {"a":"foo", "inner": {"a": "bye"}, "inner1": {"a": 7}}
  "#;
    t.assert_text(&["JSON.SET", "json", ".", json], "OK");

    t.assert_int(&["JSON.STRAPPEND", "json", "..a", "\"bar\""], 6);
    t.assert_text(
        &["JSON.GET", "json"],
        "{\"a\":\"foobar\",\"inner\":{\"a\":\"byebar\"},\"inner1\":{\"a\":7}}",
    );
}

#[test]
fn clear() {
    let mut t = Ctx::new();
    let json = r#"
    [[], [0], [0,1], [0,1,2], 1, true, null, "d"]
  "#;
    t.assert_text(&["JSON.SET", "json", ".", json], "OK");

    t.assert_int(&["JSON.CLEAR", "json", "$[*]"], 5);
    t.assert_text(&["JSON.GET", "json"], "[[],[],[],[],0,true,null,\"d\"]");

    t.assert_int(&["JSON.CLEAR", "json", "$"], 1);
    t.assert_text(&["JSON.GET", "json"], "[]");

    let json = r#"
    {"children": ["Yossi", "Rafi", "Benni", "Avraham", "Yehoshua", "Moshe"]}
  "#;
    t.assert_text(&["JSON.SET", "json", ".", json], "OK");

    t.assert_int(&["JSON.CLEAR", "json", "$.children"], 1);
    t.assert_text(&["JSON.GET", "json"], "{\"children\":[]}");

    t.assert_int(&["JSON.CLEAR", "json", "$"], 1);
    t.assert_text(&["JSON.GET", "json"], "{}");
}

#[test]
fn clear_legacy() {
    let mut t = Ctx::new();
    let json = r#"
    [[], [0], [0,1], [0,1,2], 1, true, null, "d"]
  "#;
    t.assert_text(&["JSON.SET", "json", ".", json], "OK");

    t.assert_int(&["JSON.CLEAR", "json", "[*]"], 5);
    t.assert_text(&["JSON.GET", "json"], "[[],[],[],[],0,true,null,\"d\"]");

    t.assert_int(&["JSON.CLEAR", "json", "."], 1);
    t.assert_text(&["JSON.GET", "json"], "[]");

    let json = r#"
    {"children": ["Yossi", "Rafi", "Benni", "Avraham", "Yehoshua", "Moshe"]}
  "#;
    t.assert_text(&["JSON.SET", "json", ".", json], "OK");

    t.assert_int(&["JSON.CLEAR", "json", ".children"], 1);
    t.assert_text(&["JSON.GET", "json"], "{\"children\":[]}");

    t.assert_int(&["JSON.CLEAR", "json", "."], 1);
    t.assert_text(&["JSON.GET", "json"], "{}");

    t.assert_text(&["JSON.SET", "json", ".", json], "OK");
    t.assert_int(&["JSON.CLEAR", "json"], 1);
    t.assert_text(&["JSON.GET", "json"], "{}");
}

#[test]
fn arr_pop() {
    let mut t = Ctx::new();
    let json = r#"
    [[6,1,6], [7,2,7], [8,3,8]]
  "#;
    t.assert_text(&["JSON.SET", "json", ".", json], "OK");

    let mut v = t.arr(&["JSON.ARRPOP", "json", "$[*]", "-2"]);
    assert_eq!(v.len(), 3);
    assert_eq!(v[0].text().as_deref(), Some("1"));
    assert_eq!(v[1].text().as_deref(), Some("2"));
    assert_eq!(v[2].text().as_deref(), Some("3"));
    t.assert_text(&["JSON.GET", "json"], "[[6,6],[7,7],[8,8]]");

    let json = r#"
    [[], ["a"], ["a", "b"]]
  "#;
    t.assert_text(&["JSON.SET", "json", ".", json], "OK");

    v = t.arr(&["JSON.ARRPOP", "json", "$[*]"]);
    assert_eq!(v.len(), 3);
    assert!(matches!(v[0], Value::Bulk(None)));
    assert_eq!(v[1].text().as_deref(), Some("\"a\""));
    assert_eq!(v[2].text().as_deref(), Some("\"b\""));
    t.assert_text(&["JSON.GET", "json"], "[[],[],[\"a\"]]");
}

#[test]
fn arr_pop_legacy() {
    let mut t = Ctx::new();
    let json = r#"
    [[6,1,6], [7,2,7], [8,3,8]]
  "#;
    t.assert_text(&["JSON.SET", "json", ".", json], "OK");

    t.assert_text(&["JSON.ARRPOP", "json", "[*]", "-2"], "3");
    t.assert_text(&["JSON.GET", "json"], "[[6,6],[7,7],[8,8]]");

    let json = r#"
    [[], ["a"], ["a", "b"]]
  "#;
    t.assert_text(&["JSON.SET", "json", ".", json], "OK");

    t.assert_text(&["JSON.ARRPOP", "json", "."], "[\"a\",\"b\"]");
    t.assert_text(&["JSON.GET", "json"], "[[],[\"a\"]]");

    t.assert_text(&["JSON.SET", "json", ".", json], "OK");
    t.assert_text(&["JSON.ARRPOP", "json", ".", "0"], "[]");
    t.assert_text(&["JSON.GET", "json"], "[[\"a\"],[\"a\",\"b\"]]");

    t.assert_text(&["JSON.ARRPOP", "json"], "[\"a\",\"b\"]");

    let json = r#"
    {"a":"b"}
  "#;
    t.assert_text(&["JSON.SET", "json", ".", json], "OK");
    t.assert_null(&["JSON.ARRPOP", "json", "."]);

    t.assert_text(&["JSON.SET", "json", ".", "[]"], "OK");
    t.assert_null(&["JSON.ARRPOP", "json", "."]);
}

#[test]
fn arr_pop_out_of_range() {
    let mut t = Ctx::new();
    let json = r#"
    [0,1,2,3,4,5]
  "#;

    t.assert_text(&["JSON.SET", "arr", "$", json], "OK");
    let mut v = t.arr(&["JSON.ARRPOP", "arr", "$", "-55"]);
    assert_eq!(v.len(), 1);
    assert_eq!(v[0].text().as_deref(), Some("0"));

    t.assert_text(&["JSON.SET", "arr", "$", json], "OK");
    v = t.arr(&["JSON.ARRPOP", "arr", "$", "55"]);
    assert_eq!(v.len(), 1);
    assert_eq!(v[0].text().as_deref(), Some("5"));

    // Legacy mode
    t.assert_text(&["JSON.SET", "arr", ".", json], "OK");
    t.assert_text(&["JSON.ARRPOP", "arr", ".", "-55"], "0");

    t.assert_text(&["JSON.SET", "arr", ".", json], "OK");
    t.assert_text(&["JSON.ARRPOP", "arr", ".", "55"], "5");
}

#[test]
fn arr_trim() {
    let mut t = Ctx::new();
    let json = r#"
    [[], ["a"], ["a", "b"], ["a", "b", "c"]]
  "#;
    t.assert_text(&["JSON.SET", "json", ".", json], "OK");

    let mut v = t.arr(&["JSON.ARRTRIM", "json", "$[*]", "0", "1"]);
    assert_eq!(v.len(), 4);
    assert_eq!(v[0].int(), Some(0));
    assert_eq!(v[1].int(), Some(1));
    assert_eq!(v[2].int(), Some(2));
    assert_eq!(v[3].int(), Some(2));
    t.assert_text(
        &["JSON.GET", "json"],
        "[[],[\"a\"],[\"a\",\"b\"],[\"a\",\"b\"]]",
    );

    let json = r#"
    {"a":[], "nested": {"a": [1,4]}}
  "#;
    t.assert_text(&["JSON.SET", "json", ".", json], "OK");

    v = t.arr(&["JSON.ARRTRIM", "json", "$..a", "0", "1"]);
    assert_eq!(v.len(), 2);
    assert_eq!(v[0].int(), Some(0));
    assert_eq!(v[1].int(), Some(2));
    t.assert_text(&["JSON.GET", "json"], "{\"a\":[],\"nested\":{\"a\":[1,4]}}");

    let json = r#"
    {"a":[1,2,3,2], "nested": {"a": false}}
  "#;
    t.assert_text(&["JSON.SET", "json", ".", json], "OK");

    v = t.arr(&["JSON.ARRTRIM", "json", "$..a", "1", "2"]);
    assert_eq!(v.len(), 2);
    assert_eq!(v[0].int(), Some(2));
    assert!(matches!(v[1], Value::Bulk(None)));
    t.assert_text(
        &["JSON.GET", "json"],
        "{\"a\":[2,3],\"nested\":{\"a\":false}}",
    );

    let json = r#"
    [1,2,3,4,5,6,7]
  "#;
    t.assert_text(&["JSON.SET", "json", "$", json], "OK");

    let mut v = t.arr(&["JSON.ARRTRIM", "json", "$", "2", "3"]);
    assert_eq!(v.len(), 1);
    assert_eq!(v[0].int(), Some(2));
    t.assert_text(&["JSON.GET", "json"], "[3,4]");
}

#[test]
fn arr_trim_legacy() {
    let mut t = Ctx::new();
    let json = r#"
    [[], ["a"], ["a", "b"], ["a", "b", "c"]]
  "#;
    t.assert_text(&["JSON.SET", "json", ".", json], "OK");

    t.assert_int(&["JSON.ARRTRIM", "json", "[*]", "0", "1"], 2);
    t.assert_text(
        &["JSON.GET", "json"],
        "[[],[\"a\"],[\"a\",\"b\"],[\"a\",\"b\"]]",
    );

    let json = r#"
    {"a":[], "nested": {"a": [1,4]}}
  "#;
    t.assert_text(&["JSON.SET", "json", ".", json], "OK");

    t.assert_int(&["JSON.ARRTRIM", "json", "..a", "0", "1"], 2);
    t.assert_text(&["JSON.GET", "json"], "{\"a\":[],\"nested\":{\"a\":[1,4]}}");

    let json = r#"
    {"a":[1,2,3,2], "nested": {"a": false}}
  "#;
    t.assert_text(&["JSON.SET", "json", ".", json], "OK");

    t.assert_int(&["JSON.ARRTRIM", "json", "..a", "1", "2"], 2);
    t.assert_text(
        &["JSON.GET", "json"],
        "{\"a\":[2,3],\"nested\":{\"a\":false}}",
    );

    let json = r#"
    [1,2,3,4,5,6,7]
  "#;
    t.assert_text(&["JSON.SET", "json", "$", json], "OK");

    t.assert_int(&["JSON.ARRTRIM", "json", ".", "2", "3"], 2);
    t.assert_text(&["JSON.GET", "json"], "[3,4]");

    let json = r#"
    {"a":"b"}
  "#;
    t.assert_text(&["JSON.SET", "json", ".", json], "OK");
    t.assert_err(
        &["JSON.ARRTRIM", "json", ".", "0", "0"],
        "wrong JSON type of path value",
    );
}

#[test]
fn arr_trim_out_of_range() {
    let mut t = Ctx::new();
    let arr = r#"
    [0,1,2,3,4]
  "#;

    t.assert_text(&["JSON.SET", "arr", "$", arr], "OK");
    let mut v = t.arr(&["JSON.ARRTRIM", "arr", "$", "-1", "3"]);
    assert_eq!(v.len(), 1);
    assert_eq!(v[0].int(), Some(0));
    t.assert_text(&["JSON.GET", "arr"], "[]");

    t.assert_text(&["JSON.SET", "arr", "$", arr], "OK");
    v = t.arr(&["JSON.ARRTRIM", "arr", "$", "54", "55"]);
    assert_eq!(v.len(), 1);
    assert_eq!(v[0].int(), Some(0));
    t.assert_text(&["JSON.GET", "arr"], "[]");

    t.assert_text(&["JSON.SET", "arr", "$", arr], "OK");
    v = t.arr(&["JSON.ARRTRIM", "arr", "$", "56", "55"]);
    assert_eq!(v.len(), 1);
    assert_eq!(v[0].int(), Some(0));
    t.assert_text(&["JSON.GET", "arr"], "[]");

    t.assert_text(&["JSON.SET", "arr", "$", arr], "OK");
    v = t.arr(&["JSON.ARRTRIM", "arr", "$", "-55", "-55"]);
    assert_eq!(v.len(), 1);
    assert_eq!(v[0].int(), Some(1));
    t.assert_text(&["JSON.GET", "arr"], "[0]");

    t.assert_text(&["JSON.SET", "arr", "$", arr], "OK");
    v = t.arr(&["JSON.ARRTRIM", "arr", "$", "-2", "-1"]);
    assert_eq!(v.len(), 1);
    assert_eq!(v[0].int(), Some(2));
    t.assert_text(&["JSON.GET", "arr"], "[3,4]");

    t.assert_text(&["JSON.SET", "arr", "$", arr], "OK");
    let mut v = t.arr(&["JSON.ARRTRIM", "arr", "$", "-1", "-2"]);
    assert_eq!(v.len(), 1);
    assert_eq!(v[0].int(), Some(0));
    t.assert_text(&["JSON.GET", "arr"], "[]");
}

#[test]
fn arr_insert() {
    let mut t = Ctx::new();
    let json = r#"
    [[], ["a"], ["a", "b"]]
  "#;
    t.assert_text(&["JSON.SET", "json", ".", json], "OK");

    let mut v = t.arr(&["JSON.ARRINSERT", "json", "$[*]", "0", "\"a\""]);
    assert_eq!(v.len(), 3);
    assert_eq!(v[0].int(), Some(1));
    assert_eq!(v[1].int(), Some(2));
    assert_eq!(v[2].int(), Some(3));
    t.assert_text(
        &["JSON.GET", "json"],
        "[[\"a\"],[\"a\",\"a\"],[\"a\",\"a\",\"b\"]]",
    );

    v = t.arr(&["JSON.ARRINSERT", "json", "$[*]", "-1", "\"b\""]);
    assert_eq!(v.len(), 3);
    assert_eq!(v[0].int(), Some(2));
    assert_eq!(v[1].int(), Some(3));
    assert_eq!(v[2].int(), Some(4));
    t.assert_text(
        &["JSON.GET", "json"],
        "[[\"b\",\"a\"],[\"a\",\"b\",\"a\"],[\"a\",\"a\",\"b\",\"b\"]]",
    );

    v = t.arr(&["JSON.ARRINSERT", "json", "$[*]", "1", "\"c\""]);
    assert_eq!(v.len(), 3);
    assert_eq!(v[0].int(), Some(3));
    assert_eq!(v[1].int(), Some(4));
    assert_eq!(v[2].int(), Some(5));
    t.assert_text(
        &["JSON.GET", "json"],
        "[[\"b\",\"c\",\"a\"],[\"a\",\"c\",\"b\",\"a\"],[\"a\",\"c\",\"a\",\"b\",\"b\"]]",
    );

    let json = r#"
    {"a":{"b":"c"}, "b":[["a"], ["a", "b"]]}
  "#;
    t.assert_text(&["JSON.SET", "json", ".", json], "OK");

    let v = t.arr(&["JSON.ARRINSERT", "json", "$.a", "0", "\"c\""]);
    assert_eq!(v.len(), 1);
    assert!(matches!(v[0], Value::Bulk(None)));

    // Missing value -> wrong number of arguments (like Redis).
    t.assert_err(
        &["JSON.ARRINSERT", "json", "$", "0"],
        "wrong number of arguments",
    );
}

#[test]
fn arr_insert_legacy() {
    let mut t = Ctx::new();
    let json = r#"
    [[], ["a"], ["a", "b"]]
  "#;
    t.assert_text(&["JSON.SET", "json", ".", json], "OK");

    t.assert_int(&["JSON.ARRINSERT", "json", "[*]", "0", "\"c\""], 3);
    t.assert_int(&["JSON.ARRINSERT", "json", ".", "0", "\"c\""], 4);
    t.assert_text(
        &["JSON.GET", "json"],
        "[\"c\",[\"c\"],[\"c\",\"a\"],[\"c\",\"a\",\"b\"]]",
    );

    let json = r#"
    {"a":{"b":"c"}, "b":[["a"], ["a", "b"]]}
  "#;
    t.assert_text(&["JSON.SET", "json", ".", json], "OK");
    t.assert_err(
        &["JSON.ARRINSERT", "json", ".a", "0", "\"c\""],
        "wrong JSON type of path value",
    );
}

#[test]
fn arr_insert_out_of_range() {
    let mut t = Ctx::new();
    let json = r#"
    [0,1,2,3,4,5]
  "#;
    t.assert_text(&["JSON.SET", "arr", ".", json], "OK");

    t.assert_err(
        &["JSON.ARRINSERT", "arr", "$", "-55", "6"],
        "index out of range",
    );
    t.assert_err(
        &["JSON.ARRINSERT", "arr", "$", "55", "6"],
        "index out of range",
    );
    t.assert_err(
        &["JSON.ARRINSERT", "arr", ".", "-55", "6"],
        "index out of range",
    );
    t.assert_err(
        &["JSON.ARRINSERT", "arr", ".", "55", "6"],
        "index out of range",
    );

    t.assert_text(&["JSON.SET", "arr", ".", "[]"], "OK");
    t.assert_err(
        &["JSON.ARRINSERT", "arr", "$", "-1", "2"],
        "index out of range",
    );
    t.assert_err(
        &["JSON.ARRINSERT", "arr", "$", "1", "2"],
        "index out of range",
    );
    let mut v = t.arr(&["JSON.ARRINSERT", "arr", "$", "0", "2"]);
    assert_eq!(v.len(), 1);
    assert_eq!(v[0].int(), Some(1));
    t.assert_text(&["JSON.GET", "arr"], "[2]");
}

#[test]
fn arr_append() {
    let mut t = Ctx::new();
    let json = r#"
    [[], ["a"], ["a", "b"]]
  "#;
    t.assert_text(&["JSON.SET", "json", ".", json], "OK");

    let mut v = t.arr(&["JSON.ARRAPPEND", "json", "$[*]", "\"a\""]);
    assert_eq!(v.len(), 3);
    assert_eq!(v[0].int(), Some(1));
    assert_eq!(v[1].int(), Some(2));
    assert_eq!(v[2].int(), Some(3));

    v = t.arr(&["JSON.ARRAPPEND", "json", "$[*]", "\"b\""]);
    assert_eq!(v.len(), 3);
    assert_eq!(v[0].int(), Some(2));
    assert_eq!(v[1].int(), Some(3));
    assert_eq!(v[2].int(), Some(4));

    let json = r#"
    {"a": [1], "nested": {"a": [1,2], "nested2": {"a": 42}}}
  "#;
    t.assert_text(&["JSON.SET", "json", ".", json], "OK");

    v = t.arr(&["JSON.ARRAPPEND", "json", "$..a", "3"]);
    assert_eq!(v.len(), 3);
    assert_eq!(v[0].int(), Some(2));
    assert_eq!(v[1].int(), Some(3));
    assert!(matches!(v[2], Value::Bulk(None)));
    t.assert_text(
        &["JSON.GET", "json"],
        "{\"a\":[1,3],\"nested\":{\"a\":[1,2,3],\"nested2\":{\"a\":42}}}",
    );
}

#[test]
fn arr_append_legacy() {
    let mut t = Ctx::new();
    let json = r#"
    [[], ["a"], ["a", "b"]]
  "#;
    t.assert_text(&["JSON.SET", "json", ".", json], "OK");

    t.assert_int(&["JSON.ARRAPPEND", "json", "[-1]", "\"c\""], 3);
    t.assert_int(&["JSON.ARRAPPEND", "json", ".*", "\"c\""], 4);
    t.assert_text(
        &["JSON.GET", "json"],
        "[[\"c\"],[\"a\",\"c\"],[\"a\",\"b\",\"c\",\"c\"]]",
    );

    let json = r#"
    {"a":"b"}
  "#;
    t.assert_text(&["JSON.SET", "json", ".", json], "OK");
    t.assert_err(
        &["JSON.ARRAPPEND", "json", ".", "\"c\""],
        "wrong JSON type of path value",
    );
}

#[test]
fn arr_index() {
    let mut t = Ctx::new();
    let json = r#"
    [[], ["a"], ["a", "b"], ["a", "b", "c"]]
  "#;
    t.assert_text(&["JSON.SET", "json", ".", json], "OK");

    let mut v = t.arr(&["JSON.ARRINDEX", "json", "$[*]", "\"b\""]);
    assert_eq!(v.len(), 4);
    assert_eq!(v[0].int(), Some(-1));
    assert_eq!(v[1].int(), Some(-1));
    assert_eq!(v[2].int(), Some(1));
    assert_eq!(v[3].int(), Some(1));

    let json = r#"
    {"a":["a","b","c","d"], "nested": {"a": ["c","d"]}}
  "#;
    t.assert_text(&["JSON.SET", "json", ".", json], "OK");

    v = t.arr(&["JSON.ARRINDEX", "json", "$..a", "\"b\""]);
    assert_eq!(v.len(), 2);
    assert_eq!(v[0].int(), Some(1));
    assert_eq!(v[1].int(), Some(-1));

    let json = r#"
    {"a":["a","b","c","d"], "nested": {"a": false}}
  "#;
    t.assert_text(&["JSON.SET", "json", ".", json], "OK");

    v = t.arr(&["JSON.ARRINDEX", "json", "$..a", "\"b\""]);
    assert_eq!(v.len(), 2);
    assert_eq!(v[0].int(), Some(1));
    assert!(matches!(v[1], Value::Bulk(None)));

    t.assert_text(
        &[
            "JSON.SET",
            "json",
            ".",
            r#"{"key" : ["Alice", "Bob", "Carol", "David", "Eve", "Frank"]}"#,
        ],
        "OK",
    );
    let mut v = t.arr(&["JSON.ARRINDEX", "json", "$.key", "\"Bob\""]);
    assert_eq!(v.len(), 1);
    assert_eq!(v[0].int(), Some(1));
    v = t.arr(&["JSON.ARRINDEX", "json", "$.key", "\"Bob\"", "1", "2"]);
    assert_eq!(v.len(), 1);
    assert_eq!(v[0].int(), Some(1));
}

#[test]
fn arr_index_legacy() {
    let mut t = Ctx::new();
    let json = r#"
    {"children": ["John", "Jack", "Tom", "Bob", "Mike"]}
  "#;
    t.assert_text(&["JSON.SET", "json", ".", json], "OK");

    t.assert_int(&["JSON.ARRINDEX", "json", ".children", "\"Tom\""], 2);
    t.assert_int(
        &["JSON.ARRINDEX", "json", ".children", "\"DoesNotExist\""],
        -1,
    );
    assert!(matches!(
        t.run(&["JSON.ARRINDEX", "json", ".children.[0].notexist", "3"]),
        Value::Error(_)
    ));

    let json = r#"
    {"a":"b"}
  "#;
    t.assert_text(&["JSON.SET", "json", ".", json], "OK");
    t.assert_err(
        &["JSON.ARRINDEX", "json", ".", "\"Tom\""],
        "wrong JSON type of path value",
    );
}

#[test]
fn arr_index_with_numeric_values() {
    let mut t = Ctx::new();
    let json = r#"
    [2, 3.0, 3]
  "#;
    t.assert_text(&["JSON.SET", "json", "$", json], "OK");

    let mut v = t.arr(&["JSON.ARRINDEX", "json", "$", "3"]);
    assert_eq!(v.len(), 1);
    assert_eq!(v[0].int(), Some(2));
    v = t.arr(&["JSON.ARRINDEX", "json", "$", "3.0"]);
    assert_eq!(v.len(), 1);
    assert_eq!(v[0].int(), Some(1));

    let json = r#"
    [[1, 2, 3], [1.0, 2.0, 3.0], 2.0, [1,2,3]]
  "#;
    t.assert_text(&["JSON.SET", "json", "$", json], "OK");

    v = t.arr(&["JSON.ARRINDEX", "json", "$", "[1,2,3]"]);
    assert_eq!(v.len(), 1);
    assert_eq!(v[0].int(), Some(0));
    v = t.arr(&["JSON.ARRINDEX", "json", "$", "[1.0,2.0,3.0]"]);
    assert_eq!(v.len(), 1);
    assert_eq!(v[0].int(), Some(1));

    let json = r#"
    [{"a":2},{"a":2.0},2.0]
  "#;
    t.assert_text(&["JSON.SET", "json", "$", json], "OK");

    v = t.arr(&["JSON.ARRINDEX", "json", "$", r#"{"a":2}"#]);
    assert_eq!(v.len(), 1);
    assert_eq!(v[0].int(), Some(0));
    v = t.arr(&["JSON.ARRINDEX", "json", "$", r#"{"a":2.0}"#]);
    assert_eq!(v.len(), 1);
    assert_eq!(v[0].int(), Some(1));

    let json = r#"
    [{"arr":[1,2,3],"number":2},{"arr":[1.0,2.0,3.0],"number":2.0},2]
  "#;
    t.assert_text(&["JSON.SET", "json", "$", json], "OK");

    v = t.arr(&[
        "JSON.ARRINDEX",
        "json",
        "$",
        r#"{"arr":[1,2,3],"number":2}"#,
    ]);
    assert_eq!(v.len(), 1);
    assert_eq!(v[0].int(), Some(0));
    v = t.arr(&[
        "JSON.ARRINDEX",
        "json",
        "$",
        r#"{"arr":[1.0,2.0,3.0],"number":2.0}"#,
    ]);
    assert_eq!(v.len(), 1);
    assert_eq!(v[0].int(), Some(1));
    v = t.arr(&[
        "JSON.ARRINDEX",
        "json",
        "$",
        r#"{"arr":[1,2,3],"number":2.0}"#,
    ]);
    assert_eq!(v.len(), 1);
    assert_eq!(v[0].int(), Some(-1));
    v = t.arr(&[
        "JSON.ARRINDEX",
        "json",
        "$",
        r#"{"arr":[1.0,2.0,3.0],"number":2}"#,
    ]);
    assert_eq!(v.len(), 1);
    assert_eq!(v[0].int(), Some(-1));
}

#[test]
fn arr_index_with_numeric_values_legacy() {
    let mut t = Ctx::new();
    let json = r#"
    [2, 3.0, 3]
  "#;
    t.assert_text(&["JSON.SET", "json", ".", json], "OK");

    t.assert_int(&["JSON.ARRINDEX", "json", ".", "3"], 2);
    t.assert_int(&["JSON.ARRINDEX", "json", ".", "3.0"], 1);

    let json = r#"
    [{"arr":[1,2,3],"number":2},{"arr":[1.0,2.0,3.0],"number":2.0},2]
  "#;
    t.assert_text(&["JSON.SET", "json", ".", json], "OK");

    t.assert_int(
        &[
            "JSON.ARRINDEX",
            "json",
            ".",
            r#"{"arr":[1,2,3],"number":2}"#,
        ],
        0,
    );
    t.assert_int(
        &[
            "JSON.ARRINDEX",
            "json",
            ".",
            r#"{"arr":[1.0,2.0,3.0],"number":2.0}"#,
        ],
        1,
    );
    t.assert_int(
        &[
            "JSON.ARRINDEX",
            "json",
            ".",
            r#"{"arr":[1,2,3],"number":2.0}"#,
        ],
        -1,
    );
    t.assert_int(
        &[
            "JSON.ARRINDEX",
            "json",
            ".",
            r#"{"arr":[1.0,2.0,3.0],"number":2}"#,
        ],
        -1,
    );
}

#[test]
fn arr_index_out_of_range() {
    let mut t = Ctx::new();
    t.assert_text(&["JSON.SET", "arr", ".", r#"[1,1,1,1,1]"#], "OK");

    let mut v = t.arr(&["JSON.ARRINDEX", "arr", "$", "1", "-55", "-55"]);
    assert_eq!(v.len(), 1);
    assert_eq!(v[0].int(), Some(-1));
    v = t.arr(&["JSON.ARRINDEX", "arr", "$", "1", "-55", "-56"]);
    assert_eq!(v.len(), 1);
    assert_eq!(v[0].int(), Some(-1));
    v = t.arr(&["JSON.ARRINDEX", "arr", "$", "1", "-55", "-54"]);
    assert_eq!(v.len(), 1);
    assert_eq!(v[0].int(), Some(-1));
    v = t.arr(&["JSON.ARRINDEX", "arr", "$", "1", "-2"]);
    assert_eq!(v.len(), 1);
    assert_eq!(v[0].int(), Some(3));
    v = t.arr(&["JSON.ARRINDEX", "arr", "$", "1", "-2", "-1"]);
    assert_eq!(v.len(), 1);
    assert_eq!(v[0].int(), Some(3));
    v = t.arr(&["JSON.ARRINDEX", "arr", "$", "1", "-2", "-3"]);
    assert_eq!(v.len(), 1);
    assert_eq!(v[0].int(), Some(-1));
    v = t.arr(&["JSON.ARRINDEX", "arr", "$", "1", "55", "56"]);
    assert_eq!(v.len(), 1);
    assert_eq!(v[0].int(), Some(4));
    v = t.arr(&["JSON.ARRINDEX", "arr", "$", "1", "55", "54"]);
    assert_eq!(v.len(), 1);
    assert_eq!(v[0].int(), Some(4));
    v = t.arr(&["JSON.ARRINDEX", "arr", "$", "1", "5", "4"]);
    assert_eq!(v.len(), 1);
    assert_eq!(v[0].int(), Some(-1));
}

#[test]
fn mget() {
    let mut t = Ctx::new();
    let json1 = r#"
    {"address":{"street":"14 Imber Street","city":"Petah-Tikva","country":"Israel","zipcode":"49511"}}
  "#;
    let json2 = r#"
    {"address":{"street":"Oranienburger Str. 27","city":"Berlin","country":"Germany","zipcode":"10117"}}
  "#;
    let json3 = r#"
    {"a":1, "b": 2, "nested": {"a": 3}, "c": null}
  "#;
    let json4 = r#"
    {"a":4, "b": 5, "nested": {"a": 6}, "c": null}
  "#;

    t.assert_text(&["JSON.SET", "json1", ".", json1], "OK");
    t.assert_text(&["JSON.SET", "json2", ".", json2], "OK");

    t.assert_err(
        &["JSON.MGET", "json1", "??INNNNVALID??"],
        "ERR syntax error",
    );

    let v = t.arr(&["JSON.MGET", "json1", "json2", "json3", "$.address.country"]);
    assert_eq!(v.len(), 3);
    assert_eq!(v[0].text().as_deref(), Some("[\"Israel\"]"));
    assert_eq!(v[1].text().as_deref(), Some("[\"Germany\"]"));
    assert!(matches!(v[2], Value::Bulk(None)));

    t.assert_text(&["JSON.SET", "json3", ".", json3], "OK");
    t.assert_text(&["JSON.SET", "json4", ".", json4], "OK");

    let v = t.arr(&["JSON.MGET", "json3", "json4", "$..a"]);
    assert_eq!(v.len(), 2);
    assert_eq!(v[0].text().as_deref(), Some("[1,3]"));
    assert_eq!(v[1].text().as_deref(), Some("[4,6]"));
}

#[test]
fn mget_legacy() {
    let mut t = Ctx::new();
    let json1 = r#"
    {"address":{"street":"14 Imber Street","city":"Petah-Tikva","country":"Israel","zipcode":"49511"}}
  "#;
    let json2 = r#"
    {"address":{"street":"Oranienburger Str. 27","city":"Berlin","country":"Germany","zipcode":"10117"}}
  "#;
    let json3 = r#"
    {"a":1, "b": 2, "nested": {"a": 3}, "c": null}
  "#;
    let json4 = r#"
    {"a":4, "b": 5, "nested": {"a": 6}, "c": null}
  "#;

    t.assert_text(&["JSON.SET", "json1", ".", json1], "OK");
    t.assert_text(&["JSON.SET", "json2", ".", json2], "OK");

    let v = t.arr(&["JSON.MGET", "json1", "json2", "json3", ".address.country"]);
    assert_eq!(v.len(), 3);
    assert_eq!(v[0].text().as_deref(), Some("\"Israel\""));
    assert_eq!(v[1].text().as_deref(), Some("\"Germany\""));
    assert!(matches!(v[2], Value::Bulk(None)));

    let v = t.arr(&["JSON.MGET", "json1", "json2", ".[0]"]);
    assert_eq!(v.len(), 2);
    assert!(matches!(v[0], Value::Bulk(None)));
    assert!(matches!(v[1], Value::Bulk(None)));

    t.assert_text(&["JSON.SET", "json3", ".", json3], "OK");
    t.assert_text(&["JSON.SET", "json4", ".", json4], "OK");

    let v = t.arr(&["JSON.MGET", "json3", "json4", "..a"]);
    assert_eq!(v.len(), 2);
    assert_eq!(v[0].text().as_deref(), Some("3"));
    assert_eq!(v[1].text().as_deref(), Some("6"));
}

#[test]
fn debug_help() {
    let mut t = Ctx::new();
    let v = t.arr(&["JSON.DEBUG", "HELP"]);
    assert_eq!(v.len(), 3);
    assert!(v[0].text().unwrap().contains("MEMORY"));
    assert!(v[1].text().unwrap().contains("FIELDS"));
    assert!(v[2].text().unwrap().contains("HELP"));
}

#[test]
fn debug_missing_key() {
    let mut t = Ctx::new();
    t.assert_err(&["JSON.DEBUG", "FIELDS"], "syntax error");
    t.assert_err(&["JSON.DEBUG", "MEMORY"], "syntax error");
}

#[test]
fn debug_fields() {
    let mut t = Ctx::new();
    let json = r#"
    [1, 2.3, "foo", true, null, {}, [], {"a":1, "b":2}, [1,2,3]]
  "#;
    t.assert_text(&["JSON.SET", "json1", ".", json], "OK");

    let v = t.arr(&["JSON.DEBUG", "fields", "json1", "$[*]"]);
    assert_eq!(v.len(), 9);
    let expected = [1, 1, 1, 1, 1, 0, 0, 2, 3];
    for (i, e) in expected.iter().enumerate() {
        assert_eq!(v[i].int(), Some(*e), "element {i}");
    }

    let v = t.arr(&["JSON.DEBUG", "fields", "json1", "$"]);
    assert_eq!(v.len(), 1);
    assert_eq!(v[0].int(), Some(14));

    let json = r#"
    [[1,2,3, [4,5,6,[6,7,8]]], {"a": {"b": {"c": 1337}}}]
  "#;
    t.assert_text(&["JSON.SET", "json1", ".", json], "OK");

    let v = t.arr(&["JSON.DEBUG", "fields", "json1", "$[*]"]);
    assert_eq!(v.len(), 2);
    assert_eq!(v[0].int(), Some(11));
    assert_eq!(v[1].int(), Some(3));

    let v = t.arr(&["JSON.DEBUG", "fields", "json1", "$"]);
    assert_eq!(v.len(), 1);
    assert_eq!(v[0].int(), Some(16));

    let json = r#"{"a":1, "b":2, "c":{"k1":1,"k2":2}}"#;
    t.assert_text(&["JSON.SET", "obj_doc", "$", json], "OK");

    let v = t.arr(&["JSON.DEBUG", "FIELDS", "obj_doc", "$.a"]);
    assert_eq!(v.len(), 1);
    assert_eq!(v[0].int(), Some(1));
    let v = t.arr(&["JSON.DEBUG", "fields", "obj_doc", "$.a"]);
    assert_eq!(v.len(), 1);
    assert_eq!(v[0].int(), Some(1));
}

#[test]
fn debug_fields_legacy() {
    let mut t = Ctx::new();
    let json = r#"
    [1, 2.3, "foo", true, null, {}, [], {"a":1, "b":2}, [1,2,3]]
  "#;
    t.assert_text(&["JSON.SET", "json1", ".", json], "OK");

    t.assert_int(&["JSON.DEBUG", "fields", "json1", "[*]"], 3);
    t.assert_int(&["JSON.DEBUG", "fields", "json1", "."], 14);
    t.assert_int(&["JSON.DEBUG", "fields", "json1"], 14);

    let json = r#"
    [[1,2,3, [4,5,6,[6,7,8]]], {"a": {"b": {"c": 1337}}}]
  "#;
    t.assert_text(&["JSON.SET", "json1", ".", json], "OK");

    t.assert_int(&["JSON.DEBUG", "fields", "json1", "[*]"], 3);
    t.assert_int(&["JSON.DEBUG", "fields", "json1", "."], 16);

    let json = r#"{"a":1, "b":2, "c":{"k1":1,"k2":2}}"#;
    t.assert_text(&["JSON.SET", "obj_doc", ".", json], "OK");

    t.assert_int(&["JSON.DEBUG", "FIELDS", "obj_doc", ".a"], 1);
    t.assert_int(&["JSON.DEBUG", "fields", "obj_doc", ".a"], 1);
}

#[test]
fn debug_memory() {
    let mut t = Ctx::new();
    t.assert_text(
        &[
            "JSON.SET",
            "json1",
            "$",
            r#"[1, 2.3, "foo", true, null, {}, [], {"a":1, "b":2}, [1,2,3]]"#,
        ],
        "OK",
    );

    let v = t.arr(&["JSON.DEBUG", "memory", "json1", "$[*]"]);
    assert_eq!(v.len(), 9);
    for i in 0..5 {
        assert_eq!(v[i].int(), Some(0), "scalar element {i} should be 0");
    }
    assert!(v[5].int().unwrap() >= 0);
    assert!(v[6].int().unwrap() >= 0);
    assert!(v[7].int().unwrap() > 0);
    assert!(v[8].int().unwrap() > 0);

    let v = t.arr(&["JSON.DEBUG", "memory", "json1", "$"]);
    assert!(v[0].int().unwrap() > 0);

    t.assert_text(
        &[
            "JSON.SET",
            "bigstr",
            "$",
            r#"{"text":"This is a longer string that should definitely exceed SSO buffer"}"#,
        ],
        "OK",
    );
    let v = t.arr(&["JSON.DEBUG", "memory", "bigstr", "$.text"]);
    assert!(v[0].int().unwrap() > 0);

    t.assert_text(
        &[
            "JSON.SET",
            "obj_doc",
            "$",
            r#"{"num":42, "obj":{"k1":1,"k2":2}}"#,
        ],
        "OK",
    );
    let v = t.arr(&["JSON.DEBUG", "MEMORY", "obj_doc", "$.num"]);
    assert_eq!(v[0].int(), Some(0));
    let v = t.arr(&["JSON.DEBUG", "memory", "obj_doc", "$.obj"]);
    assert!(v[0].int().unwrap() > 0);
}

#[test]
fn debug_memory_legacy() {
    let mut t = Ctx::new();
    t.assert_text(
        &[
            "JSON.SET",
            "json1",
            "$",
            r#"[1, 2.3, "foo", true, null, {}, [], {"a":1, "b":2}, [1,2,3]]"#,
        ],
        "OK",
    );

    assert!(t.int(&["JSON.DEBUG", "memory", "json1", "."]) > 0);
    assert!(t.int(&["JSON.DEBUG", "memory", "json1"]) > 0);

    t.assert_text(
        &[
            "JSON.SET",
            "primitives",
            "$",
            r#"{"num":42, "bool":true, "null":null}"#,
        ],
        "OK",
    );
    assert_eq!(t.int(&["JSON.DEBUG", "memory", "primitives", ".num"]), 0);
    assert_eq!(t.int(&["JSON.DEBUG", "memory", "primitives", ".bool"]), 0);
    assert_eq!(t.int(&["JSON.DEBUG", "memory", "primitives", ".null"]), 0);

    t.assert_text(
        &[
            "JSON.SET",
            "obj_doc",
            "$",
            r#"{"longstring":"This is a very long string that definitely exceeds SSO buffer"}"#,
        ],
        "OK",
    );
    assert!(t.int(&["JSON.DEBUG", "MEMORY", "obj_doc", ".longstring"]) > 0);

    t.assert_text(&["JSON.SET", "arr", "$", r#"[1,2,3,4,5,6,7,8,9,10]"#], "OK");
    assert!(t.int(&["JSON.DEBUG", "memory", "arr", "."]) > 0);

    t.assert_text(&["JSON.SET", "obj", "$", r#"{"a":1, "b":2, "c":3}"#], "OK");
    assert!(t.int(&["JSON.DEBUG", "memory", "obj", "."]) > 0);
}

#[test]
fn resp() {
    let mut t = Ctx::new();
    t.assert_text(&["JSON.SET", "json", ".", phonebook_json()], "OK");

    let v = t.arr(&["JSON.RESP", "json", "$"]);
    assert!(!v.is_empty());

    let v = t.arr(&["JSON.RESP", "json", "$.address.*"]);
    assert_eq!(v.len(), 4);
    assert_eq!(v[0].text().as_deref(), Some("New York"));
    assert_eq!(v[1].text().as_deref(), Some("NY"));
    assert_eq!(v[2].text().as_deref(), Some("21 2nd Street"));
    assert_eq!(v[3].text().as_deref(), Some("10021-3100"));

    let v = t.arr(&["JSON.RESP", "json", "$.isAlive"]);
    assert_eq!(v.len(), 1);
    assert_eq!(v[0].text().as_deref(), Some("true"));

    let v = t.arr(&["JSON.RESP", "json", "$.age"]);
    assert_eq!(v.len(), 1);
    assert_eq!(v[0].int(), Some(27));

    let v = t.arr(&["JSON.RESP", "json", "$.weight"]);
    assert_eq!(v.len(), 1);
    assert_eq!(v[0].text().as_deref(), Some("135.25"));
}

#[test]
fn resp_legacy() {
    let mut t = Ctx::new();
    t.assert_text(&["JSON.SET", "json", ".", phonebook_json()], "OK");

    assert!(!t.arr(&["JSON.RESP", "json"]).is_empty());

    t.assert_text(&["JSON.RESP", "json", ".address.*"], "10021-3100");
    t.assert_text(&["JSON.RESP", "json", ".isAlive"], "true");
    t.assert_int(&["JSON.RESP", "json", ".age"], 27);
    t.assert_text(&["JSON.RESP", "json", ".weight"], "135.25");
}

#[test]
fn set() {
    let mut t = Ctx::new();
    let mut json = r#"
    {"a":{"a":1, "b":2, "c":3}}
  "#;
    t.assert_text(&["JSON.SET", "json1", ".", json], "OK");
    t.assert_text(&["JSON.SET", "json1", "$.a.*", "0"], "OK");
    t.assert_text(&["JSON.GET", "json1"], "{\"a\":{\"a\":0,\"b\":0,\"c\":0}}");

    json = r#"
    {"a": [1,2,3,4,5]}
  "#;
    t.assert_text(&["JSON.SET", "json2", ".", json], "OK");
    t.assert_text(&["JSON.SET", "json2", "$.a[*]", "0"], "OK");
    t.assert_text(&["JSON.GET", "json2"], "{\"a\":[0,0,0,0,0]}");

    json = r#"
    {"a": 2}
  "#;
    t.assert_text(&["JSON.SET", "json3", "$", json], "OK");
    t.assert_text(&["JSON.SET", "json3", "$.b", "8"], "OK");
    t.assert_text(&["JSON.SET", "json3", "$.c", "[1,2,3]"], "OK");
    t.assert_null(&["JSON.SET", "json3", "$.z", "3", "XX"]);
    t.assert_null(&["JSON.SET", "json3", "$.b", "4", "NX"]);
    t.assert_text(&["JSON.GET", "json3"], "{\"a\":2,\"b\":8,\"c\":[1,2,3]}");
}

#[test]
fn set_legacy() {
    let mut t = Ctx::new();
    let mut json = r#"
    {"a":{"a":1, "b":2, "c":3}}
  "#;
    t.assert_text(&["JSON.SET", "json1", ".", json], "OK");
    t.assert_text(&["JSON.SET", "json1", ".a.*", "0"], "OK");
    t.assert_text(&["JSON.GET", "json1"], "{\"a\":{\"a\":0,\"b\":0,\"c\":0}}");

    json = r#"
    {"a": [1,2,3,4,5]}
  "#;
    t.assert_text(&["JSON.SET", "json2", ".", json], "OK");
    t.assert_text(&["JSON.SET", "json2", ".a[*]", "0"], "OK");
    t.assert_text(&["JSON.GET", "json2"], "{\"a\":[0,0,0,0,0]}");

    json = r#"
    {"a": 2}
  "#;
    t.assert_text(&["JSON.SET", "json3", ".", json], "OK");
    t.assert_text(&["JSON.SET", "json3", ".b", "8"], "OK");
    t.assert_text(&["JSON.SET", "json3", ".c", "[1,2,3]"], "OK");
    t.assert_null(&["JSON.SET", "json3", ".z", "3", "XX"]);
    t.assert_text(&["JSON.SET", "json3", ".z", "3"], "OK");
    t.assert_text(&["JSON.SET", "json3", ".z", "4", "XX"], "OK");
    t.assert_null(&["JSON.SET", "json3", ".b", "4", "NX"]);
    t.assert_text(&["JSON.SET", "json3", ".b", "5"], "OK");
    t.assert_null(&["JSON.SET", "json3", ".", "[]", "NX"]);
    t.assert_text(
        &["JSON.GET", "json3"],
        "{\"a\":2,\"b\":5,\"c\":[1,2,3],\"z\":4}",
    );

    json = r#"
    {"foo": "bar"}
  "#;
    t.assert_text(&["JSON.SET", "json4", ".", json], "OK");
    t.assert_text(&["JSON.SET", "json4", "foo", "\"baz\"", "XX"], "OK");
    t.assert_text(&["JSON.SET", "json4", "foo2", "\"qaz\"", "NX"], "OK");
}

#[test]
fn mset() {
    let mut t = Ctx::new();
    let json1 = r#"{"a":{"a":1,"b":2,"c":3}}"#;
    let json2 = r#"{"a":{"a":4,"b":5,"c":6}}"#;

    t.assert_err(&["JSON.MSET", "j1", "$"], "wrong number");
    t.assert_err(&["JSON.MSET", "j1", "$", json1, "j3", "$"], "wrong number");

    t.assert_text(
        &[
            "JSON.MSET",
            "j1",
            "$",
            json1,
            "j2",
            "$",
            json2,
            "j3",
            "$",
            json1,
            "j4",
            "$",
            json2,
        ],
        "OK",
    );

    let v = t.arr(&["JSON.MGET", "j1", "j2", "j3", "j4", "$"]);
    assert_eq!(v.len(), 4);
    assert_eq!(v[0].text().as_deref(), Some(format!("[{json1}]").as_str()));
    assert_eq!(v[1].text().as_deref(), Some(format!("[{json2}]").as_str()));
    assert_eq!(v[2].text().as_deref(), Some(format!("[{json1}]").as_str()));
    assert_eq!(v[3].text().as_deref(), Some(format!("[{json2}]").as_str()));
}

#[test]
fn mset_legacy() {
    let mut t = Ctx::new();
    let json1 = r#"{"a":{"a":1,"b":2,"c":3}}"#;
    let json2 = r#"{"a":{"a":4,"b":5,"c":6}}"#;

    t.assert_err(&["JSON.MSET", "j1", "."], "wrong number");
    t.assert_err(&["JSON.MSET", "j1", ".", json1, "j3", "."], "wrong number");

    t.assert_text(
        &[
            "JSON.MSET",
            "j1",
            ".",
            json1,
            "j2",
            ".",
            json2,
            "j3",
            ".",
            json1,
            "j4",
            ".",
            json2,
        ],
        "OK",
    );

    let v = t.arr(&["JSON.MGET", "j1", "j2", "j3", "j4", "$"]);
    assert_eq!(v.len(), 4);
    assert_eq!(v[0].text().as_deref(), Some(format!("[{json1}]").as_str()));
    assert_eq!(v[1].text().as_deref(), Some(format!("[{json2}]").as_str()));
    assert_eq!(v[2].text().as_deref(), Some(format!("[{json1}]").as_str()));
    assert_eq!(v[3].text().as_deref(), Some(format!("[{json2}]").as_str()));
}

#[test]
fn merge() {
    let mut t = Ctx::new();
    let json = r#"
  { "a": "b",
    "c": {
      "d": "e",
      "f": "g"
    }
  }
  "#;
    t.assert_text(&["JSON.SET", "j1", "$", json], "OK");

    let patch = r#"
    {
      "a":"z",
      "c": {
      "f": null
      }
    }
  "#;

    t.assert_text(&["JSON.MERGE", "new", "$", patch], "OK");
    t.assert_text(&["JSON.GET", "new"], "{\"a\":\"z\",\"c\":{\"f\":null}}");

    t.assert_text(&["JSON.MERGE", "j1", "$", patch], "OK");
    t.assert_text(&["JSON.GET", "j1"], "{\"a\":\"z\",\"c\":{\"d\":\"e\"}}");

    // The SET value is a JSON *string* holding braces; the merge patch object
    // replaces it outright (RFC 7386: non-object target, object patch).
    t.assert_text(
        &["JSON.SET", "foo", "$", r#""{\"f1\":1, \"common\":2}""#],
        "OK",
    );
    t.assert_text(&["JSON.MERGE", "foo", "$", r#"{"f2":2, "common":4}"#], "OK");
    t.assert_text(&["JSON.GET", "foo"], "{\"common\":4,\"f2\":2}");

    let json = r#"{
  "ans": {
    "x": {
      "y" : {
        "doubled": false,
        "answers": [
          "foo",
          "bar"
        ]
      }
    }
  }
}"#;
    t.assert_text(&["JSON.SET", "j2", "$", json], "OK");

    let patch = r#"
    {"z": {
      "doubled": false,
      "answers": ["xxx",  "yyy"]
     },
     "y": { "doubled": true}
     }"#;

    t.assert_text(&["JSON.MERGE", "j2", "$.ans.x", patch], "OK");
    t.assert_text(&["JSON.GET", "j2"], "{\"ans\":{\"x\":{\"y\":{\"answers\":[\"foo\",\"bar\"],\"doubled\":true},\"z\":{\"answers\":[\"xxx\",\"yyy\"],\"doubled\":false}}}}");
    // Not existing entry: merge creates it.
    t.assert_text(&["JSON.MERGE", "j3", "$", patch], "OK");
    t.assert_text(
        &["JSON.GET", "j3"],
        "{\"y\":{\"doubled\":true},\"z\":{\"answers\":[\"xxx\",\"yyy\"],\"doubled\":false}}",
    );
}

#[test]
fn merge_legacy() {
    let mut t = Ctx::new();
    let json = r#"
  { "a": "b",
    "c": {
      "d": "e",
      "f": "g"
    }
  }
  "#;
    t.assert_text(&["JSON.SET", "j1", "$", json], "OK");

    let patch = r#"
    {
      "a":"z",
      "c": {
      "f": null
      }
    }
  "#;

    t.assert_text(&["JSON.MERGE", "new", ".", patch], "OK");
    t.assert_text(&["JSON.GET", "new"], "{\"a\":\"z\",\"c\":{\"f\":null}}");

    t.assert_text(&["JSON.MERGE", "j1", ".", patch], "OK");
    t.assert_text(&["JSON.GET", "j1"], "{\"a\":\"z\",\"c\":{\"d\":\"e\"}}");

    t.assert_text(
        &["JSON.SET", "foo", "$", r#""{\"f1\":1, \"common\":2}""#],
        "OK",
    );
    t.assert_text(&["JSON.MERGE", "foo", ".", r#"{"f2":2, "common":4}"#], "OK");
    t.assert_text(&["JSON.GET", "foo"], "{\"common\":4,\"f2\":2}");

    let json = r#"{
  "ans": {
    "x": {
      "y" : {
        "doubled": false,
        "answers": [
          "foo",
          "bar"
        ]
      }
    }
  }
}"#;
    t.assert_text(&["JSON.SET", "j2", "$", json], "OK");

    let patch = r#"
    {"z": {
      "doubled": false,
      "answers": ["xxx",  "yyy"]
     },
     "y": { "doubled": true}
     }"#;

    t.assert_text(&["JSON.MERGE", "j2", ".ans.x", patch], "OK");
    t.assert_text(&["JSON.GET", "j2"], "{\"ans\":{\"x\":{\"y\":{\"answers\":[\"foo\",\"bar\"],\"doubled\":true},\"z\":{\"answers\":[\"xxx\",\"yyy\"],\"doubled\":false}}}}");

    t.assert_text(&["JSON.MERGE", "j3", ".", patch], "OK");
    t.assert_text(
        &["JSON.GET", "j3"],
        "{\"y\":{\"doubled\":true},\"z\":{\"answers\":[\"xxx\",\"yyy\"],\"doubled\":false}}",
    );
}

#[test]
fn get_string() {
    let mut t = Ctx::new();
    let json = r#"
  { "a": "b",
    "c": {
      "d": "e",
      "f": "g"
    }
  }
  "#;

    t.assert_text(&["SET", "json", json], "OK");
    t.assert_text(&["JSON.GET", "json", "$.c"], "[{\"d\":\"e\",\"f\":\"g\"}]");
    t.assert_text(&["SET", "not_json", "not_json"], "OK");
    t.assert_err(&["JSON.GET", "not_json", "$.c"], "WRONGTYPE");
}

#[test]
fn max_nesting_json_depth() {
    let mut t = Ctx::new();
    let mut invalid_json = String::from("{");
    for _ in 0..256 {
        invalid_json.push_str("\"key\": {");
    }
    invalid_json.push_str("\"key\": \"value\"");
    for _ in 0..257 {
        invalid_json.push('}');
    }
    t.assert_err(
        &["JSON.SET", "invalid_json", ".", &invalid_json],
        "failed to parse JSON",
    );
}

#[test]
fn set_nested_fields() {
    let mut t = Ctx::new();
    t.assert_text(&["JSON.SET", "json", "$", "{}"], "OK");
    t.assert_text(&["JSON.SET", "json", "$['field1']", "1"], "OK");
    t.assert_text(&["JSON.GET", "json"], "{\"field1\":1}");
    t.assert_text(&["JSON.SET", "json", "$['-field2']", "2"], "OK");
    t.assert_text(&["JSON.GET", "json"], "{\"-field2\":2,\"field1\":1}");
}

#[test]
fn arr_pop_with_format_parameter() {
    let mut t = Ctx::new();
    t.assert_err(
        &["JSON.ARRPOP", "test_resp3", "FORMAT", "EXPAND", "$.a"],
        "value is not an integer or out of range",
    );
}

#[test]
fn depth_limit_exceeded() {
    let mut t = Ctx::new();
    let deep_json = r#"{"jdiqr":{"nro":{"uzuf":{"bq":{"yc":{"zodmw":{"zbbq":{"sf":{"oule":{"j":{"mjsss":{"tap":{"bh":{"f":{"zlwgu":{"s":{"kt":{"fnmo":{"hub":{"xj":{"jo":{"ofara":{"kx":{"uw":{"z":{"mwvk":{"jo":{"qqz":{"b":{"tbp":{"esx":{"g":{"p":{"tpzk":{"i":{"azq":{"ttcd":{"wl":{"zo":{"l":{"nsq":{"tulso":{"uk":{"imfzw":{"vlub":{"k":{"ypml":{"voack":{"sosd":{"f":{"x":{"usv":{"hnw":{"ax":{"e":{"ozi":{"doi":{"k":{"bz":{"vxhp":{"e":{"vnpv":{"rhs":{"j":{"esp":{"f":{"ykyvy":{"xvmhg":{"eks":{"oijy":{"sjk":{"a":{"sejgy":{"msd":{"acyo":{"yxss":{"slbf":{"ssuns":{"c":{"kv":{"i":{"y":{"ubqz":{"uam":{"igaq":{"jl":{"vy":{"zlu":{"gscx":{"mb":{"idca":{"k":{"twx":{"ngjs":{"k":{"xcx":{"sxc":{"ye":{"fty":{"pho":{"lrn":{"wmv":{"h":{"sfuk":{"ilwzy":{"nlofv":{"mpcms":{"bg":{"jykgm":{"x":{"nbe":{"ixbyh":{"tmus":{"nqulr":{"cqxdw":{"wwpi":{"kj":{"udb":{"oct":{"tqkv":{"r":{"zev":{"rsu":{"gs":{"pyzm":{"au":{"__leaf":42}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}"#;
    t.assert_err(
        &["JSON.SET", "test", "$", deep_json],
        "ERR failed to parse JSON",
    );
}

#[test]
fn json_commands_working_with_other_types_bug() {
    let mut t = Ctx::new();
    t.assert_int(&["HSET", "k1", "field", "value"], 1);

    // JSON.SET on a hash should error.
    t.assert_err(&["JSON.SET", "k1", "$", r#"{"a":"b"}"#], "WRONGTYPE");

    // JSON.DEL must not delete the hash.
    t.assert_int(&["HSET", "k2", "field", "value"], 1);
    t.assert_err(&["JSON.DEL", "k2"], "WRONGTYPE");
    t.assert_text(&["HGET", "k2", "field"], "value");
}

#[test]
fn reset_string_key_with_set_get() {
    let mut t = Ctx::new();
    t.assert_text(&["JSON.SET", "key", "$", r#"{"a":"b"}"#], "OK");
    t.assert_text(&["JSON.GET", "key"], "{\"a\":\"b\"}");

    // Resetting the key with a string value.
    t.assert_text(&["SET", "key", r#"{"a":"b"}"#], "OK");
    t.assert_text(&["GET", "key"], "{\"a\":\"b\"}");
    t.assert_text(&["JSON.GET", "key"], "{\"a\":\"b\"}");

    // Resetting the key again with JSON.SET.
    t.assert_text(&["JSON.SET", "key", "$", r#"{"a":"b"}"#], "OK");
    t.assert_text(&["JSON.GET", "key"], "{\"a\":\"b\"}");
}

#[test]
fn del_non_existing_key() {
    let mut t = Ctx::new();
    t.assert_int(&["EXISTS", "nonexisting_key"], 0);
    t.assert_int(&["JSON.DEL", "nonexisting_key", "."], 0);
    t.assert_int(&["JSON.DEL", "nonexisting_key", "$"], 0);
    t.assert_int(&["JSON.DEL", "nonexisting_key"], 0);
}

#[test]
fn json_keys_with_dots() {
    let mut t = Ctx::new();
    t.assert_text(&["JSON.SET", "OFFERS:DBX-AGG1611-IGN", "$", r#"{"Gallery": {"Images": {"bdz1xjm.jpeg": "some_value", "bdz1xjm": "another_value"}}}"#], "OK");

    t.assert_text(
        &[
            "JSON.GET",
            "OFFERS:DBX-AGG1611-IGN",
            "$['Gallery']['Images']['bdz1xjm']",
        ],
        "[\"another_value\"]",
    );
    t.assert_text(
        &[
            "JSON.GET",
            "OFFERS:DBX-AGG1611-IGN",
            "$['Gallery']['Images']['bdz1xjm.jpeg']",
        ],
        "[\"some_value\"]",
    );
}

#[test]
fn json_set_delete_expiry_of_existing_key() {
    let mut t = Ctx::new();
    let _clock = clock_guard();
    t.assert_text(&["SET", "key", "foo", "EX", "1000"], "OK");
    t.assert_text(&["JSON.SET", "key", "$", "{}"], "OK");
    t.assert_int(&["TTL", "key"], -1);
    t.assert_int(&["EXPIRE", "key", "100"], 1);
    t.assert_int(&["TTL", "key"], 100);
}

#[test]
fn json_int_path_test() {
    let mut t = Ctx::new();
    t.assert_text(&["JSON.SET", "test:images", "$", r#"{"images":[{"id":1,"sizes":{"1":"small.jpg","10":"medium.jpg","14":"large.jpg","8":"thumb.jpg"}}]}"#], "OK");
    t.assert_text(
        &["JSON.GET", "test:images", "$.images[0].sizes.10"],
        "[\"medium.jpg\"]",
    );
    t.assert_text(
        &["JSON.GET", "test:images", "$.images[0].sizes[\"10\"]"],
        "[\"medium.jpg\"]",
    );
    t.assert_text(
        &["JSON.GET", "test:images", "$.images[0].sizes['10']"],
        "[\"medium.jpg\"]",
    );
    t.assert_text(
        &["JSON.GET", "test:images", "$.images[0][\"sizes\"][\"10\"]"],
        "[\"medium.jpg\"]",
    );
    t.assert_text(
        &["JSON.GET", "test:images", "$.images[0].sizes.8"],
        "[\"thumb.jpg\"]",
    );
    t.assert_text(
        &["JSON.GET", "test:images", "$.images[0].sizes.14"],
        "[\"large.jpg\"]",
    );
    t.assert_text(
        &["JSON.GET", "test:images", "$.images[0].sizes[\"8\"]"],
        "[\"thumb.jpg\"]",
    );
    t.assert_text(
        &["JSON.GET", "test:images", "$.images[0].sizes[\"14\"]"],
        "[\"large.jpg\"]",
    );
}

#[test]
fn arrlen_resp3_nested_array_flat() {
    let mut t = Ctx::new();
    let json = r#"{"a":[1], "b":{"a":[1,2,3]}, "c":{"x":"not_a"}}"#;
    t.assert_text(&["JSON.SET", "doc", ".", json], "OK");

    // The port always emits flat arrays (what the reference only produces under
    // RESP3): elements are integers, not arrays wrapped in arrays.
    let v = t.arr(&["JSON.ARRLEN", "doc", "$..a"]);
    assert_eq!(v.len(), 2);
    assert_eq!(v[0].int(), Some(1));
    assert_eq!(v[1].int(), Some(3));
}

#[test]
fn arrappend_resp3_nested_array_flat() {
    let mut t = Ctx::new();
    t.assert_text(
        &["JSON.SET", "doc", ".", r#"{"a":[1], "b":{"a":[1,2,3]}}"#],
        "OK",
    );

    let v = t.arr(&["JSON.ARRAPPEND", "doc", "$..a", "2"]);
    assert_eq!(v.len(), 2);
    assert_eq!(v[0].int(), Some(2));
    assert_eq!(v[1].int(), Some(4));
}

#[test]
fn arrindex_resp3_nested_array_flat() {
    let mut t = Ctx::new();
    t.assert_text(
        &[
            "JSON.SET",
            "doc",
            ".",
            r#"{"a":["x","y"], "b":{"a":["y","z"]}}"#,
        ],
        "OK",
    );

    let v = t.arr(&["JSON.ARRINDEX", "doc", "$..a", "\"y\""]);
    assert_eq!(v.len(), 2);
    assert_eq!(v[0].int(), Some(1));
    assert_eq!(v[1].int(), Some(0));
}

#[test]
fn arrpop_resp3_nested_array_flat() {
    let mut t = Ctx::new();
    t.assert_text(
        &["JSON.SET", "doc", ".", r#"{"a":[7], "b":{"a":[8]}}"#],
        "OK",
    );

    let v = t.arr(&["JSON.ARRPOP", "doc", "$..a"]);
    assert_eq!(v.len(), 2);
    assert!(v[0].text().is_some());
    assert!(v[1].text().is_some());
}

#[test]
fn arrtrim_resp3_nested_array_flat() {
    let mut t = Ctx::new();
    t.assert_text(
        &["JSON.SET", "doc", ".", r#"{"a":[1,2], "b":{"a":[3,4,5]}}"#],
        "OK",
    );

    let v = t.arr(&["JSON.ARRTRIM", "doc", "$..a", "0", "0"]);
    assert_eq!(v.len(), 2);
    assert_eq!(v[0].int(), Some(1));
    assert_eq!(v[1].int(), Some(1));
}

#[test]
fn strlen_resp3_nested_array_flat() {
    let mut t = Ctx::new();
    t.assert_text(
        &["JSON.SET", "doc", ".", r#"{"s":"hi", "b":{"s":"abc"}}"#],
        "OK",
    );

    let v = t.arr(&["JSON.STRLEN", "doc", "$..s"]);
    assert_eq!(v.len(), 2);
    assert_eq!(v[0].int(), Some(2));
    assert_eq!(v[1].int(), Some(3));
}

#[test]
fn objlen_resp3_nested_array_flat() {
    let mut t = Ctx::new();
    t.assert_text(
        &[
            "JSON.SET",
            "doc",
            ".",
            r#"{"o":{"k":1}, "b":{"o":{"k":1,"m":2}}}"#,
        ],
        "OK",
    );

    let v = t.arr(&["JSON.OBJLEN", "doc", "$..o"]);
    assert_eq!(v.len(), 2);
    assert_eq!(v[0].int(), Some(1));
    assert_eq!(v[1].int(), Some(2));
}

#[test]
fn objkeys_resp3_nested_array_flat() {
    let mut t = Ctx::new();
    t.assert_text(
        &[
            "JSON.SET",
            "doc",
            ".",
            r#"{"o":{"k":1}, "b":{"o":{"k":1,"m":2}}}"#,
        ],
        "OK",
    );

    let v = t.arr(&["JSON.OBJKEYS", "doc", "$..o"]);
    assert_eq!(v.len(), 2);
    assert_eq!(v[0].arr().unwrap().len(), 1);
    assert_eq!(v[1].arr().unwrap().len(), 2);
}

#[test]
fn strappend_resp3_nested_array_flat() {
    let mut t = Ctx::new();
    t.assert_text(
        &["JSON.SET", "doc", ".", r#"{"s":"a", "b":{"s":"zz"}}"#],
        "OK",
    );

    let v = t.arr(&["JSON.STRAPPEND", "doc", "$..s", "\"b\""]);
    assert_eq!(v.len(), 2);
    assert_eq!(v[0].int(), Some(2));
    assert_eq!(v[1].int(), Some(3));
}

#[test]
fn toggle_resp3_nested_array_flat() {
    let mut t = Ctx::new();
    t.assert_text(
        &["JSON.SET", "doc", ".", r#"{"b":true, "x":{"b":false}}"#],
        "OK",
    );

    let v = t.arr(&["JSON.TOGGLE", "doc", "$..b"]);
    assert_eq!(v.len(), 2);
    assert_eq!(v[0].int(), Some(0));
    assert_eq!(v[1].int(), Some(1));
}

#[test]
fn set_over_large_string_key() {
    let mut t = Ctx::new();
    let large_value = "x".repeat(16000);
    t.assert_text(&["SET", "key", &large_value], "OK");
    t.assert_text(&["JSON.SET", "key", "$", "1"], "OK");
    t.assert_text(&["JSON.GET", "key"], "1");
}

#[test]
fn set_full_json_invalid_on_new_key() {
    let mut t = Ctx::new();
    t.assert_err(
        &["JSON.SET", "newkey", "$", "{invalid}"],
        "failed to parse JSON",
    );
    t.assert_int(&["EXISTS", "newkey"], 0);
}
