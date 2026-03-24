## ttopic.nim -- Unit tests for topic trie.

{.experimental: "strict_funcs".}

import std/[unittest, sets]
import mqtt/topic

suite "topic trie":
  test "exact match":
    var trie = new_trie()
    trie.subscribe("sensor/temperature", 1)
    let m = trie.match("sensor/temperature")
    check 1 in m
    check m.len == 1

  test "no match":
    var trie = new_trie()
    trie.subscribe("sensor/temperature", 1)
    let m = trie.match("sensor/humidity")
    check m.len == 0

  test "single-level wildcard +":
    var trie = new_trie()
    trie.subscribe("sensor/+/reading", 1)
    let m1 = trie.match("sensor/temperature/reading")
    check 1 in m1
    let m2 = trie.match("sensor/humidity/reading")
    check 1 in m2
    let m3 = trie.match("sensor/temperature/value")
    check m3.len == 0

  test "multi-level wildcard #":
    var trie = new_trie()
    trie.subscribe("sensor/#", 1)
    let m1 = trie.match("sensor/temperature")
    check 1 in m1
    let m2 = trie.match("sensor/temperature/reading")
    check 1 in m2
    let m3 = trie.match("sensor")
    check 1 in m3

  test "# at root matches everything":
    var trie = new_trie()
    trie.subscribe("#", 1)
    let m1 = trie.match("a")
    check 1 in m1
    let m2 = trie.match("a/b/c")
    check 1 in m2

  test "multiple subscribers":
    var trie = new_trie()
    trie.subscribe("sensor/temperature", 1)
    trie.subscribe("sensor/temperature", 2)
    trie.subscribe("sensor/+", 3)
    let m = trie.match("sensor/temperature")
    check 1 in m
    check 2 in m
    check 3 in m
    check m.len == 3

  test "unsubscribe":
    var trie = new_trie()
    trie.subscribe("sensor/temperature", 1)
    trie.subscribe("sensor/temperature", 2)
    trie.unsubscribe("sensor/temperature", 1)
    let m = trie.match("sensor/temperature")
    check 1 notin m
    check 2 in m

  test "has_subscribers":
    var trie = new_trie()
    check not trie.has_subscribers("a/b")
    trie.subscribe("a/b", 1)
    check trie.has_subscribers("a/b")
    trie.unsubscribe("a/b", 1)
    check not trie.has_subscribers("a/b")

  test "+ does not match empty level":
    var trie = new_trie()
    trie.subscribe("a/+/c", 1)
    let m = trie.match("a/c")
    check m.len == 0

  test "# matches zero levels after prefix":
    var trie = new_trie()
    trie.subscribe("a/b/#", 1)
    let m1 = trie.match("a/b")
    check 1 in m1
    let m2 = trie.match("a/b/c")
    check 1 in m2
    let m3 = trie.match("a/b/c/d")
    check 1 in m3

  test "parse shared subscription":
    let (is_shared, group) = parse_shared("$share/mygroup/sensor/+")
    check is_shared
    check group.group == "mygroup"
    check group.filter == "sensor/+"

  test "parse non-shared subscription":
    let (is_shared, _) = parse_shared("sensor/temperature")
    check not is_shared
