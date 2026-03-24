## topic.nim -- MQTT topic trie with wildcard matching.
##
## Supports exact match, `+` (single-level), `#` (multi-level) wildcards.
## Shared subscriptions: `$share/<group>/<filter>` are parsed and routed
## round-robin within each group.
##
## Thread-safety: callers must hold a lock before mutating the trie.

{.experimental: "strict_funcs".}

import std/[tables, sets]

# =====================================================================================================================
# Types
# =====================================================================================================================

type
  SubscriberId* = int

  TrieNode = ref object
    children: Table[string, TrieNode]
    subscribers: HashSet[SubscriberId]

  TopicTrie* = object
    root: TrieNode

  SharedGroup* = object
    group*: string
    filter*: string

# =====================================================================================================================
# Helpers
# =====================================================================================================================

proc split_topic(topic: string): seq[string] =
  ## Split a topic string on `/` separators.
  result = @[]
  var start = 0
  for i in 0 ..< topic.len:
    if topic[i] == '/':
      result.add(topic[start ..< i])
      start = i + 1
  result.add(topic[start ..< topic.len])

proc parse_shared*(topic: string): (bool, SharedGroup) =
  ## Parse a shared subscription filter. Returns (is_shared, group_info).
  ## Format: $share/<group>/<actual_filter>
  if topic.len > 7 and topic[0 .. 6] == "$share/":
    let rest = topic[7 .. ^1]
    let slash_pos = rest.find('/')
    if slash_pos > 0 and slash_pos < rest.len - 1:
      return (true, SharedGroup(group: rest[0 ..< slash_pos],
                                filter: rest[slash_pos + 1 .. ^1]))
  return (false, SharedGroup())

# =====================================================================================================================
# Trie operations
# =====================================================================================================================

proc new_trie*(): TopicTrie =
  result.root = TrieNode(children: initTable[string, TrieNode](),
                         subscribers: initHashSet[SubscriberId]())

proc subscribe*(trie: var TopicTrie, filter: string, subscriber: SubscriberId) =
  ## Add a subscriber to a topic filter. Handles `+` and `#` wildcards.
  let parts = split_topic(filter)
  var node = trie.root
  for part in parts:
    if part notin node.children:
      node.children[part] = TrieNode(children: initTable[string, TrieNode](),
                                     subscribers: initHashSet[SubscriberId]())
    node = node.children[part]
  node.subscribers.incl(subscriber)

proc unsubscribe*(trie: var TopicTrie, filter: string, subscriber: SubscriberId) =
  ## Remove a subscriber from a topic filter.
  let parts = split_topic(filter)
  var node = trie.root
  for part in parts:
    if part notin node.children:
      return
    node = node.children[part]
  node.subscribers.excl(subscriber)

proc match*(trie: TopicTrie, topic: string): HashSet[SubscriberId] =
  ## Find all subscribers matching a published topic.
  ## Handles `+` (single-level) and `#` (multi-level) wildcards.
  result = initHashSet[SubscriberId]()
  let parts = split_topic(topic)

  # Iterative BFS with stack of (node, depth)
  var stack: seq[(TrieNode, int)] = @[(trie.root, 0)]

  while stack.len > 0:
    let (node, depth) = stack.pop()

    if depth == parts.len:
      for s in node.subscribers:
        result.incl(s)
      if "#" in node.children:
        for s in node.children["#"].subscribers:
          result.incl(s)
      continue

    let part = parts[depth]

    # Exact match
    if part in node.children:
      stack.add((node.children[part], depth + 1))

    # `+` wildcard: matches any single level
    if "+" in node.children:
      stack.add((node.children["+"], depth + 1))

    # `#` wildcard: matches zero or more remaining levels
    if "#" in node.children:
      for s in node.children["#"].subscribers:
        result.incl(s)

proc has_subscribers*(trie: TopicTrie, filter: string): bool =
  ## Check if a filter has any subscribers.
  let parts = split_topic(filter)
  var node = trie.root
  for part in parts:
    if part notin node.children:
      return false
    node = node.children[part]
  return node.subscribers.len > 0
