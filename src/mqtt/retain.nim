## retain.nim -- Retained message store.
##
## One message per topic. Empty payload clears the retained message.
## Thread-safety: callers must hold a lock before mutating the store.

{.experimental: "strict_funcs".}

import std/tables
import packet

# =====================================================================================================================
# Types
# =====================================================================================================================

type
  RetainedMessage* = object
    topic*: string
    payload*: string
    qos*: QoS
    properties*: Properties

  RetainStore* = object
    messages: Table[string, RetainedMessage]

# =====================================================================================================================
# Filter matching (used internally for retained message delivery)
# =====================================================================================================================

proc split_filter(filter: string): seq[string] =
  result = @[]
  var start = 0
  for i in 0 ..< filter.len:
    if filter[i] == '/':
      result.add(filter[start ..< i])
      start = i + 1
  result.add(filter[start ..< filter.len])

proc matches_filter(topic: string, filter_parts: seq[string]): bool =
  let topic_parts = split_filter(topic)
  var fi = 0
  var ti = 0
  while fi < filter_parts.len and ti < topic_parts.len:
    let fp = filter_parts[fi]
    if fp == "#":
      return true  # matches everything remaining
    elif fp == "+" or fp == topic_parts[ti]:
      inc fi
      inc ti
    else:
      return false
  return fi == filter_parts.len and ti == topic_parts.len

# =====================================================================================================================
# Operations
# =====================================================================================================================

proc new_retain_store*(): RetainStore =
  result.messages = initTable[string, RetainedMessage]()

proc store*(rs: var RetainStore, topic: string, payload: string, qos: QoS,
            props: Properties) =
  ## Store or clear a retained message. Empty payload clears.
  if payload.len == 0:
    rs.messages.del(topic)
  else:
    rs.messages[topic] = RetainedMessage(topic: topic, payload: payload,
                                         qos: qos, properties: props)

proc get*(rs: RetainStore, topic: string): (bool, RetainedMessage) =
  ## Get a retained message for an exact topic. Returns (found, msg).
  if topic in rs.messages:
    return (true, rs.messages[topic])
  return (false, RetainedMessage())

proc match_filter*(rs: RetainStore, filter: string): seq[RetainedMessage] =
  ## Get all retained messages matching a subscription filter.
  result = @[]
  let filter_parts = split_filter(filter)
  for topic, msg in rs.messages:
    if matches_filter(topic, filter_parts):
      result.add(msg)
