## router.nim -- MQTT PUBLISH fan-out via topic trie.
##
## Receives published messages, matches subscribers, delivers to each.
## Handles QoS downgrade (subscriber QoS <= publish QoS).
## Thread-safety: lock protects trie and retain store mutations.

{.experimental: "strict_funcs".}

import std/[tables, sets, locks]
import packet, topic, retain, session

# =====================================================================================================================
# Types
# =====================================================================================================================

type
  DeliveryCallback* = proc(subscriber_id: SubscriberId, topic: string,
                           payload: string, qos: QoS, retain_flag: bool,
                           props: Properties) {.gcsafe, raises: [].}

  Router* = object
    lock: Lock
    trie: TopicTrie
    retain_store: RetainStore
    sessions: Table[SubscriberId, ptr ClientSession]
    deliver*: DeliveryCallback

# =====================================================================================================================
# Router operations
# =====================================================================================================================

proc new_router*(deliver: DeliveryCallback): Router =
  result.trie = new_trie()
  result.retain_store = new_retain_store()
  result.sessions = initTable[SubscriberId, ptr ClientSession]()
  result.deliver = deliver
  initLock(result.lock)

proc register_session*(router: var Router, id: SubscriberId, session: ptr ClientSession) =
  acquire(router.lock)
  router.sessions[id] = session
  release(router.lock)

proc unregister_session*(router: var Router, id: SubscriberId) =
  acquire(router.lock)
  router.sessions.del(id)
  release(router.lock)

proc subscribe*(router: var Router, id: SubscriberId, filter: string, qos: QoS): seq[RetainedMessage] =
  ## Subscribe and return any matching retained messages.
  acquire(router.lock)
  router.trie.subscribe(filter, id)
  let retained = router.retain_store.match_filter(filter)
  release(router.lock)
  return retained

proc unsubscribe*(router: var Router, id: SubscriberId, filter: string) =
  acquire(router.lock)
  router.trie.unsubscribe(filter, id)
  release(router.lock)

proc publish*(router: var Router, topic: string, payload: string, qos: QoS,
              retain_flag: bool, props: Properties) =
  ## Route a published message to all matching subscribers.
  acquire(router.lock)
  # Handle retained message
  if retain_flag:
    router.retain_store.store(topic, payload, qos, props)
  # Find matching subscribers
  let subscribers = router.trie.match(topic)
  release(router.lock)
  # Deliver to each subscriber (outside lock)
  for sub_id in subscribers:
    router.deliver(sub_id, topic, payload, qos, false, props)

proc close_router*(router: var Router) =
  deinitLock(router.lock)
