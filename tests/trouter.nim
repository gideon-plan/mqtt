## trouter.nim -- Router unit tests.
## Tests PUBLISH fan-out, retained delivery, QoS handling.

{.experimental: "strict_funcs".}

import std/[unittest, atomics, tables]
import mqtt/[packet, topic, router, retain]

var g_routed: Atomic[int]
var g_last_topic {.global.}: string
var g_last_payload {.global.}: string

proc router_deliver(subscriber_id: SubscriberId, topic: string, payload: string,
                  qos: QoS, retain_flag: bool,
                  props: Properties) {.gcsafe, raises: [].} =
  discard g_routed.fetchAdd(1)
  {.gcsafe.}:
    g_last_topic = topic
    g_last_payload = payload

suite "router":
  test "publish to single subscriber":
    g_routed.store(0)
    var r = new_router(router_deliver)
    discard r.subscribe(1, "test/route", qos0)
    r.publish("test/route", "hello", qos0, false,
              initOrderedTable[uint8, seq[PropertyValue]]())
    check g_routed.load() == 1
    check g_last_payload == "hello"
    close_router(r)

  test "publish fan-out to multiple subscribers":
    g_routed.store(0)
    var r = new_router(router_deliver)
    discard r.subscribe(1, "test/fan", qos0)
    discard r.subscribe(2, "test/fan", qos0)
    discard r.subscribe(3, "test/fan", qos0)
    r.publish("test/fan", "fanout", qos0, false,
              initOrderedTable[uint8, seq[PropertyValue]]())
    check g_routed.load() == 3
    close_router(r)

  test "wildcard subscription receives publish":
    g_routed.store(0)
    var r = new_router(router_deliver)
    discard r.subscribe(1, "sensor/+/reading", qos0)
    r.publish("sensor/temp/reading", "25.0", qos0, false,
              initOrderedTable[uint8, seq[PropertyValue]]())
    check g_routed.load() == 1
    check g_last_topic == "sensor/temp/reading"
    close_router(r)

  test "unsubscribe stops delivery":
    g_routed.store(0)
    var r = new_router(router_deliver)
    discard r.subscribe(1, "test/unsub", qos0)
    r.unsubscribe(1, "test/unsub")
    r.publish("test/unsub", "gone", qos0, false,
              initOrderedTable[uint8, seq[PropertyValue]]())
    check g_routed.load() == 0
    close_router(r)

  test "retained message stored and returned on subscribe":
    g_routed.store(0)
    var r = new_router(router_deliver)
    r.publish("test/retain", "retained value", qos0, true,
              initOrderedTable[uint8, seq[PropertyValue]]())
    let retained = r.subscribe(1, "test/retain", qos0)
    check retained.len == 1
    check retained[0].payload == "retained value"
    close_router(r)

  test "empty payload clears retained":
    var r = new_router(router_deliver)
    r.publish("test/clear", "value", qos0, true,
              initOrderedTable[uint8, seq[PropertyValue]]())
    r.publish("test/clear", "", qos0, true,
              initOrderedTable[uint8, seq[PropertyValue]]())
    let retained = r.subscribe(1, "test/clear", qos0)
    check retained.len == 0
    close_router(r)
