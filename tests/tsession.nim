## tsession.nim -- Session and will tests.

{.experimental: "strict_funcs".}

import std/[unittest, net, options, tables]
import mqtt/[packet, session, will]

suite "session":
  test "new session clean start":
    let sock = newSocket()
    let sess = new_session("client1", sock, 60, true, none(WillConfig))
    check sess.client_id == "client1"
    check sess.state == ssNew
    check sess.keep_alive == 60
    check sess.subscriptions.len == 0
    sock.close()

  test "add and remove subscription":
    let sock = newSocket()
    var sess = new_session("client2", sock, 60, true, none(WillConfig))
    sess.add_subscription("test/topic", qos1)
    check "test/topic" in sess.subscriptions
    check sess.subscriptions["test/topic"] == qos1
    sess.remove_subscription("test/topic")
    check "test/topic" notin sess.subscriptions
    sock.close()

  test "inflight tracking":
    let sock = newSocket()
    var sess = new_session("client3", sock, 60, true, none(WillConfig))
    let msg = InflightMsg(packet_id: 1, topic: "t", payload: "p", qos: qos1)
    sess.add_inflight(1, msg)
    check 1'u16 in sess.inflight
    sess.remove_inflight(1)
    check 1'u16 notin sess.inflight
    sock.close()

  test "packet ID generation":
    let sock = newSocket()
    var sess = new_session("client4", sock, 60, true, none(WillConfig))
    let id1 = sess.next_id()
    check id1 == 1
    let id2 = sess.next_id()
    check id2 == 2
    sock.close()

  test "clear session":
    let sock = newSocket()
    var sess = new_session("client5", sock, 60, false, none(WillConfig))
    sess.add_subscription("a/b", qos0)
    sess.add_inflight(1, InflightMsg(packet_id: 1, topic: "t", payload: "p", qos: qos1))
    check sess.state == ssResumed
    sess.clear_session()
    check sess.subscriptions.len == 0
    check sess.inflight.len == 0
    check sess.state == ssNew
    sock.close()

suite "will":
  test "will trigger on unclean disconnect":
    var triggered_topic {.global.} = ""
    var triggered_payload {.global.} = ""

    proc will_pub(topic: string, payload: string, qos: QoS, retain: bool,
                  props: Properties) {.gcsafe, raises: [].} =
      {.gcsafe.}:
        triggered_topic = topic
        triggered_payload = payload

    var ws = new_will_store(will_pub)
    let wc = WillConfig(qos: qos0, retain: false, topic: "client/offline",
                        payload: "gone",
                        properties: initOrderedTable[uint8, seq[PropertyValue]]())
    ws.register("c1", wc)
    check ws.has_will("c1")
    ws.trigger("c1")
    check triggered_topic == "client/offline"
    check triggered_payload == "gone"
    check not ws.has_will("c1")

  test "will deregister on clean disconnect":
    proc noop(topic: string, payload: string, qos: QoS, retain: bool,
              props: Properties) {.gcsafe, raises: [].} = discard

    var ws = new_will_store(noop)
    let wc = WillConfig(qos: qos0, retain: false, topic: "t",
                        payload: "p",
                        properties: initOrderedTable[uint8, seq[PropertyValue]]())
    ws.register("c2", wc)
    ws.deregister("c2")
    check not ws.has_will("c2")
