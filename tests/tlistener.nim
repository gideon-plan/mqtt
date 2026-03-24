## tlistener.nim -- Embedded server integration tests.
## Single-threaded server: handles one client at a time.

{.experimental: "strict_funcs".}

import std/[unittest, atomics, os, tables, net]
import mqtt/[packet, conn, topic, listener]

var g_delivered: Atomic[int]

proc listener_deliver(subscriber_id: SubscriberId, topic: string, payload: string,
                  qos: QoS, retain_flag: bool,
                  props: Properties) {.gcsafe, raises: [].} =
  discard g_delivered.fetchAdd(1)

proc server_thread_a() {.thread.} =
  var server = new_server(31883, listener_deliver)
  server.serve()

proc server_thread_b() {.thread.} =
  var server = new_server(31884, listener_deliver)
  server.serve()

suite "embedded server":
  test "connect, ping, disconnect":
    g_delivered.store(0)
    var t: Thread[void]
    createThread(t, server_thread_a)
    sleep(300)

    let c = open_conn("127.0.0.1", 31883, "tlistener_ping")
    ping(c)
    close_conn(c)
    # Server thread exits handle_client when client disconnects,
    # loops back to accept. Test complete.

  test "subscribe and self-publish":
    g_delivered.store(0)
    var t: Thread[void]
    createThread(t, server_thread_b)
    sleep(300)

    # Single client: subscribe, publish to own topic, receive
    let c = open_conn("127.0.0.1", 31884, "tlistener_selfpub")

    # Subscribe
    let sub_pkt = MqttPacket(packet_type: ptSubscribe, subscribe_packet_id: 1,
                             subscribe_props: initOrderedTable[uint8, seq[PropertyValue]](),
                             subscriptions: @[Subscription(topic: "test/self", qos: qos0)])
    send_packet(c, sub_pkt)
    let suback = recv_packet(c)
    check suback.packet_type == ptSuback
    check suback.suback_reasons.len == 1

    # Publish to own topic
    let pub_pkt = MqttPacket(packet_type: ptPublish, dup: false, publish_qos: qos0,
                             retain: false, topic: "test/self", publish_packet_id: 0,
                             publish_props: initOrderedTable[uint8, seq[PropertyValue]](),
                             payload: "self msg")
    send_packet(c, pub_pkt)
    sleep(200)

    # The deliver callback should have been called
    check g_delivered.load() >= 1

    close_conn(c)
