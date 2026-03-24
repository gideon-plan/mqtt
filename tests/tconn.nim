## tconn.nim -- Integration tests for MQTT connection against NanoMQ broker.

{.experimental: "strict_funcs".}

import std/[unittest, tables]
import mqtt/[packet, conn]

when not declared(host):
  const host = "127.0.0.1"
when not declared(port):
  const port = 21883

suite "connection":
  test "connect and disconnect":
    let c = open_conn(host, port, "tconn_basic", keep_alive = 60)
    check c != nil
    close_conn(c)

  test "connect with clean start":
    let c = open_conn(host, port, "tconn_clean", clean_start = true)
    check c != nil
    close_conn(c)

  test "ping":
    let c = open_conn(host, port, "tconn_ping")
    ping(c)
    close_conn(c)

  test "multiple pings":
    let c = open_conn(host, port, "tconn_multi_ping")
    for i in 0 ..< 5:
      ping(c)
    close_conn(c)

  test "send and recv raw publish qos0":
    let c = open_conn(host, port, "tconn_raw")
    # Subscribe first
    let sub_pkt = MqttPacket(packet_type: ptSubscribe, subscribe_packet_id: 1,
                             subscribe_props: initOrderedTable[uint8, seq[PropertyValue]](),
                             subscriptions: @[Subscription(topic: "tconn/test", qos: qos0)])
    send_packet(c, sub_pkt)
    let suback = recv_packet(c)
    check suback.packet_type == ptSuback
    # Publish
    let pub_pkt = MqttPacket(packet_type: ptPublish, dup: false, publish_qos: qos0,
                             retain: false, topic: "tconn/test", publish_packet_id: 0,
                             publish_props: initOrderedTable[uint8, seq[PropertyValue]](),
                             payload: "hello")
    send_packet(c, pub_pkt)
    # Receive our own message
    let msg = recv_packet(c)
    check msg.packet_type == ptPublish
    check msg.topic == "tconn/test"
    check msg.payload == "hello"
    close_conn(c)
