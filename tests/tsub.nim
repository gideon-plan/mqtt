## tsub.nim -- Subscription tests against NanoMQ broker.

{.experimental: "strict_funcs".}

import std/[unittest, atomics, os]
import mqtt/[packet, conn, sub, publish, lattice]

when not declared(host):
  const host = "127.0.0.1"
when not declared(port):
  const port = 21883

var g_msg_count: Atomic[int]
var g_sub: MqttSubscriber

proc test_handler(topic: string, payload: string, qos: QoS, retain: bool,
                  props: Properties) {.gcsafe, raises: [].} =
  discard g_msg_count.fetchAdd(1)

proc sub_runner() {.thread.} =
  {.gcsafe.}:
    g_sub.run_loop()

suite "subscribe":
  test "subscribe and receive qos0":
    g_msg_count.store(0)
    g_sub = new_subscriber(host, port, test_handler, "tsub_qos0")
    let sub_topic = Subscription(topic: "tsub/test", qos: qos0)
    let sr = g_sub.subscribe(@[sub_topic])
    check sr.is_good

    var t: Thread[void]
    createThread(t, sub_runner)

    # Publish from a separate connection
    let pub_conn = open_conn(host, port, "tsub_pub0")
    var pub = new_publisher(pub_conn)
    sleep(200)  # let subscriber loop start
    for i in 0 ..< 3:
      discard pub.publish_qos0("tsub/test", "msg " & $i)
      sleep(50)

    sleep(300)  # let messages arrive
    # close_subscriber closes the socket, which unblocks run_loop's recv
    close_subscriber(g_sub)
    joinThread(t)
    close_conn(pub_conn)
    check g_msg_count.load() >= 2  # at least 2 of 3 should arrive

  test "subscribe qos1 with ack":
    g_msg_count.store(0)
    g_sub = new_subscriber(host, port, test_handler, "tsub_qos1")
    let sub_topic = Subscription(topic: "tsub/qos1", qos: qos1)
    let sr = g_sub.subscribe(@[sub_topic])
    check sr.is_good

    var t: Thread[void]
    createThread(t, sub_runner)

    let pub_conn = open_conn(host, port, "tsub_pub1")
    var pub = new_publisher(pub_conn)
    sleep(200)
    for i in 0 ..< 3:
      discard pub.publish_qos1("tsub/qos1", "qos1 msg " & $i)
      sleep(50)

    sleep(300)
    close_subscriber(g_sub)
    joinThread(t)
    close_conn(pub_conn)
    check g_msg_count.load() >= 2

  test "stop before run exits cleanly":
    g_sub = new_subscriber(host, port, test_handler, "tsub_stop")
    g_sub.stop()
    # run_loop checks running flag first, sees false, but still calls recv_packet
    # which blocks. close_subscriber closes socket to unblock it.
    var t: Thread[void]
    createThread(t, sub_runner)
    sleep(100)
    close_subscriber(g_sub)
    joinThread(t)
