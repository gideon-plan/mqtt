## tpublish.nim -- QoS 0/1/2 publish tests against NanoMQ broker.

{.experimental: "strict_funcs".}

import std/unittest
import mqtt/[conn, publish, lattice]

when not declared(host):
  const host = "127.0.0.1"
when not declared(port):
  const port = 21883

suite "publish":
  test "qos0 publish":
    let c = open_conn(host, port, "tpub_qos0")
    var pub = new_publisher(c)
    let r = pub.publish_qos0("test/pub/qos0", "hello qos0")
    check r.is_good
    close_conn(c)

  test "qos1 publish":
    let c = open_conn(host, port, "tpub_qos1")
    var pub = new_publisher(c)
    let r = pub.publish_qos1("test/pub/qos1", "hello qos1")
    check r.is_good
    check r.val > 0'u16
    close_conn(c)

  test "qos2 publish":
    let c = open_conn(host, port, "tpub_qos2")
    var pub = new_publisher(c)
    let r = pub.publish_qos2("test/pub/qos2", "hello qos2")
    check r.is_good
    check r.val > 0'u16
    close_conn(c)

  test "multiple qos1 publishes":
    let c = open_conn(host, port, "tpub_multi")
    var pub = new_publisher(c)
    for i in 0 ..< 5:
      let r = pub.publish_qos1("test/pub/multi", "msg " & $i)
      check r.is_good
    close_conn(c)
