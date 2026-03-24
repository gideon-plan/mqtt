## tpool.nim -- Pool tests against NanoMQ broker.

{.experimental: "strict_funcs".}

import std/[unittest]
import mqtt/[conn, pool]

when not declared(host):
  const host = "127.0.0.1"
when not declared(port):
  const port = 21883

suite "pool":
  test "basic borrow and recycle":
    let p = new_pool(2, host, port, "tpool_basic")
    let c1 = borrow(p)
    check c1 != nil
    ping(c1)
    recycle(p, c1)
    let c2 = borrow(p)
    check c2 != nil
    recycle(p, c2)
    close_pool(p)

  test "borrow all and recycle":
    let p = new_pool(3, host, port, "tpool_all")
    let c1 = borrow(p)
    let c2 = borrow(p)
    let c3 = borrow(p)
    check c1 != nil
    check c2 != nil
    check c3 != nil
    recycle(p, c3)
    recycle(p, c2)
    recycle(p, c1)
    close_pool(p)

  test "thread concurrent borrow":
    var g_pool: MqttPool
    g_pool = new_pool(4, host, port, "tpool_thread")

    proc pool_worker() {.thread.} =
      {.gcsafe.}:
        let c = borrow(g_pool)
        ping(c)
        recycle(g_pool, c)

    var threads: array[4, Thread[void]]
    for i in 0 ..< 4:
      createThread(threads[i], pool_worker)
    for i in 0 ..< 4:
      joinThread(threads[i])
    close_pool(g_pool)
