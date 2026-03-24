## pool.nim -- MQTT connection pool. Lock + Cond, borrow/recycle.
##
## Under atomicArc, MqttPool is a ref object. No manual alloc/dealloc.

{.experimental: "strict_funcs".}

import std/locks
import packet, conn

# =====================================================================================================================
# Types
# =====================================================================================================================

type
  MqttPool* = ref object
    lock: Lock
    cond: Cond
    conns: seq[MqttConn]
    host: string
    port: int
    client_id_prefix: string
    keep_alive: uint16
    clean_start: bool
    username: string
    password: string

# =====================================================================================================================
# Pool management
# =====================================================================================================================

proc new_pool*(size: int, host: string, port: int,
               client_id_prefix: string = "mqtt_pool",
               keep_alive: uint16 = 60, clean_start: bool = true,
               username: string = "", password: string = ""): MqttPool {.raises: [MqttError].} =
  result = MqttPool(host: host, port: port, client_id_prefix: client_id_prefix,
                    keep_alive: keep_alive, clean_start: clean_start,
                    username: username, password: password)
  initLock(result.lock)
  initCond(result.cond)
  for i in 0 ..< size:
    let cid = client_id_prefix & "_" & $i
    let c = open_conn(host, port, cid, keep_alive, clean_start, username, password)
    result.conns.add(c)

proc borrow*(pool: MqttPool): MqttConn {.raises: [].} =
  acquire(pool.lock)
  while pool.conns.len == 0:
    wait(pool.cond, pool.lock)
  result = pool.conns.pop()
  release(pool.lock)

proc recycle*(pool: MqttPool, conn: MqttConn) {.raises: [].} =
  acquire(pool.lock)
  pool.conns.add(conn)
  signal(pool.cond)
  release(pool.lock)

proc close_pool*(pool: MqttPool) {.raises: [].} =
  if pool == nil: return
  acquire(pool.lock)
  for c in pool.conns:
    close_conn(c)
  pool.conns.setLen(0)
  release(pool.lock)
  deinitLock(pool.lock)
  deinitCond(pool.cond)
