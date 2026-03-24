## pool.nim -- MQTT connection pool. Lock + Cond, borrow/recycle.
##
## Same pattern as valkey pool. Borrow blocks when empty.

{.experimental: "strict_funcs".}

import std/locks
import packet, conn

# =====================================================================================================================
# Types
# =====================================================================================================================

type
  MqttPoolObj = object
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

  MqttPool* = ptr MqttPoolObj

# =====================================================================================================================
# Pool management
# =====================================================================================================================

proc new_pool*(size: int, host: string, port: int,
               client_id_prefix: string = "mqtt_pool",
               keep_alive: uint16 = 60, clean_start: bool = true,
               username: string = "", password: string = ""): MqttPool {.raises: [MqttError].} =
  ## Create a pool of `size` MQTT connections.
  result = cast[MqttPool](alloc0(sizeof(MqttPoolObj)))
  initLock(result.lock)
  initCond(result.cond)
  result.conns = newSeq[MqttConn](0)
  result.host = host
  result.port = port
  result.client_id_prefix = client_id_prefix
  result.keep_alive = keep_alive
  result.clean_start = clean_start
  result.username = username
  result.password = password
  for i in 0 ..< size:
    let cid = client_id_prefix & "_" & $i
    let c = open_conn(host, port, cid, keep_alive, clean_start, username, password)
    result.conns.add(c)

proc borrow*(pool: MqttPool): MqttConn {.raises: [].} =
  ## Borrow a connection from the pool. Blocks if pool is empty.
  acquire(pool.lock)
  while pool.conns.len == 0:
    wait(pool.cond, pool.lock)
  result = pool.conns.pop()
  release(pool.lock)

proc recycle*(pool: MqttPool, conn: MqttConn) {.raises: [].} =
  ## Return a connection to the pool.
  acquire(pool.lock)
  pool.conns.add(conn)
  signal(pool.cond)
  release(pool.lock)

proc close_pool*(pool: MqttPool) {.raises: [].} =
  ## Close all connections and free the pool.
  if pool == nil: return
  acquire(pool.lock)
  for c in pool.conns:
    close_conn(c)
  pool.conns.setLen(0)
  release(pool.lock)
  deinitLock(pool.lock)
  deinitCond(pool.cond)
  dealloc(pool)
