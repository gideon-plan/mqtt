## listener.nim -- MQTT server TCP accept loop.
##
## Thread-per-client: spawns a handler thread for each accepted connection.
## Server-side CONNECT/CONNACK handshake. Blocking I/O.

import segfaults

{.experimental: "strict_funcs".}

import std/[net, atomics, tables, options, locks]
import packet, session, router, topic, will

# =====================================================================================================================
# Types
# =====================================================================================================================

type
  ClientHandler* = proc(session: var ClientSession, router: var Router,
                        will_store: var WillStore) {.gcsafe, raises: [].}

  MqttServer* = object
    sock: Socket
    running*: Atomic[bool]
    port*: int
    router*: Router
    will_store*: WillStore
    next_id: Atomic[int]
    sessions_lock: Lock
    sessions*: Table[SubscriberId, ptr ClientSession]

# =====================================================================================================================
# Server lifecycle
# =====================================================================================================================

proc new_server*(port: int, deliver: DeliveryCallback): MqttServer =
  result.port = port
  result.running.store(false)
  result.router = new_router(deliver)
  result.next_id.store(1)
  initLock(result.sessions_lock)
  result.sessions = initTable[SubscriberId, ptr ClientSession]()

  proc will_publisher(topic: string, payload: string, qos: QoS, retain: bool,
                      props: Properties) {.gcsafe, raises: [].} =
    discard  # will be wired to router.publish in serve()

  result.will_store = new_will_store(will_publisher)

proc accept_client(server: var MqttServer, client_sock: Socket): (bool, SubscriberId, ptr ClientSession) =
  ## Perform server-side CONNECT/CONNACK handshake. Returns (ok, id, session).
  # Read CONNECT packet
  var buf = ""
  # Read fixed header byte
  var header_byte: char
  let got = try:
    client_sock.recv(header_byte.addr, 1)
  except OSError:
    return (false, 0, nil)
  if got != 1:
    return (false, 0, nil)

  let ptype = uint8(header_byte) shr 4
  if ptype != ptConnect:
    try: client_sock.close() except CatchableError: discard
    return (false, 0, nil)

  # Read remaining length
  var remaining_len = 0'u32
  var multiplier = 1'u32
  for i in 0 ..< 4:
    var b: char
    let got2 = try:
      client_sock.recv(b.addr, 1)
    except OSError:
      return (false, 0, nil)
    if got2 != 1:
      return (false, 0, nil)
    remaining_len += uint32(uint8(b) and 0x7F) * multiplier
    if (uint8(b) and 0x80) == 0:
      break
    multiplier *= 128

  # Read remaining bytes
  var payload = newString(int(remaining_len))
  var total = 0
  while total < int(remaining_len):
    let n = try:
      client_sock.recv(payload[total].addr, int(remaining_len) - total)
    except OSError:
      return (false, 0, nil)
    if n <= 0:
      return (false, 0, nil)
    total += n

  # Reconstruct buffer for decode
  buf = $header_byte
  buf.add(encode_varint(remaining_len))
  buf.add(payload)

  var pos = 0
  let connect_pkt = try:
    decode(buf, pos)
  except MqttError:
    try: client_sock.close() except CatchableError: discard
    return (false, 0, nil)

  if connect_pkt.packet_type != ptConnect:
    try: client_sock.close() except CatchableError: discard
    return (false, 0, nil)

  # Allocate session
  let sid = server.next_id.fetchAdd(1)
  let sess = cast[ptr ClientSession](alloc0(sizeof(ClientSession)))
  sess[] = new_session(connect_pkt.client_id, client_sock,
                       connect_pkt.keep_alive,
                       connect_pkt.connect_flags.clean_start,
                       connect_pkt.will_config)

  # Register will if present
  if connect_pkt.connect_flags.will and connect_pkt.will_config.isSome:
    server.will_store.register(connect_pkt.client_id, connect_pkt.will_config.get)

  # Send CONNACK
  let connack = MqttPacket(packet_type: ptConnack, session_present: false,
                           connack_reason: rcSuccess,
                           connack_props: initOrderedTable[uint8, seq[PropertyValue]]())
  let connack_bytes = try:
    encode(connack)
  except MqttError:
    dealloc(sess)
    try: client_sock.close() except CatchableError: discard
    return (false, 0, nil)

  try:
    client_sock.send(connack_bytes)
  except OSError:
    dealloc(sess)
    return (false, 0, nil)

  return (true, SubscriberId(sid), sess)

proc handle_client*(server: var MqttServer, sid: SubscriberId,
                    sess: ptr ClientSession) {.gcsafe.} =
  ## Handle a single client connection. Runs on a dedicated thread.
  ## Reads packets in a loop, dispatches to router.
  server.router.register_session(sid, sess)

  # Read loop
  while server.running.load():
    # Read fixed header
    var header_byte: char
    let got = try:
      sess.sock.recv(header_byte.addr, 1)
    except OSError:
      break
    except CatchableError:
      break
    if got != 1:
      break

    let ptype = uint8(header_byte) shr 4

    # Read remaining length
    var remaining_len = 0'u32
    var multiplier = 1'u32
    var varint_ok = true
    for i in 0 ..< 4:
      var b: char
      let got2 = try:
        sess.sock.recv(b.addr, 1)
      except CatchableError:
        varint_ok = false
        break
      if got2 != 1:
        varint_ok = false
        break
      remaining_len += uint32(uint8(b) and 0x7F) * multiplier
      if (uint8(b) and 0x80) == 0:
        break
      multiplier *= 128

    if not varint_ok:
      break

    # Read remaining bytes
    var payload = ""
    if remaining_len > 0:
      payload = newString(int(remaining_len))
      var total = 0
      var read_ok = true
      while total < int(remaining_len):
        let n = try:
          sess.sock.recv(payload[total].addr, int(remaining_len) - total)
        except CatchableError:
          read_ok = false
          break
        if n <= 0:
          read_ok = false
          break
        total += n
      if not read_ok:
        break

    # Reconstruct buffer and decode
    var buf = $header_byte
    let varint_bytes = try:
      encode_varint(remaining_len)
    except MqttError:
      break
    buf.add(varint_bytes)
    buf.add(payload)

    var pos = 0
    let pkt = try:
      decode(buf, pos)
    except MqttError:
      break

    # Dispatch by packet type
    case pkt.packet_type
    of ptPublish:
      server.router.publish(pkt.topic, pkt.payload, pkt.publish_qos,
                            pkt.retain, pkt.publish_props)
      # Send ack if needed
      if pkt.publish_qos == qos1:
        let ack = MqttPacket(packet_type: ptPuback,
                             puback_packet_id: pkt.publish_packet_id,
                             puback_reason: rcSuccess,
                             puback_props: initOrderedTable[uint8, seq[PropertyValue]]())
        let ack_bytes = try: encode(ack) except MqttError: break
        try: sess.sock.send(ack_bytes) except CatchableError: break
      elif pkt.publish_qos == qos2:
        let rec = MqttPacket(packet_type: ptPubrec,
                             pubrec_packet_id: pkt.publish_packet_id,
                             pubrec_reason: rcSuccess,
                             pubrec_props: initOrderedTable[uint8, seq[PropertyValue]]())
        let rec_bytes = try: encode(rec) except MqttError: break
        try: sess.sock.send(rec_bytes) except CatchableError: break

    of ptPubrel:
      let comp = MqttPacket(packet_type: ptPubcomp,
                            pubcomp_packet_id: pkt.pubrel_packet_id,
                            pubcomp_reason: rcSuccess,
                            pubcomp_props: initOrderedTable[uint8, seq[PropertyValue]]())
      let comp_bytes = try: encode(comp) except MqttError: break
      try: sess.sock.send(comp_bytes) except CatchableError: break

    of ptSubscribe:
      var reasons: seq[uint8] = @[]
      for sub in pkt.subscriptions:
        sess[].add_subscription(sub.topic, sub.qos)
        let retained = server.router.subscribe(sid, sub.topic, sub.qos)
        reasons.add(uint8(sub.qos))
        # Deliver retained messages
        for rmsg in retained:
          server.router.deliver(sid, rmsg.topic, rmsg.payload, rmsg.qos,
                                true, rmsg.properties)
      let suback = MqttPacket(packet_type: ptSuback,
                              suback_packet_id: pkt.subscribe_packet_id,
                              suback_props: initOrderedTable[uint8, seq[PropertyValue]](),
                              suback_reasons: reasons)
      let suback_bytes = try: encode(suback) except MqttError: break
      try: sess.sock.send(suback_bytes) except CatchableError: break

    of ptUnsubscribe:
      var reasons: seq[uint8] = @[]
      for t in pkt.unsubscribe_topics:
        sess[].remove_subscription(t)
        server.router.unsubscribe(sid, t)
        reasons.add(rcSuccess)
      let unsuback = MqttPacket(packet_type: ptUnsuback,
                                unsuback_packet_id: pkt.unsubscribe_packet_id,
                                unsuback_props: initOrderedTable[uint8, seq[PropertyValue]](),
                                unsuback_reasons: reasons)
      let unsuback_bytes = try: encode(unsuback) except MqttError: break
      try: sess.sock.send(unsuback_bytes) except CatchableError: break

    of ptPingreq:
      let resp = try: encode_pingresp() except MqttError: break
      try: sess.sock.send(resp) except CatchableError: break

    of ptDisconnect:
      # Clean disconnect: deregister will
      server.will_store.deregister(sess.client_id)
      break

    of ptPuback, ptPubrec, ptPubcomp:
      # Client acknowledging a message we sent
      sess[].remove_inflight(
        case pkt.packet_type
        of ptPuback: pkt.puback_packet_id
        of ptPubrec: pkt.pubrec_packet_id
        of ptPubcomp: pkt.pubcomp_packet_id
        else: 0
      )

    else:
      discard

  # Client disconnected -- trigger will if unclean
  server.will_store.trigger(sess.client_id)

  # Cleanup
  server.router.unregister_session(sid)
  # Unsubscribe from all topics
  for filter, _ in sess.subscriptions:
    server.router.unsubscribe(sid, filter)
  try: sess.sock.close() except CatchableError: discard
  dealloc(sess)

proc serve*(server: var MqttServer) {.gcsafe.} =
  ## Start accepting connections. Blocks until stop() is called.
  ## Handles one client at a time (single-threaded accept loop).
  server.running.store(true)
  server.sock = try:
    newSocket()
  except OSError:
    return
  except CatchableError:
    return
  try:
    server.sock.setSockOpt(OptReuseAddr, true)
    server.sock.bindAddr(Port(server.port))
    server.sock.listen()
  except OSError:
    return
  except CatchableError:
    return

  while server.running.load():
    var client_sock: Socket
    try:
      server.sock.accept(client_sock)
    except OSError:
      if server.running.load():
        continue
      else:
        break
    except CatchableError:
      break

    let (ok, sid, sess) = accept_client(server, client_sock)
    if not ok:
      continue

    # Register session
    acquire(server.sessions_lock)
    server.sessions[sid] = sess
    release(server.sessions_lock)

    # Handle client inline (single-threaded for now)
    handle_client(server, sid, sess)

proc stop*(server: var MqttServer) {.raises: [].} =
  ## Stop the server. Closes the listening socket to unblock accept().
  server.running.store(false)
  if server.sock != nil:
    try: server.sock.close() except CatchableError: discard
  close_router(server.router)
  deinitLock(server.sessions_lock)
