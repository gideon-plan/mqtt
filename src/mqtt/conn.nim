## conn.nim -- MQTT 5.0 TCP connection. Blocking I/O.
##
## MqttConn = ref MqttConnObj: GC-managed under atomicArc, safe across threads.
## open_conn: TCP connect + CONNECT/CONNACK handshake.
## send_packet / recv_packet: encode/decode MqttPacket over the wire.
## ping: PINGREQ/PINGRESP keepalive.

{.experimental: "strict_funcs".}

import std/[net, options, tables]
import packet

# =====================================================================================================================
# Types
# =====================================================================================================================

type
  MqttConn* = ref object
    sock*: Socket
    keep_alive*: uint16
    client_id*: string
    server_props*: Properties  # from CONNACK

# =====================================================================================================================
# Low-level I/O
# =====================================================================================================================

proc sock_recv(sock: Socket, n: int): string {.raises: [MqttError].} =
  result = ""
  while result.len < n:
    var buf = try:
      sock.recv(n - result.len)
    except OSError as e:
      raise newException(MqttError, "recv error: " & e.msg)
    except TimeoutError as e:
      raise newException(MqttError, "recv timeout: " & e.msg)
    if buf.len == 0:
      raise newException(MqttError, "connection closed")
    result.add(buf)

proc sock_read_byte(sock: Socket): uint8 {.raises: [MqttError].} =
  let data = sock_recv(sock, 1)
  result = uint8(data[0])

proc sock_read_varint(sock: Socket): uint32 {.raises: [MqttError].} =
  var multiplier = 1'u32
  result = 0
  for i in 0 ..< 4:
    let b = sock_read_byte(sock)
    result = result + uint32(b and 0x7F) * multiplier
    if (b and 0x80) == 0:
      return
    multiplier *= 128
  raise newException(MqttError, "varint: malformed (more than 4 bytes)")

# =====================================================================================================================
# Send / Recv
# =====================================================================================================================

proc send_packet*(conn: MqttConn, pkt: MqttPacket) {.raises: [MqttError].} =
  let data = encode(pkt)
  try:
    conn.sock.send(data)
  except OSError as e:
    raise newException(MqttError, "send error: " & e.msg)

proc send_raw*(conn: MqttConn, data: string) {.raises: [MqttError].} =
  try:
    conn.sock.send(data)
  except OSError as e:
    raise newException(MqttError, "send error: " & e.msg)

proc recv_packet*(conn: MqttConn): MqttPacket {.raises: [MqttError].} =
  let first_byte = sock_read_byte(conn.sock)
  let remaining_len = int(sock_read_varint(conn.sock))
  var buf = newString(1)
  buf[0] = char(first_byte)
  let varint_encoded = encode_varint(uint32(remaining_len))
  buf.add(varint_encoded)
  if remaining_len > 0:
    buf.add(sock_recv(conn.sock, remaining_len))
  var pos = 0
  result = decode(buf, pos)

# =====================================================================================================================
# Connect / Close
# =====================================================================================================================

proc open_conn*(host: string, port: int, client_id: string = "",
                keep_alive: uint16 = 60, clean_start: bool = true,
                username: string = "", password: string = "",
                will: Option[WillConfig] = none(WillConfig),
                connect_props: Properties = initOrderedTable[uint8, seq[PropertyValue]]()
               ): MqttConn {.raises: [MqttError].} =
  result = MqttConn(keep_alive: keep_alive, client_id: client_id)
  try:
    result.sock = newSocket()
    result.sock.connect(host, Port(port))
  except OSError as e:
    raise newException(MqttError, "connect error: " & e.msg)
  var cf: ConnectFlags
  cf.clean_start = clean_start
  cf.will = will.isSome
  if will.isSome:
    cf.will_qos = will.get.qos
    cf.will_retain = will.get.retain
  cf.username = username.len > 0
  cf.password = password.len > 0
  let connect_pkt = MqttPacket(packet_type: ptConnect, connect_flags: cf,
                               keep_alive: keep_alive, connect_props: connect_props,
                               client_id: client_id, will_config: will,
                               username: username, password: password)
  try:
    send_packet(result, connect_pkt)
  except MqttError:
    try: result.sock.close() except CatchableError: discard
    raise
  let connack = try:
    recv_packet(result)
  except MqttError:
    try: result.sock.close() except CatchableError: discard
    raise
  if connack.packet_type != ptConnack:
    try: result.sock.close() except CatchableError: discard
    raise newException(MqttError, "expected CONNACK, got packet type " & $connack.packet_type)
  if connack.connack_reason != rcSuccess:
    try: result.sock.close() except CatchableError: discard
    raise newException(MqttError, "CONNACK rejected: reason code " & $connack.connack_reason)
  result.server_props = connack.connack_props

proc close_conn*(conn: MqttConn) {.raises: [].} =
  if conn == nil: return
  let disc = MqttPacket(packet_type: ptDisconnect, disconnect_reason: rcNormalDisconnection,
                        disconnect_props: initOrderedTable[uint8, seq[PropertyValue]]())
  try: send_packet(conn, disc) except CatchableError: discard
  try: conn.sock.close() except CatchableError: discard

proc ping*(conn: MqttConn) {.raises: [MqttError].} =
  send_raw(conn, encode_pingreq())
  let resp = recv_packet(conn)
  if resp.packet_type != ptPingresp:
    raise newException(MqttError, "expected PINGRESP, got packet type " & $resp.packet_type)
