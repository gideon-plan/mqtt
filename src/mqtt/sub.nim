## sub.nim -- MQTT 5.0 subscription. Dedicated connection, blocking recv loop.
##
## MqttSubscriber uses a dedicated connection for receiving messages.
## run_loop blocks on the caller's thread. stop() sets atomic flag;
## caller must unsubscribe or disconnect to unblock recv.

{.experimental: "strict_funcs".}

import std/[atomics, tables]
import basis/code/choice
import packet, conn

# =====================================================================================================================
# Types
# =====================================================================================================================

type
  MessageHandler* = proc(topic: string, payload: string, qos: QoS, retain: bool,
                         props: Properties) {.gcsafe, raises: [].}

  MqttSubscriber* = object
    conn*: MqttConn
    running: Atomic[bool]
    handler*: MessageHandler
    next_packet_id: Atomic[int]

# =====================================================================================================================
# Packet ID generation
# =====================================================================================================================

proc next_id(sub: var MqttSubscriber): uint16 =
  let v = sub.next_packet_id.fetchAdd(1)
  let id = (v mod 65535) + 1
  result = uint16(id)

# =====================================================================================================================
# Constructor
# =====================================================================================================================

proc new_subscriber*(host: string, port: int, handler: MessageHandler,
                     client_id: string = "mqtt_sub",
                     keep_alive: uint16 = 60, clean_start: bool = true,
                     username: string = "", password: string = ""
                    ): MqttSubscriber {.raises: [MqttError].} =
  result.conn = open_conn(host, port, client_id, keep_alive, clean_start, username, password)
  result.running.store(true)
  result.handler = handler
  result.next_packet_id.store(0)

# =====================================================================================================================
# Subscribe / Unsubscribe
# =====================================================================================================================

proc subscribe*(sub: var MqttSubscriber, topics: seq[Subscription]): Choice[seq[uint8]] =
  ## Send SUBSCRIBE and wait for SUBACK.
  let pid = sub.next_id()
  let pkt = MqttPacket(packet_type: ptSubscribe, subscribe_packet_id: pid,
                       subscribe_props: initOrderedTable[uint8, seq[PropertyValue]](),
                       subscriptions: topics)
  try:
    send_packet(sub.conn, pkt)
    let suback = recv_packet(sub.conn)
    if suback.packet_type != ptSuback:
      return bad[seq[uint8]]("mqtt", "expected SUBACK, got " & $suback.packet_type)
    if suback.suback_packet_id != pid:
      return bad[seq[uint8]]("mqtt", "SUBACK packet ID mismatch")
    good(suback.suback_reasons)
  except MqttError as e:
    bad[seq[uint8]]("mqtt", e.msg)

proc unsubscribe*(sub: var MqttSubscriber, topics: seq[string]): Choice[seq[uint8]] =
  ## Send UNSUBSCRIBE and wait for UNSUBACK.
  let pid = sub.next_id()
  let pkt = MqttPacket(packet_type: ptUnsubscribe, unsubscribe_packet_id: pid,
                       unsubscribe_props: initOrderedTable[uint8, seq[PropertyValue]](),
                       unsubscribe_topics: topics)
  try:
    send_packet(sub.conn, pkt)
    let unsuback = recv_packet(sub.conn)
    if unsuback.packet_type != ptUnsuback:
      return bad[seq[uint8]]("mqtt", "expected UNSUBACK, got " & $unsuback.packet_type)
    if unsuback.unsuback_packet_id != pid:
      return bad[seq[uint8]]("mqtt", "UNSUBACK packet ID mismatch")
    good(unsuback.unsuback_reasons)
  except MqttError as e:
    bad[seq[uint8]]("mqtt", e.msg)

# =====================================================================================================================
# QoS ack dispatch
# =====================================================================================================================

proc send_puback(sub: var MqttSubscriber, packet_id: uint16) {.raises: [].} =
  let ack = MqttPacket(packet_type: ptPuback, puback_packet_id: packet_id,
                       puback_reason: rcSuccess,
                       puback_props: initOrderedTable[uint8, seq[PropertyValue]]())
  try:
    send_packet(sub.conn, ack)
  except CatchableError:
    discard

proc send_pubrec(sub: var MqttSubscriber, packet_id: uint16) {.raises: [].} =
  let rec = MqttPacket(packet_type: ptPubrec, pubrec_packet_id: packet_id,
                       pubrec_reason: rcSuccess,
                       pubrec_props: initOrderedTable[uint8, seq[PropertyValue]]())
  try:
    send_packet(sub.conn, rec)
  except CatchableError:
    discard

proc send_pubcomp(sub: var MqttSubscriber, packet_id: uint16) {.raises: [].} =
  let comp = MqttPacket(packet_type: ptPubcomp, pubcomp_packet_id: packet_id,
                        pubcomp_reason: rcSuccess,
                        pubcomp_props: initOrderedTable[uint8, seq[PropertyValue]]())
  try:
    send_packet(sub.conn, comp)
  except CatchableError:
    discard

# =====================================================================================================================
# Run loop
# =====================================================================================================================

proc run_loop*(sub: var MqttSubscriber) {.gcsafe, raises: [].} =
  ## Blocking receive loop. Dispatches messages to handler.
  ## Handles QoS 0/1/2 ack protocol. Returns on error or stop().
  while sub.running.load():
    let pkt = try:
      recv_packet(sub.conn)
    except MqttError:
      return
    case pkt.packet_type
    of ptPublish:
      # Deliver to handler
      try:
        sub.handler(pkt.topic, pkt.payload, pkt.publish_qos, pkt.retain, pkt.publish_props)
      except CatchableError:
        discard
      # QoS ack
      case pkt.publish_qos
      of qos0:
        discard
      of qos1:
        send_puback(sub, pkt.publish_packet_id)
      of qos2:
        send_pubrec(sub, pkt.publish_packet_id)
    of ptPubrel:
      # QoS 2: received PUBREL, message already delivered at PUBREC stage
      send_pubcomp(sub, pkt.pubrel_packet_id)
    of ptPingresp:
      discard
    of ptDisconnect:
      return
    else:
      discard

proc stop*(sub: var MqttSubscriber) {.raises: [].} =
  ## Signal the run loop to stop. Caller must unsubscribe or disconnect to unblock recv.
  sub.running.store(false)

proc close_subscriber*(sub: var MqttSubscriber) {.raises: [].} =
  ## Stop and close the subscriber connection.
  sub.running.store(false)
  close_conn(sub.conn)
