## publish.nim -- MQTT 5.0 QoS 0/1/2 publish with ack state machine.
##
## QoS 0: fire and forget
## QoS 1: PUBLISH -> wait PUBACK
## QoS 2: PUBLISH -> wait PUBREC -> send PUBREL -> wait PUBCOMP

{.experimental: "strict_funcs".}

import std/[atomics, tables]
import packet, conn, lattice

# =====================================================================================================================
# Types
# =====================================================================================================================

type
  MqttPublisher* = object
    conn*: MqttConn
    next_packet_id: Atomic[int]

# =====================================================================================================================
# Packet ID generation
# =====================================================================================================================

proc next_id(pub: var MqttPublisher): uint16 =
  let v = pub.next_packet_id.fetchAdd(1)
  # Packet IDs are 1-65535; wrap around
  let id = (v mod 65535) + 1
  result = uint16(id)

# =====================================================================================================================
# Publish
# =====================================================================================================================

proc new_publisher*(conn: MqttConn): MqttPublisher =
  result.conn = conn
  result.next_packet_id.store(0)

proc publish_qos0*(pub: var MqttPublisher, topic: string, payload: string,
                   retain: bool = false,
                   props: Properties = initOrderedTable[uint8, seq[PropertyValue]]()
                  ): Result[void, MqttError] =
  ## Publish with QoS 0 (fire and forget). No ack expected.
  let pkt = MqttPacket(packet_type: ptPublish, dup: false, publish_qos: qos0,
                       retain: retain, topic: topic, publish_packet_id: 0,
                       publish_props: props, payload: payload)
  try:
    send_packet(pub.conn, pkt)
    Result[void, MqttError](ok: true)
  except MqttError as e:
    Result[void, MqttError].bad(e[])

proc publish_qos1*(pub: var MqttPublisher, topic: string, payload: string,
                   retain: bool = false,
                   props: Properties = initOrderedTable[uint8, seq[PropertyValue]]()
                  ): Result[uint16, MqttError] =
  ## Publish with QoS 1 (at-least-once). Waits for PUBACK.
  let pid = pub.next_id()
  let pkt = MqttPacket(packet_type: ptPublish, dup: false, publish_qos: qos1,
                       retain: retain, topic: topic, publish_packet_id: pid,
                       publish_props: props, payload: payload)
  try:
    send_packet(pub.conn, pkt)
    let ack = recv_packet(pub.conn)
    if ack.packet_type != ptPuback:
      return Result[uint16, MqttError].bad(MqttError(msg: "expected PUBACK, got " & $ack.packet_type))
    if ack.puback_packet_id != pid:
      return Result[uint16, MqttError].bad(MqttError(msg: "PUBACK packet ID mismatch"))
    if ack.puback_reason >= 0x80:
      return Result[uint16, MqttError].bad(MqttError(msg: "PUBACK rejected: " & $ack.puback_reason))
    Result[uint16, MqttError].good(pid)
  except MqttError as e:
    Result[uint16, MqttError].bad(e[])

proc publish_qos2*(pub: var MqttPublisher, topic: string, payload: string,
                   retain: bool = false,
                   props: Properties = initOrderedTable[uint8, seq[PropertyValue]]()
                  ): Result[uint16, MqttError] =
  ## Publish with QoS 2 (exactly-once). PUBLISH -> PUBREC -> PUBREL -> PUBCOMP.
  let pid = pub.next_id()
  let pkt = MqttPacket(packet_type: ptPublish, dup: false, publish_qos: qos2,
                       retain: retain, topic: topic, publish_packet_id: pid,
                       publish_props: props, payload: payload)
  try:
    send_packet(pub.conn, pkt)
    # Wait for PUBREC
    let pubrec = recv_packet(pub.conn)
    if pubrec.packet_type != ptPubrec:
      return Result[uint16, MqttError].bad(MqttError(msg: "expected PUBREC, got " & $pubrec.packet_type))
    if pubrec.pubrec_packet_id != pid:
      return Result[uint16, MqttError].bad(MqttError(msg: "PUBREC packet ID mismatch"))
    if pubrec.pubrec_reason >= 0x80:
      return Result[uint16, MqttError].bad(MqttError(msg: "PUBREC rejected: " & $pubrec.pubrec_reason))
    # Send PUBREL
    let pubrel = MqttPacket(packet_type: ptPubrel, pubrel_packet_id: pid,
                            pubrel_reason: rcSuccess,
                            pubrel_props: initOrderedTable[uint8, seq[PropertyValue]]())
    send_packet(pub.conn, pubrel)
    # Wait for PUBCOMP
    let pubcomp = recv_packet(pub.conn)
    if pubcomp.packet_type != ptPubcomp:
      return Result[uint16, MqttError].bad(MqttError(msg: "expected PUBCOMP, got " & $pubcomp.packet_type))
    if pubcomp.pubcomp_packet_id != pid:
      return Result[uint16, MqttError].bad(MqttError(msg: "PUBCOMP packet ID mismatch"))
    Result[uint16, MqttError].good(pid)
  except MqttError as e:
    Result[uint16, MqttError].bad(e[])
