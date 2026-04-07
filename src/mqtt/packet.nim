## packet.nim -- MQTT 5.0 packet codec. Pure encode/decode, no I/O.
##
## Covers all 15 control packet types. Fixed header: 1 byte (type + flags) +
## variable-length remaining length (1-4 bytes). Variable header and payload
## per packet type. Properties encoded as variable-length property section.

{.experimental: "strict_funcs".}

import std/tables
import basis/code/choice

# =====================================================================================================================
# Errors
# =====================================================================================================================

type
  MqttError* = object of CatchableError

# =====================================================================================================================
# Constants
# =====================================================================================================================

const
  ProtocolName* = "MQTT"
  ProtocolVersion5* = 5'u8

  # Packet type codes (upper 4 bits of first byte)
  ptConnect*     = 1'u8
  ptConnack*     = 2'u8
  ptPublish*     = 3'u8
  ptPuback*      = 4'u8
  ptPubrec*      = 5'u8
  ptPubrel*      = 6'u8
  ptPubcomp*     = 7'u8
  ptSubscribe*   = 8'u8
  ptSuback*      = 9'u8
  ptUnsubscribe* = 10'u8
  ptUnsuback*    = 11'u8
  ptPingreq*     = 12'u8
  ptPingresp*    = 13'u8
  ptDisconnect*  = 14'u8
  ptAuth*        = 15'u8

  # Property identifiers
  propPayloadFormatIndicator*    = 0x01'u8
  propMessageExpiryInterval*     = 0x02'u8
  propContentType*               = 0x03'u8
  propResponseTopic*             = 0x08'u8
  propCorrelationData*           = 0x09'u8
  propSubscriptionIdentifier*    = 0x0B'u8
  propSessionExpiryInterval*     = 0x11'u8
  propAssignedClientIdentifier*  = 0x12'u8
  propServerKeepAlive*           = 0x13'u8
  propAuthenticationMethod*      = 0x15'u8
  propAuthenticationData*        = 0x16'u8
  propRequestProblemInformation* = 0x17'u8
  propWillDelayInterval*         = 0x19'u8
  propRequestResponseInformation* = 0x19'u8  # Note: same as will delay in spec context
  propResponseInformation*       = 0x1A'u8
  propServerReference*           = 0x1C'u8
  propReasonString*              = 0x1F'u8
  propReceiveMaximum*            = 0x21'u8
  propTopicAliasMaximum*         = 0x22'u8
  propTopicAlias*                = 0x23'u8
  propMaximumQoS*                = 0x24'u8
  propRetainAvailable*           = 0x25'u8
  propUserProperty*              = 0x26'u8
  propMaximumPacketSize*         = 0x27'u8
  propWildcardSubscriptionAvailable* = 0x28'u8
  propSubscriptionIdentifierAvailable* = 0x29'u8
  propSharedSubscriptionAvailable* = 0x2A'u8

  # Reason codes (common subset)
  rcSuccess*                = 0x00'u8
  rcNormalDisconnection*    = 0x00'u8
  rcGrantedQoS0*            = 0x00'u8
  rcGrantedQoS1*            = 0x01'u8
  rcGrantedQoS2*            = 0x02'u8
  rcDisconnectWithWill*     = 0x04'u8
  rcNoMatchingSubscribers*  = 0x10'u8
  rcUnspecifiedError*       = 0x80'u8
  rcMalformedPacket*        = 0x81'u8
  rcProtocolError*          = 0x82'u8
  rcImplementationSpecific* = 0x83'u8
  rcUnsupportedProtocol*    = 0x84'u8
  rcClientIdNotValid*       = 0x85'u8
  rcBadUserNameOrPassword*  = 0x86'u8
  rcNotAuthorized*          = 0x87'u8
  rcServerUnavailable*      = 0x88'u8
  rcServerBusy*             = 0x89'u8
  rcBanned*                 = 0x8A'u8
  rcTopicNameInvalid*       = 0x90'u8
  rcPacketTooLarge*         = 0x95'u8
  rcQuotaExceeded*          = 0x97'u8
  rcPayloadFormatInvalid*   = 0x99'u8
  rcRetainNotSupported*     = 0x9A'u8
  rcQoSNotSupported*        = 0x9B'u8
  rcUseAnotherServer*       = 0x9C'u8
  rcServerMoved*            = 0x9D'u8
  rcSharedSubscriptionsNotSupported* = 0x9E'u8
  rcConnectionRateExceeded* = 0x9F'u8
  rcTopicAliasInvalid*      = 0x94'u8
  rcTopicFilterInvalid*     = 0xA1'u8

# =====================================================================================================================
# Types
# =====================================================================================================================

type
  QoS* = enum
    qos0 = 0
    qos1 = 1
    qos2 = 2

  PropertyValue* = object
    case is_pair*: bool
    of true:
      key*: string
      pair_val*: string
    of false:
      case has_str*: bool
      of true:
        str_val*: string
      of false:
        case has_bin*: bool
        of true:
          bin_val*: string  # binary data stored as string
        of false:
          int_val*: uint32

  Properties* = OrderedTable[uint8, seq[PropertyValue]]

  ConnectFlags* = object
    clean_start*: bool
    will*: bool
    will_qos*: QoS
    will_retain*: bool
    username*: bool
    password*: bool

  WillConfig* = object
    qos*: QoS
    retain*: bool
    topic*: string
    payload*: string
    properties*: Properties

  Subscription* = object
    topic*: string
    qos*: QoS
    no_local*: bool
    retain_as_published*: bool
    retain_handling*: uint8  # 0, 1, or 2

  MqttPacket* = object
    case packet_type*: uint8
    of ptConnect:
      connect_flags*: ConnectFlags
      keep_alive*: uint16
      connect_props*: Properties
      client_id*: string
      will_config*: Choice[WillConfig]
      username*: string
      password*: string
    of ptConnack:
      session_present*: bool
      connack_reason*: uint8
      connack_props*: Properties
    of ptPublish:
      dup*: bool
      publish_qos*: QoS
      retain*: bool
      topic*: string
      publish_packet_id*: uint16  # only for QoS 1/2
      publish_props*: Properties
      payload*: string
    of ptPuback:
      puback_packet_id*: uint16
      puback_reason*: uint8
      puback_props*: Properties
    of ptPubrec:
      pubrec_packet_id*: uint16
      pubrec_reason*: uint8
      pubrec_props*: Properties
    of ptPubrel:
      pubrel_packet_id*: uint16
      pubrel_reason*: uint8
      pubrel_props*: Properties
    of ptPubcomp:
      pubcomp_packet_id*: uint16
      pubcomp_reason*: uint8
      pubcomp_props*: Properties
    of ptSubscribe:
      subscribe_packet_id*: uint16
      subscribe_props*: Properties
      subscriptions*: seq[Subscription]
    of ptSuback:
      suback_packet_id*: uint16
      suback_props*: Properties
      suback_reasons*: seq[uint8]
    of ptUnsubscribe:
      unsubscribe_packet_id*: uint16
      unsubscribe_props*: Properties
      unsubscribe_topics*: seq[string]
    of ptUnsuback:
      unsuback_packet_id*: uint16
      unsuback_props*: Properties
      unsuback_reasons*: seq[uint8]
    of ptPingreq:
      discard
    of ptPingresp:
      discard
    of ptDisconnect:
      disconnect_reason*: uint8
      disconnect_props*: Properties
    of ptAuth:
      auth_reason*: uint8
      auth_props*: Properties
    else:
      discard

# =====================================================================================================================
# Variable-length integer encode/decode
# =====================================================================================================================

proc encode_varint*(value: uint32): string {.raises: [MqttError].} =
  ## Encode a variable-length integer (1-4 bytes, 7 bits per byte, continuation bit).
  if value > 268_435_455'u32:
    raise newException(MqttError, "varint value too large")
  result = ""
  var v = value
  while true:
    var encoded_byte = uint8(v mod 128)
    v = v div 128
    if v > 0:
      encoded_byte = encoded_byte or 0x80
    result.add(char(encoded_byte))
    if v == 0:
      break

proc decode_varint*(buf: string, pos: var int): uint32 {.raises: [MqttError].} =
  ## Decode a variable-length integer from buf starting at pos. Advances pos.
  var multiplier = 1'u32
  result = 0
  for i in 0 ..< 4:
    if pos >= buf.len:
      raise newException(MqttError, "varint: unexpected end of buffer")
    let encoded_byte = uint8(buf[pos])
    inc pos
    result = result + uint32(encoded_byte and 0x7F) * multiplier
    if (encoded_byte and 0x80) == 0:
      return
    multiplier *= 128
  raise newException(MqttError, "varint: malformed (more than 4 bytes)")

# =====================================================================================================================
# Low-level encode helpers
# =====================================================================================================================

proc encode_uint16(value: uint16): string =
  result = newString(2)
  result[0] = char(value shr 8)
  result[1] = char(value and 0xFF)

proc encode_uint32(value: uint32): string =
  result = newString(4)
  result[0] = char((value shr 24) and 0xFF)
  result[1] = char((value shr 16) and 0xFF)
  result[2] = char((value shr 8) and 0xFF)
  result[3] = char(value and 0xFF)

proc encode_utf8_string(s: string): string =
  ## Length-prefixed UTF-8 string (2-byte length + data).
  result = encode_uint16(uint16(s.len)) & s

proc encode_binary_data(data: string): string =
  ## Length-prefixed binary data (2-byte length + data).
  result = encode_uint16(uint16(data.len)) & data

# =====================================================================================================================
# Low-level decode helpers
# =====================================================================================================================

proc decode_uint16(buf: string, pos: var int): uint16 {.raises: [MqttError].} =
  if pos + 2 > buf.len:
    raise newException(MqttError, "unexpected end of buffer reading uint16")
  result = uint16(uint8(buf[pos])) shl 8 or uint16(uint8(buf[pos + 1]))
  pos += 2

proc decode_uint32(buf: string, pos: var int): uint32 {.raises: [MqttError].} =
  if pos + 4 > buf.len:
    raise newException(MqttError, "unexpected end of buffer reading uint32")
  result = uint32(uint8(buf[pos])) shl 24 or
           uint32(uint8(buf[pos + 1])) shl 16 or
           uint32(uint8(buf[pos + 2])) shl 8 or
           uint32(uint8(buf[pos + 3]))
  pos += 4

proc decode_utf8_string(buf: string, pos: var int): string {.raises: [MqttError].} =
  let length = int(decode_uint16(buf, pos))
  if pos + length > buf.len:
    raise newException(MqttError, "unexpected end of buffer reading UTF-8 string")
  result = buf[pos ..< pos + length]
  pos += length

proc decode_binary_data(buf: string, pos: var int): string {.raises: [MqttError].} =
  decode_utf8_string(buf, pos)  # same encoding

# =====================================================================================================================
# Property encode/decode
# =====================================================================================================================

proc prop_val_byte*(v: uint8): PropertyValue =
  PropertyValue(is_pair: false, has_str: false, has_bin: false, int_val: uint32(v))

proc prop_val_uint16*(v: uint16): PropertyValue =
  PropertyValue(is_pair: false, has_str: false, has_bin: false, int_val: uint32(v))

proc prop_val_uint32*(v: uint32): PropertyValue =
  PropertyValue(is_pair: false, has_str: false, has_bin: false, int_val: v)

proc prop_val_str*(s: string): PropertyValue =
  PropertyValue(is_pair: false, has_str: true, str_val: s)

proc prop_val_bin*(data: string): PropertyValue =
  PropertyValue(is_pair: false, has_str: false, has_bin: true, bin_val: data)

proc prop_val_pair*(k, v: string): PropertyValue =
  PropertyValue(is_pair: true, key: k, pair_val: v)

type PropType = enum
  ptByte, ptUint16, ptUint32, ptVarInt, ptStr, ptBin, ptPair

proc prop_type(id: uint8): PropType =
  case id
  of propPayloadFormatIndicator, propRequestProblemInformation,
     propMaximumQoS, propRetainAvailable,
     propWildcardSubscriptionAvailable, propSubscriptionIdentifierAvailable,
     propSharedSubscriptionAvailable:
    ptByte
  of propReceiveMaximum, propTopicAliasMaximum, propTopicAlias,
     propServerKeepAlive:
    ptUint16
  of propMessageExpiryInterval, propSessionExpiryInterval,
     propWillDelayInterval, propMaximumPacketSize:
    ptUint32
  of propSubscriptionIdentifier:
    ptVarInt
  of propContentType, propResponseTopic, propAssignedClientIdentifier,
     propAuthenticationMethod, propResponseInformation,
     propServerReference, propReasonString:
    ptStr
  of propCorrelationData, propAuthenticationData:
    ptBin
  of propUserProperty:
    ptPair
  else:
    ptByte  # unknown: treat as byte

proc encode_properties*(props: Properties): string {.raises: [MqttError].} =
  ## Encode properties to bytes (without the length prefix).
  result = ""
  for id, values in props:
    for pv in values:
      result.add(char(id))
      if pv.is_pair:
        result.add(encode_utf8_string(pv.key))
        result.add(encode_utf8_string(pv.pair_val))
      elif pv.has_str:
        result.add(encode_utf8_string(pv.str_val))
      elif pv.has_bin:
        result.add(encode_binary_data(pv.bin_val))
      else:
        let pt = prop_type(id)
        case pt
        of ptByte:
          result.add(char(uint8(pv.int_val)))
        of ptUint16:
          result.add(encode_uint16(uint16(pv.int_val)))
        of ptUint32:
          result.add(encode_uint32(pv.int_val))
        of ptVarInt:
          result.add(encode_varint(pv.int_val))
        else:
          result.add(char(uint8(pv.int_val)))

proc encode_properties_with_length*(props: Properties): string {.raises: [MqttError].} =
  ## Encode properties with variable-length length prefix.
  let body = encode_properties(props)
  result = encode_varint(uint32(body.len)) & body

proc decode_properties*(buf: string, pos: var int): Properties {.raises: [MqttError].} =
  ## Decode properties section: varint length + property entries.
  result = initOrderedTable[uint8, seq[PropertyValue]]()
  let prop_len = int(decode_varint(buf, pos))
  let end_pos = pos + prop_len
  if end_pos > buf.len:
    raise newException(MqttError, "properties: unexpected end of buffer")
  while pos < end_pos:
    let id = uint8(buf[pos])
    inc pos
    let pt = prop_type(id)
    var pv: PropertyValue
    case pt
    of ptByte:
      if pos >= buf.len:
        raise newException(MqttError, "properties: unexpected end reading byte")
      pv = prop_val_byte(uint8(buf[pos]))
      inc pos
    of ptUint16:
      pv = prop_val_uint16(decode_uint16(buf, pos))
    of ptUint32:
      pv = prop_val_uint32(decode_uint32(buf, pos))
    of ptVarInt:
      pv = prop_val_uint32(decode_varint(buf, pos))
    of ptStr:
      pv = prop_val_str(decode_utf8_string(buf, pos))
    of ptBin:
      pv = prop_val_bin(decode_binary_data(buf, pos))
    of ptPair:
      let k = decode_utf8_string(buf, pos)
      let v = decode_utf8_string(buf, pos)
      pv = prop_val_pair(k, v)
    result.mgetOrPut(id, @[]).add(pv)

# =====================================================================================================================
# Packet encoding
# =====================================================================================================================

proc build_fixed_header(packet_type: uint8, flags: uint8, remaining: string): string {.raises: [MqttError].} =
  let first_byte = char((packet_type shl 4) or (flags and 0x0F))
  result = $first_byte & encode_varint(uint32(remaining.len)) & remaining

proc encode_connect*(pkt: MqttPacket): string {.raises: [MqttError].} =
  ## Encode a CONNECT packet.
  var variable = ""
  # Protocol name
  variable.add(encode_utf8_string(ProtocolName))
  # Protocol level
  variable.add(char(ProtocolVersion5))
  # Connect flags
  var flags = 0'u8
  if pkt.connect_flags.clean_start: flags = flags or 0x02
  if pkt.connect_flags.will:
    flags = flags or 0x04
    flags = flags or (uint8(pkt.connect_flags.will_qos) shl 3)
    if pkt.connect_flags.will_retain: flags = flags or 0x20
  if pkt.connect_flags.password: flags = flags or 0x40
  if pkt.connect_flags.username: flags = flags or 0x80
  variable.add(char(flags))
  # Keep alive
  variable.add(encode_uint16(pkt.keep_alive))
  # Properties
  variable.add(encode_properties_with_length(pkt.connect_props))
  # Payload: client ID
  variable.add(encode_utf8_string(pkt.client_id))
  # Will properties + will topic + will payload
  if pkt.connect_flags.will and pkt.will_config.is_good:
    let wc = pkt.will_config.val
    variable.add(encode_properties_with_length(wc.properties))
    variable.add(encode_utf8_string(wc.topic))
    variable.add(encode_binary_data(wc.payload))
  # Username
  if pkt.connect_flags.username:
    variable.add(encode_utf8_string(pkt.username))
  # Password
  if pkt.connect_flags.password:
    variable.add(encode_binary_data(pkt.password))
  build_fixed_header(ptConnect, 0, variable)

proc encode_connack*(pkt: MqttPacket): string {.raises: [MqttError].} =
  var variable = ""
  variable.add(char(if pkt.session_present: 1'u8 else: 0'u8))
  variable.add(char(pkt.connack_reason))
  variable.add(encode_properties_with_length(pkt.connack_props))
  build_fixed_header(ptConnack, 0, variable)

proc encode_publish*(pkt: MqttPacket): string {.raises: [MqttError].} =
  var flags = 0'u8
  if pkt.dup: flags = flags or 0x08
  flags = flags or (uint8(pkt.publish_qos) shl 1)
  if pkt.retain: flags = flags or 0x01
  var variable = ""
  variable.add(encode_utf8_string(pkt.topic))
  if pkt.publish_qos != qos0:
    variable.add(encode_uint16(pkt.publish_packet_id))
  variable.add(encode_properties_with_length(pkt.publish_props))
  variable.add(pkt.payload)
  build_fixed_header(ptPublish, flags, variable)

proc encode_ack_packet(ptype: uint8, flags: uint8, packet_id: uint16, reason: uint8,
                       props: Properties): string {.raises: [MqttError].} =
  ## Encode PUBACK, PUBREC, PUBREL, PUBCOMP, or similar ack packets.
  var variable = ""
  variable.add(encode_uint16(packet_id))
  # If reason is 0 and no properties, we can omit them (MQTT 5.0 optimization)
  if reason != 0 or props.len > 0:
    variable.add(char(reason))
    if props.len > 0:
      variable.add(encode_properties_with_length(props))
  build_fixed_header(ptype, flags, variable)

proc encode_puback*(pkt: MqttPacket): string {.raises: [MqttError].} =
  encode_ack_packet(ptPuback, 0, pkt.puback_packet_id, pkt.puback_reason, pkt.puback_props)

proc encode_pubrec*(pkt: MqttPacket): string {.raises: [MqttError].} =
  encode_ack_packet(ptPubrec, 0, pkt.pubrec_packet_id, pkt.pubrec_reason, pkt.pubrec_props)

proc encode_pubrel*(pkt: MqttPacket): string {.raises: [MqttError].} =
  encode_ack_packet(ptPubrel, 0x02, pkt.pubrel_packet_id, pkt.pubrel_reason, pkt.pubrel_props)

proc encode_pubcomp*(pkt: MqttPacket): string {.raises: [MqttError].} =
  encode_ack_packet(ptPubcomp, 0, pkt.pubcomp_packet_id, pkt.pubcomp_reason, pkt.pubcomp_props)

proc encode_subscribe*(pkt: MqttPacket): string {.raises: [MqttError].} =
  var variable = ""
  variable.add(encode_uint16(pkt.subscribe_packet_id))
  variable.add(encode_properties_with_length(pkt.subscribe_props))
  for sub in pkt.subscriptions:
    variable.add(encode_utf8_string(sub.topic))
    var opts = uint8(sub.qos)
    if sub.no_local: opts = opts or 0x04
    if sub.retain_as_published: opts = opts or 0x08
    opts = opts or (sub.retain_handling shl 4)
    variable.add(char(opts))
  build_fixed_header(ptSubscribe, 0x02, variable)  # flags: 0x02 reserved

proc encode_suback*(pkt: MqttPacket): string {.raises: [MqttError].} =
  var variable = ""
  variable.add(encode_uint16(pkt.suback_packet_id))
  variable.add(encode_properties_with_length(pkt.suback_props))
  for rc in pkt.suback_reasons:
    variable.add(char(rc))
  build_fixed_header(ptSuback, 0, variable)

proc encode_unsubscribe*(pkt: MqttPacket): string {.raises: [MqttError].} =
  var variable = ""
  variable.add(encode_uint16(pkt.unsubscribe_packet_id))
  variable.add(encode_properties_with_length(pkt.unsubscribe_props))
  for topic in pkt.unsubscribe_topics:
    variable.add(encode_utf8_string(topic))
  build_fixed_header(ptUnsubscribe, 0x02, variable)

proc encode_unsuback*(pkt: MqttPacket): string {.raises: [MqttError].} =
  var variable = ""
  variable.add(encode_uint16(pkt.unsuback_packet_id))
  variable.add(encode_properties_with_length(pkt.unsuback_props))
  for rc in pkt.unsuback_reasons:
    variable.add(char(rc))
  build_fixed_header(ptUnsuback, 0, variable)

proc encode_pingreq*(): string {.raises: [MqttError].} =
  build_fixed_header(ptPingreq, 0, "")

proc encode_pingresp*(): string {.raises: [MqttError].} =
  build_fixed_header(ptPingresp, 0, "")

proc encode_disconnect*(pkt: MqttPacket): string {.raises: [MqttError].} =
  var variable = ""
  if pkt.disconnect_reason != 0 or pkt.disconnect_props.len > 0:
    variable.add(char(pkt.disconnect_reason))
    if pkt.disconnect_props.len > 0:
      variable.add(encode_properties_with_length(pkt.disconnect_props))
  build_fixed_header(ptDisconnect, 0, variable)

proc encode_auth*(pkt: MqttPacket): string {.raises: [MqttError].} =
  var variable = ""
  variable.add(char(pkt.auth_reason))
  variable.add(encode_properties_with_length(pkt.auth_props))
  build_fixed_header(ptAuth, 0, variable)

proc encode*(pkt: MqttPacket): string {.raises: [MqttError].} =
  ## Encode any MqttPacket to wire format.
  case pkt.packet_type
  of ptConnect: encode_connect(pkt)
  of ptConnack: encode_connack(pkt)
  of ptPublish: encode_publish(pkt)
  of ptPuback: encode_puback(pkt)
  of ptPubrec: encode_pubrec(pkt)
  of ptPubrel: encode_pubrel(pkt)
  of ptPubcomp: encode_pubcomp(pkt)
  of ptSubscribe: encode_subscribe(pkt)
  of ptSuback: encode_suback(pkt)
  of ptUnsubscribe: encode_unsubscribe(pkt)
  of ptUnsuback: encode_unsuback(pkt)
  of ptPingreq: encode_pingreq()
  of ptPingresp: encode_pingresp()
  of ptDisconnect: encode_disconnect(pkt)
  of ptAuth: encode_auth(pkt)
  else:
    raise newException(MqttError, "unknown packet type: " & $pkt.packet_type)

# =====================================================================================================================
# Packet decoding
# =====================================================================================================================

proc decode_connect(buf: string, pos: var int, end_pos: int): MqttPacket {.raises: [MqttError].} =
  # Protocol name
  let proto_name = decode_utf8_string(buf, pos)
  if proto_name != ProtocolName:
    raise newException(MqttError, "invalid protocol name: " & proto_name)
  # Protocol version
  if pos >= end_pos:
    raise newException(MqttError, "connect: unexpected end of buffer")
  let proto_ver = uint8(buf[pos])
  inc pos
  if proto_ver != ProtocolVersion5:
    raise newException(MqttError, "unsupported protocol version: " & $proto_ver)
  # Connect flags
  if pos >= end_pos:
    raise newException(MqttError, "connect: unexpected end of buffer")
  let flag_byte = uint8(buf[pos])
  inc pos
  var cf: ConnectFlags
  cf.clean_start = (flag_byte and 0x02) != 0
  cf.will = (flag_byte and 0x04) != 0
  cf.will_qos = QoS((flag_byte shr 3) and 0x03)
  cf.will_retain = (flag_byte and 0x20) != 0
  cf.password = (flag_byte and 0x40) != 0
  cf.username = (flag_byte and 0x80) != 0
  # Keep alive
  let ka = decode_uint16(buf, pos)
  # Properties
  let props = decode_properties(buf, pos)
  # Client ID
  let cid = decode_utf8_string(buf, pos)
  # Will
  var wc: Choice[WillConfig] = choice.none[WillConfig]()
  if cf.will:
    var w: WillConfig
    w.qos = cf.will_qos
    w.retain = cf.will_retain
    w.properties = decode_properties(buf, pos)
    w.topic = decode_utf8_string(buf, pos)
    w.payload = decode_binary_data(buf, pos)
    wc = good(w)
  # Username
  var uname = ""
  if cf.username:
    uname = decode_utf8_string(buf, pos)
  # Password
  var pw = ""
  if cf.password:
    pw = decode_binary_data(buf, pos)
  result = MqttPacket(packet_type: ptConnect, connect_flags: cf, keep_alive: ka,
                      connect_props: props, client_id: cid, will_config: wc,
                      username: uname, password: pw)

proc decode_connack(buf: string, pos: var int, end_pos: int): MqttPacket {.raises: [MqttError].} =
  if pos + 2 > end_pos:
    raise newException(MqttError, "connack: too short")
  let sp = (uint8(buf[pos]) and 0x01) != 0
  inc pos
  let reason = uint8(buf[pos])
  inc pos
  var props = initOrderedTable[uint8, seq[PropertyValue]]()
  if pos < end_pos:
    props = decode_properties(buf, pos)
  result = MqttPacket(packet_type: ptConnack, session_present: sp,
                      connack_reason: reason, connack_props: props)

proc decode_publish(buf: string, pos: var int, end_pos: int, flags: uint8): MqttPacket {.raises: [MqttError].} =
  let dup_flag = (flags and 0x08) != 0
  let qos = QoS((flags shr 1) and 0x03)
  let retain_flag = (flags and 0x01) != 0
  let topic = decode_utf8_string(buf, pos)
  var pid = 0'u16
  if qos != qos0:
    pid = decode_uint16(buf, pos)
  let props = decode_properties(buf, pos)
  let payload = buf[pos ..< end_pos]
  pos = end_pos
  result = MqttPacket(packet_type: ptPublish, dup: dup_flag, publish_qos: qos,
                      retain: retain_flag, topic: topic, publish_packet_id: pid,
                      publish_props: props, payload: payload)

proc decode_ack(buf: string, pos: var int, end_pos: int, ptype: uint8): tuple[pid: uint16, reason: uint8, props: Properties] {.raises: [MqttError].} =
  result.pid = decode_uint16(buf, pos)
  result.reason = rcSuccess
  result.props = initOrderedTable[uint8, seq[PropertyValue]]()
  if pos < end_pos:
    result.reason = uint8(buf[pos])
    inc pos
    if pos < end_pos:
      result.props = decode_properties(buf, pos)

proc decode_subscribe(buf: string, pos: var int, end_pos: int): MqttPacket {.raises: [MqttError].} =
  let pid = decode_uint16(buf, pos)
  let props = decode_properties(buf, pos)
  var subs: seq[Subscription]
  while pos < end_pos:
    var sub: Subscription
    sub.topic = decode_utf8_string(buf, pos)
    if pos >= end_pos:
      raise newException(MqttError, "subscribe: missing subscription options")
    let opts = uint8(buf[pos])
    inc pos
    sub.qos = QoS(opts and 0x03)
    sub.no_local = (opts and 0x04) != 0
    sub.retain_as_published = (opts and 0x08) != 0
    sub.retain_handling = (opts shr 4) and 0x03
    subs.add(sub)
  result = MqttPacket(packet_type: ptSubscribe, subscribe_packet_id: pid,
                      subscribe_props: props, subscriptions: subs)

proc decode_suback(buf: string, pos: var int, end_pos: int): MqttPacket {.raises: [MqttError].} =
  let pid = decode_uint16(buf, pos)
  let props = decode_properties(buf, pos)
  var reasons: seq[uint8]
  while pos < end_pos:
    reasons.add(uint8(buf[pos]))
    inc pos
  result = MqttPacket(packet_type: ptSuback, suback_packet_id: pid,
                      suback_props: props, suback_reasons: reasons)

proc decode_unsubscribe(buf: string, pos: var int, end_pos: int): MqttPacket {.raises: [MqttError].} =
  let pid = decode_uint16(buf, pos)
  let props = decode_properties(buf, pos)
  var topics: seq[string]
  while pos < end_pos:
    topics.add(decode_utf8_string(buf, pos))
  result = MqttPacket(packet_type: ptUnsubscribe, unsubscribe_packet_id: pid,
                      unsubscribe_props: props, unsubscribe_topics: topics)

proc decode_unsuback(buf: string, pos: var int, end_pos: int): MqttPacket {.raises: [MqttError].} =
  let pid = decode_uint16(buf, pos)
  let props = decode_properties(buf, pos)
  var reasons: seq[uint8]
  while pos < end_pos:
    reasons.add(uint8(buf[pos]))
    inc pos
  result = MqttPacket(packet_type: ptUnsuback, unsuback_packet_id: pid,
                      unsuback_props: props, unsuback_reasons: reasons)

proc decode_disconnect(buf: string, pos: var int, end_pos: int): MqttPacket {.raises: [MqttError].} =
  var reason = rcNormalDisconnection
  var props = initOrderedTable[uint8, seq[PropertyValue]]()
  if pos < end_pos:
    reason = uint8(buf[pos])
    inc pos
    if pos < end_pos:
      props = decode_properties(buf, pos)
  result = MqttPacket(packet_type: ptDisconnect, disconnect_reason: reason,
                      disconnect_props: props)

proc decode_auth(buf: string, pos: var int, end_pos: int): MqttPacket {.raises: [MqttError].} =
  var reason = rcSuccess
  var props = initOrderedTable[uint8, seq[PropertyValue]]()
  if pos < end_pos:
    reason = uint8(buf[pos])
    inc pos
    if pos < end_pos:
      props = decode_properties(buf, pos)
  result = MqttPacket(packet_type: ptAuth, auth_reason: reason, auth_props: props)

proc decode*(buf: string, pos: var int): MqttPacket {.raises: [MqttError].} =
  ## Decode one MQTT packet from buf starting at pos. Advances pos past the packet.
  if pos >= buf.len:
    raise newException(MqttError, "empty buffer")
  let first_byte = uint8(buf[pos])
  inc pos
  let ptype = first_byte shr 4
  let flags = first_byte and 0x0F
  let remaining_len = int(decode_varint(buf, pos))
  let end_pos = pos + remaining_len
  if end_pos > buf.len:
    raise newException(MqttError, "packet extends beyond buffer")
  result = case ptype
    of ptConnect: decode_connect(buf, pos, end_pos)
    of ptConnack: decode_connack(buf, pos, end_pos)
    of ptPublish: decode_publish(buf, pos, end_pos, flags)
    of ptPuback:
      let a = decode_ack(buf, pos, end_pos, ptPuback)
      MqttPacket(packet_type: ptPuback, puback_packet_id: a.pid,
                 puback_reason: a.reason, puback_props: a.props)
    of ptPubrec:
      let a = decode_ack(buf, pos, end_pos, ptPubrec)
      MqttPacket(packet_type: ptPubrec, pubrec_packet_id: a.pid,
                 pubrec_reason: a.reason, pubrec_props: a.props)
    of ptPubrel:
      let a = decode_ack(buf, pos, end_pos, ptPubrel)
      MqttPacket(packet_type: ptPubrel, pubrel_packet_id: a.pid,
                 pubrel_reason: a.reason, pubrel_props: a.props)
    of ptPubcomp:
      let a = decode_ack(buf, pos, end_pos, ptPubcomp)
      MqttPacket(packet_type: ptPubcomp, pubcomp_packet_id: a.pid,
                 pubcomp_reason: a.reason, pubcomp_props: a.props)
    of ptSubscribe: decode_subscribe(buf, pos, end_pos)
    of ptSuback: decode_suback(buf, pos, end_pos)
    of ptUnsubscribe: decode_unsubscribe(buf, pos, end_pos)
    of ptUnsuback: decode_unsuback(buf, pos, end_pos)
    of ptPingreq:
      pos = end_pos
      MqttPacket(packet_type: ptPingreq)
    of ptPingresp:
      pos = end_pos
      MqttPacket(packet_type: ptPingresp)
    of ptDisconnect: decode_disconnect(buf, pos, end_pos)
    of ptAuth: decode_auth(buf, pos, end_pos)
    else:
      raise newException(MqttError, "unknown packet type: " & $ptype)
  pos = end_pos
