## tpacket.nim -- Unit tests for MQTT 5.0 packet codec.
## Pure encode/decode round-trip tests, no broker needed.

{.experimental: "strict_funcs".}

import std/[unittest, tables, options]
import mqtt/[packet]

when not declared(empty_props):
  let empty_props = initOrderedTable[uint8, seq[PropertyValue]]()

# =====================================================================================================================
# Variable-length integer
# =====================================================================================================================

suite "varint":
  test "encode/decode 0":
    let enc = encode_varint(0)
    check enc.len == 1
    check uint8(enc[0]) == 0
    var pos = 0
    check decode_varint(enc, pos) == 0'u32
    check pos == 1

  test "encode/decode 127 (single byte max)":
    let enc = encode_varint(127)
    check enc.len == 1
    var pos = 0
    check decode_varint(enc, pos) == 127'u32

  test "encode/decode 128 (two bytes)":
    let enc = encode_varint(128)
    check enc.len == 2
    var pos = 0
    check decode_varint(enc, pos) == 128'u32

  test "encode/decode 16383 (two byte max)":
    let enc = encode_varint(16383)
    check enc.len == 2
    var pos = 0
    check decode_varint(enc, pos) == 16383'u32

  test "encode/decode 16384 (three bytes)":
    let enc = encode_varint(16384)
    check enc.len == 3
    var pos = 0
    check decode_varint(enc, pos) == 16384'u32

  test "encode/decode 2097151 (three byte max)":
    let enc = encode_varint(2097151)
    check enc.len == 3
    var pos = 0
    check decode_varint(enc, pos) == 2097151'u32

  test "encode/decode 2097152 (four bytes)":
    let enc = encode_varint(2097152)
    check enc.len == 4
    var pos = 0
    check decode_varint(enc, pos) == 2097152'u32

  test "encode/decode 268435455 (four byte max)":
    let enc = encode_varint(268435455)
    check enc.len == 4
    var pos = 0
    check decode_varint(enc, pos) == 268435455'u32

  test "varint too large raises":
    expect MqttError:
      discard encode_varint(268435456'u32)

# =====================================================================================================================
# Properties
# =====================================================================================================================

suite "properties":
  test "empty properties round-trip":
    let encoded = encode_properties_with_length(empty_props)
    var pos = 0
    let decoded = decode_properties(encoded, pos)
    check decoded.len == 0

  test "byte property round-trip":
    var props = initOrderedTable[uint8, seq[PropertyValue]]()
    props[propPayloadFormatIndicator] = @[prop_val_byte(1)]
    let encoded = encode_properties_with_length(props)
    var pos = 0
    let decoded = decode_properties(encoded, pos)
    check decoded.len == 1
    check decoded[propPayloadFormatIndicator][0].int_val == 1

  test "uint16 property round-trip":
    var props = initOrderedTable[uint8, seq[PropertyValue]]()
    props[propReceiveMaximum] = @[prop_val_uint16(1024)]
    let encoded = encode_properties_with_length(props)
    var pos = 0
    let decoded = decode_properties(encoded, pos)
    check decoded[propReceiveMaximum][0].int_val == 1024

  test "uint32 property round-trip":
    var props = initOrderedTable[uint8, seq[PropertyValue]]()
    props[propSessionExpiryInterval] = @[prop_val_uint32(3600)]
    let encoded = encode_properties_with_length(props)
    var pos = 0
    let decoded = decode_properties(encoded, pos)
    check decoded[propSessionExpiryInterval][0].int_val == 3600

  test "string property round-trip":
    var props = initOrderedTable[uint8, seq[PropertyValue]]()
    props[propContentType] = @[prop_val_str("application/json")]
    let encoded = encode_properties_with_length(props)
    var pos = 0
    let decoded = decode_properties(encoded, pos)
    check decoded[propContentType][0].str_val == "application/json"

  test "binary property round-trip":
    var props = initOrderedTable[uint8, seq[PropertyValue]]()
    props[propCorrelationData] = @[prop_val_bin("\x01\x02\x03\x04")]
    let encoded = encode_properties_with_length(props)
    var pos = 0
    let decoded = decode_properties(encoded, pos)
    check decoded[propCorrelationData][0].bin_val == "\x01\x02\x03\x04"

  test "user property (pair) round-trip":
    var props = initOrderedTable[uint8, seq[PropertyValue]]()
    props[propUserProperty] = @[prop_val_pair("key1", "val1"), prop_val_pair("key2", "val2")]
    let encoded = encode_properties_with_length(props)
    var pos = 0
    let decoded = decode_properties(encoded, pos)
    check decoded[propUserProperty].len == 2
    check decoded[propUserProperty][0].key == "key1"
    check decoded[propUserProperty][0].pair_val == "val1"
    check decoded[propUserProperty][1].key == "key2"
    check decoded[propUserProperty][1].pair_val == "val2"

# =====================================================================================================================
# CONNECT
# =====================================================================================================================

suite "CONNECT":
  test "minimal connect round-trip":
    let pkt = MqttPacket(packet_type: ptConnect,
                         connect_flags: ConnectFlags(clean_start: true),
                         keep_alive: 60, connect_props: empty_props,
                         client_id: "test_client", will_config: none(WillConfig),
                         username: "", password: "")
    let encoded = encode(pkt)
    var pos = 0
    let decoded = decode(encoded, pos)
    check decoded.packet_type == ptConnect
    check decoded.client_id == "test_client"
    check decoded.keep_alive == 60
    check decoded.connect_flags.clean_start == true
    check decoded.connect_flags.will == false

  test "connect with username/password":
    let pkt = MqttPacket(packet_type: ptConnect,
                         connect_flags: ConnectFlags(clean_start: true, username: true, password: true),
                         keep_alive: 30, connect_props: empty_props,
                         client_id: "auth_client", will_config: none(WillConfig),
                         username: "user", password: "pass")
    let encoded = encode(pkt)
    var pos = 0
    let decoded = decode(encoded, pos)
    check decoded.username == "user"
    check decoded.password == "pass"
    check decoded.connect_flags.username == true
    check decoded.connect_flags.password == true

  test "connect with will":
    let wc = WillConfig(qos: qos1, retain: true, topic: "will/topic",
                        payload: "goodbye", properties: empty_props)
    let pkt = MqttPacket(packet_type: ptConnect,
                         connect_flags: ConnectFlags(clean_start: true, will: true,
                                                     will_qos: qos1, will_retain: true),
                         keep_alive: 60, connect_props: empty_props,
                         client_id: "will_client", will_config: some(wc),
                         username: "", password: "")
    let encoded = encode(pkt)
    var pos = 0
    let decoded = decode(encoded, pos)
    check decoded.connect_flags.will == true
    check decoded.connect_flags.will_qos == qos1
    check decoded.connect_flags.will_retain == true
    check decoded.will_config.isSome
    check decoded.will_config.get.topic == "will/topic"
    check decoded.will_config.get.payload == "goodbye"

# =====================================================================================================================
# CONNACK
# =====================================================================================================================

suite "CONNACK":
  test "connack success round-trip":
    let pkt = MqttPacket(packet_type: ptConnack, session_present: false,
                         connack_reason: rcSuccess, connack_props: empty_props)
    let encoded = encode(pkt)
    var pos = 0
    let decoded = decode(encoded, pos)
    check decoded.packet_type == ptConnack
    check decoded.session_present == false
    check decoded.connack_reason == rcSuccess

  test "connack with session present":
    let pkt = MqttPacket(packet_type: ptConnack, session_present: true,
                         connack_reason: rcSuccess, connack_props: empty_props)
    let encoded = encode(pkt)
    var pos = 0
    let decoded = decode(encoded, pos)
    check decoded.session_present == true

  test "connack rejected":
    let pkt = MqttPacket(packet_type: ptConnack, session_present: false,
                         connack_reason: rcNotAuthorized, connack_props: empty_props)
    let encoded = encode(pkt)
    var pos = 0
    let decoded = decode(encoded, pos)
    check decoded.connack_reason == rcNotAuthorized

# =====================================================================================================================
# PUBLISH
# =====================================================================================================================

suite "PUBLISH":
  test "qos0 publish round-trip":
    let pkt = MqttPacket(packet_type: ptPublish, dup: false, publish_qos: qos0,
                         retain: false, topic: "test/topic", publish_packet_id: 0,
                         publish_props: empty_props, payload: "hello mqtt")
    let encoded = encode(pkt)
    var pos = 0
    let decoded = decode(encoded, pos)
    check decoded.packet_type == ptPublish
    check decoded.topic == "test/topic"
    check decoded.payload == "hello mqtt"
    check decoded.publish_qos == qos0
    check decoded.dup == false
    check decoded.retain == false

  test "qos1 publish with packet id":
    let pkt = MqttPacket(packet_type: ptPublish, dup: false, publish_qos: qos1,
                         retain: false, topic: "test/qos1", publish_packet_id: 42,
                         publish_props: empty_props, payload: "qos1 data")
    let encoded = encode(pkt)
    var pos = 0
    let decoded = decode(encoded, pos)
    check decoded.publish_qos == qos1
    check decoded.publish_packet_id == 42

  test "qos2 publish with retain and dup":
    let pkt = MqttPacket(packet_type: ptPublish, dup: true, publish_qos: qos2,
                         retain: true, topic: "test/qos2", publish_packet_id: 100,
                         publish_props: empty_props, payload: "qos2 data")
    let encoded = encode(pkt)
    var pos = 0
    let decoded = decode(encoded, pos)
    check decoded.dup == true
    check decoded.publish_qos == qos2
    check decoded.retain == true
    check decoded.publish_packet_id == 100

  test "publish with empty payload":
    let pkt = MqttPacket(packet_type: ptPublish, dup: false, publish_qos: qos0,
                         retain: false, topic: "test/empty", publish_packet_id: 0,
                         publish_props: empty_props, payload: "")
    let encoded = encode(pkt)
    var pos = 0
    let decoded = decode(encoded, pos)
    check decoded.payload == ""

# =====================================================================================================================
# PUBACK / PUBREC / PUBREL / PUBCOMP
# =====================================================================================================================

suite "ack packets":
  test "puback round-trip":
    let pkt = MqttPacket(packet_type: ptPuback, puback_packet_id: 1,
                         puback_reason: rcSuccess, puback_props: empty_props)
    let encoded = encode(pkt)
    var pos = 0
    let decoded = decode(encoded, pos)
    check decoded.packet_type == ptPuback
    check decoded.puback_packet_id == 1
    check decoded.puback_reason == rcSuccess

  test "pubrec round-trip":
    let pkt = MqttPacket(packet_type: ptPubrec, pubrec_packet_id: 2,
                         pubrec_reason: rcSuccess, pubrec_props: empty_props)
    let encoded = encode(pkt)
    var pos = 0
    let decoded = decode(encoded, pos)
    check decoded.packet_type == ptPubrec
    check decoded.pubrec_packet_id == 2

  test "pubrel round-trip":
    let pkt = MqttPacket(packet_type: ptPubrel, pubrel_packet_id: 3,
                         pubrel_reason: rcSuccess, pubrel_props: empty_props)
    let encoded = encode(pkt)
    var pos = 0
    let decoded = decode(encoded, pos)
    check decoded.packet_type == ptPubrel
    check decoded.pubrel_packet_id == 3

  test "pubcomp round-trip":
    let pkt = MqttPacket(packet_type: ptPubcomp, pubcomp_packet_id: 4,
                         pubcomp_reason: rcSuccess, pubcomp_props: empty_props)
    let encoded = encode(pkt)
    var pos = 0
    let decoded = decode(encoded, pos)
    check decoded.packet_type == ptPubcomp
    check decoded.pubcomp_packet_id == 4

  test "puback with error reason":
    let pkt = MqttPacket(packet_type: ptPuback, puback_packet_id: 5,
                         puback_reason: rcTopicNameInvalid, puback_props: empty_props)
    let encoded = encode(pkt)
    var pos = 0
    let decoded = decode(encoded, pos)
    check decoded.puback_reason == rcTopicNameInvalid

  test "puback minimal (reason 0, no props)":
    let pkt = MqttPacket(packet_type: ptPuback, puback_packet_id: 6,
                         puback_reason: rcSuccess, puback_props: empty_props)
    let encoded = encode(pkt)
    var pos = 0
    let decoded = decode(encoded, pos)
    check decoded.puback_packet_id == 6
    check decoded.puback_reason == rcSuccess

# =====================================================================================================================
# SUBSCRIBE / SUBACK
# =====================================================================================================================

suite "SUBSCRIBE/SUBACK":
  test "subscribe single topic round-trip":
    let sub = Subscription(topic: "test/#", qos: qos1, no_local: false,
                           retain_as_published: false, retain_handling: 0)
    let pkt = MqttPacket(packet_type: ptSubscribe, subscribe_packet_id: 10,
                         subscribe_props: empty_props, subscriptions: @[sub])
    let encoded = encode(pkt)
    var pos = 0
    let decoded = decode(encoded, pos)
    check decoded.packet_type == ptSubscribe
    check decoded.subscribe_packet_id == 10
    check decoded.subscriptions.len == 1
    check decoded.subscriptions[0].topic == "test/#"
    check decoded.subscriptions[0].qos == qos1

  test "subscribe multiple topics":
    let sub1 = Subscription(topic: "a/+", qos: qos0, no_local: false,
                            retain_as_published: false, retain_handling: 0)
    let sub2 = Subscription(topic: "b/#", qos: qos2, no_local: true,
                            retain_as_published: true, retain_handling: 2)
    let pkt = MqttPacket(packet_type: ptSubscribe, subscribe_packet_id: 11,
                         subscribe_props: empty_props, subscriptions: @[sub1, sub2])
    let encoded = encode(pkt)
    var pos = 0
    let decoded = decode(encoded, pos)
    check decoded.subscriptions.len == 2
    check decoded.subscriptions[0].topic == "a/+"
    check decoded.subscriptions[0].qos == qos0
    check decoded.subscriptions[1].topic == "b/#"
    check decoded.subscriptions[1].qos == qos2
    check decoded.subscriptions[1].no_local == true
    check decoded.subscriptions[1].retain_as_published == true
    check decoded.subscriptions[1].retain_handling == 2

  test "suback round-trip":
    let pkt = MqttPacket(packet_type: ptSuback, suback_packet_id: 10,
                         suback_props: empty_props,
                         suback_reasons: @[rcGrantedQoS0, rcGrantedQoS1, rcGrantedQoS2])
    let encoded = encode(pkt)
    var pos = 0
    let decoded = decode(encoded, pos)
    check decoded.packet_type == ptSuback
    check decoded.suback_packet_id == 10
    check decoded.suback_reasons == @[rcGrantedQoS0, rcGrantedQoS1, rcGrantedQoS2]

# =====================================================================================================================
# UNSUBSCRIBE / UNSUBACK
# =====================================================================================================================

suite "UNSUBSCRIBE/UNSUBACK":
  test "unsubscribe round-trip":
    let pkt = MqttPacket(packet_type: ptUnsubscribe, unsubscribe_packet_id: 20,
                         unsubscribe_props: empty_props,
                         unsubscribe_topics: @["test/#", "other/+"])
    let encoded = encode(pkt)
    var pos = 0
    let decoded = decode(encoded, pos)
    check decoded.packet_type == ptUnsubscribe
    check decoded.unsubscribe_packet_id == 20
    check decoded.unsubscribe_topics == @["test/#", "other/+"]

  test "unsuback round-trip":
    let pkt = MqttPacket(packet_type: ptUnsuback, unsuback_packet_id: 20,
                         unsuback_props: empty_props,
                         unsuback_reasons: @[rcSuccess, rcSuccess])
    let encoded = encode(pkt)
    var pos = 0
    let decoded = decode(encoded, pos)
    check decoded.packet_type == ptUnsuback
    check decoded.unsuback_packet_id == 20
    check decoded.unsuback_reasons == @[rcSuccess, rcSuccess]

# =====================================================================================================================
# PINGREQ / PINGRESP
# =====================================================================================================================

suite "PING":
  test "pingreq round-trip":
    let encoded = encode_pingreq()
    check encoded.len == 2
    var pos = 0
    let decoded = decode(encoded, pos)
    check decoded.packet_type == ptPingreq

  test "pingresp round-trip":
    let encoded = encode_pingresp()
    check encoded.len == 2
    var pos = 0
    let decoded = decode(encoded, pos)
    check decoded.packet_type == ptPingresp

# =====================================================================================================================
# DISCONNECT
# =====================================================================================================================

suite "DISCONNECT":
  test "normal disconnect round-trip":
    let pkt = MqttPacket(packet_type: ptDisconnect,
                         disconnect_reason: rcNormalDisconnection,
                         disconnect_props: empty_props)
    let encoded = encode(pkt)
    var pos = 0
    let decoded = decode(encoded, pos)
    check decoded.packet_type == ptDisconnect
    check decoded.disconnect_reason == rcNormalDisconnection

  test "disconnect with reason code":
    let pkt = MqttPacket(packet_type: ptDisconnect,
                         disconnect_reason: rcServerBusy,
                         disconnect_props: empty_props)
    let encoded = encode(pkt)
    var pos = 0
    let decoded = decode(encoded, pos)
    check decoded.disconnect_reason == rcServerBusy

# =====================================================================================================================
# AUTH
# =====================================================================================================================

suite "AUTH":
  test "auth round-trip":
    var props = initOrderedTable[uint8, seq[PropertyValue]]()
    props[propAuthenticationMethod] = @[prop_val_str("SCRAM-SHA-256")]
    props[propAuthenticationData] = @[prop_val_bin("challenge_data")]
    let pkt = MqttPacket(packet_type: ptAuth, auth_reason: rcSuccess,
                         auth_props: props)
    let encoded = encode(pkt)
    var pos = 0
    let decoded = decode(encoded, pos)
    check decoded.packet_type == ptAuth
    check decoded.auth_reason == rcSuccess
    check decoded.auth_props[propAuthenticationMethod][0].str_val == "SCRAM-SHA-256"
    check decoded.auth_props[propAuthenticationData][0].bin_val == "challenge_data"

# =====================================================================================================================
# Multi-packet decode
# =====================================================================================================================

suite "multi-packet":
  test "decode two packets from single buffer":
    let ping1 = encode_pingreq()
    let ping2 = encode_pingresp()
    let buf = ping1 & ping2
    var pos = 0
    let p1 = decode(buf, pos)
    check p1.packet_type == ptPingreq
    let p2 = decode(buf, pos)
    check p2.packet_type == ptPingresp
    check pos == buf.len

  test "decode connect + connack from single buffer":
    let connect_pkt = encode(MqttPacket(packet_type: ptConnect,
                                    connect_flags: ConnectFlags(clean_start: true),
                                    keep_alive: 60, connect_props: empty_props,
                                    client_id: "mc", will_config: none(WillConfig),
                                    username: "", password: ""))
    let connack_pkt = encode(MqttPacket(packet_type: ptConnack, session_present: false,
                                    connack_reason: rcSuccess, connack_props: empty_props))
    let buf = connect_pkt & connack_pkt
    var pos = 0
    let p1 = decode(buf, pos)
    check p1.packet_type == ptConnect
    check p1.client_id == "mc"
    let p2 = decode(buf, pos)
    check p2.packet_type == ptConnack
    check p2.connack_reason == rcSuccess
