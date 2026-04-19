## session.nim -- Per-client MQTT session state.
##
## Tracks subscriptions, inflight QoS 1/2 messages, will config,
## clean start/resume. Thread-per-client: each session is accessed
## by a single handler thread.

{.experimental: "strict_funcs".}

import std/[tables, net]
import basis/code/choice
import packet

# =====================================================================================================================
# Types
# =====================================================================================================================

type
  InflightMsg* = object
    ## Tracks an inflight QoS 1/2 message pending ack.
    packet_id*: uint16
    topic*: string
    payload*: string
    qos*: QoS

  SessionState* {.pure.} = enum
    New       ## Fresh session, no prior state
    Resumed   ## Resumed from prior session (clean_start = false)

  ClientSession* = object
    client_id*: string
    state*: SessionState
    subscriptions*: Table[string, QoS]  ## filter -> granted QoS
    inflight*: Table[uint16, InflightMsg]  ## packet_id -> pending msg
    will*: Choice[WillConfig]
    keep_alive*: uint16
    sock*: Socket
    next_packet_id: uint16

# =====================================================================================================================
# Session management
# =====================================================================================================================

proc new_session*(client_id: string, sock: Socket, keep_alive: uint16,
                  clean_start: bool, will: Choice[WillConfig]): ClientSession =
  result.client_id = client_id
  result.sock = sock
  result.keep_alive = keep_alive
  result.will = will
  result.subscriptions = initTable[string, QoS]()
  result.inflight = initTable[uint16, InflightMsg]()
  result.next_packet_id = 1
  if clean_start:
    result.state = SessionState.New
  else:
    result.state = SessionState.Resumed

proc next_id*(session: var ClientSession): uint16 =
  result = session.next_packet_id
  if session.next_packet_id == 65535:
    session.next_packet_id = 1
  else:
    inc session.next_packet_id

proc add_subscription*(session: var ClientSession, filter: string, qos: QoS) =
  session.subscriptions[filter] = qos

proc remove_subscription*(session: var ClientSession, filter: string) =
  session.subscriptions.del(filter)

proc add_inflight*(session: var ClientSession, pid: uint16, msg: InflightMsg) =
  session.inflight[pid] = msg

proc remove_inflight*(session: var ClientSession, pid: uint16) =
  session.inflight.del(pid)

proc clear_session*(session: var ClientSession) =
  ## Clear all session state (for clean_start).
  session.subscriptions.clear()
  session.inflight.clear()
  session.will = choice.none[WillConfig]()
  session.state = SessionState.New
