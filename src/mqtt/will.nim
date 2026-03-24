## will.nim -- Will message handling.
##
## Stores will config on CONNECT. Publishes via a callback on unclean disconnect.

{.experimental: "strict_funcs".}

import std/tables
import packet

# =====================================================================================================================
# Types
# =====================================================================================================================

type
  WillPublisher* = proc(topic: string, payload: string, qos: QoS, retain: bool,
                        props: Properties) {.gcsafe, raises: [].}

  WillStore* = object
    wills: Table[string, WillConfig]  ## client_id -> will config
    publisher: WillPublisher

# =====================================================================================================================
# Operations
# =====================================================================================================================

proc new_will_store*(publisher: WillPublisher): WillStore =
  result.wills = initTable[string, WillConfig]()
  result.publisher = publisher

proc register*(ws: var WillStore, client_id: string, will: WillConfig) =
  ## Register a will message for a client.
  ws.wills[client_id] = will

proc deregister*(ws: var WillStore, client_id: string) =
  ## Remove a will message (on clean disconnect).
  ws.wills.del(client_id)

proc trigger*(ws: var WillStore, client_id: string) =
  ## Publish the will message for a client (on unclean disconnect).
  if client_id in ws.wills:
    let w = ws.wills[client_id]
    ws.publisher(w.topic, w.payload, w.qos, w.retain, w.properties)
    ws.wills.del(client_id)

proc has_will*(ws: WillStore, client_id: string): bool =
  client_id in ws.wills
