namespace Micro.EventStore

type EventData =
  | StringEventData of string
  | BinaryEventData of byte[]
  | NoEventData

 type EventMeta = Map<string, string>

type Event = {
  id: string
  sequence: uint64
  meta: EventMeta
  data: EventData
}

module Event =
  let empty = {
    id = ""
    sequence = 0UL
    meta = Map.empty
    data = StringEventData ""
  }

  let getId event = event.id
  let setId id event = { event with id = id }

  let getStreamSequence event = event.sequence
  let setStreamSequence sequence event = { event with sequence = sequence }

  let getMeta event = event.meta
  let setMeta meta event = { event with meta = meta }
  let updateMeta metaFn event = setMeta (getMeta event |> metaFn) event
  let addMeta key value = updateMeta (Map.add key value)
  let removeMeta key = updateMeta (Map.remove key)
  let tryFindMeta key event = getMeta event |> Map.tryFind key

  let getData event = event.data
  let setData data event = { event with data = data }
  let setString = StringEventData >> setData
  let setBinary = BinaryEventData >> setData
