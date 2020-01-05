namespace Micro.EventStore

type EventData =
  | StringEventData of string
  | BinaryEventData of string

 type EventMeta = Map<string, string>

type Event = {
  id: string
  meta: EventMeta
  data: EventData
}
