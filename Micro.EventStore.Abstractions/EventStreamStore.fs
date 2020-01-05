namespace Micro.EventStore

open System.Threading.Tasks

type EventStreamStatus = {
    nextSequence: uint64
}

type GetStreamStatusRequest = { streamId: string }
type GetStreamStatusResponse = { status: EventStreamStatus }

type AppendEventsToStreamRequest = {
  streamId: string
  events: Event seq
  status: EventStreamStatus
}
type AppendEventsToStreamResponse = unit

type IEventStreamStore =
  abstract GetStreamStatus: GetStreamStatusRequest -> Async<GetStreamStatusResponse>
  abstract AppendEventsToStream: AppendEventsToStreamRequest -> Async<AppendEventsToStreamResponse>
