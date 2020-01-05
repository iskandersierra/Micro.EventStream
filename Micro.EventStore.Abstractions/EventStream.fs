namespace Micro.EventStore

open System
open System.Threading.Tasks

type AppendEventsRequest = {
    events: Event seq
}

type AppendEventsResponse = {
    events: Event seq
    status: EventStreamStatus
}

type GetStatusResponse = {
    status: EventStreamStatus
}

type EventStreamCommand =
    | AppendEvents of AppendEventsRequest * ReplyChannel<AppendEventsResponse>
    | GetStatus of ReplyChannel<GetStatusResponse>

type EventStream = EventStreamCommand -> unit    

type IEventStream =
    abstract AppendEvents: AppendEventsRequest -> Async<AppendEventsResponse>

    abstract GetStatus: unit -> Async<GetStatusResponse>

module EventStream =
    let getStatus channel = GetStatus channel
    let appendEvents request channel = AppendEvents(request, channel)

    let fromInterface (eventStream: IEventStream): EventStream =
        function
        | GetStatus channel ->
            channel |> ReplyChannel.fromAsync (fun () -> eventStream.GetStatus())

        | AppendEvents (request, channel) ->
            channel |> ReplyChannel.fromAsync (fun () -> eventStream.AppendEvents request)

    let toInterface (processor: EventStream): IEventStream =
        { new IEventStream with
            member _.GetStatus() =
                processor |> ReplyChannel.toAsync getStatus

            member _.AppendEvents request =
                processor |> ReplyChannel.toAsync (appendEvents request)
        }

    type internal InternalCommand =
        | PublicCommand of EventStreamCommand
        | LoadInitialStatus

    type internal InternalState = 
      | NotLoadedYet
      | Failed of exn
      | Loaded of EventStreamStatus

    let onMailbox (streamId: string) (store: IEventStreamStore) =
        let sendError error command =
            match command with
            | GetStatus channel -> error |> Error |> channel
            | AppendEvents(_, channel) -> error |> Error |> channel

        let mailbox = MailboxProcessor.Start(fun mbox ->
            let rec loop state = async {
                match! mbox.Receive() with
                | LoadInitialStatus ->
                    try
                        let! response = store.GetStreamStatus({ streamId = streamId })
                        return! loop (Loaded response.status)
                    with
                    | exn ->
                        return! loop (Failed exn)

                | PublicCommand command ->
                    match state with
                    | NotLoadedYet ->
                        sendError (InvalidOperationException() :> exn) command
                        return! loop state

                    | Failed error ->
                        sendError error command
                        return! loop state

                    | Loaded status ->
                        match command with
                        | GetStatus channel ->
                            channel (Ok { status = status })
                            return! loop (Loaded status)

                        | AppendEvents(request, channel) ->
                            channel (Ok { status = status; events = request.events })
                            return! loop (Loaded status)
            }
            loop NotLoadedYet
        )

        { new IEventStream with
            member _.GetStatus() =
                ReplyChannel.replyToAsync mailbox (getStatus >> PublicCommand)
              
            member _.AppendEvents request =
                ReplyChannel.replyToAsync mailbox (appendEvents request >> PublicCommand)
        }
    
