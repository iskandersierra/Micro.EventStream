namespace Micro.EventStore

open System.Threading.Tasks
open FSharp.Control.Tasks.V2.ContextInsensitive

type ReplyChannel<'t> = Result<'t, exn> -> unit

module ReplyChannel =
    let fromAsync
        (run: unit -> Async<'response>)
        (channel: ReplyChannel<'response>) =
        async {
            try
                let! response = run()
                Ok response |> channel
            with
            | exn ->
                Error exn |> channel
        } |> ignore

    let toAsync
        (commandFn: ReplyChannel<'response> -> 'command)
        (processor: 'command -> unit) =
        let source = TaskCompletionSource<'response>()
        let channel (result: Result<'response, exn>) =
            match result with
            | Ok value ->
                source.TrySetResult(value)
            | Error exn ->
                match exn with
                | :? TaskCanceledException ->
                    source.TrySetCanceled()
                | _ ->
                    source.TrySetException(exn)
            |> ignore
        let command = commandFn channel
        processor command
        source.Task |> Async.AwaitTask

    let replyToAsync (mailbox: MailboxProcessor<_>) createCommand = async {
        match! mailbox.PostAndAsyncReply(fun ch -> createCommand ch.Reply) with
        | Error exn -> return raise exn
        | Ok value -> return value
    }

module Lazy =
    let memoAsync (factory: unit -> Async<'t>) =
        let mutable cache = Unchecked.defaultof<TaskCompletionSource<'t>>
        let lockObj = obj()
        let getAsync() = cache.Task |> Async.AwaitTask
        fun () ->
            if isNull cache |> not then
                getAsync()
            else
                lock lockObj (fun () ->
                    if isNull cache |> not then
                        getAsync()
                    else
                        cache <- TaskCompletionSource<'t>()
                        task {
                          try
                            let! value = factory()
                            cache.TrySetResult value |> ignore
                          with
                          | exn ->
                            cache.TrySetException exn |> ignore
                        } |> ignore
                        getAsync()
                )
