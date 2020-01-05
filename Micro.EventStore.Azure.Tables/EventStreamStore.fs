namespace Micro.EventStore.Azure.Tables

open Micro.EventStore
open Micro.Azure.Tables
open Microsoft.Azure.Cosmos.Table

type AzureTablePartitionEventStreamOptions = {
    getTable: unit -> Async<CloudTable>
}

type StatusEntity(partitionKey) =
    inherit TableEntity(partitionKey, "_EventStream_Status")

    member val NextSequence = 0UL with get, set

type AzureTablePartitionEventStreamStore(options) =

    let MetaPrefix = "Meta_"
    let DataPrefix = "Data_"
    let EmptyDataType = "Empty"
    let StringDataType = "String"
    let BinaryDataType = "Binary"

    let createStatusEntity streamId nextSequence =
        let statusEntity = StatusEntity(streamId)
        statusEntity.ETag <- "*"
        statusEntity.NextSequence <- nextSequence
        statusEntity

    let createEventEntity streamId event =
        let rowKey = sprintf "%020u" event.sequence
        let entity = DynamicTableEntity(streamId, rowKey)
        entity.ETag <- "*"

        entity.Properties.Add("EventId", EntityProperty.GeneratePropertyForString(event.id))

        writeMapStringProperties MetaPrefix entity event.meta

        match event.data with
        | NoEventData ->
            entity.Properties.Add(sprintf "%sType" DataPrefix, EntityProperty.GeneratePropertyForString(EmptyDataType))
            writeEmptyContent DataPrefix entity
        | StringEventData value ->
            entity.Properties.Add(sprintf "%sType" DataPrefix, EntityProperty.GeneratePropertyForString(StringDataType))
            writeStringContent DataPrefix entity value
        | BinaryEventData value ->
            entity.Properties.Add(sprintf "%sType" DataPrefix, EntityProperty.GeneratePropertyForString(BinaryDataType))
            writeBinaryContent DataPrefix entity value

        entity

    interface IEventStreamStore with
        member _.GetStreamStatus request = async {
            let! table = options.getTable()
            let! statusEntity = tryFetchOneOf<StatusEntity> table request.streamId "_EventStream_Status"
            let nextSequence =
                match statusEntity with
                | Some statusEntity -> statusEntity.NextSequence
                | None -> 0UL

            return {
                status = {
                    nextSequence = nextSequence
                }
            }
        }

        member _.AppendEventsToStream request = async {
            let! table = options.getTable()
            let batch = TableBatchOperation()

            request.events
                |> Seq.iter (fun event -> batch.Insert(createEventEntity request.streamId event))

            batch.InsertOrReplace(createStatusEntity request.streamId request.status.nextSequence)

            let! _batchResult = table.ExecuteBatchAsync(batch) |> Async.AwaitTask

            return ()
        }
