module Micro.Azure.Tables

open Microsoft.Azure.Cosmos.Table

let tryFetchOneOf<'entity when 'entity :> ITableEntity> (table: CloudTable) (partitionKey: string) (rowKey: string) = async {
    let operation = TableOperation.Retrieve<'entity>(partitionKey, rowKey)
    let! result = table.ExecuteAsync(operation) |> Async.AwaitTask
    if isNull result.Result |> not then
        return result.Result :?> 'entity |> Some
    else
        return None
}

let tryFetchOne (table: CloudTable) (partitionKey: string) (rowKey: string) = async {
    let operation = TableOperation.Retrieve(partitionKey, rowKey)
    let! result = table.ExecuteAsync(operation) |> Async.AwaitTask
    if isNull result.Result |> not then
        return result.Result :?> DynamicTableEntity |> Some
    else
        return None
}

let fetchAllOf (table: CloudTable) (query: TableQuery<'t>) = async {
    let list = ResizeArray()
    let rec loop contToken = async {
        let! segment = table.ExecuteQuerySegmentedAsync(query, contToken) |> Async.AwaitTask
        if isNull segment.Results |> not then
            list.AddRange(segment.Results)
        if isNull segment.ContinuationToken then
            return ()
        else
            return! loop segment.ContinuationToken
    }
    do! loop null
    return list
}

let fetchAll (table: CloudTable) (query: TableQuery) = async {
    let list = ResizeArray()
    let rec loop contToken = async {
        let! segment = table.ExecuteQuerySegmentedAsync(query, contToken) |> Async.AwaitTask
        if isNull segment.Results |> not then
            list.AddRange(segment.Results)
        if isNull segment.ContinuationToken then
            return ()
        else
            return! loop segment.ContinuationToken
    }
    do! loop null
    return list
}

let MaxStringFieldLength = 32768
let MaxBinaryFieldLength = 65536

let writeMapStringProperties prefix (entity: DynamicTableEntity) (map: Map<string, string>) =
    map |> Map.toSeq
        |> Seq.iter (fun (key, value) ->
            entity.Properties.Add(sprintf "%s%s" prefix key, EntityProperty.GeneratePropertyForString(value)))

let writeEmptyContent prefix (entity: DynamicTableEntity) =
    entity.Properties.Add(sprintf "%sType" prefix, EntityProperty.GeneratePropertyForString("Empty"))

let writeStringContent prefix (entity: DynamicTableEntity) (value: string) =
    entity.Properties.Add(sprintf "%sType" prefix, EntityProperty.GeneratePropertyForString("String"))
    
    let count = ceil (float value.Length / float MaxStringFieldLength) |> int |> min 1
    entity.Properties.Add(sprintf "%sCount" prefix, EntityProperty.GeneratePropertyForInt(System.Nullable count))

    for index in 0 .. count - 1 do
        let length =
            if index < count - 1
            then MaxStringFieldLength
            else value.Length - ((count - 1) * MaxStringFieldLength)
        let part = value.Substring(index * MaxStringFieldLength, length)
        entity.Properties.Add(sprintf "%s%d" prefix index, EntityProperty.GeneratePropertyForString(part))

let writeBinaryContent prefix (entity: DynamicTableEntity) (value: byte[]) =
    entity.Properties.Add(sprintf "%sType" prefix, EntityProperty.GeneratePropertyForString("Binary"))
    
    let count = ceil (float value.Length / float MaxBinaryFieldLength) |> int |> min 1
    entity.Properties.Add(sprintf "%sCount" prefix, EntityProperty.GeneratePropertyForInt(System.Nullable count))

    for index in 0 .. count - 1 do
        let length =
            if index < count - 1
            then MaxBinaryFieldLength
            else value.Length - ((count - 1) * MaxBinaryFieldLength)
        let part = Array.zeroCreate length
        Array.blit value (index * MaxBinaryFieldLength) part 0 length
        entity.Properties.Add(sprintf "%s%d" prefix index, EntityProperty.GeneratePropertyForByteArray(part))
