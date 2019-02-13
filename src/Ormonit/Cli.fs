[<RequireQualifiedAccess>]
module internal Cli

open System

type Option =
    { name : string
      shortName : string
      help : string }

let option = 
    { name = String.Empty
      shortName = String.Empty
      help = String.Empty }

type Parser(options : Option list) =
    let options = options

    let trim (value : string) = 
        let mutable idx = 0
        while idx < value.Length && value.IndexOf("-", idx) = idx do
            idx <- idx + 1
        value.Substring(idx)
    
    let tryOption (a : string) =
        options |> List.tryFind (fun it ->
            let a = trim(a)
            a = it.name || a = it.shortName )
    
    let parseTuple a b parsedData =
        let a' = tryOption a
        let b' = tryOption b
        if a'= None then
            parsedData
        else if b' = None then
            parsedData |> Map.add a'.Value.name b
        else
            parsedData |> Map.add a'.Value.name String.Empty
    
    let rec parseTuples idx (argv : string []) state =
        let a = argv.[idx]
        let b = if idx + 1 < argv.Length then argv.[idx + 1] else String.Empty
        let nstate = state |> parseTuple a b
        if idx + 1 < argv.Length then parseTuples (idx + 1) argv nstate
        else nstate

    member __.Parse (argv : string []) =
        match argv with
        | [||] -> Map.empty
        | argv -> parseTuples 0 argv Map.empty
