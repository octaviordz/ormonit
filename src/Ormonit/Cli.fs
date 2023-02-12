[<RequireQualifiedAccess>]
module internal Cli

open System
//open System.Text.RegularExpressions
//open System.Collections.Generic

//let optionRegEx = Regex("^-\w+")
//let longOptionRegEx = Regex("^--(\w+)")

//type Arg = 
//    { mutable Option : string
//      mutable LongOption : string
//      mutable Destination : string
//      mutable Default : string
//      mutable Help : string }

//let arg = 
//    { Option = String.Empty
//      LongOption = String.Empty
//      Destination = String.Empty
//      Default = String.Empty
//      Help = String.Empty }

//let mutable private argList = List<Arg>()
//let mutable private argDic = Dictionary<string, Arg>()
//let mutable private longOptionDic = Dictionary<string, string>()

//let addArg (option : Arg) = 
//    if String.IsNullOrEmpty(option.Option) = true then raise (ArgumentException("option.Option"))
//    else 
//        if String.IsNullOrEmpty(option.LongOption) = false then longOptionDic.Add(option.LongOption, option.Option)
//        argDic.Add(option.Option, option)
//        argList.Add(option)

//let private isLongOption arg = 
//    let m = longOptionRegEx.Match(arg)
//    m.Success

//let private isShortOption arg = 
//    let m = optionRegEx.Match(arg)
//    m.Success

//let private tokenType token = 
//    if isLongOption token then "long option"
//    elif isShortOption token then "option"
//    else "string"

//let parseArgs (args : string list) = 
//    try 
//        let result = Dictionary<string, string>()
//        let tokenList = List<string>()
//        let tokenTypeList = List<string>()
//        args |> List.iteri (fun i arg -> 
//                    let token, ttype = 
//                        let mutable t = arg
//                        let tt = tokenType arg
//                        if argDic.ContainsKey(t) then t <- argDic.[t].Option
//                        else 
//                            if tt = "long option" && longOptionDic.ContainsKey(t) then 
//                                let o = longOptionDic.[t]
//                                t <- argDic.[o].Option
//                        t, tt
//                    tokenList.Add(arg)
//                    tokenTypeList.Add(ttype)
//                    let isOption = ttype = "long option" || ttype = "option"
//                    if isOption && argDic.ContainsKey(token) then 
//                        let option = argDic.[token]
                        
//                        let narg = 
//                            if args.Length > i + 1 then args.[i + 1]
//                            else ""
                        
//                        let tt = tokenType narg
//                        if tt = "string" then result.Add(option.Destination, narg))
//        Ok result
//    with ex -> 
//        printf "%A" ex
//        Error ex

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
