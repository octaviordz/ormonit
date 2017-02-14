[<RequireQualifiedAccessAttribute>]
module internal Cli

open System
open System.Text.RegularExpressions
open System.Collections.Generic

let optionRegEx = Regex("^-\w+")
let longOptionRegEx = Regex("^--(\w+)")

type Arg = {
    mutable Option : string
    mutable LongOption : string
    mutable Destination : string
    mutable Default : string
    mutable Help : string
    }

let arg = {
    Option = String.Empty
    LongOption = String.Empty
    Destination = String.Empty
    Default = String.Empty
    Help = String.Empty
    }

let mutable private argList = List<Arg>()
let mutable private argDic = Dictionary<string, Arg>()
let mutable private shortOptDic = Dictionary<string, string>()

let addArg (option : Arg) =
    argList.Add(option)
    argDic.Add(option.LongOption, option)
    shortOptDic.Add(option.Option, option.LongOption)
    ()

let private isLongOption arg =
    let m = longOptionRegEx.Match(arg)
    m.Success

let private isShortOption arg =
    let m = optionRegEx.Match(arg)
    m.Success

let private parseToken token =
    if isLongOption token then
        let tokeType = "long option"
        token, tokeType
    elif isShortOption token then
        let tokeType = "option"
        token, tokeType
    else
        let tokeType = "string"
        token, tokeType

let usage = ""

let parseArgs args =
    try
        let mutable result = Dictionary<string, string>()
        let tokenList = List<string>()
        let tokenTypeList = List<string>()
        args
        |> Array.iteri ( fun i arg ->
        let token, ttype =
            let mutable t, tt = parseToken arg
            if tt = "option" && shortOptDic.ContainsKey(t) then
                t <- shortOptDic.[t]
            t, tt
        tokenList.Add(arg)
        tokenTypeList.Add(ttype)
        let isOption = ttype = "long option" || ttype = "option"
        if isOption && argDic.ContainsKey(token) then
            let option = argDic.[token]
            let narg =
                if args.Length > i + 1 then args.[i+1]
                else ""
            let t, tt = parseToken (narg)
            if tt = "string" then
                //printfn "%A" (token, t)
                result.Add(option.Destination, t) )
        Choice1Of2(result)
    with | ex ->
    printf "%A" ex
    Choice2Of2(ex)
