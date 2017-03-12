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
let mutable private longOptionDic = Dictionary<string, string>()

let addArg (option : Arg) =
    if String.IsNullOrEmpty(option.Option) = true then
        raise (ArgumentException("option.Option"))
    else
    if String.IsNullOrEmpty(option.LongOption) = false then
        longOptionDic.Add(option.LongOption, option.Option)
    argDic.Add(option.Option, option)
    argList.Add(option)

let private isLongOption arg =
    let m = longOptionRegEx.Match(arg)
    m.Success

let private isShortOption arg =
    let m = optionRegEx.Match(arg)
    m.Success

let private tokenType token =
    if isLongOption token then
        "long option"
    elif isShortOption token then
        "option"
    else
        "string"

let parseArgs args =
    try
        let mutable result = Dictionary<string, string>()
        let tokenList = List<string>()
        let tokenTypeList = List<string>()
        args
        |> Array.iteri (fun i arg ->
            let token, ttype =
                let mutable t = arg
                let tt = tokenType arg
                if argDic.ContainsKey(t) then
                    t <- argDic.[t].Option
                elif tt = "long option" && longOptionDic.ContainsKey(t) then
                    let o = longOptionDic.[t]
                    t <- argDic.[o].Option
                t, tt
            tokenList.Add(arg)
            tokenTypeList.Add(ttype)
            let isOption = ttype = "long option" || ttype = "option"
            if isOption && argDic.ContainsKey(token) then
                let option = argDic.[token]
                let narg =
                    if args.Length > i + 1 then args.[i+1]
                    else ""
                let tt = tokenType (narg)
                if tt = "string" then
                    //printfn "%A" (token, t)
                    result.Add(option.Destination, narg) )
        Choice1Of2(result)
    with | ex ->
    printf "%A" ex
    Choice2Of2(ex)
