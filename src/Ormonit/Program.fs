﻿open Ctrl
open System
open System.Text
open System.Threading
open Ormonit.Logging
open Ormonit.Security
open System.Collections.Concurrent

let okExit = 0
let errorExit = 1

let printUsage() = printfn @"
Ormonit [options]

Options:
    start
    stop
"
let nlog = NLog.LogManager.GetLogger "Ormonit"
let olog = NLog.LogManager.GetLogger "_Ormonit.Output_"
let logConfig = NLog.Config.LoggingConfiguration()
let consoleTarget = new NLog.Targets.ColoredConsoleTarget()

logConfig.AddTarget("console", consoleTarget)

let fileTarget = new NLog.Targets.FileTarget()

logConfig.AddTarget("file", fileTarget)
consoleTarget.Layout <- NLog.Layouts.Layout.FromString @"${date:format=HH\:mm\:ss}|${logger}|${message}"
fileTarget.FileName <- NLog.Layouts.Layout.FromString @"${basedir}\logs\ormonit.log"
fileTarget.ArchiveFileName <- NLog.Layouts.Layout.FromString @"${basedir}\logs\archive\{#}.log"
fileTarget.ArchiveNumbering <- NLog.Targets.ArchiveNumberingMode.DateAndSequence
fileTarget.ArchiveAboveSize <- 10485760L //10MB
fileTarget.MaxArchiveFiles <- 3
fileTarget.ArchiveDateFormat <- "yyyy-MM-dd"

let rule1 = new NLog.Config.LoggingRule("*", NLog.LogLevel.Trace, consoleTarget)

logConfig.LoggingRules.Add(rule1)

let rule2 = new NLog.Config.LoggingRule("Ormonit*", NLog.LogLevel.Trace, fileTarget)

logConfig.LoggingRules.Add(rule2)
NLog.LogManager.Configuration <- logConfig
Ormonit.Logging.setLogFun (fun logLevel msgFunc ex formatParameters -> 
    match logLevel with
    | LogLevel.Trace -> nlog.Log(NLog.LogLevel.Trace, ex, msgFunc.Invoke(), formatParameters)
    | LogLevel.Debug -> nlog.Log(NLog.LogLevel.Debug, ex, msgFunc.Invoke(), formatParameters)
    | LogLevel.Info -> nlog.Log(NLog.LogLevel.Info, ex, msgFunc.Invoke(), formatParameters)
    | LogLevel.Warn -> nlog.Log(NLog.LogLevel.Warn, ex, msgFunc.Invoke(), formatParameters)
    | LogLevel.Error -> nlog.Log(NLog.LogLevel.Error, ex, msgFunc.Invoke(), formatParameters)
    | LogLevel.Fatal -> nlog.Log(NLog.LogLevel.Fatal, ex, msgFunc.Invoke(), formatParameters)
    | _ -> ()
    ())

let private cliOptions =
    [ { Cli.option with name = "command"
                        shortName = "cmd" }
      { Cli.option with name = "notify"
                        shortName = "n" }
      { Cli.option with name = "open-master" }
      { Cli.option with name = "open-service" }
      { Cli.option with name = "ctrl-address" }
      { Cli.option with name = "logic-id" }
      { Cli.option with name = "public-key" }
      { Cli.option with name = "process-id"
                        shortName = "pid" }
      { Cli.option with name = "process-start-time"
                        shortName = "pstartt" } ]

let parseArgs (args : string []) = 
    let parser = new Cli.Parser(cliOptions)
    try 
        let result = parser.Parse args
        Ok result
    with ex -> 
        printf "%A" ex
        Error ex

let parseAndExecute argv : int = 
    let config = 
        Map.empty.
            Add("control-address", Ctrl.controlAddress).
            Add("notify-address", Ctrl.notifyAddress)
        
    match parseArgs argv with
    | Error exn -> 
        Log.Error(exn, String.Empty)
        printUsage()
        okExit
    | Ok parsedArgs -> 
        let validArgs =
            (parsedArgs.Count = 1 && parsedArgs.ContainsKey "command") 
            || (parsedArgs.Count = 1 && parsedArgs.ContainsKey "notify") 
            || (parsedArgs.Count = 1 && parsedArgs.ContainsKey "open-master") 
            || (parsedArgs.Count = 4 && parsedArgs.ContainsKey "open-service" && parsedArgs.ContainsKey "logic-id" 
                && parsedArgs.ContainsKey "control-address")
        if not validArgs then 
            printUsage()
            okExit
        elif parsedArgs.ContainsKey "command" && parsedArgs.["command"] = "start" then 
            let mutable started = 0
            let mutable running = 0
            let mutable error = 0
            let psi = Diagnostics.ProcessStartInfo(ormonitFileName, "--open-master")
            psi.UseShellExecute <- false
            psi.RedirectStandardOutput <- true
            psi.RedirectStandardError <- true
            psi.CreateNoWindow <- true
            let p = Diagnostics.Process.Start(psi)
            p.OutputDataReceived.Add(fun args -> 
                if String.IsNullOrEmpty(args.Data) then ()
                else 
                    if Volatile.Read(&started) = 0 then 
                        Interlocked.Exchange(&started, 
                                             if args.Data.Contains "Master started" then 1
                                             else 0)
                        |> ignore
                        Interlocked.Exchange(&running, 
                                             if args.Data.Contains "Master is already running. Terminating." then 1
                                             else 0)
                        |> ignore
                    olog.Trace(args.Data))
            p.ErrorDataReceived.Add(fun args -> 
                if String.IsNullOrEmpty(args.Data) then ()
                else 
                    Interlocked.Exchange(&error, 1) |> ignore
                    olog.Error(args.Data))
            p.BeginErrorReadLine()
            p.BeginOutputReadLine()
            while Volatile.Read(&started) = 0 && Volatile.Read(&running) = 0 && Volatile.Read(&error) = 0 do
                System.Threading.Thread.CurrentThread.Join 1 |> ignore
            if error = 1 then errorExit
            elif running = 1 then errorExit
            else okExit
        elif parsedArgs.ContainsKey "command" && parsedArgs.["command"] = "stop" then 
            let note = "sys:close"
            let nsocket = Cilnn.Nn.Socket(Cilnn.Domain.SP, Cilnn.Protocol.SURVEYOR)
            let buff : byte array = Array.zeroCreate maxMessageSize
            //TODO:error handling for socket and bind
            assert (nsocket >= 0)
            assert (Cilnn.Nn.SetSockOpt(nsocket, Cilnn.SocketOption.SNDTIMEO, Ctrl.superviseInterval * 5) = 0)
            assert (Cilnn.Nn.SetSockOpt(nsocket, Cilnn.SocketOption.RCVTIMEO, Ctrl.superviseInterval * 5) = 0)
            let eid = Cilnn.Nn.Connect(nsocket, notifyAddress)
            assert (eid >= 0)
            Log.Trace(sprintf "[Stop Process] Notify \"%s\"." note)
            let send() = Comm.send nsocket ("", note)
            
            let result = 
                match send() with
                | Ok v -> Ok v
                | Error (errn, errm) -> 
                    //11 Resource unavailable, try again
                    if errn <> 11 then Error (errn, errm)
                    else 
                        //we try again
                        match send() with
                        | Ok v -> Ok v
                        | Error (errn, errm) -> Error (errn, errm)
            match result with 
            | Error (errn, errm) -> 
                Log.Warn (sprintf "[Stop Process] Unable to notify \"%s\" (send). Error %i %s." note errn errm)
            | _ -> ()
            let recv() = Comm.recv nsocket
            let mutable masterpid = -1
            
            let errn, errm = 
                match recv() with
                | Ok (_, msg) -> 
                    let nparts = msg.Split([| ' ' |], StringSplitOptions.RemoveEmptyEntries)
                    let pid = nparts.[0]
                    //let p =
                    //    match nparts |> List.ofArray with
                    //    | l when l.Length > 4 -> l.[0..l.Length - 5]
                    //    | l -> l
                    masterpid <- Int32.Parse(pid)
                    (okExit, String.Empty)
                | Error (errn, errm) -> //we try again
                    match recv() with
                    | Ok (_, npid) -> 
                        masterpid <- Int32.Parse(npid)
                        (okExit, String.Empty)
                    | Error (errn, errm) -> (errn, errm)
            if errn = okExit then 
                Log.Info(sprintf "[Stop Process] Acknowledgment of note \"%s\" (recv). Master pid: %i." note masterpid) 
            else 
                Log.Warn(sprintf "[Stop Process] No acknowledgment of note \"%s\" (recv). Error %i %s." note errn errm) 
            Cilnn.Nn.Shutdown(nsocket, eid) |> ignore
            Cilnn.Nn.Close(nsocket) |> ignore
            if errn <> okExit then errorExit
            else 
                //TODO:IMPORTANT: check process identity
                match tryGetProcess masterpid with
                | false, _ -> okExit //assume it's closed
                | true, p -> 
                    try 
                        p.WaitForExit()
                        okExit
                    with ex -> 
                        Log.Error(sprintf "[Stop Process] Error waiting for master exit. Master pid: %i." masterpid) 
                        errorExit
        elif parsedArgs.ContainsKey "notify" then 
            let note = parsedArgs.["notify"]
            let nsocket = Cilnn.Nn.Socket(Cilnn.Domain.SP, Cilnn.Protocol.SURVEYOR)
            let buff : byte array = Array.zeroCreate maxMessageSize
            //TODO:error handling for socket and bind
            assert (nsocket >= 0)
            assert (Cilnn.Nn.SetSockOpt(nsocket, Cilnn.SocketOption.SNDTIMEO, Ctrl.superviseInterval * 5) = 0)
            assert (Cilnn.Nn.SetSockOpt(nsocket, Cilnn.SocketOption.RCVTIMEO, Ctrl.superviseInterval * 5) = 0)
            let eid = Cilnn.Nn.Connect(nsocket, notifyAddress)
            assert (eid >= 0)
            Log.Trace(sprintf "Notify \"%s\" (notify process)." note)
            let bytes = Encoding.UTF8.GetBytes(note)
            let send() = Comm.send nsocket ("", note)
            match send() with
            //11 Resource unavailable, try again
            | Error (11, errm) -> //we try again
                match send() with
                | Ok _ -> ()
                | Error (errn, errm) -> 
                    Log.Warn(sprintf "Unable to notify \"%s\" (send). Error %i %s." note errn errm)
            | _ -> ()
            let recv() = Comm.recv nsocket
            let mutable masterpid = -1
            match recv() with
            | Ok (_, npid) -> masterpid <- Int32.Parse(npid)
            | Error (errn, errm) -> //we try again
                Log.Trace(sprintf "No acknowledgment of note \"%s\" (recv). Error %i %s." note errn errm)
                match recv() with
                | Ok (_, npid) -> masterpid <- Int32.Parse(npid)
                | Error (errn, errm) -> 
                    Log.Warn(sprintf "No acknowledgment of note \"%s\" (recv). Error %i %s." note errn errm)
            if masterpid <> -1 then 
                Log.Info(sprintf "Acknowledgment of note \"%s\" (recv). Master pid: %i." note masterpid)
            Cilnn.Nn.Shutdown(nsocket, eid) |> ignore
            Cilnn.Nn.Close(nsocket) |> ignore
            okExit
        elif parsedArgs.ContainsKey "open-master" then 
            //let ctrlKey = Ctrl.makeMaster()
            //Ctrl.start ctrlKey
            let key = randomKey()
            let (pubkey : string option), (prikey: string option) = 
                if parsedArgs.ContainsKey "encrypt" then 
                    let pu, pri = createAsymetricKeys()
                    Some pu, Some pri
                else (None, None)

            let states = ConcurrentStack<(string * DateTimeOffset)>()
            let ad : (string * obj) [] = 
                [| ("socketMap", System.Collections.Generic.Dictionary<int, string>() :> obj) |]
            let addressMap = Map.empty<string, SocketInfo>
            let m : Map<string, obj> = Map.ofArray ad
            let context = 
                { Context.masterKey = key
                  publicKey = pubkey
                  privateKey = prikey
                  execcType = ExeccType.ConsoleApplication
                  config = config
                  lids = Map.empty
                  services = Array.create maxOpenServices ServiceData.Default 
                  data = { AddressMap = addressMap
                           Map = m } }
            let opr = openMaster states context
            Cilnn.Nn.Term()
            opr
        elif parsedArgs.ContainsKey "open-service" then 
            let runArg = parsedArgs.["open-service"]
            let srvconfig = 
                config.Add("logicId", parsedArgs.["logicId"]).Add("publicKey", parsedArgs.["publicKey"])
                      .Add("assemblyPath", runArg)
            executeService srvconfig
            okExit
        else errorExit

[<EntryPoint>]
let main argv = 
    Log.Trace(sprintf "Run with arguments \"%s\"" (String.Join(" ", argv)))
    try 
        match argv |> List.ofArray with
        | [] -> 
            printUsage()
            okExit
        | "start" :: tail -> [ "-cmd"; "start" ] @ tail |> Array.ofList |> parseAndExecute
        | "stop" :: tail -> [ "-cmd"; "stop" ] @ tail |> Array.ofList |> parseAndExecute
        | _ -> argv |> parseAndExecute
    with ex -> 
        if isNull ex.InnerException then 
            Log.Error (sprintf "Command failed.\nErrorType:%s\nError:\n%s" (ex.GetType().Name) ex.Message)
            Log.Error (sprintf "StackTrace: %s" ex.StackTrace)
        else 
            Log.Error(sprintf "Command failed.\nErrorType:%s\nError:\n%s\nInnerException:\n%s"
                              (ex.GetType().Name) ex.Message 
                              ex.InnerException.Message)
            Log.Error(sprintf "StackTrace: %s" ex.StackTrace)
        errorExit
