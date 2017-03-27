﻿open Ctrl
open System
open System.IO
open System.Text
open System.Threading
open NNanomsg
open Ormonit.Logging

let printUsage () =
    printfn @"
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
consoleTarget.Layout <-NLog.Layouts.Layout.FromString @"${date:format=HH\:mm\:ss}|${logger}|${message}"
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
    | LogLevel.Trace ->
        nlog.Log(NLog.LogLevel.Trace, ex, msgFunc.Invoke(), formatParameters)
    | LogLevel.Debug ->
        nlog.Log(NLog.LogLevel.Debug, ex, msgFunc.Invoke(), formatParameters)
    | LogLevel.Info ->
        nlog.Log(NLog.LogLevel.Info, ex, msgFunc.Invoke(), formatParameters)
    | LogLevel.Warn ->
        nlog.Log(NLog.LogLevel.Warn, ex, msgFunc.Invoke(), formatParameters)
    | LogLevel.Error ->
        nlog.Log(NLog.LogLevel.Error, ex, msgFunc.Invoke(), formatParameters)
    | LogLevel.Fatal ->
        nlog.Log(NLog.LogLevel.Fatal, ex, msgFunc.Invoke(), formatParameters)
    | _ -> ()
    () )

let parseAndExecute argv : int =
    let ok = 0
    let unknown = Int32.MaxValue
    let config = Map.empty.Add("controlAddress", "ipc://ormonit/control.ipc")
    let execc = {
        masterKey = Int32.MinValue
        execcType = ExeccType.ConsoleApplication
        config = config
        openedSrvs = Array.create maxOpenServices OpenServiceData.Default
        thread = Thread.CurrentThread }
    Cli.addArg {Cli.arg with
                    Option = "-cmd";
                    LongOption = "--command";
                    Destination="command";}
    Cli.addArg {Cli.arg with
                    Option = "-n";
                    LongOption = "--notify";
                    Destination="notify";}
    Cli.addArg {Cli.arg with
                    Option = "--open-master";
                    Destination="openMaster";}
    Cli.addArg {Cli.arg with
                    Option = "--open-service";
                    Destination="openService";}
    Cli.addArg {Cli.arg with
                    Option = "--ctrl-address";
                    Destination="controlAddress";}
    Cli.addArg {Cli.arg with
                    Option = "--logic-id";
                    Destination="logicId";}
    match Cli.parseArgs argv with
    | Choice2Of2(exn) ->
        printUsage ()
        ok
    | Choice1Of2(parsedArgs) ->
        let validArgs =
            (parsedArgs.Count = 1 && parsedArgs.ContainsKey "command") ||
            (parsedArgs.Count = 1 && parsedArgs.ContainsKey "notify") ||
            (parsedArgs.Count = 1 && parsedArgs.ContainsKey "openMaster") ||
            (parsedArgs.Count = 3 && parsedArgs.ContainsKey "openService" &&
                parsedArgs.ContainsKey "controlAddress" &&
                parsedArgs.ContainsKey "logicId" )
        if not validArgs then
            printUsage ()
            ok
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
                    Interlocked.Exchange(
                        &started,
                        if args.Data.Contains "Master started" then 1 else 0 ) |> ignore
                    Interlocked.Exchange(
                        &running,
                        if args.Data.Contains "Master is already running. Terminating." then 1 else 0 ) |> ignore
                olog.Trace(args.Data) )
            p.ErrorDataReceived.Add(fun args ->
                if String.IsNullOrEmpty(args.Data) then ()
                else
                Interlocked.Exchange(&error, 1) |> ignore
                olog.Error(args.Data) )
            p.BeginErrorReadLine()
            p.BeginOutputReadLine()
            while Volatile.Read(&started) = 0 &&
                    Volatile.Read(&running) = 0 &&
                    Volatile.Read(&error) = 0 do
                System.Threading.Thread.CurrentThread.Join 1 |> ignore
            if error = 1 then unknown
            elif running  = 1 then unknown
            else ok
        elif parsedArgs.ContainsKey "command" && parsedArgs.["command"] = "stop" then
            let note = "close"
            let nsocket = NN.Socket(Domain.SP, Protocol.PAIR)
            let buff : byte array = Array.zeroCreate maxMessageSize
            //TODO:error handling for socket and bind
            assert (nsocket >= 0)
            assert (NN.SetSockOpt(nsocket, SocketOption.SNDTIMEO, Ctrl.superviseInterval * 5) = 0)
            assert (NN.SetSockOpt(nsocket, SocketOption.RCVTIMEO, Ctrl.superviseInterval * 5) = 0)
            let eid = NN.Connect(nsocket, notifyAddress)
            assert ( eid >= 0)
            log (tracel(sprintf "[Stop Process] Notify \"%s\"." note))
            let send () = Comm.send nsocket (Comm.Msg(execc.masterKey, note))
            let errn, errm = 
                match send () with
                | Comm.Msg _ -> (ok, String.Empty)
                | Comm.Error (errn, errm) ->
                    //11 Resource unavailable, try again
                    if errn <> 11 then
                        errn, errm
                    else
                    //we try again
                    match send () with
                    | Comm.Msg _ -> (ok, String.Empty)
                    | Comm.Error (errn, errm) -> errn, errm
            if errn <> ok then
                log (warnl (sprintf "[Stop Process] Unable to notify \"%s\" (send). Error %i %s." note errn errm))
            let recv () = Comm.recv nsocket
            let mutable masterpid = -1
            let errn, errm =
                match recv () with
                | Comm.Msg (_, npid) -> masterpid <- Int32.Parse(npid); (ok, String.Empty)
                | Comm.Error (errn, errm) -> //we try again
                    match recv () with
                    | Comm.Msg (_, npid) -> masterpid <- Int32.Parse(npid); (ok, String.Empty)
                    | Comm.Error (errn, errm) -> (errn, errm)
            if errn = ok then
                log (infol(sprintf "[Stop Process] Aknowledgment of note \"%s\" (recv). Master pid: %i." note masterpid))
            else
                log (warnl(sprintf "[Stop Process] No aknowledgment of note \"%s\" (recv). Error %i %s." note errn errm))
            NN.Shutdown(nsocket, eid) |> ignore
            NN.Close(nsocket) |> ignore
            if errn <> ok  then unknown
            else
            //TODO:IMPORTANT: check process identity
            match tryGetProcess masterpid with
            | false, _ -> ok //assume it's closed
            | true, p ->
                try
                    p.WaitForExit()
                    ok
                with
                | ex ->
                    errorl (sprintf "[Stop Process] Error waiting for master's exit. Master pid: %i." masterpid)
                    |> log
                    unknown
        
        elif parsedArgs.ContainsKey "notify" then
            let note = parsedArgs.["notify"]
            let nsocket = NN.Socket(Domain.SP, Protocol.PAIR)
            let buff : byte array = Array.zeroCreate maxMessageSize
            //TODO:error handling for socket and bind
            assert (nsocket >= 0)
            assert (NN.SetSockOpt(nsocket, SocketOption.SNDTIMEO, Ctrl.superviseInterval * 5) = 0)
            assert (NN.SetSockOpt(nsocket, SocketOption.RCVTIMEO, Ctrl.superviseInterval * 5) = 0)
            let eid = NN.Connect(nsocket, notifyAddress)
            assert (eid >= 0)
            log (tracel (sprintf "Notify \"%s\" (notify process)." note))
            let bytes = Encoding.UTF8.GetBytes(note)
            let send () = Comm.send nsocket (Comm.Msg(execc.masterKey, note))
            match send () with
            //11 Resource unavailable, try again
            | Comm.Error (11, errm) -> //we try again
                match send () with
                | Comm.Msg _ -> ()
                | Comm.Error (errn, errm) ->
                    log (warnl(sprintf "Unable to notify \"%s\" (send). Error %i %s." note errn errm))
            | _ -> ()
            let recv () = Comm.recv nsocket
            let mutable masterpid = -1
            match recv () with
            | Comm.Msg (_, npid) -> masterpid <- Int32.Parse(npid)
            | Comm.Error (errn, errm) -> //we try again
                log (tracel (sprintf "No aknowledgment of note \"%s\" (recv). Error %i %s." note errn errm))
                match recv () with
                | Comm.Msg (_, npid) -> masterpid <- Int32.Parse(npid)
                | Comm.Error (errn, errm) ->
                    log (warnl (sprintf "No aknowledgment of note \"%s\" (recv). Error %i %s." note errn errm))
            if masterpid <> -1 then
                log (infol (sprintf "Aknowledgment of note \"%s\" (recv). Master pid: %i." note masterpid))
            NN.Shutdown(nsocket, eid) |> ignore
            NN.Close(nsocket) |> ignore
            ok
        elif parsedArgs.ContainsKey "openMaster" then
            let opr = openMaster (execc)
            opr
        elif parsedArgs.ContainsKey "openService" then
            let runArg = parsedArgs.["openService"]
            //TOOD: Get arguments --ctrl-address?
            let srvconfig = config.Add("assemblyPath", runArg).
                                   Add("logicId", parsedArgs.["logicId"])
            executeService srvconfig
            ok
        else
            unknown

[<EntryPoint>]
let main argv =
    log (tracel(sprintf "Run with arguments \"%s\"" (String.Join(" ", argv))))
    try
        match List.ofArray argv with
        | [] ->
            printUsage ()
            0
        | "start" :: tail ->
            Array.ofList (["-cmd"; "start"] @ tail)
            |> parseAndExecute
        | "stop" :: tail ->
            Array.ofList (["-cmd"; "stop"] @ tail)
            |> parseAndExecute
        | argl ->
            Array.ofList argl
            |> parseAndExecute
    with
    | ex ->
        if isNull ex.InnerException then
            log (errorl(sprintf "Command failed.\nErrorType:%s\nError:\n%s" (ex.GetType().Name) ex.Message))
            log (errorl(sprintf "StackTrace: %s" ex.StackTrace))
        else
            log (errorl(sprintf "Command failed.\nErrorType:%s\nError:\n%s\nInnerException:\n%s" (ex.GetType().Name) ex.Message ex.InnerException.Message))
            log (errorl(sprintf "StackTrace: %s" ex.StackTrace))
        1
