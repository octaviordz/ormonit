open Ctrl
open System
open System.IO
open System.Text
open System.Threading
open NLog
open NLog.Layouts
open NNanomsg

let mutable config = Map.empty.Add("controlAddress", "ipc://ormonit/control.ipc")
let printUsage () =
    printfn @"
Ormonit [options]

Options:
    start
    stop
"
let log = LogManager.GetLogger "Ormonit"
let olog = LogManager.GetLogger "_Ormonit.Output_"
let logConfig = NLog.Config.LoggingConfiguration()
let consoleTarget = new NLog.Targets.ColoredConsoleTarget()
logConfig.AddTarget("console", consoleTarget)
let fileTarget = new NLog.Targets.FileTarget()
logConfig.AddTarget("file", fileTarget)
consoleTarget.Layout <- Layout.FromString @"${date:format=HH\:mm\:ss}|${logger}|${message}"
fileTarget.FileName <- Layout.FromString @"${basedir}\logs\ormonit.log"
fileTarget.ArchiveFileName <- Layout.FromString @"${basedir}\logs\archive\{#}.log"
fileTarget.ArchiveNumbering <- Targets.ArchiveNumberingMode.DateAndSequence
fileTarget.ArchiveAboveSize <- 10485760L //10MB
fileTarget.MaxArchiveFiles <- 3
fileTarget.ArchiveDateFormat <- "yyyy-MM-dd"
let rule1 = new NLog.Config.LoggingRule("*", LogLevel.Trace, consoleTarget)
logConfig.LoggingRules.Add(rule1)
let rule2 = new NLog.Config.LoggingRule("Ormonit*", LogLevel.Trace, fileTarget)
logConfig.LoggingRules.Add(rule2)
LogManager.Configuration <- logConfig

let parseAndExecute argv : int =
    let ok = 0
    let unknown = Int32.MaxValue
    let openedSrvs: OpenServiceData array = Array.create maxOpenServices OpenServiceData.Default
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
            sprintf "[Stop Process] Notify \"%s\"." note |> log.Trace
            let bytes = Encoding.UTF8.GetBytes(note)
            let send () =
                let sr = NN.Send (nsocket, bytes, SendRecvFlags.NONE)
                if sr < 0 then
                    let errn = NN.Errno()
                    let errm = NN.StrError(errn)
                    Choice2Of2 (errn, errm)
                else
                    Choice1Of2 (sr)
            let errn, errm = 
                match send () with
                | Choice1Of2 _ -> (ok, String.Empty)
                | Choice2Of2 (errn, errm) ->
                    //11 Resource unavailable, try again
                    if errn <> 11 then
                        errn, errm
                    else
                    //we try again
                    match send () with
                    | Choice1Of2 _ -> (ok, String.Empty)
                    | Choice2Of2 (errn, errm) -> errn, errm
            if errn <> ok then
                sprintf "[Stop Process] Unable to notify \"%s\" (send). Error %i %s." note errn errm |> log.Warn
            let recv () =
                let rr = NN.Recv(nsocket, buff, SendRecvFlags.NONE)
                if rr = 4 then
                    let pid = BitConverter.ToInt32(buff, 0)
                    Choice1Of2 (pid)
                elif rr < 0 then
                    let errn = NN.Errno()
                    let errm = NN.StrError(errn)
                    Choice2Of2 (errn, errm)
                else
                    Choice2Of2 (unknown, "Unknown")
            let mutable masterpid = -1
            let errn, errm =
                match recv () with
                | Choice1Of2 pid -> masterpid <- pid; (ok, String.Empty)
                | Choice2Of2 (errn, errm) -> //we try again
                    match recv () with
                    | Choice1Of2 pid -> masterpid <- pid; (ok, String.Empty)
                    | Choice2Of2 (errn, errm) -> (errn, errm)
            if errn = ok then
                sprintf "[Stop Process] Aknowledgment of note \"%s\" (recv). Master pid: %i." note masterpid |> log.Info
            else
                sprintf "[Stop Process] No aknowledgment of note \"%s\" (recv). Error %i %s." note errn errm |> log.Warn
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
                    sprintf "[Stop Process] Error waiting for master's exit. Master pid: %i." masterpid
                    |> log.Error
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
            assert ( eid >= 0)
            sprintf "Notify \"%s\" (notify process)." note |> log.Trace
            let bytes = Encoding.UTF8.GetBytes(note)
            let send () =
                let sr = NN.Send (nsocket, bytes, SendRecvFlags.NONE)
                if sr < 0 then
                    let errn = NN.Errno()
                    let errm = NN.StrError(errn)
                    errn, errm
                else 
                    0, String.Empty
            match send () with
            //11 Resource unavailable, try again
            | 11, errm -> //we try again
                match send () with
                | 0, _ -> ()
                | errn, errm ->
                    sprintf "Unable to notify \"%s\" (send). Error %i %s." note errn errm |> log.Warn
            | _ -> ()
            let recv () =
                let rr = NN.Recv(nsocket, buff, SendRecvFlags.NONE)
                if rr < 0 then
                    let errn = NN.Errno()
                    let errm = NN.StrError(errn)
                    Choice2Of2 (errn, errm)
                else
                    let pid = BitConverter.ToInt32(buff, 0)
                    Choice1Of2 (pid)
            let mutable masterpid = -1
            match recv () with
            | Choice1Of2 pid -> masterpid <- pid
            | Choice2Of2 (errn, errm) -> //we try again
                sprintf "No aknowledgment of note \"%s\" (recv). Error %i %s." note errn errm |> log.Trace
                match recv () with
                | Choice1Of2 pid -> masterpid <- pid
                | Choice2Of2 (errn, errm) ->
                    sprintf "No aknowledgment of note \"%s\" (recv). Error %i %s." note errn errm |> log.Warn
            if masterpid <> -1 then
                sprintf "Aknowledgment of note \"%s\" (recv). Master pid: %i." note masterpid |> log.Info
            NN.Shutdown(nsocket, eid) |> ignore
            NN.Close(nsocket) |> ignore
            ok
        elif parsedArgs.ContainsKey "openMaster" then
            log.Info "Master starting."
            let nsocket = NN.Socket(Domain.SP, Protocol.PAIR)
            //TODO:error handling for socket and bind
            assert (nsocket >= 0)
            assert (NN.SetSockOpt(nsocket, SocketOption.SNDTIMEO, 1000) = 0)
            assert (NN.SetSockOpt(nsocket, SocketOption.RCVTIMEO, 1000) = 0)
            let eidp = NN.Connect(nsocket, notifyAddress)
            let mutable isMasterRunning = false
            if eidp >= 0 then
                let buff : byte array = Array.zeroCreate maxMessageSize
                let sr = NN.Send(nsocket, Encoding.UTF8.GetBytes("is-master"), SendRecvFlags.NONE)
                if sr < 0 then
                    let errn = NN.Errno()
                    let errm = NN.StrError(errn)
                    //11 Resource unavailable, try again
                    if errn = 11 && errm = "Resource unavailable, try again" then
                        sprintf "Expected error checking for master (send). %s." errm |> log.Trace
                    else
                        sprintf "Error %i checking for master (send). %s." errn errm |> log.Warn
                let recv () =
                    let rr = NN.Recv(nsocket, buff, SendRecvFlags.NONE)
                    if rr < 0 then
                        let errn = NN.Errno()
                        let errm = NN.StrError(errn)
                        //11 Resource unavailable, try again
                        if errn = 11 && errm = "Resource unavailable, try again" then
                            sprintf "Expected error checking for master (recv). %s." errm |> log.Trace
                        else
                            sprintf "Error %i checking for master (recv). %s." errn errm |> log.Warn
                        false, errn
                    else
                        true, 0
                match recv () with
                | false, 11 -> recv() |> ignore //we try again
                | _ -> ()
                isMasterRunning <- buff.[0] = 1uy
            if isMasterRunning then
                sprintf "Master is already running. Terminating." |> log.Warn
                NN.Shutdown(nsocket, eidp) |> ignore
                NN.Close(nsocket) |> ignore
                ok
            else

            let socket = NN.Socket(Domain.SP, Protocol.SURVEYOR)
            //TODO: error handling for socket and bind
            assert (socket >= 0)
            let curp = System.Diagnostics.Process.GetCurrentProcess()
            //TODO: get arguments information from current process?
            //let serviceCmd = curp.MainModule.fileName + " " + curp.StartInfo.Arguments
            let openTime = DateTimeOffset(curp.StartTime)
            openedSrvs.[0] <- {
                OpenServiceData.Default with
                    logicId = 0;
                    processId = curp.Id;
                    openTime = openTime;
                    fileName = curp.MainModule.FileName;}
            let ca = config.["controlAddress"]
            let eid = NN.Bind(socket, ca)
            let eidp = NN.Bind(nsocket, notifyAddress)
            assert (eid >= 0)
            assert (eidp >= 0)
            //TODO: Check for already running ormonit services in case master is interrupted/killed externally
            //config <- RequestStatus config socket
            //config <- CollectStatus config q socket
            //let sr = NN.Send(socket, Encoding.UTF8.GetBytes("close"), SendRecvFlags.NONE)
            //TODO:Error handling NN.Errno ()
            //assert (sr >= 0)
            let index = 1
            let mutable last = 1
            let services = loadServices config Environment.CurrentDirectory
            services
            |> List.iteri (fun i it ->
                let lid =  index + i
                let args = sprintf "--open-service %s --ctrl-address %s --logic-id %d" it.Location ca lid
                let cmdArgs = sprintf "%s %s" ormonitFileName args
                log.Trace(cmdArgs)
                let p = executeProcess ormonitFileName args olog
                let openTime = DateTimeOffset(p.StartTime)
                openedSrvs.[lid] <- {
                    OpenServiceData.Default with
                        logicId = lid;
                        processId = p.Id;
                        openTime = openTime;
                        fileName = it.Location;}
                last <- lid )
            sprintf """Master started with %i %s, controlAddress: "%s".""" last (if last > 1 then "services" else "service") ca
            |> log.Info
            executeSupervisor config openedSrvs socket nsocket
            assert(NN.Shutdown(socket, eid) = 0)
            assert(NN.Shutdown(nsocket, eidp) = 0)
            assert(NN.Close(socket) = 0)
            assert(NN.Close(nsocket) = 0)
            log.Info "Master stopped."
            NN.Term()
            ok
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
    sprintf "Run with arguments \"%s\"" (String.Join(" ", argv)) |> log.Trace
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
            sprintf "Command failed.\nErrorType:%s\nError:\n%s" (ex.GetType().Name) ex.Message |> log.Error
            sprintf "StackTrace: %s" ex.StackTrace |> log.Error
        else
            sprintf "Command failed.\nErrorType:%s\nError:\n%s\nInnerException:\n%s" (ex.GetType().Name) ex.Message ex.InnerException.Message |> log.Error
            sprintf "StackTrace: %s" ex.StackTrace |> log.Error
        1
