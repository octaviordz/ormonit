open Ctrl
open System
open System.IO
open NLog
open NLog.Layouts
open System.Text
open NNanomsg

let mutable config = Map.empty.Add("controlAddress", "ipc://ormonit/control.ipc")
let notifyAddress = "ipc://ormonit/notify.ipc"
let printUsage () =
    printfn @"
Ormonit [options]

Options:
    -n close|status
"
let log = LogManager.GetLogger "Ormonit"
let olog = LogManager.GetLogger "_Ormonit.Output_"
let logConfig = NLog.Config.LoggingConfiguration()
let consoleTarget = new NLog.Targets.ColoredConsoleTarget()
logConfig.AddTarget("console", consoleTarget)
let fileTarget = new NLog.Targets.FileTarget()
logConfig.AddTarget("file", fileTarget)
consoleTarget.Layout <- Layout.FromString @"${date:format=HH\:mm\:ss}|${logger}|${message}"
fileTarget.FileName <- Layout.FromString @"${basedir}\logs\${shortdate}.log"
fileTarget.ArchiveFileName <- Layout.FromString @"${basedir}\logs\archive\{#}.log"
fileTarget.ArchiveNumbering <- Targets.ArchiveNumberingMode.DateAndSequence
fileTarget.ArchiveAboveSize <- 1048576L
fileTarget.MaxArchiveFiles <- 3
fileTarget.ArchiveDateFormat <- "yyyy-MM-dd"
let rule1 = new NLog.Config.LoggingRule("*", LogLevel.Trace, consoleTarget)
logConfig.LoggingRules.Add(rule1)
let rule2 = new NLog.Config.LoggingRule("Ormonit*", LogLevel.Trace, fileTarget)
logConfig.LoggingRules.Add(rule2)
LogManager.Configuration <- logConfig

let parseAndExecute argv =
    let openedSrvs: OpenServiceData array = Array.create maxOpenServices OpenServiceData.Default
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
    | Choice1Of2(parsedArgs) ->
        let validArgs =
            (parsedArgs.Count = 1 && parsedArgs.ContainsKey("notify")) ||
            (parsedArgs.Count = 1 && parsedArgs.ContainsKey("openMaster")) ||
            (parsedArgs.Count = 3 && parsedArgs.ContainsKey("openService") &&
                parsedArgs.ContainsKey("controlAddress") &&
                parsedArgs.ContainsKey("logicId"))
        if not validArgs then
            printUsage()
        elif parsedArgs.ContainsKey("notify") then
            let note = parsedArgs.["notify"]
            let nsocket = NN.Socket(Domain.SP, Protocol.PAIR)
            //TODO:error handling for socket and bind
            assert (nsocket >= 0)
            assert (NN.SetSockOpt(nsocket, SocketOption.SNDTIMEO, Ctrl.superviseInterval * 5) = 0)
            assert (NN.SetSockOpt(nsocket, SocketOption.RCVTIMEO, Ctrl.superviseInterval * 5) = 0)
            let eid = NN.Connect(nsocket, notifyAddress)
            assert ( eid >= 0)
            sprintf "Notify \"%s\" (notify process)." note |> log.Trace
            let sr = NN.Send(nsocket, Encoding.UTF8.GetBytes(note), SendRecvFlags.NONE)
            if sr < 0 then
                let errm = NN.StrError(NN.Errno())
                sprintf "Unable to notify \"%s\" (send)." errm |> log.Error
            NN.Shutdown(nsocket, eid) |> ignore
            NN.Close(nsocket) |> ignore
        elif parsedArgs.ContainsKey("openMaster") then
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
                        sprintf "Error %i checking for master (recv). %s." errn errm |> log.Warn
                        false, errn
                    else
                        true, 0
                let r, errn = recv ()
                if r = false && errn = 11
                then recv() |> ignore
                isMasterRunning <- buff.[0] = 1uy
            if isMasterRunning then
                sprintf "Master is already running. Terminating." |> log.Warn
                NN.Shutdown(nsocket, eidp) |> ignore
                NN.Close(nsocket) |> ignore
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
        elif parsedArgs.ContainsKey("openService") then
            let runArg = parsedArgs.["openService"]
            //TOOD: Get arguments --ctrl-address?
            let srvconfig = config.Add("assemblyPath", runArg).
                                   Add("logicId", parsedArgs.["logicId"])
            executeService srvconfig
    | Choice2Of2(exn) ->
        printUsage()

[<EntryPoint>]
let main argv =
    sprintf "Start with arguments \"%s\"" (String.Join(" ", argv)) |> log.Trace
    if argv.Length <= 0 then
        executeProcess ormonitFileName "--open-master" olog |> ignore
        0
    else
    try
        parseAndExecute argv
        0
    with
    | ex ->
        if isNull ex.InnerException then
            sprintf "Command failed.\nErrorType:%s\nError:\n%s" (ex.GetType().Name) ex.Message |> log.Error
            sprintf "StackTrace: %s" ex.StackTrace |> log.Error
        else
            sprintf "Command failed.\nErrorType:%s\nError:\n%s\nInnerException:\n%s" (ex.GetType().Name) ex.Message ex.InnerException.Message |> log.Error
            sprintf "StackTrace: %s" ex.StackTrace |> log.Error
        1
