open System
open System.IO
open NLog
open NLog.Layouts
open System.Reflection
open System.Collections.Generic
open System.Collections.Concurrent
open System.Diagnostics
open System.Text
open System.Threading
open NNanomsg

let controlAddress = "ipc://ormonit/control.ipc"
let dataAddress = "ipc://ormonit/data.ipc"
let ciAddress = "ipc://ormonit/ci.ipc"

let ormonitShellName = Path.Combine(Environment.CurrentDirectory, "Ormonit")
let printUsage cliUsage =
    printfn @"
Ormonit [options]
The most commonly Ormonit.Service usages are:
    ormonit --open-master
    ormonit --open-service service.oml

Options:
    %s" cliUsage
let log = LogManager.GetLogger "Ormonit"
let olog = LogManager.GetLogger "_Ormonit.Output_"
let logConfig = NLog.Config.LoggingConfiguration()
let consoleTarget = new NLog.Targets.ColoredConsoleTarget()
logConfig.AddTarget("console", consoleTarget)
let fileTarget = new NLog.Targets.FileTarget()
logConfig.AddTarget("file", fileTarget)
//consoleTarget.Layout <- Layout.FromString @"${date:format=HH\:mm\:ss} ${logger} ${message}"
consoleTarget.Layout <- Layout.FromString @"${date:format=HH\:mm\:ss} ${message}"
fileTarget.FileName <- Layout.FromString @"${basedir}\logs\${shortdate}.log"
let rule1 = new NLog.Config.LoggingRule("*", LogLevel.Trace, consoleTarget)
logConfig.LoggingRules.Add(rule1)
let rule2 = new NLog.Config.LoggingRule("Ormonit*", LogLevel.Trace, fileTarget)
logConfig.LoggingRules.Add(rule2)
LogManager.Configuration <- logConfig

let private q = ConcurrentQueue()
let mutable appState = Map.empty.Add("controlAddress", controlAddress).
                                 Add("dataAddress", dataAddress)

let LoadServices (astate : Map<string, string>) (basedir) =
    let asl = AssemblyLoader()
    let bdir = DirectoryInfo(basedir)
    bdir.GetFiles("*.oml")
    |> Seq.fold (fun acc it ->
        let mutable asm:Assembly option = None
        try
            sprintf "Try to load \"%s\"" it.FullName |> log.Trace
            let a = asl.LoadFromAssemblyPath(it.FullName)
            //TODO:How to guaranty this is a valid service?!
            asm <- Some(a)
        with
            | ex ->
            log.Error(ex, "Unable to load assemlby {0}", it.FullName)
            asm <- None
        match asm with
        | None  -> acc
        | _ -> asm.Value :: acc
        ) List.empty<Assembly>

let RunService (config:Map<string, string>) =
    let p= config.["assemblyPath"]
    sprintf "Run service with configuration \"%A\"" config |> log.Trace
    let (?) (t : Type) (mname : string) =
        let m = t.GetMethod(mname)
        m
    let asl = AssemblyLoader()
    let asm = asl.LoadFromAssemblyPath(p)
    let name = asm.GetName().Name + ".Control"
    let t = asm.GetType(name)
    if isNull t then sprintf "Control not found: %s value %A." name t |> log.Warn
    else sprintf "Control found: %s value %A." name t |> log.Trace
    //sprintf "Control methods: %A." (t.GetMethods()) |> log.Trace
    //t.IsAbstract && t.IsSealed
    //let control = Activator.CreateInstance(t)
    //let m = obj?GetControl
    //let control = m.Invoke(obj, [||])
    let start = t?Start
    sprintf "Control start method: %A." start |> log.Trace
    start.Invoke(t, [|config|]) |> ignore
    ()

let RequestStatus (astate:Map<string, string>) socket =
    log.Info "Ormonit master request services status."
    let ca = astate.["controlAddress"]
    let sr = NN.Send(socket, Encoding.UTF8.GetBytes("report-status"), SendRecvFlags.NONE)
    //TODO:Error handling NN.Errno ()
    assert (sr >= 0)
    astate

let CollectStatus (config: Map<string, string>) (q:ConcurrentQueue<string>) socket =
    let ca = config.["controlAddress"]
    let da = config.["dataAddress"]
    sprintf "CollectStatus: controlAddress \"%s\", dataAddress \"%s\"." ca da |> log.Trace
    //let mutable buff : byte[] = null
    let buff : byte array = Array.zeroCreate 256
    let rc = NN.Recv(socket, buff, SendRecvFlags.DONTWAIT)
    if rc <= 0 then
        log.Error("Unable to collect status. NN.Errno {0}", NN.Errno())
        config
    else
    assert (rc >= 0)
    let c = Encoding.UTF8.GetString(buff.[..rc - 1])
    let cmd =
        c.ToCharArray()
        |> Array.takeWhile (fun it -> it <> '\000')
        |> String
    sprintf "CollectStatus \"%s\" response." cmd |> log.Trace
    config

let rec Supervisefn (appState:Map<string, string>) (services:Assembly list) (socket) (ssocket) (q:ConcurrentQueue<string>): unit =
    let ca = appState.["controlAddress"]
    let da = appState.["dataAddress"]
    //let mutable buff : byte[] = null
    let buff : byte array = Array.zeroCreate 256 //'\000'
    let rc = NN.Recv(ssocket, buff, SendRecvFlags.DONTWAIT)
    let mutable signal = String.Empty
    if rc >= 0 then
        signal <- Encoding.UTF8.GetString(buff.[..rc - 1])
        sprintf "Supervise recived signal \"%s\"." signal |> log.Trace
    match signal with
    | "stop" ->
        log.Trace "Ormonit master stopping."
        log.Trace "Supervisor sending stop signal to services."
        ///(services : Assembly list)
        let rsend = NN.Send(socket, Encoding.UTF8.GetBytes("stop"), SendRecvFlags.NONE)
        //TODO:Error handling NN.Errno ()
        assert (rsend >= 0)
        let rrecv = NN.Recv(socket, buff, SendRecvFlags.NONE)
        if rrecv < 0 then
            let errno = NN.Errno()
            let errm = NN.StrError(errno)
            sprintf "Error \"%s\" on recive from send stop signal." errm |> log.Warn
    | _ ->
        //log.Trace("Supervise no signal recived NN.Errno {0}", NN.Errno())
        //let appState = RequestStatus appState socket
        //let appState = CollectStatus appState q socket
        let jr = Thread.CurrentThread.Join(250)
        Supervisefn appState services socket ssocket q

let Supervise (appState:Map<string, string>) (services:Assembly list) (socket) (ssocket) (q:ConcurrentQueue<string>): unit =
    log.Info "Ormonit start supervise."
    let ca = appState.["controlAddress"]
    let da = appState.["dataAddress"]
    sprintf "Supervise controlAddress: \"%s\", dataAddress: \"%s\"." ca da |> log.Trace
    Supervisefn appState services socket ssocket q
    log.Info "Ormonit stop supervise."

Cli.addArg {
    Cli.arg with
            Option = "-s"
            LongOption = "--signal"
            Destination="signal"
            }
Cli.addArg {
    Cli.arg with
            Option = "--open-master"
            Destination="openMaster"
            }
Cli.addArg {
    Cli.arg with
            Option = "--open-service"
            Destination="openService"
            }
Cli.addArg {
    Cli.arg with
            Option = "--ctrl-address"
            Destination="controlAddress"
            }
Cli.addArg {
    Cli.arg with
            Option = "--data-address"
            Destination="dataAddress"
            }

[<EntryPoint>]
let main argv =
    sprintf "Start ormonit with arguments \"%s\"" (String.Join(" ", argv)) |> log.Trace
    if argv.Length <= 0 then
        ExecuteProcess olog ormonitShellName "--open-master"
        0
    else
    try
        let parsed = Cli.parseArgs argv
        match parsed with
        | Choice1Of2(parsedArgs) ->
            let validArgs =
                (parsedArgs.Count = 1 && parsedArgs.ContainsKey("signal")) ||
                (parsedArgs.Count = 1 && parsedArgs.ContainsKey("openMaster")) ||
                (parsedArgs.Count = 3 && parsedArgs.ContainsKey("openService") &&
                    parsedArgs.ContainsKey("controlAddress") &&
                    parsedArgs.ContainsKey("dataAddress"))
            if not validArgs then
                printUsage Cli.usage
            elif parsedArgs.ContainsKey("signal") then
                let signal = parsedArgs.["signal"]
                //TODO:check for already running master
                let ssocket = NN.Socket(Domain.SP, Protocol.PAIR)
                //TODO:error handling for socket and bind
                assert (ssocket >= 0)
                let eid = NN.Connect(ssocket, ciAddress)
                assert ( eid >= 0)
                sprintf "Sending signal \"%s\"." (signal) |> log.Trace
                let sr = NN.Send(ssocket, Encoding.UTF8.GetBytes(signal), SendRecvFlags.NONE)
                //TODO:Error handling NN.Errno ()
                assert (sr >= 0)
                let rsh = NN.Shutdown(ssocket, eid)
                assert(rsh >= 0)
                //TODO: NN.Term()?
            elif parsedArgs.ContainsKey("openMaster") then
                log.Info "Ormonit master starting."
                let socket = NN.Socket(Domain.SP, Protocol.SURVEYOR)
                let ssocket = NN.Socket(Domain.SP, Protocol.PAIR)
                //TODO:check for already running master
                let ca = appState.["controlAddress"]
                let da = appState.["dataAddress"]
                //TODO:error handling for socket and bind
                assert (socket >= 0)
                assert (ssocket >= 0)
                let eid = NN.Bind(socket, ca)
                let eidp = NN.Bind(ssocket, ciAddress)
                assert (eid >= 0)
                assert (eidp >= 0)
                //Check for already running ormonit services
                //appState <- RequestStatus appState socket
                //appState <- CollectStatus appState q socket
                //let sr = NN.Send(socket, Encoding.UTF8.GetBytes("stop"), SendRecvFlags.NONE)
                //TODO:Error handling NN.Errno ()
                //assert (sr >= 0)
                let services = LoadServices appState Environment.CurrentDirectory
                services
                |> List.iter (fun it ->
                    let cmdarg = sprintf "--open-service %s --ctrl-address %s --data-address %s" it.Location ca da
                    log.Info(ormonitShellName + " " + cmdarg)
                    ExecuteProcess olog ormonitShellName cmdarg )
                log.Info "Ormonit master started."
                Supervise appState services socket ssocket q
                assert(NN.Shutdown(socket, eid) >= 0)
                assert(NN.Shutdown(ssocket, eidp) >= 0)
                log.Info "Ormonit master stopped."
            elif parsedArgs.ContainsKey("openService") then
                let runArg = parsedArgs.["openService"]
                //sprintf "Initial configuration %A." astate |> log.Trace
                //TOOD:Get arguments --ctrl-address and --data-address
                let state = appState.Add("assemblyPath", runArg)
                //sprintf "Updated configuration %A." c |> log.Trace
                RunService state
        | Choice2Of2(exn) ->
            printUsage Cli.usage
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
