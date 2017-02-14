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
open Ormonit.Service

let controlAddress = "ipc://ormonit/control.ipc"
let dataAddress = "ipc://ormonit/data.ipc"
let ormonitShellName = Path.Combine(Environment.CurrentDirectory, "Ormonit.Service")

let printUsage cliUsage =
    printfn @"
Ormonit.Service [options]
The most commonly Ormonit.Service usages are:
    Ormonit.Service
    Ormonit.Service --run-service service.oml

Options:
    %s" cliUsage

let log = LogManager.GetLogger "Ormonit.Service"
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
let rule2 = new NLog.Config.LoggingRule("Ormonit.*", LogLevel.Trace, fileTarget)
logConfig.LoggingRules.Add(rule2)
LogManager.Configuration <- logConfig

let socket = NN.Socket(Domain.SP, Protocol.BUS)
let private q = ConcurrentQueue()
let mutable appState = Map.empty.Add("controlAddress", controlAddress).
                                 Add("dataAddress", dataAddress)

let LoadServices (astate : Map<string, string>) (basedir) =
    let asl = AssemblyLoader()
    let bdir = DirectoryInfo(basedir)
    bdir.GetFiles("*.oml")
    |> Seq.fold (fun acc it ->
    let s =
        try
            sprintf "Try to load \"%s\"" it.FullName |> log.Trace
            let asm = asl.LoadFromAssemblyPath(it.FullName)
            //TODO:How to guaranty this is a valid service?!
            Some(asm)
        with
            | ex ->
            log.Error(ex, "Unable to load assemlby {0}", it.FullName)
            None
    match s with
    | None  -> acc
    | _ -> s.Value :: acc
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

let stop () =
    log.Info "Ormonit master stopping."
    ///(services : Assembly list)
    let ca = appState.["controlAddress"]
    let sr = NN.Send(socket, Encoding.UTF8.GetBytes("stop"), SendRecvFlags.NONE)
    //TODO:Error handling NN.Errno ()
    assert (sr >= 0)
    log.Info "Ormonit master stopped."

[<EntryPoint>]
let main argv =
    sprintf "Start ormonit with arguments %s" (String.Join(" ", argv)) |> log.Info
    if argv.Length > 0 then
        try
            Cli.addArg {
                Cli.arg with
                        Option = "-r"
                        LongOption = "--run-service"
                        Destination="runService"
                        }
            Cli.addArg {
                Cli.arg with
                        Option = "-c"
                        LongOption = "--ctrl-address"
                        Destination="controlAddress"
                        }
            Cli.addArg {
                Cli.arg with
                        Option = "-d"
                        LongOption = "--data-address"
                        Destination="dataAddress"
                        }
            let parsed = Cli.parseArgs argv
            match parsed with
            | Choice1Of2(parsedArgs) ->
                if parsedArgs.ContainsKey("runService") <> true then
                    ()
                else
                    let runArg = parsedArgs.["runService"]
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
            //Environment.FailFast("", ex)
            1
    else
        log.Info "Ormonit master starting."
        let ca = appState.["controlAddress"]
        let da = appState.["dataAddress"]
        //TODO:error handling for socket and bind
        assert (socket >= 0)
        assert (NN.Bind(socket, ca) >= 0)
        //Check for already running ormonit services
        appState <- RequestStatus appState socket
        appState <- CollectStatus appState q socket
        ///
        let sr = NN.Send(socket, Encoding.UTF8.GetBytes("stop"), SendRecvFlags.NONE)
        //TODO:Error handling NN.Errno ()
        assert (sr >= 0)
        let services = LoadServices appState Environment.CurrentDirectory
        services
        |> List.iter (fun it ->
            let cmdarg = sprintf "--run-service %s --ctrl-address %s --data-address %s" it.Location ca da
            log.Info(ormonitShellName + " " + cmdarg)
            ExecuteProcess olog ormonitShellName cmdarg )
        log.Info "Ormonit master started."
        Supervise appState services q socket
        0
