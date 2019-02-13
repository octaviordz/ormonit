namespace Ormonit.Hosting

open System
open NLog
open NLog.Layouts
open Ormonit.Security
open Ormonit
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open System.Diagnostics
open System

module internal Host = 
    let log = LogManager.GetLogger "Ormonit.Hosting"
    let logConfig = NLog.Config.LoggingConfiguration()
    let consoleTarget = new NLog.Targets.ColoredConsoleTarget()
    
    logConfig.AddTarget("console", consoleTarget)
    
    let fileTarget = new NLog.Targets.FileTarget()
    
    logConfig.AddTarget("file", fileTarget)
    consoleTarget.Layout <- Layout.FromString @"${date:format=HH\:mm\:ss}|${logger}|${message}"
    fileTarget.FileName <- Layout.FromString @"${basedir}\logs\Ormonit.Hosting.log"
    fileTarget.ArchiveFileName <- Layouts.Layout.FromString @"${basedir}\logs\archive\{#}.Ormonit.Hosting.log"
    fileTarget.ArchiveNumbering <- Targets.ArchiveNumberingMode.DateAndSequence
    fileTarget.ArchiveAboveSize <- 524288L
    fileTarget.MaxArchiveFiles <- 2
    fileTarget.ArchiveDateFormat <- "yyyy-MM-dd"
    
    let rule1 = new NLog.Config.LoggingRule("*", NLog.LogLevel.Trace, consoleTarget)
    
    logConfig.LoggingRules.Add(rule1)
    
    let rule2 = new NLog.Config.LoggingRule("*", NLog.LogLevel.Trace, fileTarget)
    
    logConfig.LoggingRules.Add(rule2)
    LogManager.Configuration <- logConfig

    Cli.addArg { Cli.arg with Option = "-pid"
                              LongOption = "--process-id"
                              Destination = "processId" }
    Cli.addArg { Cli.arg with Option = "-pstartt"
                              LongOption = "--process-start-time"
                              Destination = "processStartTime" }
    Cli.addArg { Cli.arg with Option = "--ctrl-address"
                              Destination = "controlAddress" }
    Cli.addArg { Cli.arg with Option = "--logic-id"
                              Destination = "logicId" }
    Cli.addArg { Cli.arg with Option = "--public-key"
                              Destination = "publicKey" }

[<Struct>]
type ReportStatusToken = 
    val mutable private src : ReportStatusTokenSource
    new(source : ReportStatusTokenSource) = { src = source }
    member s.Register(callback : Func<string>) : unit = s.src.Register(callback)
    member s.Report(status : string) : unit = s.src.PostStatus(status)
    static member None = Unchecked.defaultof<ReportStatusToken>

and ReportStatusTokenSource() = 
    let actions : List<Func<string>> = List<Func<string>>()
    
    member s.ReportStatus(miliseconds : float) : string = 
        let mutable status = String.Empty
        actions.ForEach(fun it -> status <- it.Invoke())
        status

    member __.PostStatus(status : string) : unit = ()
    member __.Register(callback : Func<string>) : unit = actions.Add(callback)
    member s.Token : ReportStatusToken = ReportStatusToken(s)

type ServiceData = 
    { state : string
      task : Task
      reportStatusTokenSource : ReportStatusTokenSource }
    static member None = 
        { state = String.Empty
          task = Unchecked.defaultof<Task>
          reportStatusTokenSource = Unchecked.defaultof<ReportStatusTokenSource> }

type ServiceHost() = 
    let ts = List<System.Type>()
    let services = List<ServiceData>()
    let log = Host.log
    let ckey = randomKey()
    
    member __.AddService<'T>() : unit = 
        ts.Add(typeof<'T>)
        ()
    
    member s.Run() : unit = 
        let (?) (t : Type) (mname : string) = 
            t.GetMethod(mname, [| typeof<ReportStatusToken>; typeof<CancellationToken> |])
        let matchTMsg tmsg onmsgf onerrorf = 
            match tmsg with
                | Error err -> onerrorf err
                | Ok (ckey, note) -> onmsgf ckey note

        //sprintf "[%i] Ormonit test in control loop with" lid |> log.Trace
        let nsok = Cilnn.Nn.Socket(Cilnn.Domain.SP, Cilnn.Protocol.SURVEYOR)
        //TODO:error handling for socket and bind
        assert (nsok >= 0)
        assert (Cilnn.Nn.SetSockOpt(nsok, Cilnn.SocketOption.SURVEYOR_DEADLINE, Ctrl.superviseInterval * 10) = 0)
        assert (Cilnn.Nn.SetSockOpt(nsok, Cilnn.SocketOption.SNDTIMEO, Ctrl.superviseInterval * 10) = 0)
        assert (Cilnn.Nn.SetSockOpt(nsok, Cilnn.SocketOption.RCVTIMEO, Ctrl.superviseInterval * 10) = 0)
        let eid = Cilnn.Nn.Connect(nsok, Ctrl.notifyAddress)
        assert (eid >= 0)
        let cprocess = Process.GetCurrentProcess()
        let lid = cprocess.Id
        let note = sprintf "sys:self-init --process-id %d --process-start-time %s" cprocess.Id (cprocess.StartTime.ToString("o"))
        let notyMaster () : Result<Comm.TMsg, Comm.Error> = 
            sprintf "Sending \"%s\" note." note |> log.Trace
            match Comm.sendWith Cilnn.SendRecvFlags.NONE nsok (String.Empty, note) with
            | Error (errn, errm) -> 
                match errn with
                | 156384766 -> 
                    sprintf "Unable to notify \"%s\" (send). Error %i %s." note errn errm |> log.Warn
                | _ -> 
                    sprintf """Error %i on "self-init" (send). %s.""" errn errm |> log.Warn
                Error (errn, errm)
            | r -> 
                sprintf "Sent \"%s\" note." note |> log.Trace
                match Comm.recv nsok with
                | Error (errn, errm)-> 
                    sprintf """Error %i on "self-init" (recv). %s.""" errn errm |> log.Warn
                    Error (errn, errm)
                | Ok (k, m) -> 
                    sprintf """Master notified of "%s" with response "%s".""" note m |> log.Trace
                    Ok (k, m)
        let notified = Ctrl.retrytWith 60000 notyMaster
        Cilnn.Nn.Shutdown(nsok, eid) |> ignore
        Cilnn.Nn.Close(nsok) |> ignore
        match notified with
        | Error _ -> 
            sprintf """Unable to notify master.""" |> log.Error
        | Ok (k, selfInitResponse) -> 

        let envp = { Ctrl.Envelop.from = Ctrl.notifyAddress
                     Ctrl.Envelop.msg = k, selfInitResponse
                     Ctrl.Envelop.timeStamp = DateTimeOffset.Now }
        let selfinit (msgs : Ctrl.Envelop list) = 
            let _, note, nmsg = 
                let emptyResult = String.Empty, String.Empty, []
                match msgs with
                | [] -> 
                    log.Info("[{0}] Host receive (blocking).", lid)
                    let tmsg = Comm.recv nsok
                    let whenmsg k note =
                        sprintf "[%d] Host \"%s\" note received." lid note |> log.Info
                        k, note, []
                    matchTMsg tmsg whenmsg (fun error -> 
                        let errn, errm = error
                        sprintf """[%d] Host error %i (recv). %s.""" lid errn errm |> log.Error
                        emptyResult )
                | head :: tail -> 
                    let whenmsg k n =
                        k, n, tail
                    matchTMsg (Ok head.msg) whenmsg (fun _ -> emptyResult)

            let nparts = note.Split([| ' ' |], StringSplitOptions.RemoveEmptyEntries)

            let args = 
                List.ofArray (if nparts.Length > 1 then nparts.[1..]
                              else [||])

            match Cli.parseArgs args with 
            | Error ex -> 
                sprintf "[%d] Host unable to parse arguments in note \"%s\"." lid note |> log.Warn
                Error ex.Message
            | Ok parsedArgs -> 
                let cmd = 
                    let isValidProcessId =
                        match parsedArgs.TryGetValue "processId" with
                        | false, _ -> 
                            sprintf "[%d] No processId in note \"%s\"." lid note |> log.Warn
                            false
                        | true, processId when processId <> lid.ToString() -> 
                            sprintf "[%d] Invalid processId ignoring note \"%s\"." lid note |> log.Warn
                            false
                        | _ -> true
                    let isValidLogicId = 
                        match parsedArgs.TryGetValue "logicId" with
                        | false, _ -> 
                            sprintf "[%d] No logicId in note \"%s\"." lid note |> log.Warn
                            false
                        | _ -> 
                            match Int32.TryParse (parsedArgs.["logicId"]) with
                            | false, _ -> false
                            | _ -> true
                    let isValid = isValidProcessId && isValidLogicId
                    match isValid with
                    | false -> String.Empty
                    | true -> 
                        if nparts.Length > 0 then nparts.[0]
                        else String.Empty
                sprintf "[%d] Host processing command \"%s\"." lid cmd |> log.Info
                match cmd with
                | "sys:r:self-init" -> 
                    //"sys:r:self-init --logic-id %d --process-id %d --public-key %s --ctrl-address %s"
                    Ok parsedArgs
                | unkown -> 
                    Error (sprintf "Unknown message: \"%s\"." unkown)

        match Ctrl.retrytWith 5000 (fun () -> selfinit [envp]) with 
        | Error exm -> 
            sprintf "[%d] Host unable to self-initialize. \"%s\"" lid exm |> log.Error
            ()
        | Ok config -> 

        let sok = Cilnn.Nn.Socket(Cilnn.Domain.SP, Cilnn.Protocol.RESPONDENT)
        assert (Cilnn.Nn.SetSockOpt(sok, Cilnn.SocketOption.SNDTIMEO, Ctrl.superviseInterval * 5) = 0)
        assert (Cilnn.Nn.SetSockOpt(sok, Cilnn.SocketOption.RCVTIMEO, Ctrl.superviseInterval * 5) = 0)
        //TODO:error handling for socket and connect
        assert (sok >= 0)
        let eid = Cilnn.Nn.Connect(sok, config.["controlAddress"])
        assert (eid >= 0)
        let lid = Int32.Parse config.["logicId"]
        let publicKey = 
            match config.TryGetValue "publicKey" with
            | false, _ -> None
            | true, puk -> Some puk
        use ctsrc = new CancellationTokenSource()
        ts
        |> Seq.iter (fun t -> 
               let rtsrc = ReportStatusTokenSource()
               let runAsync = t?RunAsync
               //let ckey = randomKey()
               let srv = Activator.CreateInstance(t, [||])
               let task = runAsync.Invoke(srv, [| rtsrc.Token; ctsrc.Token |]) :?> Task
               services.Add({ ServiceData.None with task = task
                                                    reportStatusTokenSource = rtsrc }) )
        let rec recvloop (msgs : Ctrl.Envelop list) = 
            //let mutable buff : byte[] = null '\000'
            let k, note, nmsg = 
                let emptyResult = String.Empty, String.Empty, []
                match msgs with
                | [] -> 
                    log.Info("[{0}] Host receive (blocking).", lid)
                    let tmsg = Comm.recv sok
                    let whenmsg k note =
                        sprintf "[%d] Host \"%s\" note received." lid note |> log.Info
                        k, note, []
                    matchTMsg tmsg whenmsg (fun error -> 
                        let errn, errm = error
                        sprintf """[%d] Error %i (recv). %s.""" lid errn errm |> log.Error
                        emptyResult )
                | head :: tail -> 
                    let whenmsg k n =
                        k, n, tail
                    matchTMsg (Ok head.msg) whenmsg (fun _ -> emptyResult)

            let nparts = note.Split([| ' ' |], StringSplitOptions.RemoveEmptyEntries)
            let args = 
                List.ofArray (if nparts.Length > 1 then nparts.[1..]
                              else [||])
    
            match Cli.parseArgs args with 
            | Error _ -> 
                sprintf "[%d] Unable to parse arguments in note \"%s\"." lid note |> log.Warn
                recvloop nmsg
            | Ok parsedArgs -> 
                let command = 
                    match parsedArgs.TryGetValue "logicId" with
                    | false, _ -> 
                        sprintf "[%d] No logicId in note \"%s\"." lid note |> log.Warn
                        Some note
                    | true, logicId when logicId <> lid.ToString() -> 
                        sprintf "[%d] Invalid logicId ignoring note \"%s\"." lid note |> log.Warn
                        None
                    | true, _ -> 
                        if nparts.Length > 0 then Some nparts.[0]
                        else None
                match command with
                | None -> recvloop nmsg
                | Some cmd ->

                sprintf "[%d] Host processing command \"%s\"." lid cmd |> log.Info
                match cmd with
                | "sys:close" -> 
                    //log.Trace("""[{0}] Processing "close" notification. continue value {1}""", lid, flag)
                    ctsrc.Cancel()
                    log.Trace("""[{0}] Sending "close" aknowledgement.""", lid)
                    match (ckey, "closing") |> Comm.send sok with
                    | Error (errn, errm) -> 
                        sprintf """Error %i (send). %s.""" errn errm |> log.Error
                        recvloop nmsg
                    | Ok _ -> ()
                    //log.Trace("""[{0}] Sent "close" aknowledgement.""", lid)
                | "sys:report-status" -> 
                    log.Trace("""[{0}] Processing "report-status" command.""", lid)
                    let errors = 
                        services |> Seq.fold (fun notOk it -> 
                                        let status = it.reportStatusTokenSource.ReportStatus(float (Ctrl.actionTimeout))
                                        match status with
                                        | "ok" -> notOk
                                        | nok -> { it with state = nok } :: notOk ) List.empty

                    let summaryNote = 
                        match errors with
                        | [] -> "ok"
                        | _ -> "error"

                    log.Trace("""[{0}] Sending "report-status" aknowledgement.""", lid)
                    match (ckey, summaryNote) |> Comm.send sok with
                    | Error (errn, errm) -> sprintf """Error %i (send). %s.""" errn errm |> log.Error
                    | Ok _ -> ()
                    log.Trace("""[{0}] Sent "report-status" aknowledgement.""", lid)
                    recvloop nmsg
                | "sys:client-key" -> 
                    log.Trace("""[{0}] Sending client-key: "{1}".""", lid, ckey)
                    let note =
                        match publicKey with
                        | None -> ckey
                        | Some pk -> 
                            let encrypted = encrypt pk ckey
                            Convert.ToBase64String(encrypted)
                    match (lid.ToString(), note) |> Comm.send sok with
                    | Error (errn, errm) -> sprintf """Error %i (send). %s.""" errn errm |> log.Error
                    | Ok _ -> log.Trace("""[{0}] Sent client-key: "{1}".""", lid, ckey)
                    recvloop nmsg
                | _ -> recvloop nmsg
        recvloop []
        assert (Cilnn.Nn.Shutdown(sok, eid) = 0)
        assert (Cilnn.Nn.Close(sok) = 0)
