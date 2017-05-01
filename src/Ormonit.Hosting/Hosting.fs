namespace Ormonit.Hosting

open System
open NLog
open NLog.Layouts
open System.Collections.Generic
open System.Text
open System.Threading
open System.Threading.Tasks
open System.Security.Cryptography
open NNanomsg
open Comm
open Ormonit.Security

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
    
    let rule1 = new NLog.Config.LoggingRule("*", LogLevel.Trace, consoleTarget)
    
    logConfig.LoggingRules.Add(rule1)
    
    let rule2 = new NLog.Config.LoggingRule("*", LogLevel.Trace, fileTarget)
    
    logConfig.LoggingRules.Add(rule2)
    LogManager.Configuration <- logConfig

[<Struct>]
type ReportStatusToken = 
    val mutable src : ReportStatusTokenSource
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
    
    member s.PostStatus(status : string) : unit = ()
    member s.Register(callback : Func<string>) : unit = actions.Add(callback)
    member s.Token : ReportStatusToken = ReportStatusToken(s)

type ServiceData = 
    { state : string
      task : Task
      rtsrc : ReportStatusTokenSource }
    static member None = 
        { state = String.Empty
          task = Unchecked.defaultof<Task>
          rtsrc = Unchecked.defaultof<ReportStatusTokenSource> }

type ServiceHost() = 
    let mutable ts = List.empty
    let mutable services = List.empty
    let log = Host.log
    let ckey = randomKey()
    
    member s.AddService<'T>() : unit = 
        ts <- typeof<'T> :: ts
        ()
    
    member s.Run() : unit = 
        let mutable rtsrcs = List.empty<ReportStatusTokenSource>
        let mutable publicKey = String.Empty
        let mutable lid = "host"
        let emptyDic = Dictionary<string, string>()
        let ca = Ctrl.controlAddress
        let (?) (t : Type) (mname : string) = t.GetMethod(mname)
        use ctsrc = new CancellationTokenSource()
        ts
        |> List.rev
        |> List.iter (fun t -> 
               let rtsrc = ReportStatusTokenSource()
               rtsrcs <- rtsrc :: rtsrcs
               let runAsync = t?RunAsync
               let ckey = randomKey()
               let srv = Activator.CreateInstance(t, [||])
               let task = runAsync.Invoke(srv, [| rtsrc.Token, ctsrc.Token |]) :?> Task
               services <- { ServiceData.None with task = task
                                                   rtsrc = rtsrc }
                           :: services)
        //sprintf "[%i] Ormonit test in control loop with" lid |> log.Trace
        let s = NN.Socket(Domain.SP, Protocol.RESPONDENT)
        //TODO:error handling for socket and connect
        assert (s >= 0)
        let eid = NN.Connect(s, ca)
        assert (eid >= 0)
        let rec recvloop() = 
            //let mutable buff : byte[] = null '\000'
            //log.Info("[{0}] Ormonit test receive (blocking).", lid)
            match recv s with
            | Error(errn, errm) -> 
                sprintf """[%s] Error %i (recv). %s.""" lid errn errm |> log.Error
                recvloop()
            | Msg(_, note) -> 
                sprintf "[%s] Ormonit test \"%s\" note received." lid note |> log.Info
                let nparts = note.Split([| ' ' |], StringSplitOptions.RemoveEmptyEntries)
                
                let args = 
                    if nparts.Length > 1 then nparts.[1..]
                    else [||]
                
                let cmd, parsed = 
                    match Cli.parseArgs args with
                    | Choice2Of2 exn -> 
                        sprintf "[%s] Unable to parse arguments in note \"%s\"." lid note |> log.Warn
                        note, emptyDic
                    | Choice1Of2 parsed -> 
                        match parsed.TryGetValue "logicId" with
                        | false, _ -> 
                            sprintf "[%s] No logicId in note \"%s\"." lid note |> log.Warn
                            note, parsed
                        | true, logicId when logicId <> lid.ToString() -> 
                            sprintf "[%s] Invalid logicId ignoring note \"%s\". " lid note |> log.Warn
                            String.Empty, emptyDic
                        | true, logicId -> 
                            if nparts.Length > 0 then nparts.[0], parsed
                            else String.Empty, emptyDic
                
                sprintf "[%s] Ormonit test command \"%s\"." lid cmd |> log.Info
                match cmd with
                | "sys:close" -> 
                    //log.Trace("""[{0}] Processing "close" notification. continue value {1}""", lid, flag)
                    ctsrc.Cancel()
                    log.Trace("""[{0}] Sending "close" aknowledgement.""", lid)
                    match Msg(ckey, "closing") |> send s with
                    | Error(errn, errm) -> 
                        sprintf """Error %i (send). %s.""" errn errm |> log.Error
                        recvloop()
                    | Msg _ -> ()
                //log.Trace("""[{0}] Sent "close" aknowledgement.""", lid)
                | "sys:report-status" -> 
                    log.Trace("""[{0}] Processing "report-status" command.""", lid)
                    let errors = 
                        services |> List.fold (fun notOk it -> 
                                        let status = it.rtsrc.ReportStatus(Ctrl.actionTimeout)
                                        match status with
                                        | "ok" -> notOk
                                        | nok -> { it with state = nok } :: notOk) List.empty
                    
                    let summaryNote = 
                        match errors with
                        | [] -> "ok"
                        | _ -> "error"
                    
                    log.Trace("""[{0}] Sending "report-status" aknowledgement.""", lid)
                    match Msg(ckey, summaryNote) |> send s with
                    | Error(errn, errm) -> sprintf """Error %i (send). %s.""" errn errm |> log.Error
                    | Msg _ -> ()
                    log.Trace("""[{0}] Sent "report-status" aknowledgement.""", lid)
                    recvloop()
                | "sys:public-key" -> 
                    lid <- parsed.["logicId"]
                    publicKey <- parsed.["publicKey"]
                    log.Trace("""[{0}] Reciving public-key: "{1}".""", lid, ckey)
                    match Msg(lid.ToString(), "ok") |> send s with
                    | Error(errn, errm) -> sprintf "Error %i sending public-key aknowledge. %s." errn errm |> log.Error
                    | Msg _ -> log.Trace("[{0}] Sent public-key aknowledge.", lid)
                    recvloop()
                | "sys:client-key" -> 
                    log.Trace("""[{0}] Sending client-key: "{1}".""", lid, ckey)
                    let encrypted = encrypt publicKey ckey
                    let note = Convert.ToBase64String(encrypted)
                    match Msg(lid.ToString(), note) |> send s with
                    | Error(errn, errm) -> sprintf """Error %i (send). %s.""" errn errm |> log.Error
                    | Msg _ -> log.Trace("""[{0}] Sent client-key: "{1}".""", lid, ckey)
                    recvloop()
                | _ -> recvloop()
        recvloop()
        assert (NN.Shutdown(s, eid) = 0)
        assert (NN.Close(s) = 0)
