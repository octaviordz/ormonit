module Ormonit.ServiceTest.Control

open System
open System.IO
open NLog
open NLog.Layouts
open System.Collections.Generic
open System.Text
open System.Threading
open System.Threading.Tasks
open System.Security.Cryptography
open Cilnn
open Comm

let log = LogManager.GetLogger "Ormonit.Test"
let logConfig = NLog.Config.LoggingConfiguration()
let consoleTarget = new NLog.Targets.ColoredConsoleTarget()

logConfig.AddTarget("console", consoleTarget)

let fileTarget = new NLog.Targets.FileTarget()

logConfig.AddTarget("file", fileTarget)
consoleTarget.Layout <- Layout.FromString @"${date:format=HH\:mm\:ss}|${logger}|${message}"
fileTarget.FileName <- Layout.FromString @"${basedir}\logs\Ormonit.ServiceTest.log"
fileTarget.ArchiveFileName <- Layouts.Layout.FromString @"${basedir}\logs\archive\{#}.Ormonit.ServiceTest.log"
fileTarget.ArchiveNumbering <- Targets.ArchiveNumberingMode.DateAndSequence
fileTarget.ArchiveAboveSize <- 524288L
fileTarget.MaxArchiveFiles <- 2
fileTarget.ArchiveDateFormat <- "yyyy-MM-dd"
fileTarget.KeepFileOpen <- true
fileTarget.OpenFileCacheTimeout <- 10
fileTarget.ConcurrentWrites  <- false


let rule1 = new NLog.Config.LoggingRule("*", LogLevel.Trace, consoleTarget)

logConfig.LoggingRules.Add(rule1)

let rule2 = new NLog.Config.LoggingRule("*", LogLevel.Trace, fileTarget)

logConfig.LoggingRules.Add(rule2)
LogManager.Configuration <- logConfig

let mutable lid = -1
let mutable private continu = true
let private mlock = obj()

let encrypt publicKey (data : string) : byte array = 
    let cspParams = CspParameters()
    cspParams.ProviderType <- 1
    use rsaProvider = new RSACryptoServiceProvider(cspParams)
    rsaProvider.ImportCspBlob(Convert.FromBase64String(publicKey))
    let plain = Encoding.UTF8.GetBytes(data)
    let encrypted = rsaProvider.Encrypt(plain, false)
    encrypted

let randomKey() = 
    use rngCryptoServiceProvider = new RNGCryptoServiceProvider()
    let randomBytes = Array.zeroCreate 9
    rngCryptoServiceProvider.GetBytes(randomBytes)
    let r = Convert.ToBase64String(randomBytes)
    r

let private ctrlloop (config : Map<string, string>) = 
    let ca = config.["controlAddress"]
    sprintf "[%i] Ormonit test in control loop with" lid |> log.Trace
    let s = Nn.Socket(Domain.SP, Protocol.RESPONDENT)
    //TODO:error handling for socket and connect
    assert (s >= 0)
    let eid = Nn.Connect(s, ca)
    assert (eid >= 0)
    let random = Random()
    let rec recvloop() = 
        //let mutable buff : byte[] = null '\000'
        //log.Info("[{0}] Ormonit test receive (blocking).", lid)
        match recv s with
        | Error (errn, errm) -> 
            sprintf """[%i] Error %i (recv). %s.""" lid errn errm |> log.Error
            recvloop()
        | Ok (_, note) -> 
            sprintf "[%i] Ormonit test \"%s\" note received." lid note |> log.Info
            let nparts = note.Split([| ' ' |], StringSplitOptions.RemoveEmptyEntries)
            
            let args = 
                if nparts.Length > 1 then nparts.[1..]
                else [||]
            
            let cmd = 
                match Cli.parseArgs args with
                | Error exn -> 
                    sprintf "[%i] Unable to parse arguments in note \"%s\"." lid note |> log.Warn
                    note
                | Ok parsed -> 
                    match parsed.TryGetValue "logicId" with
                    | false, _ -> 
                        sprintf "[%i] No logicId in note \"%s\"." lid note |> log.Warn
                        note
                    | true, logicId when logicId <> lid.ToString() -> 
                        sprintf "[%i] Invalid logicId ignoring note \"%s\". " lid note |> log.Warn
                        String.Empty
                    | true, logicId -> 
                        if nparts.Length > 0 then nparts.[0]
                        else String.Empty
            
            sprintf "[%i] Ormonit test command \"%s\"." lid cmd |> log.Info
            match cmd with
            | "sys:close" -> 
                let flag = 
                    lock mlock (fun () -> 
                        continu <- false
                        continu)
                //log.Trace("""[{0}] Processing "close" notification. continue value {1}""", lid, flag)
                log.Trace("""[{0}] Sending "close" aknowledgement.""", lid)
                match (config.["ckey"], "closing") |> send s with
                | Error (errn, errm) -> 
                    sprintf """Error %i (send). %s.""" errn errm |> log.Error
                    recvloop()
                | _ -> ()
            //log.Trace("""[{0}] Sent "close" aknowledgement.""", lid)
            | "sys:report-status" -> 
                let uncertainty = random.NextDouble()
                if uncertainty > 0.2 then 
                    //log.Trace("""[{0}] Processing "report-status" command.""", lid)
                    log.Trace("""[{0}] Sending "report-status" aknowledgement.""", lid)
                    match (config.["ckey"], "ok") |> send s with
                    | Error(errn, errm) -> sprintf """Error %i (send). %s.""" errn errm |> log.Error
                    | _ -> ()
                //log.Trace("""[{0}] Sent "report-status" aknowledgement.""", lid)
                else log.Warn("""[{0}] Processing "report-status" failed (simulated).""", lid)
                recvloop()
            | "sys:client-key" -> 
                log.Trace("""[{0}] Sending client-key: "{1}".""", lid, config.["ckey"])
                let encrypted = encrypt config.["publicKey"] config.["ckey"]
                let note = Convert.ToBase64String(encrypted)
                match (lid.ToString(), note) |> send s with
                | Error (errn, errm) -> sprintf """Error %i (send). %s.""" errn errm |> log.Error
                | Ok _ -> log.Trace("""[{0}] Sent client-key: "{1}".""", lid, config.["ckey"])
                recvloop()
            | _ -> recvloop()
    recvloop()
    assert (Nn.Shutdown(s, eid) = 0)
    assert (Nn.Close(s) = 0)

let rec private action() = 
    //log.Trace("[{0}] Ormonit test action enter", lid)
    match lock mlock (fun () -> continu) with
    | false -> 
        //log.Trace("continue value {0}", continu)
        log.Trace("[{0}] Ormonit test action exit", lid)
    | true -> 
        //simulate long running task 1-5s
        let t = Random().Next(1000, 5000)
        log.Trace("[{0}] Ormonit test action blocking for {1}ms", lid, t)
        let jr = Thread.CurrentThread.Join(t)
        //log.Trace("Ormonit test action jr {0} continue {1}", jr, continu)
        //log.Trace("continue value {0} flag", continu)
        action()

let Start(config : Map<string, string>) = 
    Cli.addArg { Cli.arg with Option = "--logic-id"
                              Destination = "logicId" }
    //let ckey = randomKey()
    let ckey = "0123456789AB"
    let p, logicId = Int32.TryParse config.["logicId"]
    if not p then raise (ArgumentException("locigId"))
    lid <- logicId
    let nconfig = config.Add("ckey", ckey)
    let smsg = sprintf "[%i] Start with configuration: %A." lid nconfig
    log.Info(smsg)
    log.Info("[{0}] Current directory {1}.", lid, Environment.CurrentDirectory)
    let task = Task.Run(action)
    ctrlloop nconfig
    log.Info("[{0}] Ormonit test exit control loop.", lid)
    lock mlock (fun () -> continu <- false)
    task.Wait()
    log.Info("[{0}] Ormonit test exit.", lid)

