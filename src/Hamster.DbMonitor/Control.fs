module Hamster.DbMonitor.Control

open System
open System.IO
open NLog
open NLog.Layouts
open System.Collections.Generic
open System.Text
open NNanomsg
open Comm

let destinationDbPath = @"W:\Dropbox\rts\hamster.db"
let backupDbPath = @"W:\Dropbox\rts\hamster.db.backup"

let log = LogManager.GetLogger "Hamster.DbMonitor"
let logConfig = NLog.Config.LoggingConfiguration()
let consoleTarget = new NLog.Targets.ColoredConsoleTarget()
logConfig.AddTarget("console", consoleTarget)
let fileTarget = new NLog.Targets.FileTarget()
logConfig.AddTarget("file", fileTarget)
consoleTarget.Layout <- Layout.FromString @"${date:format=HH\:mm\:ss}|${logger}|${message}"
fileTarget.FileName <- Layout.FromString @"${basedir}\logs\Hamster.DbMonitor.log"
fileTarget.ArchiveFileName <- Layouts.Layout.FromString @"${basedir}\logs\archive\{#}.Hamster.DbMonitor.log"
fileTarget.ArchiveNumbering <- Targets.ArchiveNumberingMode.DateAndSequence
fileTarget.ArchiveAboveSize <- 1048576L
fileTarget.MaxArchiveFiles <- 2
fileTarget.ArchiveDateFormat <- "yyyy-MM-dd"
let rule1 = new NLog.Config.LoggingRule("*", LogLevel.Trace, consoleTarget)
logConfig.LoggingRules.Add(rule1)
let rule2 = new NLog.Config.LoggingRule("Hamster.*", LogLevel.Trace, fileTarget)
logConfig.LoggingRules.Add(rule2)
LogManager.Configuration <- logConfig


let on_hamster_db_change (e:FileSystemEventArgs) : unit =
    sprintf "\"%s\" event for %s" (e.ChangeType.ToString()) e.FullPath |> log.Info
    if e.Name <> "hamster.db" then ()
    else
    let s = FileInfo(e.FullPath)
    let d = FileInfo(destinationDbPath)
    if d.LastWriteTimeUtc > s.LastWriteTimeUtc then
        sprintf "Destination db \"%s\" has a more recent LastWriteTimeUtc than source. \"%s\" > \"%s\"" destinationDbPath (d.LastWriteTimeUtc.ToString("o")) (s.LastWriteTimeUtc.ToString("o"))
        |> log.Warn
        File.Copy(destinationDbPath, backupDbPath, true)
    File.Copy(e.FullPath, destinationDbPath, true)
    sprintf "Source \"%s\" copied to destination \"%s\"." e.FullPath destinationDbPath |> log.Trace
    ()

let ctrlloop (lid:string) (ca:string) =
    sprintf "In control loop with: controlAddress \"%s\", logicId \"%s\"." ca lid |> log.Trace
    let s = NN.Socket(Domain.SP, Protocol.RESPONDENT)
    //TODO:error handling for socket and connect
    assert (s >= 0)
    assert (NN.Connect(s, ca) >= 0)
    let rec recvloop () =
        //let mutable buff : byte[] = null
        //buff is initialized with '\000'
        let buff : byte array = Array.zeroCreate 256
        log.Info("Waiting for note.")
        match recv s with
        | Error (errn, errm) ->
            sprintf """Error %i (recv). %s.""" errn errm |> log.Error
            recvloop ()
        | Msg (_, note) ->

        sprintf "\"%s\" note received." note |> log.Trace
        match note with
        | "sys:close" ->
            log.Trace("""[{0}] Sending "close" aknowledgement.""", lid)
            match Msg(lid, "closing") |> send s with
            | Error (errn, errm) -> sprintf "Error %i (send). %s." errn errm |> log.Error
            | _ -> ()
        | "sys:report-status" ->
            log.Trace("""[{0}] Sending "report-status" aknowledgement.""", lid)
            match Msg(lid, "ok") |> send s with
            | Error (errn, errm) -> sprintf "Error %i (send). %s." errn errm |> log.Error
            | _ -> ()
            recvloop ()
        | _ -> recvloop ()
    recvloop ()

let Start (config:IDictionary<string, string>) =
    let ca = config.["controlAddress"]
    let lid = config.["logicId"]
    if String.IsNullOrEmpty lid then
        raise (System.ArgumentException("logicId"))
    //sprintf "Start with configuration %A" config |> log.Trace
    //%APPDATA%\Roaming\hamster-applet
    let app_data = Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData)
    let p  = Path.Combine(app_data, "hamster-applet")
    use watcher = new FileSystemWatcher()
    sprintf "Watcher path: \"%s\"." p |> log.Info
    watcher.Path <- p
    // Watch for changes in LastAccess and LastWrite times, and
    // the renaming of files or directories.
    watcher.NotifyFilter <- NotifyFilters.LastAccess
        ||| NotifyFilters.LastWrite
        ||| NotifyFilters.FileName
    watcher.Filter <- "*.db"
    watcher.Changed.Add(on_hamster_db_change)
    watcher.Created.Add(on_hamster_db_change)
    //log.Trace("Begin watching.")
    watcher.EnableRaisingEvents <- true
    //log.Info("Enter control loop.")
    ctrlloop lid ca
    log.Info("Exit control loop.")
    watcher.Dispose()


