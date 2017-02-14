module Hamster.DbMonitor.Control

open System
open System.IO
open NLog
open NLog.Layouts
open System.Collections.Generic
open System.Text
open NNanomsg

let destinationDbPath = @"W:\Dropbox\rts\hamster.db"
let backupDbPath = @"W:\Dropbox\rts\hamster.db.backup"

let log = LogManager.GetLogger "Hamster.DbMonitor"
let logConfig = NLog.Config.LoggingConfiguration()
let consoleTarget = new NLog.Targets.ColoredConsoleTarget()
logConfig.AddTarget("console", consoleTarget)
let fileTarget = new NLog.Targets.FileTarget()
logConfig.AddTarget("file", fileTarget)
consoleTarget.Layout <- Layout.FromString @"${date:format=HH\:mm\:ss} ${logger} ${message}"
fileTarget.FileName <- Layout.FromString @"${basedir}\logs\${shortdate}.log"
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

let ctrlloop (config : IDictionary<string, string>) =
    let ca = config.["controlAddress"]
    let da = config.["dataAddress"]
    sprintf "In control loop with: controlAddress \"%s\", dataAddress \"%s\"." ca da |> log.Trace
    let s = NN.Socket(Domain.SP, Protocol.RESPONDENT)
    //TODO:error handling for socket and connect
    assert (s >= 0)
    assert (NN.Connect(s, ca) >= 0)
    let rec recvloop config =
        //let mutable buff : byte[] = null
        //buff is initialized with '\000'
        let buff : byte array = Array.zeroCreate 256
        log.Info("Waiting for command.")
        let rc = NN.Recv(s, buff, SendRecvFlags.NONE)
        //TODO:Error handling NN.Errno ()
        assert (rc >= 0)
        let cmd = Encoding.UTF8.GetString(buff.[..rc - 1])
        sprintf "\"%s\" command received." cmd |> log.Trace
        match cmd with
        | "stop" ->
            let nbytes = NN.Send(s, [|0uy|], SendRecvFlags.NONE)
            assert(nbytes = 1)
            ()
        | "report-status" ->
            let sr = NN.Send(s, [|1uy|], SendRecvFlags.NONE)
            //TODO:Error handling NN.Errno ()
            assert (sr >= 0)
            recvloop config
        | _ -> recvloop config
    recvloop config

let Start (config : IDictionary<string, string>) =
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
    ctrlloop config
    log.Info("Exit control loop.")
    watcher.Dispose()


