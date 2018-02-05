open Ormonit

// Learn more about F# at http://fsharp.org
// See the 'F# Tutorial' project for more help.


let nlog = NLog.LogManager.GetLogger "Ormonit"
let olog = NLog.LogManager.GetLogger "_Ormonit.Output_"
let logConfig = NLog.Config.LoggingConfiguration()
let consoleTarget = new NLog.Targets.ColoredConsoleTarget()

logConfig.AddTarget("console", consoleTarget)

let fileTarget = new NLog.Targets.FileTarget()

logConfig.AddTarget("file", fileTarget)
consoleTarget.Layout <- NLog.Layouts.Layout.FromString @"${date:format=HH\:mm\:ss}|${logger}|${message}"
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

[<EntryPoint>]
let main argv = 
    printfn "%A" argv
    CtrlTest.thereShouldOnlyBeOneMaster()
    0 // return an integer exit code
