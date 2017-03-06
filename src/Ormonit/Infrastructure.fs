namespace Ormonit

open System
open System.Reflection

#if DNXCORE50
type AssemblyLoader(folderPath) =
    inherit System.Runtime.Loader.AssemblyLoadContext()
    let fpath = folderPath
    member override s.Load (assemblyName:AssemblyName) =
        let deps = DependencyContext.Default
        let res = deps.CompileLibraries.Where(d => d.Name.Contains(assemblyName.Name)).ToList()
        let apiApplicationFileInfo = FileInfo(sprintf "%s%s%s.dll" folderPath (Path.DirectorySeparatorChar.ToString()) assemblyName.Name)
        if res.Count > 0 then
            return Assembly.Load(new AssemblyName(res.First().Name));
        elif File.Exists(apiApplicationFileInfo.FullName) then
                var asl = new AssemblyLoader(apiApplicationFileInfo.DirectoryName)
                return asl.LoadFromAssemblyPath(apiApplicationFileInfo.FullName)
        else
            return Assembly.Load(assemblyName)
#else
type AssemblyLoader(folderPath) =
    let fpath = folderPath
    member s.LoadFromAssemblyPath (path) : Assembly =
        Assembly.LoadFrom(path)
#endif

[<AutoOpen>]
module Logging =
    type public LogLevel =
        | Trace = 0
        | Debug = 1
        | Info = 2
        | Warn = 3
        | Error = 4
        | Fatal = 5

    let mutable private logf = fun (logLevel:LogLevel) (msgFunc:Func<string>) (ex:exn) (formatParameters:obj array) -> ()

    let setLogFun (logFun : LogLevel -> Func<string> -> exn -> obj array -> unit) =
        logf <- logFun
        ()

    let private asFunc<'T when 'T : not struct> (value:'T) : Func<'T> =
        Func<'T>(fun _ -> value)

    type LogFormat<'T>(logLevel:LogLevel, ex:exn, msgFunc:Func<string>, formatParameters:obj array) =
        new (logLevel, msgFunc:Func<string>) = LogFormat (logLevel, null, msgFunc, [||])
        new (logLevel, msg:string) = LogFormat (logLevel, null, asFunc(msg), [||])
        member x.LogLevel = logLevel
        member x.MsgFunc = msgFunc
        member x.Exception = ex

    type tracel<'T>(ex:exn, msgFunc:Func<string>, formatParameters:obj array) =
        inherit LogFormat<'T>(LogLevel.Trace, msgFunc)
        new (ex:exn, msg:string) = tracel (ex, asFunc(msg), [||])
        new (msg:string) = tracel (null, asFunc(msg), [||])
        new (msgFunc:Func<string>) = tracel (null, msgFunc, [||])

    type debugl<'T>(ex:exn, msgFunc:Func<string>, formatParameters:obj array) =
        inherit LogFormat<'T>(LogLevel.Debug, msgFunc)
        new (msg:string) = debugl (null, asFunc(msg), [||])
    
    type infol<'T>(ex:exn, msgFunc:Func<string>, formatParameters:obj array) =
        inherit LogFormat<'T>(LogLevel.Info, msgFunc)
        new (msg:string) = infol (null, asFunc(msg), [||])

    type warnl<'T>(ex:exn, msgFunc:Func<string>, formatParameters:obj array) =
        inherit LogFormat<'T>(LogLevel.Warn, msgFunc)
        new (msg:string) = warnl (null, asFunc(msg), [||])

    type errorl<'T>(ex:exn, msgFunc:Func<string>, formatParameters:obj array) =
        inherit LogFormat<'T>(LogLevel.Error, msgFunc)
        new (ex:exn, msg:string) = errorl (ex, asFunc(msg), [||])
        new (msg:string) = errorl (null, asFunc(msg), [||])

    type fatall<'T>(ex:exn, msgFunc:Func<string>, formatParameters:obj array) =
        inherit LogFormat<'T>(LogLevel.Fatal, msgFunc)
        new (msg:string) = fatall (null, asFunc(msg), [||])

    let log (logFormat:LogFormat<'T>) : unit =
        logf logFormat.LogLevel logFormat.MsgFunc logFormat.Exception [||]

    //let nlogWith (logger:ILog) (logLevel:LogLevel) (msgFunc:Func<string>) (ex:exn) (formatParameters:obj array) : unit =
    //    let nlogger = logger :> NLog.ILogger
    //    match logLevel with
    //    | LogLevel.Trace ->
    //        nlogger.Log(NLog.LogLevel.Trace, ex, msgFunc.Invoke(), formatParameters)
    //    | LogLevel.Debug ->
    //        nlogger.Log(NLog.LogLevel.Debug, ex, msgFunc.Invoke(), formatParameters)
    //    | LogLevel.Info ->
    //        nlogger.Log(NLog.LogLevel.Info, ex, msgFunc.Invoke(), formatParameters)
    //    | LogLevel.Warn ->
    //        nlogger.Log(NLog.LogLevel.Warn, ex, msgFunc.Invoke(), formatParameters)
    //    | LogLevel.Error ->
    //        nlogger.Log(NLog.LogLevel.Error, ex, msgFunc.Invoke(), formatParameters)
    //    | LogLevel.Fatal ->
    //        nlogger.Log(NLog.LogLevel.Fatal, ex, msgFunc.Invoke(), formatParameters)
    //    | _ -> ()

    //let logConfig = {createLogger = createNLogLogger; logWith = nlogWith;}
    //type NLoggerWrapper (logger:NLog.Logger) =
    //    let logger = logger
    ////    member private x.logger:NLog.Logger = logger
    //    interface ILog with
    //        member s.log (logLevel:LogLevel)  (msgFunc:Func<string>) (ex:exn) (formatParameters:obj array) : unit =
    //            let nlogger = logger :> NLog.ILogger
    //            match logLevel with
    //            | LogLevel.Trace ->
    //                nlogger.Log(NLog.LogLevel.Trace, ex, msgFunc.Invoke(), formatParameters)
    //            | LogLevel.Debug ->
    //                nlogger.Log(NLog.LogLevel.Debug, ex, msgFunc.Invoke(), formatParameters)
    //            | LogLevel.Info ->
    //                nlogger.Log(NLog.LogLevel.Info, ex, msgFunc.Invoke(), formatParameters)
    //            | LogLevel.Warn ->
    //                nlogger.Log(NLog.LogLevel.Warn, ex, msgFunc.Invoke(), formatParameters)
    //            | LogLevel.Error ->
    //                nlogger.Log(NLog.LogLevel.Error, ex, msgFunc.Invoke(), formatParameters)
    //            | LogLevel.Fatal ->
    //                nlogger.Log(NLog.LogLevel.Fatal, ex, msgFunc.Invoke(), formatParameters)
    //            | _ -> ()

    //type ILogProvider =
    //    abstract create : name:string -> ILog
    //
    //let mutable logProvider:ILogProvider = null
    //let setLogProvider (provider:ILogProvider) =
    //    logProvider <- provider

    //type NLogProvider () =
    //    let create (name) =
    //        NLog.LogManager.GetLogger name
    //    member x.wrap (logger:NLog.Logger) : ILog =

    //let nlogProvider = NLogProvider()
    //Ormonit.Logging.setLogProvider (nlogProvider)
    //let createNLogLogger (name:string) : ILog =
    //    NLog.LogManager.GetLogger "Ormonit"

    //let nlogWith (logger:ILog) (logLevel:LogLevel) (msgFunc:Func<string>) (ex:exn) (formatParameters:obj array) : unit =
    //    let nlogger = logger :> NLog.ILogger
    //    match logLevel with
    //    | LogLevel.Trace ->
    //        nlogger.Log(NLog.LogLevel.Trace, ex, msgFunc.Invoke(), formatParameters)
    //    | LogLevel.Debug ->
    //        nlogger.Log(NLog.LogLevel.Debug, ex, msgFunc.Invoke(), formatParameters)
    //    | LogLevel.Info ->
    //        nlogger.Log(NLog.LogLevel.Info, ex, msgFunc.Invoke(), formatParameters)
    //    | LogLevel.Warn ->
    //        nlogger.Log(NLog.LogLevel.Warn, ex, msgFunc.Invoke(), formatParameters)
    //    | LogLevel.Error ->
    //        nlogger.Log(NLog.LogLevel.Error, ex, msgFunc.Invoke(), formatParameters)
    //    | LogLevel.Fatal ->
    //        nlogger.Log(NLog.LogLevel.Fatal, ex, msgFunc.Invoke(), formatParameters)
    //    | _ -> ()

    //let logConfig = {createLogger = createNLogLogger; logWith = nlogWith;}

module Log =
    let asFunc<'T when 'T : not struct> (value:'T) : Func<'T> =
        Func<'T>(fun _ -> value)

    let warn (msg:string) (logger:ILog) : unit =
        logger.log LogLevel.Warn (msg |> asFunc) null [||]

    let create (name:string) (config:LoggingConfig) : Logger =
        let logger = config.createLogger name
        Logger(logger, config)

