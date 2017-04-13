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
    
    let mutable private logf = 
        fun (logLevel : LogLevel) (msgFunc : Func<string>) (ex : exn) (formatParameters : obj array) -> ()
    
    let setLogFun (logFun : LogLevel -> Func<string> -> exn -> obj array -> unit) = 
        logf <- logFun
        ()
    
    let private asFunc<'T when 'T : not struct> (value : 'T) : Func<'T> = Func<'T>(fun _ -> value)
    
    type LogFormat<'T>(logLevel : LogLevel, ex : exn, msgFunc : Func<string>, formatParameters : obj array) = 
        new(logLevel, msgFunc : Func<string>) = LogFormat(logLevel, null, msgFunc, [||])
        new(logLevel, msg : string) = LogFormat(logLevel, null, asFunc (msg), [||])
        member x.LogLevel = logLevel
        member x.MsgFunc = msgFunc
        member x.Exception = ex
    
    type tracel<'T>(ex : exn, msgFunc : Func<string>, formatParameters : obj array) = 
        inherit LogFormat<'T>(LogLevel.Trace, msgFunc)
        new(ex : exn, msg : string) = tracel (ex, asFunc (msg), [||])
        new(msg : string) = tracel (null, asFunc (msg), [||])
        new(msgFunc : Func<string>) = tracel (null, msgFunc, [||])
    
    type debugl<'T>(ex : exn, msgFunc : Func<string>, formatParameters : obj array) = 
        inherit LogFormat<'T>(LogLevel.Debug, msgFunc)
        new(msg : string) = debugl (null, asFunc (msg), [||])
    
    type infol<'T>(ex : exn, msgFunc : Func<string>, formatParameters : obj array) = 
        inherit LogFormat<'T>(LogLevel.Info, msgFunc)
        new(msg : string) = infol (null, asFunc (msg), [||])
    
    type warnl<'T>(ex : exn, msgFunc : Func<string>, formatParameters : obj array) = 
        inherit LogFormat<'T>(LogLevel.Warn, msgFunc)
        new(msg : string) = warnl (null, asFunc (msg), [||])
    
    type errorl<'T>(ex : exn, msgFunc : Func<string>, formatParameters : obj array) = 
        inherit LogFormat<'T>(LogLevel.Error, msgFunc)
        new(ex : exn, msg : string) = errorl (ex, asFunc (msg), [||])
        new(msg : string) = errorl (null, asFunc (msg), [||])
    
    type fatall<'T>(ex : exn, msgFunc : Func<string>, formatParameters : obj array) = 
        inherit LogFormat<'T>(LogLevel.Fatal, msgFunc)
        new(msg : string) = fatall (null, asFunc (msg), [||])
    
    let log (logFormat : LogFormat<'T>) : unit = logf logFormat.LogLevel logFormat.MsgFunc logFormat.Exception [||]
