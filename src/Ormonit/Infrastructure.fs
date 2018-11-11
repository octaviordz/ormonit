namespace Ormonit

open System
open System.Reflection
open System.Security.Cryptography
open System.Text

#if DNXCORE50

type AssemblyLoader(folderPath) = 
    inherit System.Runtime.Loader.AssemblyLoadContext()
    let fpath = folderPath
    
    override s.Load (assemblyName:AssemblyName) = 
        let deps = DependencyContext.Default
        let res = deps.CompileLibraries.Where(d => d.Name.Contains(assemblyName.Name)).ToList()
        let apiApplicationFileInfo = FileInfo(sprintf "%s%s%s.dll" folderPath (Path.DirectorySeparatorChar.ToString()) assemblyName.Name)
        if res.Count > 0 then
            return Assembly.Load(new AssemblyName(res.First().Name));
        elif File.Exists(apiApplicationFileInfo.FullName) then
            let asl = new AssemblyLoader(apiApplicationFileInfo.DirectoryName)
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
    
    type TraceLevel<'T>(ex : exn, msgFunc : Func<string>, formatParameters : obj array) = 
        inherit LogFormat<'T>(LogLevel.Trace, msgFunc)
        new(ex : exn, msg : string) = TraceLevel(ex, asFunc (msg), [||])
        new(msg : string) = TraceLevel(null, asFunc (msg), [||])
        new(msgFunc : Func<string>) = TraceLevel(null, msgFunc, [||])
    
    type DebugLevel<'T>(ex : exn, msgFunc : Func<string>, formatParameters : obj array) = 
        inherit LogFormat<'T>(LogLevel.Debug, msgFunc)
        new(ex : exn, msg : string) = DebugLevel(ex, asFunc (msg), [||])
        new(msg : string) = DebugLevel(null, asFunc (msg), [||])
        new(msgFunc : Func<string>) = DebugLevel(null, msgFunc, [||])
    
    type InfoLevel<'T>(ex : exn, msgFunc : Func<string>, formatParameters : obj array) = 
        inherit LogFormat<'T>(LogLevel.Info, msgFunc)
        new(ex : exn, msg : string) = InfoLevel(ex, asFunc (msg), [||])
        new(msg : string) = InfoLevel(null, asFunc (msg), [||])
        new(msgFunc : Func<string>) = InfoLevel(null, msgFunc, [||])
    
    type WarnLevel<'T>(ex : exn, msgFunc : Func<string>, formatParameters : obj array) = 
        inherit LogFormat<'T>(LogLevel.Warn, msgFunc)
        new(ex : exn, msg : string) = WarnLevel(ex, asFunc (msg), [||])
        new(msg : string) = WarnLevel(null, asFunc (msg), [||])
        new(msgFunc : Func<string>) = WarnLevel(null, msgFunc, [||])
    
    type ErrorLevel<'T>(ex : exn, msgFunc : Func<string>, formatParameters : obj array) = 
        inherit LogFormat<'T>(LogLevel.Error, msgFunc)
        new(ex : exn, msg : string) = ErrorLevel(ex, asFunc (msg), [||])
        new(msg : string) = ErrorLevel(null, asFunc (msg), [||])
        new(msgFunc : Func<string>) = ErrorLevel(null, msgFunc, [||])
    
    type FatalLevel<'T>(ex : exn, msgFunc : Func<string>, formatParameters : obj array) = 
        inherit LogFormat<'T>(LogLevel.Fatal, msgFunc)
        new(ex : exn, msg : string) = FatalLevel(ex, asFunc (msg), [||])
        new(msg : string) = FatalLevel(null, asFunc (msg), [||])
        new(msgFunc : Func<string>) = FatalLevel(null, msgFunc, [||])
    
    let log (logFormat : LogFormat<'T>) : unit = logf logFormat.LogLevel logFormat.MsgFunc logFormat.Exception [||]
    
    [<Sealed>]
    type Log = 
        static member Trace(msg : string) = 
            log (TraceLevel msg)

        static member Trace(ex : exn, msg : string) = 
            log (TraceLevel(ex,  msg))

        static member Trace(msgFunc : Func<string>) = 
            log (TraceLevel(msgFunc))

        static member Debug(msg : string) = 
            log (DebugLevel msg)

        static member Debug(ex : exn, msg : string) = 
            log (DebugLevel(ex,  msg))

        static member Debug(msgFunc : Func<string>) = 
            log (DebugLevel(msgFunc))
        
        static member Info(msg : string) = 
            log (InfoLevel msg)

        static member Info(ex : exn, msg : string) = 
            log (InfoLevel(ex,  msg))

        static member Info(msgFunc : Func<string>) = 
            log (InfoLevel(msgFunc))
        
        static member Warn(msg : string) = 
            log (WarnLevel msg)

        static member Warn(ex : exn, msg : string) = 
            log (WarnLevel(ex,  msg))

        static member Warn(msgFunc : Func<string>) = 
            log (WarnLevel(msgFunc))
        
        static member Error(msg : string) = 
            log (ErrorLevel msg)

        static member Error(ex : exn, msg : string) = 
            log (ErrorLevel(ex,  msg))

        static member Error(msgFunc : Func<string>) = 
            log (ErrorLevel(msgFunc))

        static member Fatal(msg : string) = 
            log (FatalLevel msg)

        static member Fatal(ex : exn, msg : string) = 
            log (FatalLevel(ex,  msg))

        static member Fatal(msgFunc : Func<string>) = 
            log (FatalLevel(msgFunc))
            
module Security =
    let randomKey() = 
        use rngCryptoServiceProvider = new RNGCryptoServiceProvider()
        let randomBytes = Array.zeroCreate 9
        rngCryptoServiceProvider.GetBytes(randomBytes)
        let r = Convert.ToBase64String(randomBytes)
        r

    let createAsymetricKeys() = 
        let cspParams = CspParameters()
        cspParams.ProviderType <- 1
        use rsaProvider = new RSACryptoServiceProvider(1024, cspParams)
        let publicKey = Convert.ToBase64String(rsaProvider.ExportCspBlob(false))
        let privateKey = Convert.ToBase64String(rsaProvider.ExportCspBlob(true))
        publicKey, privateKey
    //http://stackoverflow.com/questions/18850030/aes-256-encryption-public-and-private-key-how-can-i-generate-and-use-it-net
    let encrypt publicKey (data : string) : byte array = 
        let cspParams = CspParameters()
        cspParams.ProviderType <- 1
        use rsaProvider = new RSACryptoServiceProvider(cspParams)
        rsaProvider.ImportCspBlob(Convert.FromBase64String(publicKey))
        let plain = Encoding.UTF8.GetBytes(data)
        let encrypted = rsaProvider.Encrypt(plain, false)
        encrypted

    let decrypt privateKey (encrypted : byte array) : string = 
        let cspParams = CspParameters()
        cspParams.ProviderType <- 1
        use rsaProvider = new RSACryptoServiceProvider(cspParams)
        rsaProvider.ImportCspBlob(Convert.FromBase64String(privateKey))
        let bytes = rsaProvider.Decrypt(encrypted, false)
        let plain = Encoding.UTF8.GetString(bytes, 0, bytes.Length)
        plain