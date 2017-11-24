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
    
    type Tracel<'T>(ex : exn, msgFunc : Func<string>, formatParameters : obj array) = 
        inherit LogFormat<'T>(LogLevel.Trace, msgFunc)
        new(ex : exn, msg : string) = Tracel(ex, asFunc (msg), [||])
        new(msg : string) = Tracel(null, asFunc (msg), [||])
        new(msgFunc : Func<string>) = Tracel(null, msgFunc, [||])
    
    type Debugl<'T>(ex : exn, msgFunc : Func<string>, formatParameters : obj array) = 
        inherit LogFormat<'T>(LogLevel.Debug, msgFunc)
        new(msg : string) = Debugl(null, asFunc (msg), [||])
    
    type Infol<'T>(ex : exn, msgFunc : Func<string>, formatParameters : obj array) = 
        inherit LogFormat<'T>(LogLevel.Info, msgFunc)
        new(msg : string) = Infol(null, asFunc (msg), [||])
    
    type Warnl<'T>(ex : exn, msgFunc : Func<string>, formatParameters : obj array) = 
        inherit LogFormat<'T>(LogLevel.Warn, msgFunc)
        new(msg : string) = Warnl(null, asFunc (msg), [||])
    
    type Errorl<'T>(ex : exn, msgFunc : Func<string>, formatParameters : obj array) = 
        inherit LogFormat<'T>(LogLevel.Error, msgFunc)
        new(ex : exn, msg : string) = Errorl(ex, asFunc (msg), [||])
        new(msg : string) = Errorl(null, asFunc (msg), [||])
    
    type Fatall<'T>(ex : exn, msgFunc : Func<string>, formatParameters : obj array) = 
        inherit LogFormat<'T>(LogLevel.Fatal, msgFunc)
        new(msg : string) = Fatall(null, asFunc (msg), [||])
    
    let log (logFormat : LogFormat<'T>) : unit = logf logFormat.LogLevel logFormat.MsgFunc logFormat.Exception [||]

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