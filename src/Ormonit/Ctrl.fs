module internal Ctrl

open System
open System.IO
open System.Threading
open System.Reflection
open System.Diagnostics
open System.Text
open System.Collections.Generic
open System.Security.Cryptography
open System.Linq
open NLog
open Ormonit
open Ormonit.Logging
open System.Collections.Concurrent

let maxOpenServices = 50
let maxMessageSize = Comm.maxMessageSize
//time in milliseconds supervise waits before runs again
let superviseInterval = 100
//time in milliseconds a service has to reply before timingout
let serviceTimeout = 3000
let actionTimeout = 30
let ormonitFileName = Path.Combine(Environment.CurrentDirectory, "Ormonit")
let controlAddress = "ipc://ormonit/control.ipc"
let notifyAddress = "ipc://ormonit/notify.ipc"
let inprocAddress = "inproc://ormonit/"
let private olog = LogManager.GetLogger "_Ormonit.Output_"
let private addressMap = Dictionary<string, int>()
let private socketMap = Dictionary<int, string>()

[<StructuredFormatDisplay("{StructuredDisplayString}")>]
type ServiceData = 
    { logicId : int
      key : string
      processId : int
      lastReply : DateTimeOffset option
      openTime : DateTimeOffset
      isClosing : bool
      closeTime : DateTimeOffset option
      fileName : string }
    
    override x.ToString() = 
        let sb = StringBuilder("{")
        Printf.bprintf sb "logicId = %i;" x.logicId
        Printf.bprintf sb "\n   processId = %i;" x.processId
        Printf.bprintf sb "\n   lastReply = %s;" (match x.lastReply with
                                                  | Some d -> d.ToString("o")
                                                  | None -> "null")
        Printf.bprintf sb "\n   openTime = %s;" (x.openTime.ToString("o"))
        Printf.bprintf sb "\n   isClosing = %A;" x.isClosing
        Printf.bprintf sb "\n   closeTime = %s;" (match x.closeTime with
                                                  | Some d -> d.ToString("o")
                                                  | None -> "null")
        Printf.bprintf sb "\n   fileName = \"%s\";" x.fileName
        Printf.bprintf sb "}"
        sb.ToString()
    
    member x.StructuredDisplayString = x.ToString()
    static member Default = 
        { logicId = -1
          key = String.Empty
          processId = -1
          lastReply = None
          openTime = DateTimeOffset.MinValue
          isClosing = false
          closeTime = None
          fileName = String.Empty }

type ExeccType = 
    | ConsoleApplication = 0
    | WindowsService = 1

type ServiceSettings = 
    { controlAddress : string
      notifyAddress : string
      execcType : ExeccType }

type Context = 
    { masterKey : string
      publicKey : string
      privateKey : string
      execcType : ExeccType
      config : Map<string, string>
      lids : Map<string, int>
      services : ServiceData array }

type internal Execc(thread : Thread, states : ConcurrentStack<(string * DateTimeOffset)>, context : Context) = 
    [<CompiledName("Thread")>]
    member __.thread = thread
    [<CompiledName("Context")>]
    member __.context = context
    [<CompiledName("States")>]
    member __.states = states
    [<CompiledName("UpdateState")>]
    member __.updateState newState = states.Push(newState)
    [<CompiledName("CurrentState")>]
    member __.currentState = 
        match states.TryPeek() with 
        | false, _ -> Error()
        | true, s -> Ok(s)

type Ctlkey = 
    { key : string }

let rec retryt f = 
    retrytWith actionTimeout f

and retrytWith timeo f = 
    let timestamp = Environment.TickCount
    tryagint timestamp timeo f

and tryagint timestamp timeo f = 
    match f() with
    | Ok r -> Ok r
    | Error err -> 
        if (Environment.TickCount - timestamp) >= timeo then Error err
        else tryagint timestamp timeo f

let randomKey() = 
    use rngCryptoServiceProvider = new RNGCryptoServiceProvider()
    let randomBytes = Array.zeroCreate 12
    rngCryptoServiceProvider.GetBytes(randomBytes)
    Convert.ToBase64String(randomBytes)

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

let tryDecrypt privateKey (encrypted : byte array) : bool * string = 
    try
        let r = decrypt privateKey encrypted
        true, r
    with
        | :? CryptographicException -> false, String.Empty

let executeProcess (fileName : string) (arguments) (olog : NLog.Logger) = 
    let psi = ProcessStartInfo(fileName, arguments)
    psi.UseShellExecute <- false
    psi.RedirectStandardOutput <- true
    psi.RedirectStandardError <- true
    psi.CreateNoWindow <- true
    let p = Process.Start(psi)
    p.OutputDataReceived.Add(fun args -> 
        if not (isNull args.Data) then olog.Trace(args.Data))
    p.ErrorDataReceived.Add(fun args -> 
        if not (isNull args.Data) then olog.Error(args.Data))
    p.BeginErrorReadLine()
    p.BeginOutputReadLine()
    p

let tryLoadAssembly (fullName) = 
    let asl = AssemblyLoader()
    try 
        log (Tracel(sprintf "Try load assembly \"%s\"" fullName))
        let a = asl.LoadFromAssemblyPath(fullName)
        //TODO:How to guarantee this is a valid service?!
        Some(a)
    with ex -> 
        log (Errorl(ex, (sprintf "Unable to load assembly %s" fullName)))
        None

let loadServices (config : Map<string, string>) (basedir) = 
    let bdir = DirectoryInfo(basedir)
    bdir.GetFiles("*.oml") |> Seq.fold (fun acc it -> 
                                  match tryLoadAssembly it.FullName with
                                  | Some(asm) -> asm :: acc
                                  | None -> acc) List.empty<Assembly>

let executeService (config : Map<string, string>) : unit = 
    let p = config.["assemblyPath"]
    log (Tracel(sprintf "Execute service with configuration \"%A\"" config))
    let (?) (t : Type) (mname : string) = t.GetMethod(mname)
    let asl = AssemblyLoader()
    let asm = asl.LoadFromAssemblyPath(p)
    let name = asm.GetName().Name + ".Control"
    let t = asm.GetType(name)
    if isNull t then log (Errorl(sprintf "Control not found: %s value %A." name t))
    else log (Tracel(sprintf "Control found: %s value %A." name t))
    let start = t?Start
    log (Tracel(sprintf "Control start method: %A." start))
    start.Invoke(t, [| config |]) |> ignore

let requestStatus (lid : int32) socket = 
    log (Infol "Master requests services status.")
    let cbytes = Encoding.UTF8.GetBytes("sys:report-status")
    let bytes = Array.zeroCreate 5
    Buffer.BlockCopy(BitConverter.GetBytes(lid), 0, bytes, 0, 4)
    Buffer.BlockCopy(cbytes, 4, bytes, 0, cbytes.Length)
    let sr = Cilnn.Nn.Send(socket, bytes, Cilnn.SendRecvFlags.NONE)
    //TODO:Error handling Nn.Errno ()
    assert (sr >= 0)

let collectStatus (config : Map<string, string>) socket = 
    let ca = config.["controlAddress"]
    log (Tracel(sprintf "CollectStatus: controlAddress \"%s\"." ca))
    //let mutable buff : byte[] = null
    let buff : byte array = Array.zeroCreate maxMessageSize
    let rc = Cilnn.Nn.Recv(socket, buff, Cilnn.SendRecvFlags.DONTWAIT)
    if rc < 0 then 
        log (Warnl(sprintf "Unable to collect status. Nn.Errno %s" (Cilnn.Nn.StrError(Cilnn.Nn.Errno()))))
        config
    else 
        let cmd = Encoding.UTF8.GetString(buff.[..rc - 1])
        log (Warnl(sprintf "CollectStatus \"%s\" reply." cmd))
        config

let traceState (context : Context) services = 
    log (Tracel(fun () -> 
             let strBuilder = StringBuilder()
             Printf.bprintf strBuilder "[%s] OpenedSrvs: " context.masterKey
             services
             |> Array.takeWhile (fun it -> it <> ServiceData.Default)
             |> Printf.bprintf strBuilder "%A"
             strBuilder.ToString()))

let tryGetProcess processId = 
    try 
        let p = Process.GetProcessById(processId)
        true, p
    with ex -> 
        log (Tracel(ex, (sprintf "Unable to get process %i" processId)))
        false, null

let rec closeTimedoutSrvs (timeoutMark : DateTimeOffset) (service : ServiceData) (context : Context) = 
    let ca = context.config.["controlAddress"]
    match service with
    | srv when srv = ServiceData.Default -> ()
    | { logicId = 0 } -> 
        match Array.tryItem 1 context.services with
        | Some(n) -> closeTimedoutSrvs timeoutMark n context
        | None -> ()
    | srv -> 
        let iso = timeoutMark >= srv.openTime && srv.lastReply = None
        let isl = srv.lastReply <> None && timeoutMark >= srv.lastReply.Value
        if not iso && not isl then 
            match Array.tryItem (srv.logicId + 1) context.services with
            | Some(n) -> closeTimedoutSrvs timeoutMark n context
            | None -> ()
        else 
            log (Debugl(sprintf """[%i] "report-status" timedout %A.""" srv.logicId srv))
            match tryGetProcess srv.processId with
            | true, p -> 
                let startTime = DateTimeOffset(p.StartTime)
                //check if it's the same process
                if srv.openTime = startTime then 
                    log (Debugl(sprintf "[%i] Killing process %i." srv.logicId srv.processId))
                    p.Kill()
                else 
                    log 
                        (Warnl(sprintf "[%i] Process identity not confirmed %i %A." srv.logicId srv.processId startTime))
            | false, _ -> ()
            let lid = srv.logicId
            let args = 
                sprintf "--logic-id %d --public-key %s --ctrl-address %s --open-service %s" lid context.publicKey ca 
                    srv.fileName
            if not context.services.[0].isClosing then 
                log (Debugl(sprintf "[%i] Opening service with %s." srv.logicId args))
                let np = executeProcess ormonitFileName args olog
                context.services.[lid] <- { ServiceData.Default with logicId = lid
                                                                     processId = np.Id
                                                                     openTime = DateTimeOffset(np.StartTime)
                                                                     fileName = srv.fileName }
                traceState context context.services
                match Array.tryItem (lid + 1) context.services with
                | Some(n) -> closeTimedoutSrvs timeoutMark n context
                | None -> ()

let sendWith (flags) (sok) (envp : Comm.Envelop) : Result<Comm.Envelop, Comm.Error> =
    match Comm.sendWith flags sok envp.msg with
    | Error err -> Result.Error err
    | Ok _ -> Result.Ok envp

let send = sendWith Cilnn.SendRecvFlags.NONE
let sendImmediately = sendWith Cilnn.SendRecvFlags.DONTWAIT


let recvWith (flags) (address : Comm.Address) : Result<Comm.Envelop, Comm.Error> =
    let sok = addressMap.[address]
    match Comm.recvWith flags sok with
    | Error err -> Result.Error err
    | Ok msg -> Result.Ok { Comm.Envelop.from = address
                            Comm.Envelop.msg = msg 
                            Comm.Envelop.timeStamp = DateTimeOffset.Now }

let recv = recvWith Cilnn.SendRecvFlags.NONE
let recvInproc () = recv inprocAddress

//let matchCommMessage msgfn errorfn msg = 
//    match msg with
//    | Error (errn, errm) -> errorfn errn errm
//    | Ok (ckey, note) -> msgfn ckey note

//let ismsgBuild (context : Context) k n = 
//    log (Tracel(sprintf "[%s] Received note \"%s\"." context.masterKey n))
//    { Comm.Envelop.timeStamp = DateTimeOffset.Now
//      Comm.Envelop.msg = (k, n) }
//    |> Option.Some

//let iserrBuild (context : Context) errn errm = 
//    log (Warnl(sprintf "[%s] Received error. Error %i. %s." context.masterKey errn errm))
//    Option.None

let listenOn (sok) (context : Context) : unit = 
    let csok = Cilnn.Nn.Socket(Cilnn.Domain.SP, Cilnn.Protocol.PUSH)
    //TODO: error handling for socket and bind
    assert (csok >= 0)
    let eidInproc = Cilnn.Nn.Connect(csok, inprocAddress)
    assert (eidInproc >= 0)
    //sendInproc it |> ignore
    let rec listen () = 
        let r = 
            match Comm.recv sok with
            | Error (errn, errm) -> 
                log (Warnl(sprintf "[%s] Received error. Error %i. %s." context.masterKey errn errm))
                Option.None
            | Ok (ckey, note) -> 
                log (Tracel(sprintf "[%s] Received note \"%s\"." context.masterKey note))
                let addrs = socketMap.[sok]
                { Comm.Envelop.from = addrs
                  Comm.Envelop.timeStamp = DateTimeOffset.Now
                  Comm.Envelop.msg = (ckey, note) }
                |> Option.Some
        match r with
        | Option.None -> listen ()
        | Option.Some envelop -> 
            let _, note = envelop.msg
            let nparts = note.Split([| ' ' |], StringSplitOptions.RemoveEmptyEntries)
            let cmd = 
                if nparts.Length > 0 then nparts.[0]
                else String.Empty
            let args = 
                List.ofArray (if nparts.Length > 1 then nparts.[1..]
                              else [||])
            match Cli.parseArgs (Array.ofList args) with 
            | Error _ -> 
                listen ()
            | Ok _ -> 
                match send csok envelop with
                | Error _  -> () //TODO:
                | Ok _ -> listen ()
    listen ()
    assert (Cilnn.Nn.Shutdown(csok, eidInproc) = 0)

let rec supervise (ctrlsok) (pushsok) (context : Context) : unit = 
    match recvInproc () with
    | Error (errn, errm) -> 
        log(Warnl(sprintf """[%s] Unable to received in supervise. Error %i. %s.""" context.masterKey errn errm))
        supervise ctrlsok pushsok context
    | Ok envp -> 

    let ckey, note = envp.msg
    log (Tracel(sprintf "[%s] Supervise received note \"%s\"." context.masterKey note))

    let nparts = note.Split([| ' ' |], StringSplitOptions.RemoveEmptyEntries)
    
    let cmd = 
        if nparts.Length > 0 then nparts.[0]
        else String.Empty
    
    let args = 
        List.ofArray (if nparts.Length > 1 then nparts.[1..]
                      else [||])
    
    match Cli.parseArgs (Array.ofList args) with 
    | Error _ -> supervise ctrlsok pushsok context
    | Ok parsedArgs -> 

    match cmd with
    | "sys:is-master" -> 
        let addrs = context.config.["notifyAddress"]
        let resp = 
            { Comm.Envelop.from = addrs
              Comm.Envelop.msg = ("", "master")
              Comm.Envelop.timeStamp = DateTimeOffset.Now }
        match sendImmediately (addressMap.[addrs]) resp with
        | Error (errn, errm) -> 
            log (Warnl(sprintf """[%s] Error %i on "is-master" (send). %s.""" context.masterKey errn errm))
        | _ -> ()
        supervise ctrlsok pushsok context
    | "sys:close" -> 
        if context.execcType = ExeccType.WindowsService && context.masterKey <> ckey then 
            log (Warnl "Not supported when running as Windows Service.")
            supervise ctrlsok pushsok context
        else 
            context.services.[0] <- { context.services.[0] with isClosing = true }
            let npid = context.services.[0].processId.ToString()
            let resp = 
                { Comm.Envelop.from = context.config.["notifyAddress"]
                  Comm.Envelop.msg = ("", npid)
                  Comm.Envelop.timeStamp = DateTimeOffset.Now }
            // send acknowledgment of closing to note sender
            match send (addressMap.[envp.from]) resp with
            | Error (errn, errm) -> 
                log(Warnl(sprintf """[%s] Unable to acknowledge "close". Error %i. %s.""" context.masterKey errn errm))
            | Ok _ -> 
                log(Tracel(sprintf """[%s] Acknowledgment of "close" sent.""" context.masterKey))
            log (Debugl("""Supervisor closing. Notify "close" to services."""))
            let notifySrvs () = 
                match ("", "sys:close") |> Comm.send ctrlsok with
                | Error (errn, errm) -> 
                    match errn with
                    | 156384766 -> 
                        log(Warnl(sprintf """[%s] Unable to notify services (send). Error %i. %s.""" context.masterKey errn errm))
                    | _ -> 
                        log(Errorl(sprintf """[%s] Unable to notify services (send). Error %i. %s.""" context.masterKey errn errm))
                    Result.Error (errn, errm)
                | r -> 
                    log (Tracel("""Services notified of "close"."""))
                    r
            retryt notifySrvs |> ignore
            match Comm.recv ctrlsok with
            | Error (errn, errm) -> 
                //156384766 Operation cannot be performed in this state
                if errn = 156384766 then 
                    log(Tracel(sprintf """[%s] Expected error on "close" (recv). Error %i. %s.""" context.masterKey errn errm))
                else 
                    log(Errorl(sprintf """[%s] Unexpected error on "close" (recv). Error %i. %s.""" context.masterKey errn errm))
            | Ok (ckey, note) -> 
                let exitCode = note
                match context.lids.TryFind ckey with
                | None -> log (Warnl(sprintf """[%s] Unable to find ckey "%s". Aknowledgement %s.""" context.masterKey ckey exitCode))
                | Some lid -> 
                    log (Debugl(sprintf """[%s] Aknowledgement received from [%i] exit code %s.""" context.masterKey lid exitCode))
                    assert (lid > 0)
                    assert (lid < context.services.Length)
                    let srv = context.services.[lid]
                    if ServiceData.Default = srv then ()
                    else 
                        let updatedSrv = { srv with closeTime = Some(DateTimeOffset.Now) }
                        context.services.[lid] <- updatedSrv
            log(Tracel(sprintf """[%s] Master stopping.""" context.masterKey))
            let waitOn = 
                List.ofArray context.services 
                |> List.filter (fun it -> it <> ServiceData.Default && it.logicId <> 0 && it.closeTime = None)
            waitFor context ctrlsok waitOn
    | "sys:init-service" -> 
        let mutable nexecc = context
        let n = String.Join(" ", Array.ofList ("sys:client-key" :: args))
        match ("", n) |> Comm.send ctrlsok with
        | Ok (lid, n) -> log (Tracel(sprintf """Sent "client-key" note."""))
        | Error (errn, errm) -> log (Errorl(sprintf """Error %i on "client-key" (send). %s.""" errn errm))
        match Comm.recv ctrlsok with
        | Error(errn, errm) -> 
            log (Errorl(sprintf """Error %i on "client-key" (recv). %s.""" errn errm))
            let timeout = TimeSpan.FromMilliseconds(float (serviceTimeout))
            let timeoutMark = DateTimeOffset.Now - timeout
            let stale = 
                if timeoutMark >= envp.timeStamp then Some envp
                else None
            match stale with
            | None -> 
                //TODO: Error handle
                send pushsok envp |> ignore
            | Some e -> 
                match Int32.TryParse parsedArgs.["logicId"] with 
                | false, _ -> 
                    Errorl(sprintf """Stale message %A. Invalid logicId "%s".""" e (parsedArgs.["logicId"]))
                    |> log
                | true, lid ->  
                    context.services.[lid] <- ServiceData.Default
                    log (Errorl(sprintf "Stale message %A." e))
        | Ok (logicId, ekey) -> 
            let lid = Int32.Parse(logicId)
            let dr, clientKey = tryDecrypt context.privateKey (Convert.FromBase64String(ekey))
            let vckey = 
                match Array.tryItem lid context.services with
                | None -> false
                | Some srv -> not (context.lids.ContainsKey clientKey) && String.IsNullOrEmpty(srv.key)
            let invalidKey = not (dr && vckey)
            nexecc <- 
                if dr && vckey then { context with lids = context.lids.Add(clientKey, lid) }
                else context
            if invalidKey then
                let n = String.Join(" ", Array.ofList ("sys:invalid-client-key" :: args))
                ("", n)
                |> Comm.send ctrlsok
                |> ignore
        supervise ctrlsok pushsok nexecc
    | "sys:self-init" -> 
        if not (parsedArgs.ContainsKey "processId") then 
            Errorl("""No processId in "self-init" note.""") |> log
        if not (parsedArgs.ContainsKey "processId") then 
            Errorl("""No processStartTime in "self-init" note.""") |> log
        let isValidArgs = 
            parsedArgs.ContainsKey "processId"
            && parsedArgs.ContainsKey "processStartTime"
        if not isValidArgs then 
            supervise ctrlsok pushsok context
        else 
        
        let pidr, pid = Int32.TryParse (parsedArgs.["processId"])
        let ptr, pt = DateTime.TryParse (parsedArgs.["processStartTime"])
        match pidr, ptr, pid, pt with 
        | false, false, _, _ -> 
            Errorl("""Invalid processId in "self-init" note.""") |> log
            Errorl("""Invalid processStartTime in "self-init" note.""") |> log
            supervise ctrlsok pushsok context
        | false, true, _, _ -> 
            Errorl("""Invalid processId in "self-init" note.""") |> log
            supervise ctrlsok pushsok context
        | true, false, _, _ -> 
            Errorl("""Invalid processStartTime in "self-init" note.""") |> log
            supervise ctrlsok pushsok context
        | true, true, pid, pt -> 

        let index = 
            context.services
            |> Array.tryFindIndex (fun it -> it = ServiceData.Default )
        match index with 
        | None -> Errorl("Services limit reached.") |> log
        | Some lid -> 
            let n = sprintf "sys:resp:self-init --logic-id %d --process-id %d --public-key %s --ctrl-address %s" lid pid context.publicKey context.config.["controlAddress"]
            match (String.Empty, n) |> Comm.send (addressMap.[envp.from]) with
            | Ok _ -> 
                log (Tracel(sprintf """Sent "resp:self-init" note."""))
                let n = sprintf "sys:init-service --logic-id %i" lid
                let resp = 
                    { envp with
                           timeStamp = DateTimeOffset.Now
                           msg = (context.masterKey, n) }
                //TODO: error handling
                send pushsok resp |> ignore
                context.services.[lid] <- { ServiceData.Default with logicId = lid
                                                                     processId = pid
                                                                     openTime = DateTimeOffset(pt) }
            | Error (errn, errm) -> 
                log (Errorl(sprintf """Error %i on "resp:self-init" (send). %s.""" errn errm))
            supervise ctrlsok pushsok context
    | _ -> 
        log(Tracel(sprintf "[%s] Requesting report-status." context.masterKey))
        match (String.Empty, "sys:report-status") |> Comm.send ctrlsok with
        | Error (errn, errm) -> 
            log (Warnl(sprintf """[%s] Error on "report-status" (send). Error %i. %s.""" 
                    context.masterKey errn errm))
        | _ -> 
            match Comm.recv ctrlsok with
            | Error (errn, errm) -> 
                log (Warnl(sprintf """[%s] Error on "report-status" (recv). Error %i. %s.""" 
                    context.masterKey errn errm))
            | Ok (ckey, status) -> 
                match context.lids.TryFind ckey with
                | None -> 
                    log 
                        (Warnl
                             (sprintf """Reply "report-status" from unknown service [%s] status code "%s".""" ckey status))
                | Some lid -> 
                    log <| Tracel(sprintf """Reply "report-status" from [%i] status code "%s".""" lid status)
                    assert (lid > 0)
                    assert (lid < context.services.Length)
                    let srv = context.services.[lid]
                    let updatedSrv = { srv with lastReply = Some(DateTimeOffset.Now) }
                    context.services.[lid] <- updatedSrv
                    traceState context context.services
        let timeout = TimeSpan.FromMilliseconds(float (serviceTimeout))
        let timeoutMark = DateTimeOffset.Now - timeout
        closeTimedoutSrvs timeoutMark context.services.[0] context
        log(Tracel(sprintf "[%s] Supervisor blocking for %ims." context.masterKey superviseInterval))
        let jr = System.Threading.Thread.CurrentThread.Join(superviseInterval)
        supervise ctrlsok pushsok context

and waitFor (context : Context) (sok : int) (wl : ServiceData list) = 
    let timeout = TimeSpan.FromMilliseconds(float (serviceTimeout))
    let timeoutMark = DateTimeOffset.Now - timeout
    log (Tracel (sprintf "[%s] Waiting on" context.masterKey))
    traceState context (Array.ofList wl)
    match wl with
    | [] -> ()
    | wl -> 
        let nwl = 
            wl |> List.fold (fun state srv -> 
                      let iso = timeoutMark >= srv.openTime && srv.lastReply = None
                      let isl = srv.lastReply <> None && timeoutMark >= srv.lastReply.Value
                      if not iso && not isl then srv :: state
                      else 
                          log (Debugl(sprintf """Service [%i] timedout %A.""" srv.logicId srv))
                          match tryGetProcess srv.processId with
                          | true, p -> 
                              let startTime = DateTimeOffset(p.StartTime)
                              //check if it's the same process
                              if srv.openTime = startTime then 
                                  log (Debugl(sprintf "[%i] Killing process %i." srv.logicId srv.processId))
                                  try 
                                      p.Kill()
                                  with ex -> 
                                      Errorl(ex, sprintf "[%i] Error killing process %i." srv.logicId srv.processId) 
                                      |> log
                              else 
                                  Warnl
                                      (sprintf "[%i] Process identity not confirmed %i %A" srv.logicId srv.processId 
                                           startTime) |> log
                          | false, p -> log (Warnl(sprintf "[%i] Process not found %i." srv.logicId srv.processId))
                          let lid = srv.logicId
                          let srv = { srv with closeTime = Some(DateTimeOffset.Now) }
                          context.services.[lid] <- srv
                          state) List.empty
        match Comm.recv sok with
        | Error (errn, errm) -> 
            //156384766 Operation cannot be performed in this state
            if errn = 156384766 then log (Tracel(sprintf """Expected error %i on "close" (recv). %s.""" errn errm))
            else log (Errorl(sprintf """Unexpected error %i on "close" (recv). %s.""" errn errm))
        | Ok (ckey, note) -> 
            let exitCode = note
            log (Debugl(sprintf """Aknowledgement received from [%s] exit code %s.""" ckey exitCode))
            let lid = context.lids.[ckey]
            assert (lid > 0)
            assert (lid < context.services.Length)
            let srv = context.services.[lid]
            if ServiceData.Default = srv then ()
            else 
                //Should we care if service process is still running?
                let updatedSrv = { srv with closeTime = Some(DateTimeOffset.Now) }
                context.services.[lid] <- updatedSrv
        System.Threading.Thread.CurrentThread.Join(superviseInterval/2) |> ignore
        waitFor context sok nwl

let executeSupervisor (context : Context) (ctrlsok) (nsocket) (msgs : Comm.Envelop list) : unit = 
    let pushsok = Cilnn.Nn.Socket(Cilnn.Domain.SP, Cilnn.Protocol.PUSH)
    //TODO: error handling for socket and bind
    assert (pushsok >= 0)
    let eidInproc = Cilnn.Nn.Connect(pushsok, inprocAddress)
    assert (eidInproc >= 0)
    let initializeList = Thread(ThreadStart(fun () -> 
        msgs |> List.iter (fun envp -> 
            //TODO: error handling
            send pushsok envp |> ignore)))
    //let listenOnCtrl = Thread(ThreadStart(fun () -> listenOn ctrlsok context))
    let listenOnNoty = Thread(ThreadStart(fun () -> listenOn nsocket context))
    initializeList.IsBackground <- true
    //listenOnCtrl.IsBackground <- true
    listenOnNoty.IsBackground <- true
    initializeList.Start()
    //listenOnCtrl.Start()
    listenOnNoty.Start()
    log (Debugl (sprintf "[%s] Start supervisor." context.masterKey))
    supervise ctrlsok pushsok context
    traceState context context.services
    //TODO:Wait for process exit
    log (Debugl (sprintf "[%s] Supervisor stop." context.masterKey))
    assert (Cilnn.Nn.Shutdown(pushsok, eidInproc) = 0)

let openMaster (states : ConcurrentStack<(string * DateTimeOffset)>) (context : Context) = 
    states.Push("init", DateTimeOffset.Now)
    let config = context.config
    let ok = 0
    log (Debugl (sprintf "[%s] Master starting." context.masterKey))
    let nsok = Cilnn.Nn.Socket(Cilnn.Domain.SP, Cilnn.Protocol.SURVEYOR)
    //TODO:error handling for socket and bind
    assert (nsok >= 0)
    assert (Cilnn.Nn.SetSockOpt(nsok, Cilnn.SocketOption.SNDTIMEO, 1000) = 0)
    assert (Cilnn.Nn.SetSockOpt(nsok, Cilnn.SocketOption.RCVTIMEO, 1000) = 0)
    assert (Cilnn.Nn.SetSockOpt(nsok, Cilnn.SocketOption.RCVBUF, 262144) = 0)
    let eidp = Cilnn.Nn.Connect(nsok, config.["notifyAddress"])
    let isMaster () = 
        if eidp < 0 then 
            let errn = Cilnn.Nn.Errno()
            let errm = Cilnn.Nn.StrError(Cilnn.Nn.Errno())
            log (Warnl(sprintf "Error %i checking for master (conn). %s." errn errm))
            Result.Ok false
        else
        match ("", "sys:is-master") |> Comm.send nsok with
        | Ok _ -> ()
        | Error(errn, errm) -> 
            //11 Resource unavailable, try again
            if errn = 11 && errm = "Resource unavailable, try again" then 
                log (Tracel(sprintf "[%s] Expected error checking for master (send). Error %i. %s." 
                    context.masterKey errn errm))
            else 
                log (Warnl(sprintf "[%s] Error checking for master (send). Error %i. %s." 
                    context.masterKey errn errm))
        match Comm.recv nsok with
        | Ok _ -> Result.Ok true
        | Error (errn, errm) -> 
            //11 Resource unavailable, try again
            if errn = 11 && errm = "Resource unavailable, try again" then 
                log (Tracel(sprintf "[%s] Expected error checking for master (recv). Error %i. %s." 
                    context.masterKey errn errm))
            else 
                log (Warnl(sprintf "[%s] Error checking for master (recv). Error %i. %s." 
                    context.masterKey errn errm))
            match Comm.recv nsok with
            | Ok _ -> Result.Ok true
            | Error(errn, errm) -> Result.Error (errn, errm)
    let isMasterRunning = 
        match retryt isMaster with 
        | Result.Ok x -> x
        | Result.Error _ -> false
    Cilnn.Nn.Shutdown(nsok, eidp) |> ignore
    Cilnn.Nn.Close(nsok) |> ignore
    if isMasterRunning then 
        log (Warnl(sprintf "[%s] Master is already running. Terminating." context.masterKey))
        states.Push("term", DateTimeOffset.Now)
        ok
    else 
        let nsok = Cilnn.Nn.Socket(Cilnn.Domain.SP, Cilnn.Protocol.RESPONDENT)
        let socket = Cilnn.Nn.Socket(Cilnn.Domain.SP, Cilnn.Protocol.SURVEYOR)
        let csok = Cilnn.Nn.Socket(Cilnn.Domain.SP, Cilnn.Protocol.PULL)
        //TODO: error handling for socket and bind
        //let errn = Cilnn.Nn.Errno()
        //let errm = Cilnn.Nn.StrError(Cilnn.Nn.Errno())
        assert (nsok >= 0)
        assert (socket >= 0)
        assert (csok >= 0)
        let curp = System.Diagnostics.Process.GetCurrentProcess()
        //TODO: get arguments information from current process?
        //let serviceCmd = curp.MainModule.fileName + " " + curp.StartInfo.Arguments
        let openTime = DateTimeOffset(curp.StartTime)
        context.services.[0] <- { ServiceData.Default with logicId = 0
                                                           processId = curp.Id
                                                           openTime = openTime
                                                           fileName = curp.MainModule.FileName }
        let eid = 
            match Cilnn.Nn.Bind(socket, config.["controlAddress"]) with 
            | endpointId when endpointId < 0 -> 
                let errn = Cilnn.Nn.Errno()
                let errm = Cilnn.Nn.StrError(Cilnn.Nn.Errno())
                log (Errorl(sprintf """[%s] Unable to bind "%s". %s. Error %i.""" 
                        context.masterKey 
                        (config.["controlAddress"]) 
                        errm 
                        errn))
                //Address in use. Error 100.
                if errn = 100 then 
                    log (Tracel(sprintf """[%s] Connecting instead "%s".""" 
                        context.masterKey (config.["controlAddress"])))
                    Cilnn.Nn.Connect(socket, config.["controlAddress"])
                else endpointId
            | endpointId -> endpointId
        let eidr = 
            match Cilnn.Nn.Bind(nsok, config.["notifyAddress"]) with 
            | endpointId when endpointId < 0 -> 
                let errn = Cilnn.Nn.Errno()
                let errm = Cilnn.Nn.StrError(Cilnn.Nn.Errno())
                log (Errorl(sprintf """[%s] Unable to bind "%s". %s. Error %i.""" 
                        context.masterKey 
                        (config.["notifyAddress"]) 
                        errm 
                        errn))
                //Address in use. Error 100.
                if errn = 100 then 
                    log (Tracel(sprintf """[%s] Connecting instead "%s".""" 
                        context.masterKey (config.["controlAddress"])))
                    Cilnn.Nn.Connect(nsok, config.["notifyAddress"])
                else endpointId
            | endpointId -> endpointId
        let eidInproc = 
            match Cilnn.Nn.Bind(csok, inprocAddress) with 
            | endpointId when endpointId < 0 -> 
                let errn = Cilnn.Nn.Errno()
                let errm = Cilnn.Nn.StrError(Cilnn.Nn.Errno())
                log (Errorl(sprintf """[%s] Unable to bind "%s". %s. Error %i.""" 
                        context.masterKey 
                        (config.["controlAddress"]) 
                        errm 
                        errn))
                //Address in use. Error 100.
                if errn = 100 then 
                    log (Tracel(sprintf """[%s] Connecting instead "%s".""" 
                        context.masterKey inprocAddress))
                    Cilnn.Nn.Connect(socket, inprocAddress)
                else endpointId
            | endpointId -> endpointId
        assert (eid >= 0)
        assert (eidr >= 0)
        assert (eidInproc >= 0)
        addressMap.Add(config.["controlAddress"], socket)
        addressMap.Add(config.["notifyAddress"], nsok)
        addressMap.Add(inprocAddress, csok)
        socketMap.Add(socket, config.["controlAddress"])
        socketMap.Add(nsok, config.["notifyAddress"])
        socketMap.Add(csok, inprocAddress)
        //TODO: Check for already running ormonit services in case master is interrupted/killed externally
        //config <- RequestStatus config socket
        //config <- CollectStatus config q socket
        //let sr = Nn.Send(socket, Encoding.UTF8.GetBytes("close"), SendRecvFlags.NONE)
        //TODO:Error handling Nn.Errno ()
        //assert (sr >= 0)
        let mutable msgs = List.empty
        let index = 1
        let mutable last = 1
        let asms = loadServices config Environment.CurrentDirectory
        asms |> List.iteri (fun i it -> 
                    //let ckey = randomKey()
                    let lid = index + i
                    let args = 
                        sprintf "--logic-id %d --public-key %s --ctrl-address %s --open-service %s" lid context.publicKey 
                            (config.["controlAddress"]) it.Location
                    let cmdArgs = sprintf "%s %s" ormonitFileName args
                    log (Tracel cmdArgs)
                    let p = executeProcess ormonitFileName args olog
                    let note = sprintf "sys:init-service --logic-id %i" lid
                    msgs <- List.append msgs [
                        { Comm.Envelop.from = inprocAddress
                          Comm.Envelop.timeStamp = DateTimeOffset.Now
                          Comm.Envelop.msg = (context.masterKey, note) }]
                    let openTime = DateTimeOffset(p.StartTime)
                    context.services.[lid] <- { ServiceData.Default with logicId = lid
                                                                         processId = p.Id
                                                                         openTime = openTime
                                                                         fileName = it.Location }
                    last <- lid)
        log (Infol(sprintf """[%s] Master started with %i %s, controlAddress: "%s".""" 
                           context.masterKey 
                           last 
                           (if last > 1 then "services" else "service") 
                           (config.["controlAddress"])))
        states.Push("started", DateTimeOffset.Now)
        executeSupervisor context socket nsok msgs
        assert (Cilnn.Nn.Shutdown(socket, eid) = 0)
        assert (Cilnn.Nn.Shutdown(nsok, eidr) = 0)
        assert (Cilnn.Nn.Shutdown(csok, eidInproc) = 0)
        assert (Cilnn.Nn.Close(socket) = 0)
        assert (Cilnn.Nn.Close(nsok) = 0)
        assert (Cilnn.Nn.Close(csok) = 0)
        states.Push("stopped", DateTimeOffset.Now)
        log (Infol (sprintf "[%s] Master stopped." context.masterKey))
        ok

let private shash = Dictionary<string, Execc>()

let makeMaster (configuration : ServiceSettings) = 
    let pubkey, prikey = createAsymetricKeys()
    let key = randomKey()
    let config = 
        Map.empty.
            Add("controlAddress", configuration.controlAddress).
            Add("notifyAddress", configuration.notifyAddress)
    let states  = ConcurrentStack<(string * DateTimeOffset)>()
    let context = 
        { Context.masterKey = key
          publicKey = pubkey
          privateKey = prikey
          execcType = configuration.execcType
          config = config
          lids = Map.empty
          services = Array.create maxOpenServices ServiceData.Default }
    let main = fun () -> openMaster states context |> ignore
    let t = Thread(main)
    t.Name <- context.masterKey
    let execc = Execc(t, states, context)
    let ckey = { key = key }
    shash.Add(key, execc)
    ckey

let private isOpen (execc : Execc) = 
    let s = lock execc (fun () -> execc.currentState)
    //log (Tracel(sprintf "[%s] Execc ThreadState:%A" execc.thread.Name execc.thread.ThreadState))
    match s with 
    | Error _ -> false
    | Ok (state, _) -> state = "started"

let private isFailed (execc : Execc) = 
    let s = lock execc (fun () -> execc.currentState)
    //log (Tracel(sprintf "[%s] Execc ThreadState:%A" execc.thread.Name execc.thread.ThreadState))
    match s with 
    | Error _ -> false
    | Ok (state, _) -> state <> "started" && state <> "stopped" && not (isNull state)

let start (ckey : Ctlkey) = 
    let ok = 0
    let unknown = Int32.MaxValue
    let execc = shash.[ckey.key]
    //try to run master
    execc.thread.Start()
    //log (Tracel(sprintf "[%s] Execc ThreadState:%A" execc.thread.Name execc.thread.ThreadState))
    let tstates = 
        [| ThreadState.StopRequested; 
           ThreadState.Stopped; 
           ThreadState.AbortRequested; 
           ThreadState.Aborted; |]
    while not (isOpen execc) && not (tstates.Contains(execc.thread.ThreadState)) do
        System.Threading.Thread.CurrentThread.Join 10 |> ignore
    if (isOpen execc) then ok
    else unknown

let stop (ckey : Ctlkey) = 
    let ok = 0
    let unknown = Int32.MaxValue
    let execc = shash.[ckey.key]
    let note = "sys:close"
    let rec state () = 
        match execc.currentState with 
        | Result.Ok (s, _) -> s
        | Result.Error _ -> state()
    if state() = "stopped" then 
        log (Tracel(sprintf "[Stop Thread][%s] Stopped." ckey.key))
        ok
    elif state() = "term" then 
        log (Tracel(sprintf "[Stop Thread][%s] Terminated." ckey.key))
        ok
    else
    let nsocket = Cilnn.Nn.Socket(Cilnn.Domain.SP, Cilnn.Protocol.SURVEYOR)
    //TODO:error handling for socket and bind
    assert (nsocket >= 0)
    assert (Cilnn.Nn.SetSockOpt(nsocket, Cilnn.SocketOption.SNDTIMEO, 1300) = 0)
    assert (Cilnn.Nn.SetSockOpt(nsocket, Cilnn.SocketOption.RCVTIMEO, 1300) = 0)
    let eid = Cilnn.Nn.Connect(nsocket, execc.context.config.["notifyAddress"])
    assert (eid >= 0)
    log (Tracel(sprintf "[Stop Thread][%s] Notify \"%s\"." ckey.key note))
    let mutable masterpid = -1
    let sendr = 
        retrytWith 3000  (fun () -> 
            match (execc.context.masterKey, note) |> Comm.send nsocket with
            //11 Resource unavailable, try again
            | Error (11, errm) -> Result.Error (11, errm) //we try again
            | m -> m )
    match sendr with
    | Error(errn, errm) -> 
        log (Warnl(sprintf "[Stop Thread][%s] Unable to notify \"%s\" (send). Error %i %s." ckey.key note errn errm))
    | _ -> ()
    let recvr = 
        retrytWith 3000 (fun () -> 
            match Comm.recv nsocket with
            | Error (errn, errm) -> 
                log (Tracel(sprintf "[Stop Thread][%s] No acknowledgment of note \"%s\" (recv). Error %i. %s." 
                    ckey.key note errn errm))
                Result.Error (errn, errm) //we try again
            | Ok (m, npid) -> 
                masterpid <- Int32.Parse(npid) 
                Result.Ok (m, npid) )
    match recvr with
    | Error(errn, errm) -> 
        log (Warnl(sprintf "[Stop Thread][%s] No acknowledgment of note \"%s\" (recv). Error %i. %s." 
                    ckey.key note errn errm))
    | _ -> ()
    if masterpid <> -1 then 
        log (Infol(sprintf "[Stop Thread][%s] Acknowledgment of note \"%s\" (recv). Master pid: %i." 
            ckey.key note masterpid))
    Cilnn.Nn.Shutdown(nsocket, eid) |> ignore
    Cilnn.Nn.Close(nsocket) |> ignore
    //TODO: masterpid needed?
    if masterpid = -1 then 
        //force stop
        execc.thread.Abort()
        unknown
    else 
        //TODO: 5000m enough why?
        let thjr = execc.thread.Join(5000)
        if execc.thread.ThreadState <> ThreadState.Stopped then 
            let m = 
                (sprintf "[Stop Thread][%s] Error waiting for master exit. Master threadId: %i." 
                     ckey.key execc.thread.ManagedThreadId)
            log (Fatall m)
            execc.thread.Abort()
            unknown
        else ok
