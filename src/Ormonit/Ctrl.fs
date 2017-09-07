module internal Ctrl

open System
open System.IO
open System.Threading
open System.Reflection
open System.Diagnostics
open System.Text
open System.Collections.Generic
open System.Security.Cryptography
open Cilnn
open NLog
open Ormonit
open Ormonit.Logging
open Comm

let maxOpenServices = 50
let maxMessageSize = Comm.maxMessageSize
//time in milliseconds supervise waits before runs again
let superviseInterval = 100
//time in milliseconds a service has to reply before timingout
let serviceTimeout = 3000.0
let actionTimeout = 20.0
let ormonitFileName = Path.Combine(Environment.CurrentDirectory, "Ormonit")
let controlAddress = "ipc://ormonit/control.ipc"
let notifyAddress = "ipc://ormonit/notify.ipc"
let private olog = LogManager.GetLogger "_Ormonit.Output_"

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

type Execc = 
    { [<DefaultValue>]
      mutable state : string
      masterKey : string
      publicKey : string
      privateKey : string
      execcType : ExeccType
      config : Map<string, string>
      lids : Map<string, int>
      services : ServiceData array
      thread : Thread }

type Ctlkey = 
    { key : string }

let rec retryt f = retrytWith actionTimeout f

and retrytWith t f = 
    let timeoutMark = DateTime.Now + TimeSpan.FromMilliseconds(t)
    tryagint timeoutMark f

and tryagint tm f = 
    match f() with
    | Choice1Of2 r -> Choice1Of2 r //success
    | Choice2Of2 err -> 
        if DateTime.Now > tm then Choice2Of2 err
        else tryagint tm f

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
    let sr = Nn.Send(socket, bytes, SendRecvFlags.NONE)
    //TODO:Error handling Nn.Errno ()
    assert (sr >= 0)

let collectStatus (config : Map<string, string>) socket = 
    let ca = config.["controlAddress"]
    log (Tracel(sprintf "CollectStatus: controlAddress \"%s\"." ca))
    //let mutable buff : byte[] = null
    let buff : byte array = Array.zeroCreate maxMessageSize
    let rc = Nn.Recv(socket, buff, SendRecvFlags.DONTWAIT)
    if rc < 0 then 
        log (Warnl(sprintf "Unable to collect status. Nn.Errno %s" (Nn.StrError(Nn.Errno()))))
        config
    else 
        let cmd = Encoding.UTF8.GetString(buff.[..rc - 1])
        log (Warnl(sprintf "CollectStatus \"%s\" reply." cmd))
        config

let traceState (services) = 
    log (Tracel(fun () -> 
             let strBuilder = StringBuilder("OpenedSrvs: ")
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

let rec closeTimedoutSrvs (timeoutMark : DateTimeOffset) (service : ServiceData) (execc : Execc) = 
    let ca = execc.config.["controlAddress"]
    match service with
    | srv when srv = ServiceData.Default -> ()
    | { logicId = 0 } -> 
        match Array.tryItem 1 execc.services with
        | Some(n) -> closeTimedoutSrvs timeoutMark n execc
        | None -> ()
    | srv -> 
        let iso = timeoutMark >= srv.openTime && srv.lastReply = None
        let isl = srv.lastReply <> None && timeoutMark >= srv.lastReply.Value
        if not iso && not isl then 
            match Array.tryItem (srv.logicId + 1) execc.services with
            | Some(n) -> closeTimedoutSrvs timeoutMark n execc
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
                sprintf "--logic-id %d --public-key %s --ctrl-address %s --open-service %s" lid execc.publicKey ca 
                    srv.fileName
            if not execc.services.[0].isClosing then 
                log (Debugl(sprintf "[%i] Opening service with %s." srv.logicId args))
                let np = executeProcess ormonitFileName args olog
                execc.services.[lid] <- { ServiceData.Default with logicId = lid
                                                                   processId = np.Id
                                                                   openTime = DateTimeOffset(np.StartTime)
                                                                   fileName = srv.fileName }
                traceState execc.services
                match Array.tryItem (lid + 1) execc.services with
                | Some(n) -> closeTimedoutSrvs timeoutMark n execc
                | None -> ()

let rec supervise (sok) (nsok) (msgs : Comm.Envelop list) (execc : Execc) : unit = 
    let mutable nmsgs = List.empty
    
    let matchTMsg tmsg msgf errorf = 
        match tmsg with
            | Comm.Error(errn, errm) -> errorf errn errm
            | Comm.Msg(ckey, note) -> msgf ckey note

    let ckey, note, envp = 
        let emptyResult = String.Empty, String.Empty, Comm.EmptyEnvelop
        let envpFrom ckey note = 
            //let envp = Comm.Envelop(DateTimeOffset.Now, Comm.Msg(ckey, note))
            let envp = 
                { timeStamp = DateTimeOffset.Now
                  msg = Comm.Msg(ckey, note) }
            envp
        match msgs with
        | [] -> 
            let tmsg = Comm.recvWith nsok SendRecvFlags.DONTWAIT
            let ismsg k n =
                log (Tracel(sprintf "Supervise recived note \"%s\"." n))
                k, n, (envpFrom k n)
            matchTMsg tmsg ismsg (fun _ _ -> emptyResult)
        | env :: tail -> 
            nmsgs <- tail
            let ismsg k n =
                log (Tracel(sprintf "Supervise recived internal note \"%s\"." n))
                k, n, env
            matchTMsg env.msg ismsg (fun _ _ -> emptyResult)

    let nparts = note.Split([| ' ' |], StringSplitOptions.RemoveEmptyEntries)
    
    let cmd = 
        if nparts.Length > 0 then nparts.[0]
        else String.Empty
    
    let args = 
        List.ofArray (if nparts.Length > 1 then nparts.[1..]
                      else [||])
    
    //let parsed = Cli.parseArgs args
    match cmd with
    | "sys:is-master" -> 
        match Comm.Msg("", "master") |> Comm.sendWith nsok SendRecvFlags.DONTWAIT with
        | Comm.Error(errn, errm) -> log (Warnl(sprintf """Error %i on "is-master" (send). %s.""" errn errm))
        | _ -> ()
        supervise sok nsok nmsgs execc
    | "sys:close" -> 
        if execc.execcType = ExeccType.WindowsService && execc.masterKey <> ckey then 
            log (Warnl "Not supported when running as Windows Service.")
            supervise sok nsok nmsgs execc
        else 
            execc.services.[0] <- { execc.services.[0] with isClosing = true }
            let npid = execc.services.[0].processId.ToString()
            // send aknowledgment of closing to note sender
            Comm.send nsok (Comm.Msg("", npid)) |> ignore
            log (Debugl("""Supervisor closing. Notify "close" to services."""))
            let notifySrvs() = 
                match Comm.Msg("", "sys:close") |> Comm.send sok with
                | Comm.Error(errn, errm) -> 
                    match errn with
                    | 156384766 -> log <| Warnl(sprintf """Error %i on "close" (send). %s.""" errn errm)
                    | _ -> log <| Errorl(sprintf """Error %i on "close" (send). %s.""" errn errm)
                    Choice2Of2(errn, errm)
                | r -> 
                    log (Tracel("""Services notified of "close"."""))
                    Choice1Of2 r
            retryt notifySrvs |> ignore
            match Comm.recv sok with
            | Comm.Error(errn, errm) -> 
                //156384766 Operation cannot be performed in this state
                if errn = 156384766 then log <| Tracel(sprintf """Expected error %i on "close" (recv). %s.""" errn errm)
                else log <| Errorl(sprintf """Unexpected error %i on "close" (recv). %s.""" errn errm)
            | Comm.Msg(ckey, note) -> 
                let exitCode = note
                match execc.lids.TryFind ckey with
                | None -> log (Warnl(sprintf """Unable to find ckey "%s". Aknowledgement %s.""" ckey exitCode))
                | Some lid -> 
                    log (Debugl(sprintf """Aknowledgement recived from [%i] exit code %s.""" lid exitCode))
                    assert (lid > 0)
                    assert (lid < execc.services.Length)
                    let srv = execc.services.[lid]
                    if ServiceData.Default = srv then ()
                    else 
                        let updatedSrv = { srv with closeTime = Some(DateTimeOffset.Now) }
                        execc.services.[lid] <- updatedSrv
            log (Tracel "Master stopping.")
            let waitOn = 
                List.ofArray execc.services 
                |> List.filter (fun it -> it <> ServiceData.Default && it.logicId <> 0 && it.closeTime = None)
            waitFor execc sok waitOn
    | "sys:init-service" -> 
        let mutable nexecc = execc
        let n = String.Join(" ", Array.ofList ("sys:client-key" :: args))
        match Comm.Msg("", n) |> Comm.send sok with
        | Comm.Msg(lid, n) -> log (Tracel(sprintf """Sent "client-key" note."""))
        | Comm.Error(errn, errm) -> log (Errorl(sprintf """Error %i on "client-key" (send). %s.""" errn errm))
        match Comm.recv sok with
        | Comm.Error(errn, errm) -> 
            log (Errorl(sprintf """Error %i on "client-key" (recv). %s.""" errn errm))
            let timeout = TimeSpan.FromMilliseconds(serviceTimeout)
            let timeoutMark = DateTimeOffset.Now - timeout
            let stale = 
                if timeoutMark >= envp.timeStamp then Some envp
                else None
            match stale with
            | None -> nmsgs <- envp :: nmsgs
            | Some e ->
                log (Errorl(sprintf """Stale message %A.""" e))
        | Comm.Msg(logicId, ekey) -> 
            let lid = Int32.Parse(logicId)
            let dr, clientKey = tryDecrypt execc.privateKey (Convert.FromBase64String(ekey))
            let vckey = 
                match Array.tryItem lid execc.services with
                | None -> false
                | Some srv -> not (execc.lids.ContainsKey clientKey) && String.IsNullOrEmpty(srv.key)
            let invalidKey = not (dr && vckey)
            nexecc <- 
                if dr && vckey then { execc with lids = execc.lids.Add(clientKey, lid) }
                else execc
            if invalidKey then
                let n = String.Join(" ", Array.ofList ("sys:invalid-client-key" :: args))
                Comm.Msg("", n)
                |> Comm.send sok
                |> ignore
        supervise sok nsok nmsgs nexecc
    | _ -> 
        log (Tracel "Requesting report-status.")
        match Comm.Msg("", "sys:report-status") |> Comm.send sok with
        | Comm.Error(errn, errm) -> log <| Warnl(sprintf """Error %i on "report-status" (send). %s.""" errn errm)
        | _ -> 
            match Comm.recv sok with
            | Comm.Error(errn, errm) -> log <| Warnl(sprintf """Error %i on "report-status" (recv). %s.""" errn errm)
            | Comm.Msg(ckey, status) -> 
                match execc.lids.TryFind ckey with
                | None -> 
                    log 
                        (Warnl
                             (sprintf """Reply "report-status" from unknown service [%s] status code %s.""" ckey status))
                | Some lid -> 
                    log <| Tracel(sprintf """Reply "report-status" from [%i] status code %s.""" lid status)
                    assert (lid > 0)
                    assert (lid < execc.services.Length)
                    let srv = execc.services.[lid]
                    let updatedSrv = { srv with lastReply = Some(DateTimeOffset.Now) }
                    execc.services.[lid] <- updatedSrv
                    traceState execc.services
        let timeout = TimeSpan.FromMilliseconds(serviceTimeout)
        let timeoutMark = DateTimeOffset.Now - timeout
        closeTimedoutSrvs timeoutMark execc.services.[0] execc
        log <| Tracel(sprintf "Supervisor blocking for %ims." superviseInterval)
        let jr = System.Threading.Thread.CurrentThread.Join(superviseInterval)
        supervise sok nsok nmsgs execc

and waitFor (execc : Execc) (sok : int) (wl : ServiceData list) = 
    let timeout = TimeSpan.FromMilliseconds(serviceTimeout)
    let timeoutMark = DateTimeOffset.Now - timeout
    log (Tracel "Waiting on")
    traceState (Array.ofList wl)
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
                          execc.services.[lid] <- srv
                          state) List.empty
        match Comm.recv sok with
        | Comm.Error(errn, errm) -> 
            //156384766 Operation cannot be performed in this state
            if errn = 156384766 then log (Tracel(sprintf """Expected error %i on "close" (recv). %s.""" errn errm))
            else log (Errorl(sprintf """Unexpected error %i on "close" (recv). %s.""" errn errm))
        | Comm.Msg(ckey, note) -> 
            let exitCode = note
            log (Debugl(sprintf """Aknowledgement recived from [%s] exit code %s.""" ckey exitCode))
            let lid = execc.lids.[ckey]
            assert (lid > 0)
            assert (lid < execc.services.Length)
            let srv = execc.services.[lid]
            if ServiceData.Default = srv then ()
            else 
                //Should we care if service process is still running?
                let updatedSrv = { srv with closeTime = Some(DateTimeOffset.Now) }
                execc.services.[lid] <- updatedSrv
        System.Threading.Thread.CurrentThread.Join(superviseInterval/2) |> ignore
        waitFor execc sok nwl

let executeSupervisor (execc : Execc) (socket) (nsocket) (msgs) : unit = 
    log (Debugl "Start supervisor.")
    supervise socket nsocket msgs execc
    traceState execc.services
    //TODO:Wait for process exit
    log (Debugl "Supervisor stop.")

let openMaster (execc : Execc) = 
    let config = execc.config
    let ok = 0
    let unknown = Int32.MaxValue
    log (Infol "Master starting.")
    let nsok = Nn.Socket(Domain.SP, Protocol.PAIR)
    //TODO:error handling for socket and bind
    assert (nsok >= 0)
    assert (Nn.SetSockOpt(nsok, SocketOption.SNDTIMEO, 1000) = 0)
    assert (Nn.SetSockOpt(nsok, SocketOption.RCVTIMEO, 1000) = 0)
    assert (Nn.SetSockOpt(nsok, SocketOption.RCVBUF, 262144) = 0)
    let eidp = Nn.Connect(nsok, notifyAddress)
    let mutable isMasterRunning = false
    if eidp >= 0 then 
        match Comm.Msg("", "sys:is-master") |> Comm.send nsok with
        | Comm.Msg _ -> ()
        | Comm.Error(errn, errm) -> 
            //11 Resource unavailable, try again
            if errn = 11 && errm = "Resource unavailable, try again" then 
                log (Tracel(sprintf "Expected error checking for master (send). %s." errm))
            else log (Warnl(sprintf "Error %i checking for master (send). %s." errn errm))
        match Comm.recv nsok with
        | Comm.Msg _ -> isMasterRunning <- true
        | Comm.Error(errn, errm) -> 
            //11 Resource unavailable, try again
            if errn = 11 && errm = "Resource unavailable, try again" then 
                log (Tracel(sprintf "Expected error checking for master (recv). %s." errm))
            else log (Warnl(sprintf "Error %i checking for master (recv). %s." errn errm))
            match Comm.recv nsok with
            | Comm.Msg _ -> isMasterRunning <- true
            | Comm.Error(errn, errm) -> ()
    Nn.Shutdown(nsok, eidp) |> ignore
    if isMasterRunning then 
        log (Warnl(sprintf "Master is already running. Terminating."))
        execc.state <- "term"
        Nn.Close(nsok) |> ignore
        ok
    else 
        let socket = Nn.Socket(Domain.SP, Protocol.SURVEYOR)
        //TODO: error handling for socket and bind
        assert (socket >= 0)
        let curp = System.Diagnostics.Process.GetCurrentProcess()
        //TODO: get arguments information from current process?
        //let serviceCmd = curp.MainModule.fileName + " " + curp.StartInfo.Arguments
        let openTime = DateTimeOffset(curp.StartTime)
        execc.services.[0] <- { ServiceData.Default with logicId = 0
                                                         processId = curp.Id
                                                         openTime = openTime
                                                         fileName = curp.MainModule.FileName }
        let ca = config.["controlAddress"]
        let eid = Nn.Bind(socket, ca)
        let eidp = Nn.Bind(nsok, notifyAddress)
        assert (eid >= 0)
        assert (eidp >= 0)
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
                    let ckey = randomKey()
                    let lid = index + i
                    let args = 
                        sprintf "--logic-id %d --public-key %s --ctrl-address %s --open-service %s" lid execc.publicKey 
                            ca it.Location
                    let cmdArgs = sprintf "%s %s" ormonitFileName args
                    log (Tracel cmdArgs)
                    let p = executeProcess ormonitFileName args olog
                    let note = sprintf "sys:init-service --logic-id %i" lid
                    msgs <- List.append msgs [
                        { timeStamp = DateTimeOffset.Now
                          msg = Comm.Msg(execc.masterKey, note) }]
                    let openTime = DateTimeOffset(p.StartTime)
                    execc.services.[lid] <- { ServiceData.Default with logicId = lid
                                                                       processId = p.Id
                                                                       openTime = openTime
                                                                       fileName = it.Location }
                    last <- lid)
        log (Infol(sprintf """Master started with %i %s, controlAddress: "%s".""" last (if last > 1 then "services"
                                                                                        else "service") ca))
        execc.state <- "started"
        executeSupervisor execc socket nsok msgs
        assert (Nn.Shutdown(socket, eid) = 0)
        assert (Nn.Shutdown(nsok, eidp) = 0)
        assert (Nn.Close(socket) = 0)
        assert (Nn.Close(nsok) = 0)
        log (Infol "Master stopped.")
        execc.state <- "stopped"
        Nn.Term()
        ok

let private shash = Dictionary<string, Execc>()

let makeMaster() = 
    let pubkey, prikey = createAsymetricKeys()
    let key = randomKey()
    let config = Map.empty.Add("controlAddress", controlAddress).Add("notifyAddress", notifyAddress)
    let mutable main = fun () -> ()
    let t = Thread(main)
    
    let execc = 
        { masterKey = key
          publicKey = pubkey
          privateKey = prikey
          execcType = ExeccType.WindowsService
          config = config
          lids = Map.empty
          services = Array.create maxOpenServices ServiceData.Default
          thread = t }
    main <- fun () -> openMaster execc |> ignore
    let ckey = { key = key }
    shash.Add(key, execc)
    ckey

let private isOpen (execc) = 
    let state = lock execc (fun () -> execc.state)
    //log (tracel(sprintf "Execc ThreadState:%A" execc.thread.ThreadState))
    state = "started"

let private isFailed (execc) = 
    let state = lock execc (fun () -> execc.state)
    //log (tracel(sprintf "Execc ThreadState:%A" execc.thread.ThreadState))
    state <> "started" && state <> "stopped" && not (isNull state)

let start (ckey : Ctlkey) = 
    let ok = 0
    let unknown = Int32.MaxValue
    let execc = shash.[ckey.key]
    execc.thread.Start()
    log (Tracel(sprintf "Execc ThreadState:%A" execc.thread.ThreadState))
    while not (isOpen execc) && not (isFailed execc) do
        System.Threading.Thread.CurrentThread.Join 1 |> ignore
    if isFailed execc then unknown
    else ok

let stop (ckey : Ctlkey) = 
    let ok = 0
    let unknown = Int32.MaxValue
    let execc = shash.[ckey.key]
    let na = execc.config.["notifyAddress"]
    let note = "sys:close"
    let nsocket = Nn.Socket(Domain.SP, Protocol.PAIR)
    let buff : byte array = Array.zeroCreate maxMessageSize
    //TODO:error handling for socket and bind
    assert (nsocket >= 0)
    assert (Nn.SetSockOpt(nsocket, SocketOption.SNDTIMEO, superviseInterval * 5) = 0)
    assert (Nn.SetSockOpt(nsocket, SocketOption.RCVTIMEO, superviseInterval * 5) = 0)
    let eid = Nn.Connect(nsocket, notifyAddress)
    assert (eid >= 0)
    log (Tracel(sprintf "[Stop Thread] Notify \"%s\"." note))
    let sendr = 
        retryt (fun () -> 
            match Comm.Msg(execc.masterKey, note) |> Comm.send nsocket with
            //11 Resource unavailable, try again
            | Comm.Error(11, errm) -> Choice2Of2(11, errm) //we try again
            | m -> Choice1Of2 m)
    match sendr with
    | Choice2Of2(errn, errm) -> 
        log (Warnl(sprintf "[Stop Thread] Unable to notify \"%s\" (send). Error %i %s." note errn errm))
    | _ -> ()
    let recv() = Comm.recv nsocket
    let mutable masterpid = -1
    match recv() with
    | Comm.Msg(_, npid) -> masterpid <- Int32.Parse(npid)
    | Comm.Error(errn, errm) -> //we try again
        log (Tracel(sprintf "[Stop Thread] No aknowledgment of note \"%s\" (recv). Error %i %s." note errn errm))
        match recv() with
        | Comm.Msg(_, npid) -> masterpid <- Int32.Parse(npid)
        | Comm.Error(errn, errm) -> 
            log (Warnl(sprintf "[Stop Thread] No aknowledgment of note \"%s\" (recv). Error %i %s." note errn errm))
    if masterpid <> -1 then 
        log (Infol(sprintf "[Stop Thread] Aknowledgment of note \"%s\" (recv). Master pid: %i." note masterpid))
    Nn.Shutdown(nsocket, eid) |> ignore
    Nn.Close(nsocket) |> ignore
    //TODO: masterpid needed?
    if masterpid = -1 then unknown
    else 
        //TODO: 5000m enough why?
        let thjr = execc.thread.Join(5000)
        if execc.thread.ThreadState <> ThreadState.Stopped then 
            let m = 
                (sprintf "[Stop Thread] Error waiting for master exit. Master thread: %i." 
                     execc.thread.ManagedThreadId)
            log (Fatall m)
            execc.thread.Abort()
            unknown
        else ok
