module internal Ctrl

open System
open System.IO
open NLog
open System.Reflection
open System.Diagnostics
open System.Text
open System.Collections.Generic
open NNanomsg
open System.Threading
open Ormonit
open Ormonit.Logging

let maxOpenServices = 50
let maxMessageSize = 128
//time in milliseconds supervise waits before runs again
let superviseInterval = 100
//time in milliseconds a service has to reply before timingout
let serviceTimeout = 3000.0
let closenTimeout = 20.0
let ormonitFileName = Path.Combine(Environment.CurrentDirectory, "Ormonit")
let controlAddress = "ipc://ormonit/control.ipc"
let notifyAddress = "ipc://ormonit/notify.ipc"
let private olog = LogManager.GetLogger "_Ormonit.Output_"

type OpenServiceData =
    {logicId: int;
     processId: int;
     lastReply: DateTimeOffset option;
     openTime: DateTimeOffset;
     isClosing: bool;
     closeTime: DateTimeOffset option;
     fileName: string;}
     static member Default =
        {logicId = -1;
         processId = -1;
         lastReply = None;
         openTime = DateTimeOffset.MinValue;
         isClosing = false;
         closeTime = None;
         fileName = String.Empty;}

type Execc =
    {[<DefaultValue>]mutable state: string
     config: Map<string, string>
     openedSrvs: OpenServiceData array
     thread: System.Threading.Thread}

type Ctlkey =
    {key: string}

let executeProcess (fileName:string) (arguments) (olog:NLog.Logger) =
    let psi = ProcessStartInfo(fileName, arguments)
    psi.UseShellExecute <- false
    psi.RedirectStandardOutput <- true
    psi.RedirectStandardError <- true
    psi.CreateNoWindow <- true
    let p = Process.Start(psi)
    p.OutputDataReceived.Add(fun args ->
        olog.Trace(args.Data) )
    p.ErrorDataReceived.Add(fun args ->
        olog.Error(args.Data) )
    p.BeginErrorReadLine()
    p.BeginOutputReadLine()
    p

let tryLoadAssembly (fullName) =
    let asl = AssemblyLoader()
    try
        tracel (sprintf "Try load assembly \"%s\"" fullName) |> log
        let a = asl.LoadFromAssemblyPath(fullName)
        //TODO:How to guarantee this is a valid service?!
        Some(a)
    with
    | ex ->
        log <| errorl(ex, (sprintf "Unable to load assembly %s" fullName))
        None

let loadServices (config:Map<string, string>) (basedir) =
    let bdir = DirectoryInfo(basedir)
    bdir.GetFiles("*.oml")
    |> Seq.fold (fun acc it ->
        match tryLoadAssembly it.FullName with
        | Some(asm) -> asm :: acc
        | None  -> acc
        ) List.empty<Assembly>

let executeService (config:Map<string, string>): unit =
    let p = config.["assemblyPath"]
    log <| tracel(sprintf "Execute service with configuration \"%A\"" config)
    let (?) (t : Type) (mname : string) =
        t.GetMethod(mname)
    let asl = AssemblyLoader()
    let asm = asl.LoadFromAssemblyPath(p)
    let name = asm.GetName().Name + ".Control"
    let t = asm.GetType(name)
    if isNull t then log <| errorl(sprintf "Control not found: %s value %A." name t)
    else log <| tracel(sprintf "Control found: %s value %A." name t)
    let start = t?Start
    log <| tracel(sprintf "Control start method: %A." start)
    start.Invoke(t, [|config|]) |> ignore

let requestStatus (lid:int32) socket =
    log <| infol "Master requests services status."
    let cbytes = Encoding.UTF8.GetBytes("report-status")
    let bytes = Array.zeroCreate 5
    Buffer.BlockCopy(BitConverter.GetBytes(lid), 0, bytes, 0, 4)
    Buffer.BlockCopy(cbytes, 4, bytes, 0, cbytes.Length)
    let sr = NN.Send(socket, bytes, SendRecvFlags.NONE)
    //TODO:Error handling NN.Errno ()
    assert (sr >= 0)

let collectStatus (config:Map<string, string>) socket =
    let ca = config.["controlAddress"]
    log <| tracel(sprintf "CollectStatus: controlAddress \"%s\"." ca)
    //let mutable buff : byte[] = null
    let buff : byte array = Array.zeroCreate maxMessageSize
    let rc = NN.Recv(socket, buff, SendRecvFlags.DONTWAIT)
    if rc < 0 then
        log <| warnl(sprintf "Unable to collect status. NN.Errno %s" (NN.StrError(NN.Errno())))
        config
    else

    let cmd = Encoding.UTF8.GetString(buff.[..rc - 1])
    log <| warnl(sprintf "CollectStatus \"%s\" reply." cmd)
    config

let traceState (openedSrvs) =
    log <| tracel (fun () ->
        let strBuilder = StringBuilder("OpenedSrvs: ")
        openedSrvs
        |> Array.takeWhile (fun it -> it <> OpenServiceData.Default)
        |> Printf.bprintf strBuilder "%A"
        strBuilder.ToString() )

let tryGetProcess processId =
    try
        let p = Process.GetProcessById(processId)
        true, p
    with
    | ex ->
        log <| tracel(ex, (sprintf "Unable to get process %i" processId))
        false, null
        
let rec closeTimedoutSrvs (config:Map<string, string>) (timeoutMark:DateTimeOffset) (service:OpenServiceData) (openedSrvs:OpenServiceData array) =
    let ca = config.["controlAddress"]
    match service with
    | srv when srv = OpenServiceData.Default -> ()
    | {logicId = 0} ->
        match Array.tryItem 1 openedSrvs with
        | Some(n) -> closeTimedoutSrvs config timeoutMark n openedSrvs
        | None -> ()
    | srv ->
        let iso = timeoutMark >= srv.openTime && srv.lastReply = None
        let isl = srv.lastReply <> None && timeoutMark >= srv.lastReply.Value
        if not iso && not isl then
            match Array.tryItem (srv.logicId + 1) openedSrvs with
            | Some(n) -> closeTimedoutSrvs config timeoutMark n openedSrvs
            | None -> ()
        else

        log <| debugl(sprintf """[%i] "report-status" timedout %A.""" srv.logicId srv)
        match tryGetProcess srv.processId with
        | true, p ->
            let startTime = DateTimeOffset(p.StartTime)
            //check if it's the same process
            if srv.openTime = startTime then
                log <| debugl(sprintf "[%i] Killing process %i." srv.logicId srv.processId)
                p.Kill()
            else
                log <| warnl(sprintf "[%i] Process identity not confirmed %i %A." srv.logicId srv.processId startTime)
        | false, _ -> ()
        let lid = srv.logicId
        let args = sprintf "--open-service %s --ctrl-address %s --logic-id %d" srv.fileName ca lid
        if not openedSrvs.[0].isClosing then
            log <| debugl(sprintf "[%i] Opening service with %s." srv.logicId args)
            let np = executeProcess ormonitFileName args olog
            openedSrvs.[lid] <- {
                OpenServiceData.Default with
                    logicId = lid;
                    processId = np.Id;
                    openTime = DateTimeOffset(np.StartTime);
                    fileName = srv.fileName;}
            traceState openedSrvs
            match Array.tryItem (lid + 1) openedSrvs with
            | Some(n) -> closeTimedoutSrvs config timeoutMark n openedSrvs
            | None -> ()

let rec supervise (config:Map<string, string>) (openedSrvs:OpenServiceData array) (sok) (nsok): unit =
    let mutable note = String.Empty
    let ca = config.["controlAddress"]
    let buff : byte array = Array.zeroCreate maxMessageSize
    let rc = NN.Recv(nsok, buff, SendRecvFlags.DONTWAIT)
    if rc > 0 then
        note <- Encoding.UTF8.GetString(buff.[..rc - 1])
        log <| tracel(sprintf "Supervise recived note \"%s\"." note)
    match note with
    | "is-master" ->
        let rc = NN.Send(nsok, [|1uy|], SendRecvFlags.DONTWAIT)
        if rc < 0 then
            let errn = NN.Errno()
            let errm = NN.StrError(NN.Errno())
            log <| warnl(sprintf """Error %i on "is-master" (send). %s.""" errn errm)
        assert (rc > 0)
        supervise config openedSrvs sok nsok
    | "close" ->
        openedSrvs.[0] <- {openedSrvs.[0] with isClosing = true}
        // send aknowledgment of closing to note sender
        NN.Send(nsok, BitConverter.GetBytes(openedSrvs.[0].processId), SendRecvFlags.NONE) |> ignore
        let timeoutMark = DateTime.Now + TimeSpan.FromMilliseconds(closenTimeout)
        log <| debugl("""Supervisor closing. Notify "close" to services.""")
        let rec notifySrvs () =
            match Comm.Msg (0, "close") |> Comm.send sok with
            | Comm.Error (errn, errm) ->
                match errn with
                | 156384766 ->
                    log <| warnl(sprintf """Error %i on "close" (send). %s.""" errn errm)
                | _ ->
                    log <| errorl(sprintf """Error %i on "close" (send). %s.""" errn errm)
                if DateTime.Now > timeoutMark
                then ()
                else notifySrvs ()
            | _ -> ()
        notifySrvs ()
        match Comm.recv sok SendRecvFlags.NONE with
        | Comm.Error (errn, errm) ->
            //156384766 Operation cannot be performed in this state
            if errn = 156384766
            then log <| tracel(sprintf """Expected error %i on "close" (recv). %s.""" errn errm)
            else log <| errorl(sprintf """Unexpected error %i on "close" (recv). %s.""" errn errm)
        | Comm.Msg (lid, note) ->
            let exitCode = note
            log <| debugl(sprintf """Aknowledgement recived from [%d] exit code %s.""" lid exitCode)
            assert (lid > 0)
            assert (lid < openedSrvs.Length)
            let openedSrv = openedSrvs.[lid]
            if OpenServiceData.Default = openedSrv then
                ()
            else
                let updatedSrv = {openedSrv with closeTime = Some(DateTimeOffset.Now)}
                openedSrvs.[lid] <- updatedSrv
        log <| tracel "Master stopping."
        let waitOn = List.ofArray openedSrvs |> List.filter (fun it ->
            it <> OpenServiceData.Default &&
            it.logicId <> 0 &&
            it.closeTime = None )
        waitFor sok openedSrvs waitOn
    | _ ->
        log(tracel "Requesting report-status.")
        match Comm.Msg (0, "report-status") |> Comm.send sok with
        | Comm.Error (errn, errm) ->
            log <| warnl(sprintf """Error %i on "report-status" (send). %s.""" errn errm)
        | _ ->
            match Comm.recv sok SendRecvFlags.NONE with
            | Comm.Error (errn, errm) ->
                log <| warnl(sprintf """Error %i on "report-status" (recv). %s.""" errn errm)
            | Comm.Msg (lid, status) ->
                log <| tracel(sprintf """Reply "report-status" from [%i] status code %s.""" lid status)
                assert (lid > 0)
                assert (lid < openedSrvs.Length)
                let openedSrv = openedSrvs.[lid]
                let updatedSrv = {openedSrv with lastReply = Some(DateTimeOffset.Now)}
                openedSrvs.[lid] <- updatedSrv
                traceState openedSrvs
        let timeout = TimeSpan.FromMilliseconds(serviceTimeout)
        let timeoutMark = DateTimeOffset.Now - timeout
        closeTimedoutSrvs config timeoutMark openedSrvs.[0] openedSrvs
        log <| tracel(sprintf "Supervisor blocking for %ims." superviseInterval)
        let jr = System.Threading.Thread.CurrentThread.Join(superviseInterval)
        supervise config openedSrvs sok nsok

and waitFor (sok:int) (openedSrvs:OpenServiceData array) (waitOn:OpenServiceData list) =
    let timeout = TimeSpan.FromMilliseconds(serviceTimeout)
    let timeoutMark = DateTimeOffset.Now - timeout
    match waitOn with
    | [] -> ()
    | wl ->
        let nwl = wl |> List.fold (fun state srv ->
            let iso = timeoutMark >= srv.openTime && srv.lastReply = None
            let isl = srv.lastReply <> None && timeoutMark >= srv.lastReply.Value
            if not iso && not isl then
                state
            else
                log <| debugl(sprintf """Service [%i] timedout %A.""" srv.logicId srv)
                match tryGetProcess srv.processId with
                | true, p ->
                    let startTime = DateTimeOffset(p.StartTime)
                    //check if it's the same process
                    if srv.openTime = startTime then
                        log <| debugl(sprintf "[%i] Killing process %i." srv.logicId srv.processId)
                        try
                            p.Kill()
                        with
                        | ex ->
                            errorl (ex, sprintf "[%i] Error killing process %i." srv.logicId srv.processId)
                            |> log
                    else
                        warnl (sprintf "[%i] Process identity not confirmed %i %A" srv.logicId srv.processId startTime)
                        |> log
                | false, p ->
                    log <| warnl(sprintf "[%i] Process not found %i." srv.logicId srv.processId)
                let lid = srv.logicId
                let srv = {srv with closeTime = Some(DateTimeOffset.Now)}
                openedSrvs.[lid] <- srv
                state ) List.empty
        match Comm.recv sok SendRecvFlags.NONE with
        | Comm.Error (errn, errm) ->
            //156384766 Operation cannot be performed in this state
            if errn = 156384766
            then log <| tracel(sprintf """Expected error %i on "close" (recv). %s.""" errn errm)
            else log <| errorl(sprintf """Unexpected error %i on "close" (recv). %s.""" errn errm)
        | Comm.Msg (lid, note) ->
            let exitCode = note
            log <| debugl(sprintf """Aknowledgement recived from [%d] exit code %s.""" lid exitCode)
            assert (lid > 0)
            assert (lid < openedSrvs.Length)
            let openedSrv = openedSrvs.[lid]
            if OpenServiceData.Default = openedSrv then
                ()
            else
                //Should we care if service process is still running?
                let updatedSrv = {openedSrv with closeTime = Some(DateTimeOffset.Now)}
                openedSrvs.[lid] <- updatedSrv
        log(tracel "Waiting on")
        traceState <| Array.ofList nwl
        waitFor sok openedSrvs nwl

let executeSupervisor (config:Map<string, string>) (openedSrvs:OpenServiceData array) (socket) (nsocket) : unit =
    log <| debugl "Start supervisor."
    supervise config openedSrvs socket nsocket
    traceState openedSrvs
    //TODO:Wait for process exit
    log <| debugl "Supervisor stop."

let openMaster (execc:Execc) =
    let config = execc.config
    let openedSrvs = execc.openedSrvs
    let ok = 0
    let unknown = Int32.MaxValue
    log (infol "Master starting.")
    let nsocket = NN.Socket(Domain.SP, Protocol.PAIR)
    //TODO:error handling for socket and bind
    assert (nsocket >= 0)
    assert (NN.SetSockOpt(nsocket, SocketOption.SNDTIMEO, 1000) = 0)
    assert (NN.SetSockOpt(nsocket, SocketOption.RCVTIMEO, 1000) = 0)
    let eidp = NN.Connect(nsocket, notifyAddress)
    let mutable isMasterRunning = false
    if eidp >= 0 then
        let buff : byte array = Array.zeroCreate maxMessageSize
        let sr = NN.Send(nsocket, Encoding.UTF8.GetBytes("is-master"), SendRecvFlags.NONE)
        if sr < 0 then
            let errn = NN.Errno()
            let errm = NN.StrError(errn)
            //11 Resource unavailable, try again
            if errn = 11 && errm = "Resource unavailable, try again" then
                log (tracel(sprintf "Expected error checking for master (send). %s." errm))
            else
                log (warnl(sprintf "Error %i checking for master (send). %s." errn errm))
        let recv () =
            let rr = NN.Recv(nsocket, buff, SendRecvFlags.NONE)
            if rr < 0 then
                let errn = NN.Errno()
                let errm = NN.StrError(errn)
                //11 Resource unavailable, try again
                if errn = 11 && errm = "Resource unavailable, try again" then
                    log (tracel(sprintf "Expected error checking for master (recv). %s." errm))
                else
                    log (warnl(sprintf "Error %i checking for master (recv). %s." errn errm))
                false, errn
            else
                true, 0
        match recv () with
        | false, 11 -> recv() |> ignore //we try again
        | _ -> ()
        isMasterRunning <- buff.[0] = 1uy
    if isMasterRunning then
        log (warnl(sprintf "Master is already running. Terminating."))
        execc.state <- "term"
        NN.Shutdown(nsocket, eidp) |> ignore
        NN.Close(nsocket) |> ignore
        ok
    else

    let socket = NN.Socket(Domain.SP, Protocol.SURVEYOR)
    //TODO: error handling for socket and bind
    assert (socket >= 0)
    let curp = System.Diagnostics.Process.GetCurrentProcess()
    //TODO: get arguments information from current process?
    //let serviceCmd = curp.MainModule.fileName + " " + curp.StartInfo.Arguments
    let openTime = DateTimeOffset(curp.StartTime)
    openedSrvs.[0] <- {
        OpenServiceData.Default with
            logicId = 0;
            processId = curp.Id;
            openTime = openTime;
            fileName = curp.MainModule.FileName;}
    let ca = config.["controlAddress"]
    let eid = NN.Bind(socket, ca)
    let eidp = NN.Bind(nsocket, notifyAddress)
    assert (eid >= 0)
    assert (eidp >= 0)
    //TODO: Check for already running ormonit services in case master is interrupted/killed externally
    //config <- RequestStatus config socket
    //config <- CollectStatus config q socket
    //let sr = NN.Send(socket, Encoding.UTF8.GetBytes("close"), SendRecvFlags.NONE)
    //TODO:Error handling NN.Errno ()
    //assert (sr >= 0)
    let index = 1
    let mutable last = 1
    let services = loadServices config Environment.CurrentDirectory
    services
    |> List.iteri (fun i it ->
        let lid =  index + i
        let args = sprintf "--open-service %s --ctrl-address %s --logic-id %d" it.Location ca lid
        let cmdArgs = sprintf "%s %s" ormonitFileName args
        log (tracel cmdArgs)
        let p = executeProcess ormonitFileName args olog
        let openTime = DateTimeOffset(p.StartTime)
        openedSrvs.[lid] <- {
            OpenServiceData.Default with
                logicId = lid;
                processId = p.Id;
                openTime = openTime;
                fileName = it.Location;}
        last <- lid )
    log (infol(sprintf """Master started with %i %s, controlAddress: "%s".""" last (if last > 1 then "services" else "service") ca))
    execc.state <- "started"
    executeSupervisor config openedSrvs socket nsocket
    assert(NN.Shutdown(socket, eid) = 0)
    assert(NN.Shutdown(nsocket, eidp) = 0)
    assert(NN.Close(socket) = 0)
    assert(NN.Close(nsocket) = 0)
    log (infol "Master stopped.")
    execc.state <- "stopped"
    NN.Term()
    0

let private shash:Dictionary<string, Execc> = Dictionary<string, Execc>()

let makeMaster () =
    let key = "0"//TODO: create an actually random key
    let config = Map.empty.Add("controlAddress", controlAddress)
                          .Add("notifyAddress", notifyAddress)
    let openedSrvs:OpenServiceData array = Array.create maxOpenServices OpenServiceData.Default
    let mutable main = fun () -> ()
    let t = Thread (main)
    let execc = {config = config; openedSrvs = openedSrvs; thread = t; }
    main <- fun () -> openMaster execc |> ignore
    let ckey = {key = key;}
    shash.Add(key, execc)
    ckey

let private isOpen (execc) =
    let state = lock execc (fun () -> execc.state)
    log (tracel(sprintf "Execc ThreadState:%A" execc.thread.ThreadState))
    state = "started" 

let private isFailed (execc) =
    let state = lock execc (fun () -> execc.state)
    log (tracel(sprintf "Execc ThreadState:%A" execc.thread.ThreadState))
    state <> "started" && state <> "stopped" && state <> null
    
let start (ckey:Ctlkey) =
    let ok = 0
    let unknown = Int32.MaxValue
    let execc = shash.[ckey.key]
    execc.thread.Start()
    log (tracel(sprintf "Execc ThreadState:%A" execc.thread.ThreadState))
    while not (isOpen execc) && not (isFailed execc) do
        System.Threading.Thread.CurrentThread.Join 1 |> ignore
    if isFailed execc then unknown
    else ok

let stop (ckey:Ctlkey) =
    let ok = 0
    let unknown = Int32.MaxValue
    let execc = shash.[ckey.key]
    let na = execc.config.["notifyAddress"]
    let note = "close"
    let nsocket = NN.Socket(Domain.SP, Protocol.PAIR)
    let buff : byte array = Array.zeroCreate maxMessageSize
    //TODO:error handling for socket and bind
    assert (nsocket >= 0)
    assert (NN.SetSockOpt(nsocket, SocketOption.SNDTIMEO, superviseInterval * 5) = 0)
    assert (NN.SetSockOpt(nsocket, SocketOption.RCVTIMEO, superviseInterval * 5) = 0)
    let eid = NN.Connect(nsocket, notifyAddress)
    assert ( eid >= 0)
    log (tracel(sprintf "[Stop Thread] Notify \"%s\"." note))
    let bytes = Encoding.UTF8.GetBytes(note)
    let send () =
        let sr = NN.Send (nsocket, bytes, SendRecvFlags.NONE)
        if sr < 0 then
            let errn = NN.Errno()
            let errm = NN.StrError(errn)
            Choice2Of2 (errn, errm)
        else
            Choice1Of2 (sr)
    let errn, errm = 
        match send () with
        | Choice1Of2 _ -> (ok, String.Empty)
        | Choice2Of2 (errn, errm) ->
            //11 Resource unavailable, try again
            if errn <> 11 then
                errn, errm
            else
            //we try again
            match send () with
            | Choice1Of2 _ -> (ok, String.Empty)
            | Choice2Of2 (errn, errm) -> errn, errm
    if errn <> ok then
        log (warnl(sprintf "[Stop Thread] Unable to notify \"%s\" (send). Error %i %s." note errn errm))
    let recv () =
        let rr = NN.Recv(nsocket, buff, SendRecvFlags.NONE)
        if rr = 4 then
            let pid = BitConverter.ToInt32(buff, 0)
            Choice1Of2 (pid)
        elif rr < 0 then
            let errn = NN.Errno()
            let errm = NN.StrError(errn)
            Choice2Of2 (errn, errm)
        else
            Choice2Of2 (unknown, "Unknown")
    let mutable masterpid = -1
    let errn, errm =
        match recv () with
        | Choice1Of2 pid -> masterpid <- pid; (ok, String.Empty)
        | Choice2Of2 (errn, errm) -> //we try again
            match recv () with
            | Choice1Of2 pid -> masterpid <- pid; (ok, String.Empty)
            | Choice2Of2 (errn, errm) -> (errn, errm)
    if errn = ok then
        log (infol(sprintf "[Stop Thread] Aknowledgment of note \"%s\" (recv). Master pid: %i." note masterpid))
    else
        log (warnl(sprintf "[Stop Thread] No aknowledgment of note \"%s\" (recv). Error %i %s." note errn errm))
    NN.Shutdown(nsocket, eid) |> ignore
    NN.Close(nsocket) |> ignore
    if errn <> ok  then unknown
    else
    let thjr = execc.thread.Join(5000)
    if execc.thread.ThreadState <> ThreadState.Stopped then
        log (fatall(sprintf "[Stop Thread] Error waiting for master's exit. Master thread: %i." execc.thread.ManagedThreadId))
        execc.thread.Abort()
        unknown
    else
    ok

