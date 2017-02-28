module Ctrl

open System
open System.IO
open NLog
open System.Reflection
open System.Diagnostics
open System.Text
open NNanomsg

let maxOpenServices = 50
let maxMessageSize = 128
//time in milliseconds supervise waits before runs again
let superviseInterval = 100
//time in milliseconds a service has to reply before timingout
let serviceTimeout = 3000.0
let ormonitFileName = Path.Combine(Environment.CurrentDirectory, "Ormonit")
let log = LogManager.GetLogger "Ormonit"
let olog = LogManager.GetLogger "_Ormonit.Output_"

type OpenServiceData = 
    {logicId: int;
     processId: int;
     lastReply: DateTimeOffset option;
     openTime: DateTimeOffset;
     closeTime: DateTimeOffset option;
     fileName: string;}
     static member Default =
        {logicId = -1;
         processId = -1;
         lastReply = None;
         openTime = DateTimeOffset.MinValue;
         closeTime = None;
         fileName = String.Empty;}

let executeProcess (fileName:string) (arguments) (olog:Logger) =
    let psi = new ProcessStartInfo(fileName, arguments)
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
    let asl = Infrastructure.AssemblyLoader()
    try
        sprintf "Try load assembly \"%s\"" fullName |> log.Trace
        let a = asl.LoadFromAssemblyPath(fullName)
        //TODO:How to guaranty this is a valid service?!
        Some(a)
    with
    | ex ->
        log.Error(ex, "Unable to load assembly {0}", fullName)
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
    sprintf "Execute service with configuration \"%A\"" config |> log.Trace
    let (?) (t : Type) (mname : string) =
        t.GetMethod(mname)
    let asl = Infrastructure.AssemblyLoader()
    let asm = asl.LoadFromAssemblyPath(p)
    let name = asm.GetName().Name + ".Control"
    let t = asm.GetType(name)
    if isNull t then sprintf "Control not found: %s value %A." name t |> log.Error
    else sprintf "Control found: %s value %A." name t |> log.Trace
    let start = t?Start
    sprintf "Control start method: %A." start |> log.Trace
    start.Invoke(t, [|config|]) |> ignore

let requestStatus (lid:int32) socket =
    log.Info "Master requests services status."
    let cbytes = Encoding.UTF8.GetBytes("report-status")
    let bytes = Array.zeroCreate 5
    Buffer.BlockCopy(BitConverter.GetBytes(lid), 0, bytes, 0, 4)
    Buffer.BlockCopy(cbytes, 4, bytes, 0, cbytes.Length)
    let sr = NN.Send(socket, bytes, SendRecvFlags.NONE)
    //TODO:Error handling NN.Errno ()
    assert (sr >= 0)

let collectStatus (config:Map<string, string>) socket =
    let ca = config.["controlAddress"]
    sprintf "CollectStatus: controlAddress \"%s\"." ca |> log.Trace
    //let mutable buff : byte[] = null
    let buff : byte array = Array.zeroCreate maxMessageSize
    let rc = NN.Recv(socket, buff, SendRecvFlags.DONTWAIT)
    if rc < 0 then
        log.Warn("Unable to collect status. NN.Errno {0}", NN.StrError(NN.Errno()))
        config
    else

    let cmd = Encoding.UTF8.GetString(buff.[..rc - 1])
    sprintf "CollectStatus \"%s\" reply." cmd |> log.Trace
    config

let traceState (openedSrvs) =
    if not log.IsTraceEnabled then ()
    else

    let strBuilder = StringBuilder("OpenedSrvs: ")
    openedSrvs
    |> Array.takeWhile (fun it -> it <> OpenServiceData.Default)
    |> Printf.bprintf strBuilder "%A"
    log.Trace(strBuilder.ToString())

let tryGetProcess processId =
    try
        let p = Process.GetProcessById(processId)
        true, p
    with
    | ex ->
        (ex, sprintf "Unable to get process %i" processId) |> log.Warn
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

        sprintf """[%i] "report-status" timedout %A.""" srv.logicId srv |> log.Debug
        let r, p = tryGetProcess srv.processId
        if r = true then
            let startTime = DateTimeOffset(p.StartTime)
            //check if it's the same process
            if srv.openTime = startTime then
                sprintf "[%i] Killing process %i" srv.logicId srv.processId |> log.Debug
                p.Kill()
            else
                sprintf "[%i] Process identity not confirmed %i %A" srv.logicId srv.processId startTime |> log.Warn
        let lid = srv.logicId
        let args = sprintf "--open-service %s --ctrl-address %s --logic-id %d" srv.fileName ca lid
        sprintf "[%i] Opening service with %s" srv.logicId args |> log.Debug
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

let rec supervise (config:Map<string, string>) (openedSrvs:OpenServiceData array) (socket) (nsocket): unit =
    let mutable note = String.Empty
    let ca = config.["controlAddress"]
    let buff : byte array = Array.zeroCreate maxMessageSize
    let rc = NN.Recv(nsocket, buff, SendRecvFlags.DONTWAIT)
    if rc > 0 then
        note <- Encoding.UTF8.GetString(buff.[..rc - 1])
        sprintf "Supervise recived note \"%s\"." note |> log.Trace
    match note with
    | "is-master" ->
        let rc = NN.Send(nsocket, [|1uy|], SendRecvFlags.DONTWAIT)
        if rc < 0 then
            let errn = NN.Errno()
            let errm = NN.StrError(NN.Errno())
            sprintf """Error %i on "is-master" (send). %s.""" errn errm |> log.Warn
        assert (rc > 0)
        supervise config openedSrvs socket nsocket
    | "close" ->
        log.Debug """Supervisor closing. Notify "close" to services."""
        let rsend = NN.Send(socket, Encoding.UTF8.GetBytes("close"), SendRecvFlags.NONE)
        if rsend < 0 then
            let errn = NN.Errno()
            let errm = NN.StrError(NN.Errno())
            sprintf """Error %i on "close" (send). %s.""" errn errm |> log.Error
        else

        let rec waitForClose () =
            let rrecv = NN.Recv(socket, buff, SendRecvFlags.NONE)
            if rrecv < 0 then
                let errn = NN.Errno()
                let errm = NN.StrError(errn)
                //156384766 Operation cannot be performed in this state
                if errn = 156384766 then
                    sprintf """Expected error %i on "close" (recv). %s.""" errn errm |> log.Trace
                else
                    sprintf """Unexpected error %i on "close" (recv). %s.""" errn errm |> log.Error
                let index = openedSrvs |> Array.tryFindIndex (fun it ->
                    it <> OpenServiceData.Default &&
                    it.logicId <> 0 &&
                    it.closeTime = None )
                match index with
                | None -> ()
                | Some(i) ->
                    let timeout = TimeSpan.FromMilliseconds(serviceTimeout)
                    let timeoutMark = DateTimeOffset.Now - timeout
                    let srv = openedSrvs.[i]
                    let iso = timeoutMark >= srv.openTime && srv.lastReply = None
                    let isl = srv.lastReply <> None && timeoutMark >= srv.lastReply.Value
                    if iso || isl then
                        sprintf """Service [%i] timedout %A.""" srv.logicId srv |> log.Debug
                        let r, p = tryGetProcess srv.processId
                        let startTime = DateTimeOffset(p.StartTime)
                        //check if it's the same process
                        if r && srv.openTime = startTime then
                            sprintf "[%i] Killing process %i" srv.logicId srv.processId |> log.Debug
                            p.Kill()
                        elif r then
                            sprintf "[%i] Process identity not confirmed %i %A" srv.logicId srv.processId startTime |> log.Warn
                        openedSrvs.[srv.logicId] <- {
                                srv with closeTime = Some(DateTimeOffset.Now)}
                        traceState openedSrvs
                    else
                        sprintf "Waiting on [%i] %A." i srv |> log.Trace
                    waitForClose()
            else

            let msg = buff.[..rrecv - 1]
            let lid = BitConverter.ToInt32(msg, 0)
            let exitCode = BitConverter.ToInt32(msg.[3..], 0)
            sprintf """Aknowledgement recived from [%d] exit code %i.""" lid exitCode |> log.Debug
            assert (lid > 0)
            assert (lid < openedSrvs.Length)
            let openedSrv = openedSrvs.[lid]
            if OpenServiceData.Default = openedSrv then
                waitForClose()
            else

            let updatedSrv = {openedSrv with closeTime = Some(DateTimeOffset.Now)}
            openedSrvs.[lid] <- updatedSrv
            traceState openedSrvs
            let index = openedSrvs |> Array.tryFindIndex (fun it ->
                it <> OpenServiceData.Default &&
                it.logicId <> 0 &&
                it.closeTime = None )
            match index with
            | None -> ()
            | Some(i) ->
                sprintf "Waiting on [%i] for aknowledgement %A." i openedSrvs.[i] |> log.Trace
                waitForClose()
        log.Trace "Master stopping."
        waitForClose()
    | _ ->
        log.Trace "Requesting report-status."
        let sr = NN.Send(socket, Encoding.UTF8.GetBytes("report-status"), SendRecvFlags.NONE)
        if sr < 0 then
            let errn = NN.Errno()
            let errm = NN.StrError(errn)
            sprintf """Error %i on "report-status" (send). %s.""" errn errm |> log.Warn
        let buff : byte array = Array.zeroCreate maxMessageSize
        let rc = NN.Recv(socket, buff, SendRecvFlags.NONE)
        if rc < 0 then
            let errn = NN.Errno()
            let errm = NN.StrError(errn)
            sprintf """Error %i on "report-status" (recv). %s.""" errn errm |> log.Warn
        else
            let msg = buff.[..rc - 1]
            let lid = BitConverter.ToInt32(msg, 0)
            let status = BitConverter.ToInt32(msg.[3..], 0)
            sprintf """Reply "report-status" from [%i] status code %i.""" lid status |> log.Trace
            assert (lid > 0)
            assert (lid < openedSrvs.Length)
            let openedSrv = openedSrvs.[lid]
            let updatedSrv = {openedSrv with lastReply = Some(DateTimeOffset.Now)}
            openedSrvs.[lid] <- updatedSrv
            traceState openedSrvs
        let timeout = TimeSpan.FromMilliseconds(serviceTimeout)
        let timeoutMark = DateTimeOffset.Now - timeout
        closeTimedoutSrvs config timeoutMark openedSrvs.[0] openedSrvs
        sprintf "Supervisor blocking for %ims." superviseInterval |> log.Trace
        let jr = System.Threading.Thread.CurrentThread.Join(superviseInterval)
        supervise config openedSrvs socket nsocket

let executeSupervisor (config:Map<string, string>) (openedSrvs:OpenServiceData array) (socket) (nsocket) : unit =
    log.Debug "Start supervisor."
    supervise config openedSrvs socket nsocket
    traceState openedSrvs
    //TODO:Wait for process exit
    log.Debug "Supervisor stop."

