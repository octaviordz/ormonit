module internal Ctrl

open System
open System.IO
open NLog
open System.Reflection
open System.Diagnostics
open System.Text
open NNanomsg
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

