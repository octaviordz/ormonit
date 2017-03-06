namespace Ormonit.Hosting

type ServiceHost() =
    let srvs = List.empty
    member s.Run(): unit = ()
    member s.AddService<'T> (): unit =
        let args = [|Map.empty<string, string>|]
        let srv = System.Activator.CreateInstance(typeof<'T>, args)
        ()



//
//let log = LogManager.GetLogger "Ormonit.Test"
//let logConfig = NLog.Config.LoggingConfiguration()
//let consoleTarget = new NLog.Targets.ColoredConsoleTarget()
//logConfig.AddTarget("console", consoleTarget)
//let fileTarget = new NLog.Targets.FileTarget()
//logConfig.AddTarget("file", fileTarget)
//consoleTarget.Layout <- Layout.FromString @"${date:format=HH\:mm\:ss}|${logger}|${message}"
//fileTarget.FileName <- Layout.FromString @"${basedir}\logs\Ormonit.ServiceTest.log"
//let rule1 = new NLog.Config.LoggingRule("*", LogLevel.Trace, consoleTarget)
//logConfig.LoggingRules.Add(rule1)
//let rule2 = new NLog.Config.LoggingRule("*", LogLevel.Trace, fileTarget)
//logConfig.LoggingRules.Add(rule2)
//LogManager.Configuration <- logConfig
//
//let mutable lid = 0
//let maxMessageSize = 256
//let mutable private continu = true
//let private mlock = obj()
//
//let private ctrlloop (config:IDictionary<string, string>) =
//    let ca = config.["controlAddress"]
//    sprintf "[%i] Ormonit test in control loop with" lid |> log.Trace
//    let s = NN.Socket(Domain.SP, Protocol.RESPONDENT)
//    //TODO:error handling for socket and connect
//    assert (s >= 0)
//    let eid = NN.Connect(s, ca)
//    assert (eid >= 0)
//    let rec recvloop config =
//        //let mutable buff : byte[] = null '\000'
//        let buff : byte array = Array.zeroCreate maxMessageSize
//        //log.Info("[{0}] Ormonit test receive (blocking).", lid)
//        let nbytes = NN.Recv(s, buff, SendRecvFlags.NONE)
//        //TODO:Error handling NN.Errno ()
//        assert (nbytes >= 0)
//        let msg = Encoding.UTF8.GetString(buff.[..nbytes - 1])
//        let cmd = msg
//        sprintf "[%i] Ormonit test \"%s\" note received." lid cmd |> log.Info
//        match cmd with
//        | "close" ->
//            let flag = lock mlock (fun () -> continu <- false; continu)
//            //log.Trace("""[{0}] Processing "close" notification. continue value {1}""", lid, flag)
//            let bytes = Array.zeroCreate 8
//            Buffer.BlockCopy(BitConverter.GetBytes(lid), 0, bytes, 0, 4)
//            Buffer.BlockCopy(BitConverter.GetBytes(0), 0, bytes, 4, 4)
//            log.Trace("""[{0}] Sending "close" aknowledgement.""", lid)
//            let sr = NN.Send(s, bytes, SendRecvFlags.NONE)
//            assert(sr > 0)
//            //log.Trace("""[{0}] Sent "close" aknowledgement.""", lid)
//        | "report-status" ->
//            let rand =Random().NextDouble()
//            if rand > 0.8 then
//                //log.Trace("""[{0}] Processing "report-status" command.""", lid)
//                let bytes = Array.zeroCreate 8
//                Buffer.BlockCopy(BitConverter.GetBytes(lid), 0, bytes, 0, 4)
//                Buffer.BlockCopy(BitConverter.GetBytes(0), 0, bytes, 4, 4)
//                log.Trace("""[{0}] Sending "report-status" aknowledgement.""", lid)
//                let sr = NN.Send(s, bytes, SendRecvFlags.NONE)
//                //TODO:Error handling NN.Errno ()
//                assert (sr > 0)
//                //log.Trace("""[{0}] Sent "report-status" aknowledgement.""", lid)
//            else
//                log.Warn("""[{0}] Processing "report-status" failed (simulated).""", lid)
//            recvloop config
//        | _ -> recvloop config
//    recvloop config
//    assert(NN.Shutdown(s, eid) = 0)
//    assert(NN.Close(s) = 0)
//
//let rec private action () =
//    //log.Trace("[{0}] Ormonit test action enter", lid)
//    match lock mlock (fun () -> continu) with
//    | false ->
//        //log.Trace("continue value {0}", continu)
//        log.Trace("[{0}] Ormonit test action exit", lid)
//    | true ->
//        //simulate long running task 1-5s
//        let t = Random().Next(1000, 5000)
//        log.Trace("[{0}] Ormonit test action blocking for {1}ms", lid, t)
//        let jr = Thread.CurrentThread.Join(t)
//        //log.Trace("Ormonit test action jr {0} continue {1}", jr, continu)
//        //log.Trace("continue value {0} flag", continu)
//        action()
//
//let Start (config: IDictionary<string, string>) =
//    let p, logicId = Int32.TryParse config.["logicId"]
//    if not p then
//        raise (System.ArgumentException("logicId"))
//    lid <- logicId
//    log.Info("[{0}] Start. Current directory {1}", lid, Environment.CurrentDirectory)
//    let task = Task.Run(action)
//    ctrlloop config
//    log.Info("[{0}] Ormonit test exit control loop.", lid)
//    lock mlock (fun () -> continu <- false)
//    task.Wait()
//    log.Info("[{0}] Ormonit test exit.", lid)
//
//
