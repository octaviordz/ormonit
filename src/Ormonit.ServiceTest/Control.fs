module Ormonit.ServiceTest.Control

open System
open System.IO
open NLog
open NLog.Layouts
open System.Collections.Generic
open System.Text
open System.Threading
open System.Threading.Tasks
open NNanomsg

let log = LogManager.GetLogger "Ormonit.Test"
let logConfig = NLog.Config.LoggingConfiguration()
let consoleTarget = new NLog.Targets.ColoredConsoleTarget()
logConfig.AddTarget("console", consoleTarget)
let fileTarget = new NLog.Targets.FileTarget()
logConfig.AddTarget("file", fileTarget)
consoleTarget.Layout <- Layout.FromString @"${date:format=HH\:mm\:ss} ${logger} ${message}"
fileTarget.FileName <- Layout.FromString @"${basedir}\logs\${shortdate}.log"
let rule1 = new NLog.Config.LoggingRule("*", LogLevel.Trace, consoleTarget)
logConfig.LoggingRules.Add(rule1)
let rule2 = new NLog.Config.LoggingRule("*", LogLevel.Trace, fileTarget)
logConfig.LoggingRules.Add(rule2)
LogManager.Configuration <- logConfig

let mutable private continu = true
let private mlock = obj()

let private Ctrlloop (config:IDictionary<string, string>) =
    let ca = config.["controlAddress"]
    let da = config.["dataAddress"]
    sprintf "Ormonit test in control loop with: controlAddress \"%s\", dataAddress \"%s\"." ca da |> log.Trace
    let s = NN.Socket(Domain.SP, Protocol.RESPONDENT)
    //TODO:error handling for socket and connect
    assert (s >= 0)
    assert (NN.Connect(s, ca) >= 0)
    let rec recvloop config =
        //let mutable buff : byte[] = null
        let buff : byte array = Array.zeroCreate 256
        log.Info("Ormonit test waiting for command.")
        let nbytes = NN.Recv(s, buff, SendRecvFlags.NONE)
        //TODO:Error handling NN.Errno ()
        assert (nbytes >= 0)
        let c = Encoding.UTF8.GetString(buff.[..nbytes - 1])
        let cmd =
            c.ToCharArray()
            |> Array.takeWhile (fun it -> it <> '\000')
            |> String
        sprintf "Ormonit test command \"%s\" received." cmd |> log.Trace
        match cmd with
        | "stop" ->
            let flag = lock mlock (fun () -> continu <- false; continu)
            log.Trace("Processing \"stop\" command. continue value {0}", flag)
            let nbytes = NN.Send(s, [|0uy|], SendRecvFlags.NONE)
            assert(nbytes = 1)
        | "report-status" ->
            let sr = NN.Send(s, [|1uy|], SendRecvFlags.NONE)
            //TODO:Error handling NN.Errno ()
            assert (sr >= 0)
            recvloop config
        | _ -> recvloop config
    recvloop config

let rec private Action () =
    log.Trace("Ormonit test action enter")
    match lock mlock (fun () -> continu) with
    | false ->
        log.Trace("continue value {0}", continu)
        log.Trace("Ormonit test action exit")
    | true -> 
        log.Trace("Ormonit test action waiting")
        let jr = Thread.CurrentThread.Join(1000)
        log.Trace("Ormonit test action jr {0} continue {1}", jr, continu)
        log.Trace("continue value {0} flag", continu)
        Action()

let Start (config : IDictionary<string, string>) =
    log.Trace("Start. Current directory {0}", Environment.CurrentDirectory)
    let task = Task.Run(Action)
    Ctrlloop config
    log.Info("Ormonit test exit control loop.")
    lock mlock (fun () -> continu <- false)
    task.Wait()

