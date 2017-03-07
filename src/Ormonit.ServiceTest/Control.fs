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
open Comm

let log = LogManager.GetLogger "Ormonit.Test"
let logConfig = NLog.Config.LoggingConfiguration()
let consoleTarget = new NLog.Targets.ColoredConsoleTarget()
logConfig.AddTarget("console", consoleTarget)
let fileTarget = new NLog.Targets.FileTarget()
logConfig.AddTarget("file", fileTarget)
consoleTarget.Layout <- Layout.FromString @"${date:format=HH\:mm\:ss}|${logger}|${message}"
fileTarget.FileName <- Layout.FromString @"${basedir}\logs\Ormonit.ServiceTest.log"
let rule1 = new NLog.Config.LoggingRule("*", LogLevel.Trace, consoleTarget)
logConfig.LoggingRules.Add(rule1)
let rule2 = new NLog.Config.LoggingRule("*", LogLevel.Trace, fileTarget)
logConfig.LoggingRules.Add(rule2)
LogManager.Configuration <- logConfig

let mutable lid = 0
let mutable private continu = true
let private mlock = obj()

let private ctrlloop (config:IDictionary<string, string>) =
    let ca = config.["controlAddress"]
    sprintf "[%i] Ormonit test in control loop with" lid |> log.Trace
    let s = NN.Socket(Domain.SP, Protocol.RESPONDENT)
    //TODO:error handling for socket and connect
    assert (s >= 0)
    let eid = NN.Connect(s, ca)
    assert (eid >= 0)
    let rec recvloop () =
        //let mutable buff : byte[] = null '\000'
        //log.Info("[{0}] Ormonit test receive (blocking).", lid)
        match recv s SendRecvFlags.NONE with
        | Error (errn, errm) ->
            sprintf """Error %i (recv). %s.""" errn errm |> log.Error
            recvloop ()
        | Msg (_, note) ->
        sprintf "[%i] Ormonit test \"%s\" note received." lid note |> log.Info
        match note with
        | "close" ->
            let flag = lock mlock (fun () -> continu <- false; continu)
            //log.Trace("""[{0}] Processing "close" notification. continue value {1}""", lid, flag)
            log.Trace("""[{0}] Sending "close" aknowledgement.""", lid)
            match Msg(lid, "closing") |> send s SendRecvFlags.NONE with
            | Error (errn, errm) ->
                sprintf """Error %i (send). %s.""" errn errm |> log.Error
                recvloop ()
            | Msg _ ->
                //log.Trace("""[{0}] Sent "close" aknowledgement.""", lid)
                ()
        | "report-status" ->
            let rand = Random().NextDouble()
            if rand > 0.8 then
                //log.Trace("""[{0}] Processing "report-status" command.""", lid)
                log.Trace("""[{0}] Sending "report-status" aknowledgement.""", lid)
                match Msg(lid, "ok") |> send s SendRecvFlags.NONE with
                | Error (errn, errm) ->
                    sprintf """Error %i (send). %s.""" errn errm |> log.Error
                    recvloop ()
                | Msg _ ->
                    //log.Trace("""[{0}] Sent "report-status" aknowledgement.""", lid)
                    ()
            else
                log.Warn("""[{0}] Processing "report-status" failed (simulated).""", lid)
            recvloop ()
        | _ -> recvloop ()
    recvloop ()
    assert(NN.Shutdown(s, eid) = 0)
    assert(NN.Close(s) = 0)

let rec private action () =
    //log.Trace("[{0}] Ormonit test action enter", lid)
    match lock mlock (fun () -> continu) with
    | false ->
        //log.Trace("continue value {0}", continu)
        log.Trace("[{0}] Ormonit test action exit", lid)
    | true ->
        //simulate long running task 1-5s
        let t = Random().Next(1000, 5000)
        log.Trace("[{0}] Ormonit test action blocking for {1}ms", lid, t)
        let jr = Thread.CurrentThread.Join(t)
        //log.Trace("Ormonit test action jr {0} continue {1}", jr, continu)
        //log.Trace("continue value {0} flag", continu)
        action()

let Start (config: IDictionary<string, string>) =
    let p, logicId = Int32.TryParse config.["logicId"]
    if not p then
        raise (System.ArgumentException("logicId"))
    lid <- logicId
    log.Info("[{0}] Start. Current directory {1}", lid, Environment.CurrentDirectory)
    let task = Task.Run(action)
    ctrlloop config
    log.Info("[{0}] Ormonit test exit control loop.", lid)
    lock mlock (fun () -> continu <- false)
    task.Wait()
    log.Info("[{0}] Ormonit test exit.", lid)


