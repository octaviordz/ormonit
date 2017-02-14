[<AutoOpen()>]
module internal OrmonitFunc

open System.Threading
open System.Reflection
open NLog
open NNanomsg
open System.Text
open System
open System.IO
open NLog
open NLog.Layouts
open System.Reflection
open System.Collections.Generic
open System.Diagnostics
open System.Text
open System.Collections.Concurrent
open NNanomsg
open Ormonit.Service

let log = LogManager.GetLogger "Ormonit.Service"

let RequestStatus (astate:Map<string, string>) socket =
    log.Info "Ormonit master request services status."
    let ca = astate.["controlAddress"]
    let sr = NN.Send(socket, Encoding.UTF8.GetBytes("report-status"), SendRecvFlags.NONE)
    //TODO:Error handling NN.Errno ()
    assert (sr >= 0)
    astate

let CollectStatus (config: Map<string, string>) (q:ConcurrentQueue<string>) socket =
    let ca = config.["controlAddress"]
    let da = config.["dataAddress"]
    sprintf "CollectStatus: controlAddress \"%s\", dataAddress \"%s\"." ca da |> log.Trace
    //let mutable buff : byte[] = null
    let buff : byte array = Array.zeroCreate 256
    let rc = NN.Recv(socket, buff, SendRecvFlags.DONTWAIT)
    if rc <= 0 then
        log.Error("Unable to collect status. NN.Errno {0}", NN.Errno())
        config
    else

    assert (rc >= 0)
    let c = Encoding.UTF8.GetString(buff.[..rc])
    let cmd =
        c.ToCharArray()
        |> Array.takeWhile (fun it -> it <> '\000')
        |> String
    sprintf "\"%s\" respone." cmd |> log.Trace
    config

let Ctrlloop (config : IDictionary<string, string>) =
    let ca = config.["controlAddress"]
    let da = config.["dataAddress"]
    sprintf "In control loop with: controlAddress \"%s\", dataAddress \"%s\"." ca da |> log.Trace
    let s = NN.Socket(Domain.SP, Protocol.BUS)
    //TODO:error handling for socket and bind
    assert (s >= 0)
    assert (NN.Bind(s, ca) >= 0)
    let rec recvloop config =
        //let mutable buff : byte[] = null
        let buff : byte array = Array.zeroCreate 256
        log.Info("Waiting for command.")
        let rc = NN.Recv(s, buff, SendRecvFlags.NONE)
        //TODO:Error handling NN.Errno ()
        assert (rc >= 0)
        let c = Encoding.UTF8.GetString(buff.[..rc])
        let cmd =
            c.ToCharArray()
            |> Array.takeWhile (fun it -> it <> '\000')
            |> String
        sprintf "\"%s\" command received." cmd |> log.Trace
        match cmd with
        | "stop" -> ()
        | "hearbeat" ->
            let sr = NN.Send(s, [|1uy|], SendRecvFlags.NONE)
            //TODO:Error handling NN.Errno ()
            assert (sr >= 0)
            recvloop config
        | _ -> recvloop config
    recvloop config


let Supervise (appState:Map<string, string>) (services:Assembly list) (q:ConcurrentQueue<string>) socket =
    log.Info "Ormonit service supervise."
    let rec supervisor (appState:Map<string, string>) (services:Assembly list) (q:ConcurrentQueue<string>) (socket) : unit =
        let appState = RequestStatus appState socket
        let appState = CollectStatus appState q socket
        let jr = Thread.CurrentThread.Join(250)
        supervisor appState services q socket
    supervisor appState services q socket
    log.Info "Ormonit service stopping supervise."
    ()

