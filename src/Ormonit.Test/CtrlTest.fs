module CtrlTest

open Xunit
open Ctrl
open System.Threading
open System.Collections.Generic
open System.Collections.Concurrent

let internal makeMaster () = 
    let controlAddress = "ipc://ormonit/test/control.ipc"
    let notifyAddress = "ipc://ormonit/test/notify.ipc"
    Ctrl.makeMaster
        { controlAddress = controlAddress
          notifyAddress = notifyAddress
          execcType = ExeccType.ConsoleApplication }

[<Fact>]
let thereShouldOnlyBeOneMasterSingleThreaded () : unit = 
    for _ in 0 .. 0 do
        let ck0 = makeMaster()
        let ck1 = makeMaster()
        let r0 = Ctrl.start ck0
        //assert (r0 = 0)
        if r0 <> 0 then printfn "ERROR Unable to start"
        let r1 = Ctrl.start ck1
        //assert (r1 <> 0)
        if r1 = 0 then printfn "ERROR multiple master instances"
        if r0 = 0 then Ctrl.stop ck0 |> ignore
        if r1 = 0 then Ctrl.stop ck1 |> ignore
        r0 = 0 |> Assert.True
        r1 <> 0 |> Assert.True

[<Fact>]
let thereShouldOnlyBeOneMasterMultiThreaded () : unit = 
    let bag = ConcurrentBag<int * Ctrl.Ctlkey * bool>()
    let threads = List<Thread>()
    for i in 1 .. 2 do
        let t = Thread(fun () ->
            let ck = makeMaster()
            let r = Ctrl.start ck 
            bag.Add(i, ck, (r = 0)) )
        threads.Add(t)
        t.Start()
    for t in threads do
        t.Join()
    let l = bag |> Seq.filter (fun (_, _, started ) -> started = true) |> List.ofSeq
    if l.Length > 1 then  printfn "ERROR"
    for idx, ck, started in bag do
        printfn "%i %A %b" idx ck started
        Ctrl.stop ck |> ignore
    Cilnn.Nn.Term()
 
[<Fact>]
let thereShouldOnlyBeOneMaster () : unit = 
    thereShouldOnlyBeOneMasterSingleThreaded ()
    //checkOneMasterMultiThreaded()
    ()
