open System
open System.IO
open System.Diagnostics
open Topshelf
open Time
open NLog
open NLog.Layouts

let ormonitFileName = Path.Combine(Environment.CurrentDirectory, "Ormonit")
let private ckey = Ctrl.makeMaster ()

[<EntryPoint>]
let main argv =
    let start hc =
        0 = Ctrl.start ckey

    let stop hc =
        0 = Ctrl.stop ckey

    Service.Default
    |> display_name "Ormonit"
    |> service_name "Ormonit"
    |> with_start start
    |> with_recovery (ServiceRecovery.Default |> restart (min 10))
    |> with_stop stop
    |> run
