open System
open System.IO
open System.Diagnostics
open Topshelf
open Time
open NLog
open NLog.Layouts

let ormonitFileName = Path.Combine(Environment.CurrentDirectory, "Ormonit")

[<EntryPoint>]
let main argv =
    let start hc =
        let psi = ProcessStartInfo(ormonitFileName, "start")
        psi.UseShellExecute <- false
        psi.RedirectStandardOutput <- true
        psi.RedirectStandardError <- true
        psi.CreateNoWindow <- true
        let p = Process.Start(psi)
        p.WaitForExit()
        p.ExitCode = 0

    let stop hc =
        let psi = ProcessStartInfo(ormonitFileName, "stop")
        psi.UseShellExecute <- false
        psi.RedirectStandardOutput <- true
        psi.RedirectStandardError <- true
        psi.CreateNoWindow <- true
        let p = Process.Start(psi)
        p.WaitForExit()
        p.ExitCode = 0

    Service.Default
    |> with_start start
    |> with_recovery (ServiceRecovery.Default |> restart (min 10))
    |> with_stop stop
    |> run
