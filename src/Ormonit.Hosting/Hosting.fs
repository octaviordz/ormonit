namespace Ormonit.Hosting

open System
open System.Collections.Generic
open System.Threading

[<Struct>]
type ReportStatusToken =
    val actions: IList<Action>
    member s.Register(callback:Action): unit =
        s.actions.Add(callback)
    static member None = ReportStatusToken()

type ServiceHost() =
    let mutable ts = List.empty
    member s.Run(): unit =
        let (?) (t : Type) (mname : string) = t.GetMethod(mname)
        ts |> List.iter (fun t ->
            let paramArray:obj array = [||]
            let runAsync = t?RunAsync
            let srv = Activator.CreateInstance(t, paramArray)
            runAsync.Invoke (srv, [|ReportStatusToken.None, CancellationToken.None|])
            |> ignore )
        Console.ReadLine() |> ignore

    member s.AddService<'T> (): unit =
        ts <- typeof<'T> :: ts
        ()
