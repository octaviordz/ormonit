[<AutoOpen>]
module internal Infrastructure

open System
open System.IO
open NLog
open NLog.Layouts
open System.Reflection
open System.Collections.Generic
open System.Collections.Concurrent
open System.Diagnostics
open System.Text
open System.Threading
open NNanomsg

#if DNXCORE50
type AssemblyLoader(folderPath) =
    inherit System.Runtime.Loader.AssemblyLoadContext()
    let fpath = folderPath
    member override s.Load (assemblyName:AssemblyName) =
        let deps = DependencyContext.Default
        let res = deps.CompileLibraries.Where(d => d.Name.Contains(assemblyName.Name)).ToList()
        let apiApplicationFileInfo = FileInfo(sprintf "%s%s%s.dll" folderPath (Path.DirectorySeparatorChar.ToString()) assemblyName.Name)
        if res.Count > 0 then
            return Assembly.Load(new AssemblyName(res.First().Name));
        elif File.Exists(apiApplicationFileInfo.FullName) then
                var asl = new AssemblyLoader(apiApplicationFileInfo.DirectoryName)
                return asl.LoadFromAssemblyPath(apiApplicationFileInfo.FullName)
        else
            return Assembly.Load(assemblyName)
#else
type AssemblyLoader(folderPath) =
    let fpath = folderPath
    member s.LoadFromAssemblyPath (path) : Assembly =
        Assembly.LoadFrom(path)
#endif


type ProcessResult = { exitCode : int; stdout : string; stderr : string }

let ExecuteProcess (olog:NLog.Logger) exe cmdline =
    let psi = new ProcessStartInfo(exe, cmdline)
    psi.UseShellExecute <- false
    psi.RedirectStandardOutput <- true
    psi.RedirectStandardError <- true
    psi.CreateNoWindow <- true
    let p = Process.Start(psi)
    let output = StringBuilder()
    let error = StringBuilder()
    p.OutputDataReceived.Add(fun args ->
        olog.Trace(args.Data)
        output.Append(args.Data) |> ignore)
    p.ErrorDataReceived.Add(fun args ->
        olog.Error(args.Data)
        error.Append(args.Data) |> ignore)
    p.BeginErrorReadLine()
    p.BeginOutputReadLine()
    //p.WaitForExit()
    //{ exitCode = p.ExitCode; stdout = output.ToString(); stderr = error.ToString() }


