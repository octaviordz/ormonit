using Archivar.Logging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Archivar
{
    class ArchiveService
    {
        static ILog log = LogProvider.GetLogger("Archivar");

        public ArchiveService(IDictionary<string, string> config)
        {
            //let ctrlloop(lid:int) (ca:string) =
            //    sprintf "In control loop with: controlAddress \"%s\", logicId \"%d\"." ca lid |> log.Trace
            //    let s = NN.Socket(Domain.SP, Protocol.RESPONDENT)
            //    //TODO:error handling for socket and connect
            //    assert(s >= 0)
            //    assert(NN.Connect(s, ca) >= 0)
            //    let rec recvloop() =
            //        //let mutable buff : byte[] = null
            //        //buff is initialized with '\000'
            //        let buff : byte array = Array.zeroCreate 256
            //        log.Info("Waiting for note.")
            //        let rc = NN.Recv(s, buff, SendRecvFlags.NONE)
            //        //TODO:Error handling NN.Errno ()
            //        assert(rc >= 0)
            //        let cmd = Encoding.UTF8.GetString(buff.[..rc - 1])
            //        sprintf "\"%s\" note received." cmd |> log.Trace
            //        match cmd with
            //        | "close" ->
            //            let bytes = Array.zeroCreate 8
            //            Buffer.BlockCopy(BitConverter.GetBytes(lid), 0, bytes, 0, 4)
            //            Buffer.BlockCopy(BitConverter.GetBytes(0), 0, bytes, 4, 4)
            //            log.Trace("""[{0}] Sending "close" aknowledgement.""", lid)
            //            let sr = NN.Send(s, bytes, SendRecvFlags.NONE)
            //            assert(sr > 0)
            //            ()
            //        | "report-status" ->
            //            let bytes = Array.zeroCreate 8
            //            Buffer.BlockCopy(BitConverter.GetBytes(lid), 0, bytes, 0, 4)
            //            Buffer.BlockCopy(BitConverter.GetBytes(0), 0, bytes, 4, 4)
            //            log.Trace("""[{0}] Sending "report-status" aknowledgement.""", lid)
            //            let sr = NN.Send(s, bytes, SendRecvFlags.NONE)
            //            //TODO:Error handling NN.Errno ()
            //            assert(sr > 0)
            //            recvloop()
            //        | _ -> recvloop()
            //    recvloop()
        }
        public async Task Run(CancellationToken cancellationToken)
        {
            //var task = new Task<int>();
            using (var watcher = new FileSystemWatcher())
            {
                //watcher.WaitForChanged
                //    sprintf "Watcher path: \"%s\"." p |> log.Info
                watcher.Path = "";
                // Watch for changes in LastAccess and LastWrite times, and
                // the renaming of files or directories.
                watcher.NotifyFilter = NotifyFilters.LastAccess |
                    NotifyFilters.LastWrite |
                    NotifyFilters.FileName;
                var tcs = new TaskCompletionSource<bool>();
                tcs.TrySetResult(true);
                //watcher.Filter = ""
                watcher.Changed += WatcherOnChanged;
                watcher.Created += WatcherOnChanged;
                //log.Trace("Begin watching.")
                watcher.EnableRaisingEvents = true;
                //log.Info("Enter control loop.")
                //    ctrlloop lid ca
                //    log.Info("Exit control loop.")
                //    watcher.Dispose()
            }

            //    let ca = config.["controlAddress"]
            //    let p, lid = Int32.TryParse config.["logicId"]
            //    if not p then
            //        raise(System.ArgumentException("logicId"))
            //    //sprintf "Start with configuration %A" config |> log.Trace
            //    //%APPDATA%\Roaming\hamster-applet
            //    let app_data = Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData)
            //    let p = Path.Combine(app_data, "hamster-applet")
            //    use watcher = new FileSystemWatcher()
            //    sprintf "Watcher path: \"%s\"." p |> log.Info
            //    watcher.Path<- p
            //    // Watch for changes in LastAccess and LastWrite times, and
            //    // the renaming of files or directories.
            //    watcher.NotifyFilter<- NotifyFilters.LastAccess
            //        ||| NotifyFilters.LastWrite
            //        ||| NotifyFilters.FileName
            //    watcher.Filter<- "*.db"
            //    watcher.Changed.Add(on_hamster_db_change)
            //    watcher.Created.Add(on_hamster_db_change)
            //    //log.Trace("Begin watching.")
            //    watcher.EnableRaisingEvents<- true
            //    //log.Info("Enter control loop.")
            //    ctrlloop lid ca
            //    log.Info("Exit control loop.")
            //    watcher.Dispose()
        }

        private void WatcherOnChanged(object sender, FileSystemEventArgs e)
        {

            //    sprintf "\"%s\" event for %s" (e.ChangeType.ToString()) e.FullPath |> log.Info
            //    if e.Name<> "hamster.db" then()
            //    
            var destinationFullPath = "";
            Func.CopyFile(e.FullPath, destinationFullPath);
        //    let s = FileInfo(e.FullPath)
        //    let d = FileInfo(destinationDbPath)
        //    if d.LastWriteTimeUtc > s.LastWriteTimeUtc then
        //        sprintf "Destination db \"%s\" has a more recent LastWriteTimeUtc than source. \"%s\" > \"%s\"" destinationDbPath(d.LastWriteTimeUtc.ToString("o")) (s.LastWriteTimeUtc.ToString("o"))
        //        |> log.Warn
        //        File.Copy(destinationDbPath, backupDbPath, true)
        //    File.Copy(e.FullPath, destinationDbPath, true)
        //    sprintf "Source \"%s\" copied to destination \"%s\"." e.FullPath destinationDbPath |> log.Trace
        //    ()
        }
    }

    //let ctrlloop(lid:int) (ca:string) =
    //    sprintf "In control loop with: controlAddress \"%s\", logicId \"%d\"." ca lid |> log.Trace
    //    let s = NN.Socket(Domain.SP, Protocol.RESPONDENT)
    //    //TODO:error handling for socket and connect
    //    assert(s >= 0)
    //    assert(NN.Connect(s, ca) >= 0)
    //    let rec recvloop() =
    //        //let mutable buff : byte[] = null
    //        //buff is initialized with '\000'
    //        let buff : byte array = Array.zeroCreate 256
    //        log.Info("Waiting for note.")
    //        let rc = NN.Recv(s, buff, SendRecvFlags.NONE)
    //        //TODO:Error handling NN.Errno ()
    //        assert(rc >= 0)
    //        let cmd = Encoding.UTF8.GetString(buff.[..rc - 1])
    //        sprintf "\"%s\" note received." cmd |> log.Trace
    //        match cmd with
    //        | "close" ->
    //            let bytes = Array.zeroCreate 8
    //            Buffer.BlockCopy(BitConverter.GetBytes(lid), 0, bytes, 0, 4)
    //            Buffer.BlockCopy(BitConverter.GetBytes(0), 0, bytes, 4, 4)
    //            log.Trace("""[{0}] Sending "close" aknowledgement.""", lid)
    //            let sr = NN.Send(s, bytes, SendRecvFlags.NONE)
    //            assert(sr > 0)
    //            ()
    //        | "report-status" ->
    //            let bytes = Array.zeroCreate 8
    //            Buffer.BlockCopy(BitConverter.GetBytes(lid), 0, bytes, 0, 4)
    //            Buffer.BlockCopy(BitConverter.GetBytes(0), 0, bytes, 4, 4)
    //            log.Trace("""[{0}] Sending "report-status" aknowledgement.""", lid)
    //            let sr = NN.Send(s, bytes, SendRecvFlags.NONE)
    //            //TODO:Error handling NN.Errno ()
    //            assert(sr > 0)
    //            recvloop()
    //        | _ -> recvloop()
    //    recvloop()

    //let Start(config:IDictionary<string, string>) =
    //    let ca = config.["controlAddress"]
    //    let p, lid = Int32.TryParse config.["logicId"]
    //    if not p then
    //        raise(System.ArgumentException("logicId"))
    //    //sprintf "Start with configuration %A" config |> log.Trace
    //    //%APPDATA%\Roaming\hamster-applet
    //    let app_data = Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData)
    //    let p = Path.Combine(app_data, "hamster-applet")
    //    use watcher = new FileSystemWatcher()
    //    sprintf "Watcher path: \"%s\"." p |> log.Info
    //    watcher.Path<- p
    //    // Watch for changes in LastAccess and LastWrite times, and
    //    // the renaming of files or directories.
    //    watcher.NotifyFilter<- NotifyFilters.LastAccess
    //        ||| NotifyFilters.LastWrite
    //        ||| NotifyFilters.FileName
    //    watcher.Filter<- "*.db"
    //    watcher.Changed.Add(on_hamster_db_change)
    //    watcher.Created.Add(on_hamster_db_change)
    //    //log.Trace("Begin watching.")
    //    watcher.EnableRaisingEvents<- true
    //    //log.Info("Enter control loop.")
    //    ctrlloop lid ca
    //    log.Info("Exit control loop.")
    //    watcher.Dispose()

}