using Archivar.Logging;
using Ormonit.Hosting;
using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Archivar
{
    class ArchiveService
    {
        static ILog log = LogProvider.GetLogger("Archivar");
        static string appData = Environment.GetFolderPath(
                Environment.SpecialFolder.ApplicationData);

        public ArchiveService()
        {
        }

        public Task RunAsync(ReportStatusToken reportStatusToken, CancellationToken cancellationToken)
        {
            //TODO: Build ReportStatusToken
            var cwait = cancellationToken.WaitHandle;
            //%APPDATA%\Roaming\hamster-applet
            var p = Path.Combine(appData, "hamster-applet");
            var taskf = new TaskFactory();
            var result = Task.Run(() =>
            {
                using (var watcher = new FileSystemWatcher())
                {
                    watcher.Path = p;
                    watcher.NotifyFilter = NotifyFilters.LastAccess |
                        NotifyFilters.LastWrite |
                        NotifyFilters.FileName;
                    //var tcs = new TaskCompletionSource<bool>();
                    //tcs.TrySetResult(true);
                    watcher.Filter = "*.db";
                    watcher.Changed += WatcherOnChanged;
                    watcher.Created += WatcherOnChanged;
                    log.Trace("Begin watching.");
                    watcher.EnableRaisingEvents = true;
                    cwait.WaitOne();
                    watcher.Changed -= WatcherOnChanged;
                    watcher.Created -= WatcherOnChanged;
                }
            });
            //reportStatusToken.Register();
            return result;
        }

        private void WatcherOnChanged(object sender, FileSystemEventArgs e)
        {
            //    sprintf "\"%s\" event for %s" (e.ChangeType.ToString()) e.FullPath |> log.Info
            //    if e.Name<> "hamster.db" then()
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
}