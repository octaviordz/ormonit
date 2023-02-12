﻿using Archivar.Logging;
using Ormonit.Hosting;
using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Archivar
{
    class ArchiveService
    {
        static ILog log = LogProvider.GetLogger($"{nameof(Archivar)}.{nameof(ArchiveService)}");
        static string appData = Environment.GetFolderPath(
                Environment.SpecialFolder.ApplicationData);

        public ArchiveService()
        {
        }

        public Task RunAsync(ReportStatusToken reportStatusToken, CancellationToken cancellationToken)
        {
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
                }
                while (!cancellationToken.IsCancellationRequested)
                {
                    reportStatusToken.Report();
                }
                watcher.Changed -= WatcherOnChanged;
                watcher.Created -= WatcherOnChanged;
            });
            return result;
        }

        private void WatcherOnChanged(object sender, FileSystemEventArgs e)
        {
            //var destinationFullPath = ;
            //Funs.CopyFile(e.FullPath, destinationFullPath);
        }
    }
}