using Archivar.Logging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Archivar
{
    static class Funs
    {
        static ILog log = LogProvider.GetLogger("Archivar");
        internal static void CopyFile(string source, string destination)
        {
            var s = new FileInfo(source);
            var d = new FileInfo(destination);
            if (d.LastWriteTimeUtc > s.LastWriteTimeUtc)
            {
                log.Warn($"Destination \"{destination}\" has a more recent LastWriteTimeUtc than source. \"{(d.LastWriteTimeUtc.ToString("o"))}\" > \"{(s.LastWriteTimeUtc.ToString("o"))}\"");
                File.Copy(destination, destination + ".backup", true);
            }
            File.Copy(source, destination, true);
            log.Trace($"Source \"{source}\" copied to destination \"{destination}\".");
        }
    }
}
