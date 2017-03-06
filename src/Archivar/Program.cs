using Archivar.Logging;
using Ormonit.Hosting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Archivar
{
    class Program
    {
        static ILog log = LogProvider.GetLogger("Archivar");

        static void Main(string[] args)
        {
            var host = new ServiceHost();
            host.AddService<ArchiveService>();

            host.Run();
        }

    }
}
