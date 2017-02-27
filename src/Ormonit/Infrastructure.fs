module internal Infrastructure

open System.Reflection

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
