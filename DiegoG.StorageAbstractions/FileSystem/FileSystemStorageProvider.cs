using DiegoG.StorageAbstractions;

namespace DiegoG.StorageAbstractions.FileSystem;

public class FileSystemStorageProvider : IStorageProvider
{
    public const string ProviderName = "Operating System File System Storage Provider";

    public string Provider => ProviderName;

    public string? Root { get; }

    private readonly bool disposeRoot;

    public string PreparePath(string path)
    {
        var fp = Path.Combine(Root ?? "", path);
        var d = Path.GetDirectoryName(fp);
        Directory.CreateDirectory(d!);
        return fp;
    }

    public FileSystemStorageProvider(string? root, bool deleteRootOnDispose = false)
    {
        Root = root;
        disposeRoot = deleteRootOnDispose;
    }

    public void WriteData(string path, FileMode mode, ReadOnlySpan<byte> data)
    {
        using var file = File.Open(PreparePath(path), mode, FileAccess.Write, FileShare.ReadWrite);
        file.Write(data);
    }

    public void WriteData(string path, FileMode mode, IEnumerable<byte> data)
    {
        using var file = File.Open(PreparePath(path), mode, FileAccess.Write, FileShare.ReadWrite);
        foreach (var b in data)
            file.WriteByte(b);
    }

    public void WriteData(string path, FileMode mode, Stream data)
    {
        using var file = File.Open(PreparePath(path), mode, FileAccess.Write, FileShare.ReadWrite);
        data.CopyTo(file);
    }

    public Task WriteDataAsync(string path, FileMode mode, IEnumerable<byte> data, CancellationToken ct = default)
        => Task.Run(() =>
        {
            using var file = File.Open(PreparePath(path), mode, FileAccess.Write, FileShare.ReadWrite);
            foreach (var b in data)
                file.WriteByte(b);
        }, ct);

    public async Task WriteDataAsync(string path, FileMode mode, Stream data, CancellationToken ct = default)
    {
        using var file = File.Open(PreparePath(path), mode, FileAccess.Write, FileShare.ReadWrite);
        await data.CopyToAsync(file, ct);
    }

    public async Task WriteDataAsync(string path, FileMode mode, byte[] data, CancellationToken ct = default)
    {
        using var file = File.Open(PreparePath(path), mode, FileAccess.Write, FileShare.ReadWrite);
        await file.WriteAsync(data, ct);
    }

    public Stream GetReadStream(string path)
        => File.Open(PreparePath(path), FileMode.Open, FileAccess.Read, FileShare.ReadWrite);

    public ValueTask<Stream> GetReadStreamAsync(string path, CancellationToken ct = default)
        => ValueTask.FromResult<Stream>(File.Open(PreparePath(path), FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

    public Stream GetWriteStream(string path, FileMode mode)
        => File.Open(PreparePath(path), mode, FileAccess.Write, FileShare.ReadWrite);

    public ValueTask<Stream> GetWriteStreamAsync(string path, FileMode mode, CancellationToken ct = default)
        => ValueTask.FromResult<Stream>(File.Open(PreparePath(path), mode, FileAccess.Write, FileShare.ReadWrite));

    public Stream GetReadWriteStream(string path, FileMode mode)
        => File.Open(PreparePath(path), mode, FileAccess.ReadWrite, FileShare.ReadWrite);

    public ValueTask<Stream> GetReadWriteStreamAsync(string path, FileMode mode, CancellationToken ct = default)
        => ValueTask.FromResult<Stream>(File.Open(PreparePath(path), mode, FileAccess.ReadWrite, FileShare.ReadWrite));

    public bool DeleteDirectory(string path, bool recursive = false)
    {
        var p = PreparePath(path);
        if (Directory.Exists(p))
        {
            Directory.Delete(p, recursive);
            return true;
        }
        return false;
    }

    public Task<bool> DeleteDirectoryAsync(string path, bool recursive = false, CancellationToken ct = default)
        => Task.Run(() => DeleteDirectory(PreparePath(PreparePath(path)), recursive), ct);

    public bool CreateDirectory(string path)
    {
        var p = PreparePath(path);
        if (Directory.Exists(p) is false)
        {
            Directory.CreateDirectory(p);
            return true;
        }
        return false;
    }

    public Task<bool> CreateDirectoryAsync(string path, CancellationToken ct)
        => Task.Run(() => CreateDirectory(PreparePath(path)), ct);

    public bool DirectoryExists(string path)
        => Directory.Exists(PreparePath(path));

    public Task<bool> DirectoryExistsAsync(string path, CancellationToken ct)
        => Task.Run(() => DirectoryExists(PreparePath(path)), ct);

    public string GetDirectoryName(string path)
    {
        ArgumentNullException.ThrowIfNull(path);
        return Path.GetDirectoryName(PreparePath(path)) ?? "";
    }

    public Task<string> GetDirectoryNameAsync(string path, CancellationToken ct)
        => Task.Run(() => GetDirectoryName(PreparePath(path)), ct);

    public void MoveFile(string path, string newPath, bool overwrite)
    {
        File.Move(PreparePath(path), PreparePath(newPath), overwrite);
    }

    public Task MoveFileAsync(string path, string newPath, bool overwrite, CancellationToken ct)
    {
        MoveFile(PreparePath(path), PreparePath(newPath), overwrite);
        return Task.CompletedTask;
    }

    public bool DeleteFile(string path)
    {
        ArgumentNullException.ThrowIfNull(path);
        var p = PreparePath(path);
        if (File.Exists(p) is false) return false;
        File.Delete(p);
        return true;
    }

    public Task<bool> DeleteFileAsync(string path, CancellationToken ct)
        => Task.Run(() => DeleteFile(PreparePath(path)), ct);

    public bool FileExists(string path)
        => File.Exists(PreparePath(path));

    public Task<bool> FileExistsAsync(string path, CancellationToken ct)
        => Task.Run(() => File.Exists(PreparePath(path)), ct);

    public void Dispose()
    {
        if (disposeRoot && Root is not null)
            Directory.Delete(Root, true);
        GC.SuppressFinalize(this);
    }

    public void CopyFile(string path, string newPath, bool overwrite = false)
    {
        File.Copy(PreparePath(path), PreparePath(newPath), overwrite);
    }

    public Task CopyFileAsync(string path, string newPath, bool overwrite = false, CancellationToken ct = default)
        => Task.Run(() => File.Copy(PreparePath(path), PreparePath(newPath), overwrite), ct);

    public IEnumerable<string> ListFiles(string? path)
        => Directory.EnumerateFiles(PreparePath(path ?? ""));

    public async IAsyncEnumerable<string> ListFilesAsync(string? path)
    {
        foreach (var x in Directory.EnumerateFiles(PreparePath(path ?? "")))
            yield return x;
    }

    public IEnumerable<string> ListDirectories(string? path)
        => Directory.EnumerateDirectories(PreparePath(path ?? ""));

    public async IAsyncEnumerable<string> ListDirectoriesAsync(string? path)
    {
        foreach (var x in Directory.EnumerateDirectories(PreparePath(path ?? "")))
            yield return x;
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
