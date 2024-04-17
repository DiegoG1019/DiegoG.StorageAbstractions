using System.Diagnostics.CodeAnalysis;

namespace DiegoG.StorageAbstractions;

public interface IStorageProvider : IDisposable, IAsyncDisposable
{
    public string Provider { get; }
    public string? Root { get; }
    public void WriteData(string path, FileMode mode, ReadOnlySpan<byte> data);
    public void WriteData(string path, FileMode mode, IEnumerable<byte> data);
    public void WriteData(string path, FileMode mode, Stream data);
    public Task WriteDataAsync(string path, FileMode mode, IEnumerable<byte> data, CancellationToken ct = default);
    public Task WriteDataAsync(string path, FileMode mode, Stream data, CancellationToken ct = default);
    public Task WriteDataAsync(string path, FileMode mode, byte[] data, CancellationToken ct = default);
    public Stream GetReadStream(string path);
    public ValueTask<Stream> GetReadStreamAsync(string path, CancellationToken ct = default);
    public Stream GetWriteStream(string path, FileMode mode);
    public ValueTask<Stream> GetWriteStreamAsync(string path, FileMode mode, CancellationToken ct = default);

    [return: NotNullIfNotNull(nameof(path))]
    public string? PreparePath(string? path);

    public bool DeleteDirectory(string path, bool recursive = false);
    public Task<bool> DeleteDirectoryAsync(string path, bool recursive = false, CancellationToken ct = default);

    public bool CreateDirectory(string path);
    public Task<bool> CreateDirectoryAsync(string path, CancellationToken ct = default);

    public bool DirectoryExists(string path);
    public Task<bool> DirectoryExistsAsync(string path, CancellationToken ct = default);

    public void MoveFile(string path, string newPath, bool overwrite = false);
    public Task MoveFileAsync(string path, string newPath, bool overwrite = false, CancellationToken ct = default);

    public void CopyFile(string path, string newPath, bool overwrite = false);
    public Task CopyFileAsync(string path, string newPath, bool overwrite = false, CancellationToken ct = default);

    public bool DeleteFile(string path);
    public Task<bool> DeleteFileAsync(string path, CancellationToken ct = default);

    public bool FileExists(string path);
    public Task<bool> FileExistsAsync(string path, CancellationToken ct = default);

    public IEnumerable<string> ListFiles(string path);
    public IAsyncEnumerable<string> ListFilesAsync(string path);

    public IEnumerable<string> ListDirectories(string path);
    public IAsyncEnumerable<string> ListDirectoriesAsync(string path);
}
