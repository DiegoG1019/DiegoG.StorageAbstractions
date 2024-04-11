using System.Buffers;
using System.Collections.Concurrent;
using System.IO;
using System.Security.Permissions;
using System.Text.RegularExpressions;
using Google.Apis.Drive.v3;
using Google.Apis.Drive.v3.Data;
using MailKit;
using Microsoft.IO;
using WarlockCode.Common.Business.Buffers;
using GFile = Google.Apis.Drive.v3.Data.File;

namespace DiegoG.StorageAbstractions.GoogleDrive;
public class GoogleDriveProvider : IStorageProvider
{
    private const string FolderType = "application/vnd.google-apps.folder";
    private readonly ConcurrentDictionary<string, string> FolderIdCache = new();
    private readonly static Regex DirectorySplitRegex = new(@"[/|\\]", RegexOptions.Compiled);

    public string Provider => "GoogleDriveV3";
    public string? Root { get; }

    private readonly DriveService Drive;
    private readonly static RecyclableMemoryStreamManager MemoryStreamManager
        = new(
              71_680, // 71 KB, about 14 KB from being allocated into the LOH
              104_857_600 // 100 MB for large files, anything larger than that should be handled differently. In fact, it should be handled differently way before this point
          );
    private static RecyclableMemoryStream GetStream()
        => new(MemoryStreamManager, Guid.NewGuid());

    public GoogleDriveProvider(string? root, string apiKey)
    {
        if (apiKey is null) throw new ArgumentNullException(nameof(apiKey));
        if (string.IsNullOrWhiteSpace(apiKey)) throw new ArgumentException("apiKey cannot be empty or just be whitespace", nameof(apiKey));
        Root = string.IsNullOrWhiteSpace(root) ? Path.Combine("WardianDesktop") : root;

        Drive = new(new Google.Apis.Services.BaseClientService.Initializer()
        {
            ApplicationName = "WardianDesktop.SynchronizationService",
            ApiKey = apiKey
        });
    }

    public void WriteData(string path, FileMode mode, ReadOnlySpan<byte> data)
    {
        using var mem = GetStream();
        mem.Write(data);
        WriteData(path, mode, mem);
    }

    public void WriteData(string path, FileMode mode, IEnumerable<byte> data)
    {
        var stream = new IEnumerableStream(data);
        WriteData(path, mode, stream);
    }

    public void WriteData(string path, FileMode mode, Stream data)
    {
        FilesResource.ListRequest listr;
        GFile? file = null;
        switch (mode)
        {
            case FileMode.CreateNew:
                listr = Drive.Files.List();
                listr.Q = $"name='{path}'";
                if (listr.Execute().Files.Any(x => x.Name == path))
                    throw new InvalidOperationException("The file already exists on Google Drive and cannot be created new");
                break;
            case FileMode.Truncate:
            case FileMode.Create:
                listr = Drive.Files.List();
                listr.Q = $"name='{path}'";
                if (listr.Execute().Files.FirstOrDefault(x => x.Name == path) is GFile cf)
                    Drive.Files.Delete(cf.Id).Execute();
                break;
            case FileMode.Append:
                listr = Drive.Files.List();
                listr.Q = $"name='{path}'";
                if (listr.Execute().Files.FirstOrDefault(x => x.Name == path) is GFile af)
                {
                    file = af;
                    var mem = new MemoryStream();
                    Drive.Files.Get(af.Id).Download(mem);

                    data = new ConcatenatedStream(mem, data);
                    Drive.Files.Delete(af.Id).Execute();
                }
                break;

            case FileMode.Open:
            case FileMode.OpenOrCreate:
                throw new ArgumentException($"Cannot use {mode} for write operations", nameof(mode));
            default:
                throw new ArgumentException($"Unknown FileMode {mode}", nameof(mode));
        }

        file ??= new GFile()
        {
            Name = PreparePath(path)
        };

        var filereq = Drive.Files.Create(
            file,
            data,
            ""
        );

        filereq.Upload();
    }

    public async Task WriteDataAsync(string path, FileMode mode, Stream data, CancellationToken ct = default)
    {
        FilesResource.ListRequest listr;
        GFile? file = null;
        switch (mode)
        {
            case FileMode.CreateNew:
                listr = Drive.Files.List();
                listr.Q = $"name='{path}'";
                if ((await listr.ExecuteAsync(ct)).Files.Any(x => x.Name == path))
                    throw new InvalidOperationException("The file already exists on Google Drive and cannot be created new");
                break;
            case FileMode.Truncate:
            case FileMode.Create:
                listr = Drive.Files.List();
                listr.Q = $"name='{path}'";
                if ((await listr.ExecuteAsync(ct)).Files.FirstOrDefault(x => x.Name == path) is GFile cf)
                    await Drive.Files.Delete(cf.Id).ExecuteAsync(ct);
                break;
            case FileMode.Append:
                listr = Drive.Files.List();
                listr.Q = $"name='{path}'";
                if ((await listr.ExecuteAsync(ct)).Files.FirstOrDefault(x => x.Name == path) is GFile af)
                {
                    file = af;
                    var mem = new MemoryStream();
                    await Drive.Files.Get(af.Id).DownloadAsync(mem, ct);

                    data = new ConcatenatedStream(mem, data);
                    await Drive.Files.Delete(af.Id).ExecuteAsync(ct);
                }
                break;

            case FileMode.Open:
            case FileMode.OpenOrCreate:
                throw new ArgumentException($"Cannot use {mode} for write operations", nameof(mode));
            default:
                throw new ArgumentException($"Unknown FileMode {mode}", nameof(mode));
        }

        file ??= new GFile()
        {
            Name = PreparePath(path)
        };

        var filereq = Drive.Files.Create(
            file,
            data,
            ""
        );

        await filereq.UploadAsync(ct);
    }

    public Task WriteDataAsync(string path, FileMode mode, IEnumerable<byte> data, CancellationToken ct = default)
    {
        var stream = new IEnumerableStream(data);
        return WriteDataAsync(path, mode, stream, ct);
    }

    public Task WriteDataAsync(string path, FileMode mode, byte[] data, CancellationToken ct = default)
    {
        return WriteDataAsync(path, mode, new MemoryStream(data), ct);
    }

    public Stream GetReadStream(string path)
    {
        var mem = GetStream();
        var f = GetFiles($"name='{path}'").FirstOrDefault() ?? throw new FileNotFoundException($"Could not find the file in the client's Google Drive");
        Drive.Files.Get(f.Id).Download(mem);
        return new Stream(mem, true);
    }

    public async ValueTask<Stream> GetReadStreamAsync(string path, CancellationToken ct = default)
    {
        var mem = GetStream();
        var f = GetFiles($"name='{path}'").FirstOrDefault() ?? throw new FileNotFoundException($"Could not find the file in the client's Google Drive");
        await Drive.Files.Get(f.Id).DownloadAsync(mem, ct);
        return new Stream(mem, true);
    }

    public Stream GetWriteStream(string path, FileMode mode)
    {
#warning Buffer it and then upload
        throw new NotSupportedException("Obtaining Write Streams is not supported by GoogleDriveProvider");
    }

    public ValueTask<Stream> GetWriteStreamAsync(string path, FileMode mode, CancellationToken ct = default)
    {
        throw new NotSupportedException("Obtaining Write Streams is not supported by GoogleDriveProvider");
    }

    public Stream GetReadWriteStream(string path, FileMode mode)
    {
#warning Buffer it, stage changes and upload
        throw new NotSupportedException("Obtaining Write Streams is not supported by GoogleDriveProvider");
    }

    public ValueTask<Stream> GetReadWriteStreamAsync(string path, FileMode mode, CancellationToken ct = default)
    {
        throw new NotSupportedException("Obtaining Streams is not supported by GoogleDriveProvider");
    }

    public bool DeleteDirectory(string path, bool recursive = false)
    {
    }

    public ValueTask<bool> DeleteDirectoryAsync(string path, bool recursive = false)
    {
    }

    public bool CreateDirectory(string path)
    {
        var components = DirectorySplitRegex.Split(path);
        bool created = false;
        string? previousId = null;
        foreach (var folder in components)
        {
            var fq = Drive.Files.List();
            fq.Q = previousId is not null ? $"name='{folder}' '{previousId}' in parents" : $"name='{folder}'";
            previousId = fq.Execute().Files.FirstOrDefault(x => x.MimeType == FolderType)?.Id;
            if (previousId is null)
            {
                var file = Drive.Files.Create(new GFile()
                {
                    Name = folder,
                    MimeType = FolderType
                }).Execute();
                previousId = file.Id;
            }
        }
        return created;
    }

    public ValueTask<bool> CreateDirectoryAsync(string path)
    {

    }

    public bool DirectoryExists(string path)
    {
    }

    public ValueTask<bool> DirectoryExistsAsync(string path)
    {
    }

    public ValueTask<string> GetDirectoryNameAsync(string path)
    {
        throw new NotImplementedException();
    }

    public void MoveFile(string path, string newPath)
    {
        throw new NotImplementedException();
    }

    public ValueTask MoveFileAsync(string path, string newPath)
    {
        throw new NotImplementedException();
    }

    public bool DeleteFile(string path)
    {
        throw new NotImplementedException();
    }

    public ValueTask<bool> DeleteFileAsync(string path)
    {
        throw new NotImplementedException();
    }

    public bool FileExists(string path)
    {
        throw new NotImplementedException();
    }

    public ValueTask<bool> FileExistsAsync(string path)
    {
        throw new NotImplementedException();
    }

    public string PreparePath(string path)
    {
        throw new NotImplementedException();
    }

    private async Task<IList<GFile>> GetFilesAsync(string q, CancellationToken ct)
    {
        var fq = Drive.Files.List();
        fq.Q = q;
        return (await fq.ExecuteAsync(ct)).Files;
    }

    private IList<GFile> GetFiles(string q)
    {
        var fq = Drive.Files.List();
        fq.Q = q;
        return fq.Execute().Files;
    }

    private async Task<string?> GetFolderIdAsync(string path, CancellationToken ct)
    {
        var components = DirectorySplitRegex.Split(path);
        string? previous = null;
        foreach (var folder in components)
        {
            var fq = Drive.Files.List();
            fq.Q = previous is not null ? $"name='{folder}' '{previous}' in parents" : $"name='{folder}'";
            previous = (await fq.ExecuteAsync(ct)).Files.FirstOrDefault(x => x.MimeType == FolderType)?.Id;
            if (previous is null)
                return null;
        }
        return previous;
    }

    private string? GetFolderId(string path)
    {
        var components = DirectorySplitRegex.Split(path);
        string? previous = null;
        foreach (var folder in components)
        {
            var fq = Drive.Files.List();
            fq.Q = previous is not null ? $"name='{folder}' '{previous}' in parents" : $"name='{folder}'";
            previous = fq.Execute().Files.FirstOrDefault(x => x.MimeType == FolderType)?.Id;
            if (previous is null)
                return null;
        }
        return previous;
    }
}
