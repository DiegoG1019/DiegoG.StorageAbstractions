using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DiegoG.StorageAbstractions.Buffers;
using FluentFTP;
using NeoSmart.AsyncLock;

namespace DiegoG.StorageAbstractions.FTP;

// *** Asynchronous *** method implementations for FTPStorageProvider
public partial class FTPStorageProvider
{
    private readonly AsyncLock asyncClientLock = new();
    private AsyncFtpClient? asyncClient;
    private bool asyncClientPreviouslyConnected;
    private async Task<AsyncFtpClient> CreateAndConnectAsync(CancellationToken ct)
    {
        lock (asyncClientLock)
        {
            if (asyncClient is null || asyncClient.IsDisposed)
            {
                asyncClient = asyncFactory(this);
                asyncClientPreviouslyConnected = false;
            }
        }

        using (await ftpprofilelock.LockAsync(ct))
            ftpprofile 
                ??= await asyncClient.AutoConnect(ct) 
                ?? throw new IOException("Could not find a valid profile to connect to the server with; this could mean the server does not exist");

        using (await asyncClientLock.LockAsync(ct))
        {
            if (asyncClient.IsConnected is false)
                if (asyncClientPreviouslyConnected)
                    await asyncClient.Connect(true, ct);
                else
                    await asyncClient.Connect(ftpprofile, ct);
            asyncClientPreviouslyConnected = true;
        }

        return asyncClient;
    }

    private async Task<Stream> InternalOpenReadStreamAsync(AsyncFtpClient ftp, string path, FileMode mode, CancellationToken ct = default)
    {
        path = PreparePath(path);
        string dn = Path.GetDirectoryName(path)!;
        await ftp.CreateDirectory(dn, ct);

        switch (mode)
        {
            case FileMode.CreateNew:
                if (await ftp.FileExists(path, ct))
                    throw new IOException($"Cannot create a new file '{path}' when it already exists");
                goto case FileMode.OpenOrCreate;

            case FileMode.Create:
                if (await ftp.FileExists(path, ct))
                    await ftp.DeleteFile(path, ct);
                goto case FileMode.OpenOrCreate;

            case FileMode.Open:
                if (await ftp.FileExists(path, ct) is false)
                    throw new FileNotFoundException($"Could not find the file {path}");
                goto case FileMode.OpenOrCreate;

            case FileMode.OpenOrCreate:
                return await ftp.OpenRead(path, FtpDataType.Binary, 0, true, ct);

            case FileMode.Truncate:
            case FileMode.Append:
                throw new ArgumentException($"Cannot apply FileMode {mode} when attempting to read from a file");

            default:
                throw new ArgumentException($"Unknown FileMode {mode}", nameof(mode));
        }
    }

    private async Task<Stream> InternalOpenWriteStreamAsync(AsyncFtpClient ftp, string path, FileMode mode, CancellationToken ct = default)
    {
        path = PreparePath(path);
        string dn = Path.GetDirectoryName(path)!;
        await ftp.CreateDirectory(dn, ct);

        Stream stream;

        switch (mode)
        {
            case FileMode.CreateNew:
                if (await ftp.FileExists(path, ct))
                    throw new IOException($"Cannot create a new file '{path}' when it already exists");
                goto case FileMode.Create;

            case FileMode.OpenOrCreate:
            case FileMode.Create:
                stream = await ftp.OpenWrite(path, FtpDataType.Binary, true, ct);
                return stream;

            case FileMode.Truncate:
                if (await ftp.FileExists(path, ct) is false)
                    throw new FileNotFoundException(path);
                stream = await ftp.OpenWrite(path, FtpDataType.Binary, true, ct);
                stream.SetLength(0);
                return stream;

            case FileMode.Open:
            case FileMode.Append:
                if (await ftp.FileExists(path, ct) is false)
                    throw new FileNotFoundException(path);
                goto case FileMode.Create;

            default:
                throw new ArgumentException($"Unknown FileMode {mode}", nameof(mode));
        }
    }

    private static async Task ThrowIfNotSuccessAsync(AsyncFtpClient ftp, CancellationToken ct = default)
    {
        var response = await ftp.GetReply(ct);
        if (response.Type is not FtpResponseType.PositiveCompletion)
            throw new IOException($"The FTP server did not respond with a Positive Completion message: {response.Type}; Code: {response.Code}; Message: {response.Message}; Error Message: {response.ErrorMessage}");
    }

    public async Task WriteDataAsync(string path, FileMode mode, byte[] data, CancellationToken ct = default)
    {
        var ftp = await CreateAndConnectAsync(ct);
        using (var stream = await InternalOpenWriteStreamAsync(ftp, path, mode, ct))
            stream.Write(data);

        await ThrowIfNotSuccessAsync(ftp, ct);
    }

    public async Task WriteDataAsync(string path, FileMode mode, IEnumerable<byte> data, CancellationToken ct = default)
    {
        var ftp = await CreateAndConnectAsync(ct);
        using (var stream = await InternalOpenWriteStreamAsync(ftp, path, mode, ct))
        {
            var buffer = ArrayPool<byte>.Shared.Rent(TransferChunkSize);

            try
            {
                int i = 0;
                foreach (var b in data)
                {
                    while (i < buffer.Length)
                        buffer[i++] = b;
                    await stream.WriteAsync(buffer.AsMemory()[..i], ct);
                    i = 0;
                }
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer);
            }
        }

        await ThrowIfNotSuccessAsync(ftp, ct);
    }

    public async Task WriteDataAsync(string path, FileMode mode, Stream data, CancellationToken ct = default)
    {
        var ftp = await CreateAndConnectAsync(ct);
        using (var stream = await InternalOpenWriteStreamAsync(ftp, path, mode, ct))
            data.CopyTo(stream);

        await ThrowIfNotSuccessAsync(ftp, ct);
    }

    public async ValueTask<Stream> GetReadStreamAsync(string path, CancellationToken ct = default)
    {
        var ftp = await CreateAndConnectAsync(ct);
        var stream = await InternalOpenReadStreamAsync(ftp, path, FileMode.OpenOrCreate, ct);
        return new DependantStream(stream, ftp);
    }

    public async ValueTask<Stream> GetWriteStreamAsync(string path, FileMode mode, CancellationToken ct = default)
    {
        var ftp = await CreateAndConnectAsync(ct);
        var stream = await InternalOpenWriteStreamAsync(ftp, path, mode, ct);
        return new DependantStream(stream, ftp);
    }

    public async Task<bool> DeleteDirectoryAsync(string path, bool recursive = false, CancellationToken ct = default)
    {
        var ftp = await CreateAndConnectAsync(ct);
        path = PreparePath(path);
        await ftp.DeleteDirectory(path, recursive ? FtpListOption.Recursive : 0, ct);
        await ThrowIfNotSuccessAsync(ftp, ct);
        return true;
    }

    public async Task<bool> CreateDirectoryAsync(string path, CancellationToken ct = default)
    {
        var ftp = await CreateAndConnectAsync(ct);
        path = PreparePath(path);
        return await ftp.CreateDirectory(path, ct);
    }

    public async Task<bool> DirectoryExistsAsync(string path, CancellationToken ct = default)
    {
        var ftp = await CreateAndConnectAsync(ct);
        path = PreparePath(path);
        return await ftp.DirectoryExists(path, ct);
    }

    public async Task CopyFileAsync(string path, string newPath, bool overwrite, CancellationToken ct = default)
    {
        var ftp = await CreateAndConnectAsync(ct);

        path = PreparePath(path);

        newPath = PreparePath(newPath);
        var dn = Path.GetDirectoryName(newPath)!;
        await ftp.CreateDirectory(dn, ct);

        if ((await ftp.FileExists(path, ct)) is false)
            throw new FileNotFoundException($"Could not find file '{path}' to copy");
        if (!overwrite && await ftp.FileExists(newPath, ct))
            throw new IOException($"There already exists a file named '{newPath}'");

        using (var read = await InternalOpenReadStreamAsync(ftp, path, FileMode.Open, ct))
        using (var write = await InternalOpenWriteStreamAsync(ftp, newPath, overwrite ? FileMode.OpenOrCreate : FileMode.CreateNew, ct))
        {
            write.SetLength(0);
            await read.CopyToAsync(write, ct);
        }

        await ThrowIfNotSuccessAsync(ftp, ct);
    }

    public async Task MoveFileAsync(string path, string newPath, bool overwrite, CancellationToken ct = default)
    {
        var ftp = await CreateAndConnectAsync(ct);

        path = PreparePath(path);

        newPath = PreparePath(newPath);
        var dn = Path.GetDirectoryName(newPath)!;
        await ftp.CreateDirectory(dn, ct);

        if (!overwrite && await ftp.FileExists(newPath, ct))
            throw new IOException($"There already exists a file named '{newPath}'");
        if (await ftp.MoveFile(path, newPath, FtpRemoteExists.Overwrite, ct) is false)
            throw new IOException($"Could not move the file '{path}' to '{newPath}'");
    }

    public async Task<bool> DeleteFileAsync(string path, CancellationToken ct = default)
    {
        var ftp = await CreateAndConnectAsync(ct);
        path = PreparePath(path);
        await ftp.DeleteFile(path, ct);
        return true;
    }

    public async Task<bool> FileExistsAsync(string path, CancellationToken ct = default)
    {
        var ftp = await CreateAndConnectAsync(ct);
        path = PreparePath(path);
        return await ftp.FileExists(path, ct);
    }
}
