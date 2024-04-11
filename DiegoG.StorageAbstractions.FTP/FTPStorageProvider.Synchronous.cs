using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DiegoG.StorageAbstractions.Buffers;
using FluentFTP;

namespace DiegoG.StorageAbstractions.FTP;

// *** Synchronous *** method implementations for FTPStorageProvider
public partial class FTPStorageProvider
{
    private readonly object syncClientLock = new();
    private FtpClient? syncClient;
    private bool syncClientPreviouslyConnected;
    private FtpClient CreateAndConnect()
    {
        lock (syncClientLock)
        {
            if (syncClient is null || syncClient.IsDisposed)
            {
                syncClient = syncFactory(this);
                syncClientPreviouslyConnected = false;
            }
        }

        using (ftpprofilelock.Lock())
            ftpprofile ??= syncClient.AutoConnect() ?? throw new IOException("Could not find a valid profile to connect to the server with; this could mean the server does not exist");

        lock (syncClientLock)
        {
            if (syncClient.IsConnected is false)
                if (syncClientPreviouslyConnected)
                    syncClient.Connect(true);
                else
                    syncClient.Connect(ftpprofile);
            syncClientPreviouslyConnected = true;
        }

        return syncClient;
    }

    private Stream InternalOpenReadStream(FtpClient ftp, string path, FileMode mode)
    {
        path = PreparePath(path);
        string dn = Path.GetDirectoryName(path)!;
        ftp.CreateDirectory(dn);

        switch (mode)
        {
            case FileMode.CreateNew:
                if (ftp.FileExists(path))
                    throw new IOException($"Cannot create a new file '{path}' when it already exists");
                goto case FileMode.OpenOrCreate;

            case FileMode.Create:
                if (ftp.FileExists(path))
                    ftp.DeleteFile(path);
                goto case FileMode.OpenOrCreate;

            case FileMode.Open:
                if (ftp.FileExists(path) is false)
                    throw new FileNotFoundException($"Could not find the file {path}");
                goto case FileMode.OpenOrCreate;

            case FileMode.OpenOrCreate:
                return ftp.OpenRead(path, FtpDataType.Binary, 0);

            case FileMode.Truncate:
            case FileMode.Append:
                throw new ArgumentException($"Cannot apply FileMode {mode} when attempting to read from a file");

            default:
                throw new ArgumentException($"Unknown FileMode {mode}", nameof(mode));
        }
    }

    private Stream InternalOpenWriteStream(FtpClient ftp, string path, FileMode mode)
    {
        path = PreparePath(path);
        string dn = Path.GetDirectoryName(path)!;
        ftp.CreateDirectory(dn);

        Stream stream;

        switch (mode)
        {
            case FileMode.CreateNew:
                if (ftp.FileExists(path))
                    throw new IOException($"Cannot create a new file '{path}' when it already exists");
                goto case FileMode.Create;

            case FileMode.OpenOrCreate:
            case FileMode.Create:
                stream = ftp.OpenWrite(path, FtpDataType.Binary);
                return stream;

            case FileMode.Truncate:
                if (ftp.FileExists(path) is false)
                    throw new FileNotFoundException(path);
                stream = ftp.OpenWrite(path, FtpDataType.Binary);
                stream.SetLength(0);
                return stream;

            case FileMode.Open:
            case FileMode.Append:
                if (ftp.FileExists(path) is false)
                    throw new FileNotFoundException(path);
                goto case FileMode.Create;

            default:
                throw new ArgumentException($"Unknown FileMode {mode}", nameof(mode));
        }
    }

    private static void ThrowIfNotSuccess(FtpClient ftp)
    {
        var response = ftp.GetReply();
        if (response.Type is not FtpResponseType.PositiveCompletion)
            throw new IOException($"The FTP server did not respond with a Positive Completion message: {response.Type}; Code: {response.Code}; Message: {response.Message}; Error Message: {response.ErrorMessage}");
    }

    public void WriteData(string path, FileMode mode, ReadOnlySpan<byte> data)
    {
        var ftp = CreateAndConnect();
        using (var stream = InternalOpenWriteStream(ftp, path, mode))
            stream.Write(data);
        ThrowIfNotSuccess(ftp);
    }

    public void WriteData(string path, FileMode mode, IEnumerable<byte> data)
    {
        var ftp = CreateAndConnect();
        using (var stream = InternalOpenWriteStream(ftp, path, mode))
        {
            Span<byte> buffer = TransferChunkSize > 4096 ? new byte[TransferChunkSize] : stackalloc byte[TransferChunkSize];
            // Since TransferChunkSize is a `const`, this ternary expression gets optimized away into one of the two

            int i = 0;
            foreach (var b in data)
            {
                while (i < buffer.Length)
                    buffer[i++] = b;
                stream.Write(buffer[..i]);
                i = 0;
            }
        }

        ThrowIfNotSuccess(ftp);
    }

    public void WriteData(string path, FileMode mode, Stream data)
    {
        var ftp = CreateAndConnect();
        using (var stream = InternalOpenWriteStream(ftp, path, mode))
            data.CopyTo(stream);

        ThrowIfNotSuccess(ftp);
    }

    public Stream GetReadStream(string path)
    {
        var ftp = CreateAndConnect();
        var stream = InternalOpenReadStream(ftp, path, FileMode.OpenOrCreate);
        return new DependantStream(stream, ftp);
    }

    public Stream GetWriteStream(string path, FileMode mode)
    {
        var ftp = CreateAndConnect();
        var stream = InternalOpenWriteStream(ftp, path, mode);
        return new DependantStream(stream, ftp);
    }

    public Stream GetReadWriteStream(string path, FileMode mode)
    {
        throw new NotSupportedException("Obtaining ReadWrite Streams is not supported by FTPStorageProvider");
    }

    public bool DeleteDirectory(string path, bool recursive = false)
    {
        var ftp = CreateAndConnect();
        
        path = PreparePath(path);

        ftp.DeleteDirectory(path, recursive ? FtpListOption.Recursive : 0);
        ThrowIfNotSuccess(ftp);
        return true;
    }

    public bool CreateDirectory(string path)
    {
        var ftp = CreateAndConnect();

        path = PreparePath(path);

        return ftp.CreateDirectory(path);
    }

    public bool DirectoryExists(string path)
    {
        var ftp = CreateAndConnect();

        path = PreparePath(path);

        return ftp.DirectoryExists(path);
    }

    public void MoveFile(string path, string newPath, bool overwrite)
    {
        var ftp = CreateAndConnect();

        path = PreparePath(path);

        newPath = PreparePath(newPath);
        var dn = Path.GetDirectoryName(newPath)!;
        ftp.CreateDirectory(dn);

        if (ftp.FileExists(path) is false)
            throw new FileNotFoundException($"Could not find file '{path}' to move");
        if (!overwrite && ftp.FileExists(newPath))
            throw new IOException($"There already exists a file named '{newPath}'");
        if (ftp.MoveFile(path, newPath) is false)
            throw new IOException($"Could not move the file '{path}' to '{newPath}'");
    }

    public void CopyFile(string path, string newPath, bool overwrite)
    {
        var ftp = CreateAndConnect();

        path = PreparePath(path);

        newPath = PreparePath(newPath);
        var dn = Path.GetDirectoryName(newPath)!;
        ftp.CreateDirectory(dn);

        if (ftp.FileExists(path) is false)
            throw new FileNotFoundException($"Could not find file '{path}' to copy");
        if (!overwrite && ftp.FileExists(newPath))
            throw new IOException($"There already exists a file named '{newPath}'");

        using (var read = InternalOpenReadStream(ftp, path, FileMode.Open))
        using (var write = InternalOpenWriteStream(ftp, newPath, overwrite ? FileMode.Create : FileMode.CreateNew))
        {
            write.SetLength(0);
            read.CopyTo(write);
        }

        ThrowIfNotSuccess(ftp);
    }

    public bool DeleteFile(string path)
    {
        var ftp = CreateAndConnect();
        path = PreparePath(path);
        ftp.DeleteFile(path);
        return true;
    }

    public bool FileExists(string path)
    {
        var ftp = CreateAndConnect();
        path = PreparePath(path);
        return ftp.FileExists(path);
    }
}

