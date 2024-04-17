namespace DiegoG.StorageAbstractions.MEGA;

public sealed class MEGAStorageBufferedStream(MEGAStorageProvider provider, string filepath, FileMode mode) : MemoryStream
{
    private bool isFlushing = false;

    public override void Flush()
    {
        base.Position = 0;
        provider.InvalidateCache();
        isFlushing = true;
        provider.WriteData(filepath, mode, this);
        isFlushing = false;
    }

    public override async Task FlushAsync(CancellationToken cancellationToken)
    {
        base.Position = 0;
        provider.InvalidateCache();
        isFlushing = true;
        await provider.WriteDataAsync(filepath, mode, this, cancellationToken);
        isFlushing = false;
    }

    public override int Read(byte[] buffer, int offset, int count)
    {
        return isFlushing is false
            ? throw new InvalidOperationException("This stream can only be read while it's flushing.")
            : base.Read(buffer, offset, count);
    }

    public override long Seek(long offset, SeekOrigin origin)
    {
        throw new NotSupportedException();
    }

    public override void SetLength(long value)
    {
        throw new NotSupportedException();
    }

    public override bool CanRead => false;

    public override bool CanSeek => false;

    public override bool CanWrite => base.CanWrite;

    public override long Length => base.Length;

    public override long Position
    {
        get => base.Position;
        set => throw new NotSupportedException();
    }
}
