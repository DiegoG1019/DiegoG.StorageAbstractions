namespace DiegoG.StorageAbstractions.Buffers;

public class DependantStream : Stream
{
    private readonly Stream Inner;
    private readonly IDisposable Dependence;

    public DependantStream(Stream inner, IDisposable dependence)
    {
        ArgumentNullException.ThrowIfNull(inner);
        Inner = inner;

        ArgumentNullException.ThrowIfNull(dependence);
        Dependence = dependence;
    }

    public override void Flush()
    {
        Inner.Flush();
    }

    public override int Read(byte[] buffer, int offset, int count)
    {
        return Inner.Read(buffer, offset, count);
    }

    public override long Seek(long offset, SeekOrigin origin)
    {
        return Inner.Seek(offset, origin);
    }

    public override void SetLength(long value)
    {
        Inner.SetLength(value);
    }

    public override void Write(byte[] buffer, int offset, int count)
    {
        Inner.Write(buffer, offset, count);
    }

    public override bool CanRead => Inner.CanRead;

    public override bool CanSeek => Inner.CanSeek;

    public override bool CanWrite => Inner.CanWrite;

    public override long Length => Inner.Length;

    public override long Position { get => Inner.Position; set => Inner.Position = value; }

    protected override void Dispose(bool disposing)
    {
        base.Dispose(disposing);
        Inner.Dispose();
        Dependence.Dispose();
    }
}
