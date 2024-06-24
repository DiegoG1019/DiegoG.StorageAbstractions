using System.Buffers;

namespace DiegoG.StorageAbstractions.Buffers;

public class IEnumerableStream : Stream
{
    private readonly IEnumerator<byte> _data;
    private readonly byte[] Buffer;
    private int bufferpos = 0;
    private int bufferlen = 0;
    private long pos = 0;
    private bool complete = false;

    public IEnumerableStream(IEnumerable<byte> data, int? bufferLength = null)
    {
        _data = data?.GetEnumerator() ?? throw new ArgumentNullException(nameof(data));
        if (data.TryGetNonEnumeratedCount(out int count))
            Length = count;

        Buffer = ArrayPool<byte>.Shared.Rent(bufferLength ?? nint.Size * 8);
    }

    public override void Flush() { }

    private bool ReadFromBuffer(out byte val)
    {
        if (complete)
        {
            val = 0;
            return false;
        }

        var i = Interlocked.Increment(ref bufferpos) - 1;
        Interlocked.Increment(ref pos);
        if (bufferpos >= bufferlen)
            lock (Buffer)
            {
                if (bufferpos >= bufferlen)
                {
                    i = bufferpos = 0;
                    var ci = 0;
                    while (_data.MoveNext() && ci < Buffer.Length) Buffer[ci++] = _data.Current;
                    bufferlen = ci;
                    if (ci == 0)
                        complete = true;
                }
            }

        val = Buffer[i];
        return true;
    }

    public override int Read(byte[] buffer, int offset, int count)
    {
        int i = offset;
        for (; i < offset + count && ReadFromBuffer(out byte val); i++) buffer[i] = val;
        return i - offset;
    }

    public override long Seek(long offset, SeekOrigin origin)
    {
        throw new NotSupportedException("Seeking is not supported by IEnumerableStream");
    }

    public override void SetLength(long value)
    {
        throw new NotSupportedException("Setting the length of the stream is not supported by IEnumerableStream");
    }

    public override void Write(byte[] buffer, int offset, int count)
    {
        throw new NotSupportedException("Writing is not supported by IEnumerableStream");
    }

    public override bool CanRead { get; } = true;
    public override bool CanSeek { get; } = false;
    public override bool CanWrite { get; } = false;
    public override long Length { get; }
    public override long Position
    {
        get => pos;
        set => throw new NotSupportedException("Seeking is not supported by IEnumerableStream");
    }

    protected override void Dispose(bool disposing)
    {
        base.Dispose(disposing);
        ArrayPool<byte>.Shared.Return(Buffer);
        _data.Dispose();
    }
}

//public class IEnumerableStream<TContent> : Stream
//    where TContent : unmanaged
//{
//    private readonly IEnumerator<TContent> _data;
//    private readonly byte[] Buffer;
//    private int bufferpos = 0;
//    private int bufferlen = 0;
//    private bool complete = false;

//    public IEnumerableStream(IEnumerable<TContent> data, int? bufferLength = null)
//    {
//        _data = data?.GetEnumerator() ?? throw new ArgumentNullException(nameof(data));
//        if (data.TryGetNonEnumeratedCount(out int count))
//            Length = count;

//        Buffer = ArrayPool<byte>.Shared.Rent(bufferLength ?? IntPtr.Size * Unsafe.SizeOf<TContent>());
//    }

//    public override void Flush() { }

//    private unsafe bool ReadFromBuffer(out byte val)
//    {
//        if (complete)
//        {
//            val = 0;
//            return false;
//        }

//        var i = Interlocked.Increment(ref bufferpos) - 1;
//        if (bufferpos >= bufferlen)
//            lock (Buffer)
//            {
//                if (bufferpos >= bufferlen)
//                {
//                    i = bufferpos = 0;
//                    var ci = 0;
//                    while (_data.MoveNext() && ci < Buffer.Length)
//                    {

//                    }

//                    bufferlen = ci * Unsafe.SizeOf<TContent>();
//                    if (ci == 0)
//                        complete = true;
//                }
//            }

//        val = Buffer[i];
//        return true;
//    }

//    public override int Read(byte[] buffer, int offset, int count)
//    {
//        int i = offset;
//        for (; i < (offset + count) && ReadFromBuffer(out byte val); i++) buffer[i] = val;
//        return i - offset;
//    }

//    public override long Seek(long offset, SeekOrigin origin)
//    {
//        throw new NotSupportedException("Seeking is not supported by IEnumerableStream");
//    }

//    public override void SetLength(long value)
//    {
//        throw new NotSupportedException("Setting the length of the stream is not supported by IEnumerableStream");
//    }

//    public override void Write(byte[] buffer, int offset, int count)
//    {
//        throw new NotSupportedException("Writing is not supported by IEnumerableStream");
//    }

//    public override bool CanRead { get; } = true;
//    public override bool CanSeek { get; } = false;
//    public override bool CanWrite { get; } = false;
//    public override long Length { get; }
//    public override long Position
//    {
//        get => pos;
//        set => throw new NotSupportedException("Seeking is not supported by IEnumerableStream");
//    }

//    protected override void Dispose(bool disposing)
//    {
//        base.Dispose(disposing);
//        ArrayPool<byte>.Shared.Return(Buffer);
//        _data.Dispose();
//    }
//}
