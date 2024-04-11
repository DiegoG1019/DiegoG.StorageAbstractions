namespace DiegoG.StorageAbstractions.Buffers;

public class MultiStream : Stream
{
    private readonly Stream[] Streams;

    public MultiStream(IEnumerable<Stream> streams) : this(streams?.ToArray() ?? throw new ArgumentNullException(nameof(streams))) { }

    public MultiStream(params Stream[] streams)
    {
        ArgumentNullException.ThrowIfNull(streams);
        Span<int> hashes = stackalloc int[streams.Length];

        for (int i = 0; i < streams.Length; i++)
        {
            if (streams[i].CanWrite is false) throw new ArgumentException($"Not all the streams can be written to");
            Length = Math.Min(long.MaxValue, streams[i].Length);
            CanSeek = CanSeek && streams[i].CanSeek;

            hashes[i] = streams[i].GetHashCode();
            for (int x = 0; x < i; x++)
                if (hashes[x] == hashes[i])
                    throw new ArgumentException("One or more streams is repeated in the enumerable");
        }

        Streams = streams ?? throw new ArgumentNullException(nameof(streams));
    }
    public override void Flush()
    {
        for (int i = 0; i < Streams.Length; i++) Streams[i].Flush();
    }

    public override int Read(byte[] buffer, int offset, int count)
        => throw new NotSupportedException("Reading from the stream is not supported by MultiStream");

    public override long Seek(long offset, SeekOrigin origin)
    {
        ThrowIfCantSeek();
        foreach (var stream in Streams)
            stream.Seek(offset, origin);
        return _pos = Streams[0].Position;
    }

    public override void SetLength(long value)
        => throw new NotSupportedException("Setting the length of the stream is not supported by MultiStream");

    public override void Write(byte[] buffer, int offset, int count)
    {
        Span<byte> b = stackalloc byte[0];
        if (count <= 1024)
        {
            if (count % 16 == 0)
                b = stackalloc byte[count / 16];
            else if (count % 14 == 0)
                b = stackalloc byte[count / 14];
            else if (count % 12 == 0)
                b = stackalloc byte[count / 12];
            else if (count % 10 == 0)
                b = stackalloc byte[count / 10];
            else if (count % 8 == 0)
                b = stackalloc byte[count / 8];
            else if (count % 6 == 0)
                b = stackalloc byte[count / 6];
            else if (count % 4 == 0)
                b = stackalloc byte[count / 4];
            else if (count % 2 == 0)
                b = stackalloc byte[count / 2];
        }

        for (int i = offset; i < offset + count;)
        {
            if (offset + i + (count - i) <= buffer.Length)
            {
                Span<byte> x = buffer.AsSpan(offset + i, count - i);
                foreach (var stream in Streams)
                    stream.Write(x);
                i += x.Length;
            }
            else
            {
                for (; i < offset + count; i++)
                    foreach (var stream in Streams)
                        stream.WriteByte(buffer[i]);
            }
        }
    }

    private void ThrowIfCantSeek()
    {
        if (CanSeek is false)
            throw new InvalidOperationException("Not all of the streams in this MultiStream support Seeking");
    }

    private long _pos;
    public override bool CanRead => false;
    public override bool CanWrite => true;
    public override long Length { get; }
    public override bool CanSeek { get; }
    public override long Position
    {
        get => _pos;
        set
        {
            ThrowIfCantSeek();
            foreach (var stream in Streams) stream.Position = value;
            _pos = value;
        }
    }
}
