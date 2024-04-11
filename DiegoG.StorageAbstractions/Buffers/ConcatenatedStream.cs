namespace DiegoG.StorageAbstractions.Buffers;

public class ConcatenatedStream : Stream
{
    private readonly Stream[] Streams;

    public ConcatenatedStream(IEnumerable<Stream> streams) : this(streams?.ToArray() ?? throw new ArgumentNullException(nameof(streams))) { }

    public ConcatenatedStream(params Stream[] streams)
    {
        ArgumentNullException.ThrowIfNull(streams);
        Span<int> hashes = stackalloc int[streams.Length];

        for (int i = 0; i < streams.Length; i++)
        {
            CanRead = CanRead && streams[i].CanRead;
            CanWrite = CanWrite && streams[i].CanWrite;
            Length += streams[i].Length;

            hashes[i] = streams[i].GetHashCode();
            for (int x = 0; x < i; x++)
                if (hashes[x] == hashes[i])
                    throw new ArgumentException("One or more streams is repeated in the enumerable");
        }

        if (CanRead is false && CanWrite is false)
            throw new ArgumentException($"The streams have contradictory settings: One or more streams can't be read and one or more streams can't be written to; causing this ConcatenatedStream to be unusable");

        Streams = streams ?? throw new ArgumentNullException(nameof(streams));
    }

    public override void Flush()
    {
        for (int i = 0; i < Streams.Length; i++)
            Streams[i].Flush();
    }

    public override int Read(byte[] buffer, int offset, int count)
    {
        if (streamind >= Streams.Length) return 0;
        int i = offset;
        for (; i < offset + count; i++)
        {
            var stream = Streams[streamind];
            i += stream.Read(buffer, i, count - (i - offset));
            if (stream.Position == stream.Length)
            {
                streamind++;
                if (streamind >= Streams.Length) break;
            }
        }
        int read = i - offset;
        pos += read;
        return read;
    }

    public override long Seek(long offset, SeekOrigin origin)
        => throw new NotSupportedException("Seeking in the stream is not supported by ConcatenatedStream");

    public override void SetLength(long value)
        => throw new NotSupportedException("Setting the length of the stream is not supported by ConcatenatedStream");

    public override void Write(byte[] buffer, int offset, int count)
    {
        if (streamind >= Streams.Length) return;
        long i = offset;
        for (; i < offset + count; i++)
        {
            var stream = Streams[streamind];

            long start = stream.Position;
            stream.Write(buffer, (int)i, count - ((int)i - offset));

            i += stream.Position - start;

            if (stream.Position == stream.Length)
            {
                streamind++;
                if (streamind >= Streams.Length) break;
            }
        }
        pos += i - offset;
    }

    private long pos;
    private int streamind;

    public override bool CanRead { get; }
    public override bool CanSeek => false;
    public override bool CanWrite { get; }
    public override long Length { get; }
    public override long Position
    {
        get => pos;
        set => throw new NotSupportedException("Setting the position of the stream is not supported by ConcatenatedStream");
    }
}
