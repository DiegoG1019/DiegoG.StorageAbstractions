using System.Text.RegularExpressions;

namespace DiegoG.StorageAbstractions;
public static partial class Regexes
{
    [GeneratedRegex(@"(?<=[/\\]){0,}[\w\d]*?\.?[\w\d]+$")]
    public static partial Regex GetFileOrDirectoryNameRegex();

    [GeneratedRegex(@".*(?=[/\\][\w\d]*?\.?[\w\d]+$)")]
    public static partial Regex GetPathWithoutFileOrDirectoryNameRegex();
}
