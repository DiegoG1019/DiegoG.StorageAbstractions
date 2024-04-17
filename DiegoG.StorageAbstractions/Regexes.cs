using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace DiegoG.StorageAbstractions;
public static partial class Regexes
{
    [GeneratedRegex(@"(?<=[/\\]){0,}[\w\d]*?\.?[\w\d]+$")]
    public static partial Regex GetFileOrDirectoryNameRegex();

    [GeneratedRegex(@".*(?=[/\\][\w\d]*?\.?[\w\d]+$)")]
    public static partial Regex GetPathWithoutFileOrDirectoryNameRegex();
}
