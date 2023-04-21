using Parquet;
using System;
using System.ComponentModel;
using System.ComponentModel.DataAnnotations;
using System.Text;

#pragma warning disable CS1591

namespace Frends.Community.Apache
{
    public class WriteInput
    {
        /// <summary>
        /// Data columns types for Parquet as JSON.
        /// </summary>
        [DisplayFormat(DataFormatString = "Json")]
        [DefaultValue(@"[
    {""name"": ""Id"", ""type"": ""int""},
    {""name"": ""Name"", ""type"": ""string""},
    {""name"": ""Desc"", ""type"": ""string""}
]")]
        public string Schema { get; set; }

        /// <summary>
        /// CSV filename.
        /// </summary>
        [DisplayFormat(DataFormatString = "Text")]
        [DefaultValue(@"C:\temp\test.csv")]
        public string CsvFileName { get; set; }

        /// <summary>
        /// Output filename.
        /// </summary>
        [DisplayFormat(DataFormatString = "Text")]
        [DefaultValue(@"c:\temp\test.parquet")]
        public string OuputFileName { get; set; }

        /// <summary>
        /// Throw exception if return code of request is not successful.
        /// </summary>
        [DefaultValue("true")]
        public bool ThrowExceptionOnErrorResponse { get; set; } = true;
    }

    public enum FileEncoding { UTF8, ANSI, ASCII, Unicode, Other }

    public enum CompressionType { Gzip, Snappy, None }

    public enum Timezone { GMTStandardTime, CentralEuropeStandardTime, FLEStandardTime, Other }

    public class WriteParquetOptions
    {
        /// <summary>
        /// Parquet files row group size.
        /// Batch size should be large enough because of perfomance later.
        /// </summary>
        [DefaultValue("5000")]
        public long ParquetRowGroupSize { get; set; } = 5000;

        /// <summary>
        /// Parquet Compression type: None / Snappy / GZip (smallest filesize).
        /// </summary>
        [DefaultValue(CompressionType.Gzip)]
        public CompressionType ParquetCompressionMethod { get; set; } = CompressionType.Gzip;

        /// <summary>
        /// Does task read whole file and count rows before processing.
        /// </summary>
        [DefaultValue(false)]
        public bool CountRowsBeforeProcessing { get; set; } = false;

        /// <summary>
        /// Timezone for datetime values.
        /// </summary>
        [DefaultValue(Timezone.GMTStandardTime)]
        public Timezone Timezone { get; set; } = Timezone.GMTStandardTime;

        /// <summary>
        /// Timezone for other timezone value. Full list on: https://learn.microsoft.com/en-us/windows-hardware/manufacture/desktop/default-time-zones?view=windows-11
        /// </summary>
        [UIHint(nameof(Timezone), "", Timezone.Other)]
        public string OtherTimezone { get; set; }
    }

    public class WriteCSVOptions
    {
        /// <summary>
        /// The separator used in the csv string.
        /// </summary>
        [DisplayFormat(DataFormatString = "Text")]
        [DefaultValue("\";\"")]

        public string CsvDelimiter { get; set; }
        /// <summary>
        /// This flag tells the reader if there is a header row in the CSV string.
        /// </summary>
        [DefaultValue("true")]
        public bool ContainsHeaderRow { get; set; } = true;

        /// <summary>
        /// This flag tells the reader to trim whitespace from the beginning and ending of the field value when reading.
        /// </summary>
        [DefaultValue("false")]
        public bool TrimOutput { get; set; } = false;

        /// <summary>
        /// This flag indicating if quotes should be ignored when parsing and treated like any other character.
        /// </summary>
        [DefaultValue("true")]
        public bool IgnoreQuotes { get; set; } = true;

        /// <summary>
        /// The culture info to read/write the entries with, e.g. for decimal separators. InvariantCulture will be used by default. See list of cultures here: https://msdn.microsoft.com/en-us/library/ee825488(v=cs.20).aspx; use the Language Culture Name.
        /// NOTE: Due to an issue with the CsvHelpers library, all CSV tasks will use the culture info setting of the first CSV task in the process; you cannot use different cultures for reading and parsing CSV files in the same process.|
        /// </summary>
        [DisplayFormat(DataFormatString = "Text")]
        public string CultureInfo { get; set; } = "";

        /// <summary>
        /// Encoding for the written content.
        /// By selecting 'Other' you can use any encoding.
        /// </summary>
        public FileEncoding FileEncoding { get; set; }

        [UIHint(nameof(FileEncoding), "", FileEncoding.UTF8)]
        public bool EnableBom { get; set; }

        /// <summary>
        /// File encoding to be used.
        /// A partial list of possible encodings: https://en.wikipedia.org/wiki/Windows_code_page#List
        /// </summary>
        [UIHint(nameof(FileEncoding), "", FileEncoding.Other)]
        [DisplayFormat(DataFormatString = "Text")]
        public string EncodingInString { get; set; }
    }

    public class WriteResult
    {
        /// <summary>
        /// Parquet file was created?
        /// </summary>
        public bool Success { get; set; }

        /// <summary>
        /// Status message / error message.
        /// </summary>
        public string StatusMessage { get; set; }

        /// <summary>
        /// Output filename.
        /// </summary>
        public string ParquetFileName { get; set; }

        /// <summary>
        /// Number of CSV (and Parquet) data rows.
        /// </summary>
        public int Rows { get; set; }
    }


    public static class Definitions
    {
        /// <summary>
        /// Returns Encoding.
        /// </summary>
        /// <param name="optionsFileEncoding"></param>
        /// <param name="optionsEnableBom"></param>
        /// <param name="optionsEncodingInString"></param>
        /// <returns></returns>
        public static Encoding GetEncoding(FileEncoding optionsFileEncoding, bool optionsEnableBom, string optionsEncodingInString)
        {
            return optionsFileEncoding switch
            {
                FileEncoding.Other => Encoding.GetEncoding(optionsEncodingInString),
                FileEncoding.ASCII => Encoding.ASCII,
                FileEncoding.ANSI => Encoding.Default,
                FileEncoding.UTF8 => optionsEnableBom ? new UTF8Encoding(true) : new UTF8Encoding(false),
                FileEncoding.Unicode => Encoding.Unicode,
                _ => throw new ArgumentOutOfRangeException(optionsFileEncoding.ToString()),
            };
        }

        /// <summary>
        /// Gets compression method.
        /// </summary>
        /// <param name="optionsCompressType">Gzip/Snappy/None</param>
        /// <returns>CompressionMethod</returns>
        public static CompressionMethod GetCompressionMethod(CompressionType optionsCompressType)
        {
            return optionsCompressType switch
            {
                CompressionType.Gzip => CompressionMethod.Gzip,
                CompressionType.Snappy => CompressionMethod.Snappy,
                CompressionType.None => CompressionMethod.None,
                _ => throw new ArgumentOutOfRangeException(optionsCompressType.ToString()),
            };
        }
    }
}
