using NUnit.Framework;
using Parquet;
using System;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using Parquet.Data;
using System.Runtime.InteropServices;

namespace Frends.Community.Apache.Tests
{
    public class Tests
    { 
        // File paths
        private static readonly string _basePath = Path.GetTempPath();
        private readonly string _inputCsvFileName = Path.Combine(_basePath, "testi-csv-" + Path.GetRandomFileName());
        private readonly string _inputCsvFileNameQuotes = Path.Combine(_basePath, "testi-quot-csv-" + Path.GetRandomFileName());
        private readonly string _inputCsvFileNameDecDot = Path.Combine(_basePath, "testi-dec-csv-" + Path.GetRandomFileName());
        private readonly string _inputCsvFileNameDecComma = Path.Combine(_basePath, "testi-dec2-csv-" + Path.GetRandomFileName());
        private readonly string _inputCsvFileNameNoNulls = Path.Combine(_basePath, "testi-nn1-csv-" + Path.GetRandomFileName());
        private readonly string _inputCsvFileNameLarge = Path.Combine(_basePath, "testi-large-csv-" + Path.GetRandomFileName());
        private readonly string _outputFileName = Path.Combine(_basePath, "testi-parquet-" + Path.GetRandomFileName());

        private const string _commonSchema = @"[
    { ""name"": ""Id"", ""type"": ""int?""},
    {""name"": ""Time"", ""type"": ""datetime?"", ""format"": ""dd.MM.yyyy""},
    {""name"": ""Decimal"", ""type"": ""decimal?"", ""culture"": ""en-US""},
    { ""name"": ""Description"", ""type"": ""string?""},
]";
        private const string _commonSchemaFalse = @"[
    { ""name"": ""Id"", ""typr"": ""int?""},
    {""name"": ""Time"", ""type"": ""datetime?"", ""format"": ""dd.MM.yyyy""},
    {""name"": ""Decimal"", ""tpe"": ""decimal?"", ""culture"": ""en-US""},
    { ""name"": ""Description"", ""type"": ""string?""},
]";

        [SetUp]
        public void Setup()
        {
            // Write csv test file
            File.WriteAllText(_inputCsvFileName, @"Id;Date;Decimal;Text
1;01.10.2019;5.0;Testirivi 1
1;15.04.2018;3.5;Testirivi 2 - pidempi teksti ja ��kk�si�
;;;Tyhj� rivi 1
4;1.1.2020;9.999;
3;11.11.2011;1.2345;Viimeinen rivi
", Encoding.UTF8);
            File.WriteAllText(_inputCsvFileNameQuotes, @"Id;Date;Decimal;Text
1;01.10.2019;5.0;Testirivi 1
1;15.04.2018;3.5;Testirivi 2 - ""pidempi teksti"" ja ��kk�si�
;;;Tyhj� rivi 1
4;1.1.2020;9.999;
3;11.11.2011;1.2345;Viimeinen rivi
", Encoding.UTF8);

            File.WriteAllText(_inputCsvFileNameDecComma, @"Id;Decimal
1;12345,6789;12345,6789;12345,6789
2;2,3;2,3;2,3
3;4,4;4,4;4,4
");
            File.WriteAllText(_inputCsvFileNameNoNulls, @"Id;Date;Decimal;Text
1;01.10.2019;5.0;Testirivi 1
2;15.04.2018;3.5;Testirivi 2 - pidempi teksti ja ��kk�si�
3;31.12.2019;1.12020;Testirivi 3
4;01.01.2020;9.999; Testirivi 4
3;11.11.2011;1.2345;Viimeinen rivi
", Encoding.UTF8);

            File.WriteAllText(_inputCsvFileNameDecDot, @"Id;Decimal
1;12345.6789;12345.6789;12345.6789
2;2.3;2.3;2.3
3;4.4;4.4;4.4
", Encoding.UTF8);
        }

        [TearDown]
        public void TearDown()
        {
            // Remove all test files
            foreach (var name in new string[] { _inputCsvFileNameNoNulls, _inputCsvFileName, _inputCsvFileNameLarge, _inputCsvFileNameQuotes, _inputCsvFileNameDecComma, _inputCsvFileNameDecDot, _outputFileName})
            {
                if (File.Exists(name))
                {
                    File.Delete(name);
                }
            }
        }

        /// <summary>
        /// Simple csv -> parquet test case.
        /// </summary>
        [Test]
        public void WriteParquetFile()
        {
            TestTools.RemoveOutputFile(_outputFileName);

            var options = new WriteCSVOptions()
            {
                CsvDelimiter = ";",
                FileEncoding = FileEncoding.UTF8,
                EnableBom = false,
                EncodingInString = ""
            };

            var poptions = new WriteParquetOptions()
            {
                ParquetRowGroupSize = 5000,
                ParquetCompressionMethod = CompressionType.Gzip,
                
            };

            var input = new WriteInput()
            {
                CsvFileName = _inputCsvFileName,
                OuputFileName = _outputFileName,
                ThrowExceptionOnErrorResponse = true,
                Schema = _commonSchema
            };

            ApacheTasks.ConvertCsvToParquet(input, options, poptions, new CancellationToken());

            var hash = TestTools.MD5Hash(_outputFileName);
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
            {
                var errMessage = $"File checksum doesn't match. Generated checksum: '{hash}' differs the expected checksum: '045c6a617a1d37e0a1b464ccdeea2979'";
                Assert.AreEqual(hash, "045c6a617a1d37e0a1b464ccdeea2979", errMessage);
            }
            else
            {
                var errMessage = $"File checksum doesn't match. Generated checksum: '{hash}' differs the expected checksum: '3d6f72d1664b6a4040d2f12457264060'";
                Assert.AreEqual(hash, "3d6f72d1664b6a4040d2f12457264060", errMessage);
            }
        }

        /// <summary>
        /// Simple csv -> parquet with invalid schema test.
        /// Tests also InnerException.
        /// </summary>
        [Test]
        public void TestInvalidSchema()
        {
            TestTools.RemoveOutputFile(_outputFileName);

            var options = new WriteCSVOptions()
            {
                CsvDelimiter = ";",
                FileEncoding = FileEncoding.UTF8,
                EnableBom = false,
                EncodingInString = ""
            };

            var poptions = new WriteParquetOptions()
            {
                ParquetRowGroupSize = 5000,
                ParquetCompressionMethod = CompressionType.Gzip,

            };

            var input = new WriteInput()
            {
                CsvFileName = _inputCsvFileName,
                OuputFileName = _outputFileName,
                ThrowExceptionOnErrorResponse = true,
                Schema = _commonSchemaFalse
            };

            void ConvertCsvToParquetThatThrowsWithTypos()
            {
                ApacheTasks.ConvertCsvToParquet(input, options, poptions, new CancellationToken());
            }

            Assert.That(ConvertCsvToParquetThatThrowsWithTypos,
                Throws.TypeOf<Exception>()
                    .With.Message.EqualTo("Invalid schema"));

            var ex = Assert.Throws<Exception>((() => ConvertCsvToParquetThatThrowsWithTypos()));
            Assert.That(ex.InnerException, Is.TypeOf<ArgumentException>()
                .With.Message.StartsWith("Data columns type was incorrect at line"));

        }

        /// <summary>
        /// Simple csv, no null values.
        /// </summary>
        [Test]
        public void WriteParquetFileNoNulls()
        {
            TestTools.RemoveOutputFile(_outputFileName);

            var options = new WriteCSVOptions()
            {
                CsvDelimiter = ";",
                FileEncoding = FileEncoding.UTF8,
                EnableBom = false,
                EncodingInString = ""
            };

            var poptions = new WriteParquetOptions()
            {
                ParquetRowGroupSize = 5000,
                ParquetCompressionMethod = CompressionType.Gzip
            };

            var input = new WriteInput()
            {
                CsvFileName = _inputCsvFileNameNoNulls,
                OuputFileName = _outputFileName,
                ThrowExceptionOnErrorResponse = true,
                Schema = _commonSchema.Replace("?\"","\"")
            };

            ApacheTasks.ConvertCsvToParquet(input, options, poptions, new CancellationToken());

            var hash = TestTools.MD5Hash(_outputFileName);
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
            {
                var errMessage = $"File checksum doesn't match. Generated checksum: '{hash}' differs the expected checksum: '6c80e7c86c8adf39b8544f7bc90724c8'";
                Assert.AreEqual(hash, "6c80e7c86c8adf39b8544f7bc90724c8", errMessage);
            }
            else
            {
                var errMessage = $"File checksum doesn't match. Generated checksum: '{hash}' differs the expected checksum: 'd5dcfc43ecd64da5f5013dab3095b777'";
                Assert.AreEqual(hash, "d5dcfc43ecd64da5f5013dab3095b777", errMessage);
            }
        }

        /// <summary>
        /// Quotes test.
        /// </summary>
        [Test]
        public void WriteParquetFileQuotes()
        {
            TestTools.RemoveOutputFile(_outputFileName);

            var options = new WriteCSVOptions()
            {
                CsvDelimiter = ";",
                FileEncoding = FileEncoding.UTF8,
                EnableBom = false,
                EncodingInString = "",
                IgnoreQuotes = true
            };

            var poptions = new WriteParquetOptions()
            {
                ParquetRowGroupSize = 5000,
                ParquetCompressionMethod = CompressionType.Gzip
            };

            var input = new WriteInput()
            {
                CsvFileName = _inputCsvFileNameQuotes,
                OuputFileName = _outputFileName,
                ThrowExceptionOnErrorResponse = true,
                Schema = _commonSchema
            };

            ApacheTasks.ConvertCsvToParquet(input, options, poptions, new CancellationToken());

            var hash = TestTools.MD5Hash(_outputFileName);
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
            {
                var errMessage = $"File checksum doesn't match. Generated checksum: '{hash}' differs the expected checksum: '86692194196efecc823d48384bd2a5a5'";
                Assert.AreEqual(hash, "86692194196efecc823d48384bd2a5a5", errMessage);
            }
            else
            {
                var errMessage = $"File checksum doesn't match. Generated checksum: '{hash}' differs the expected checksum: '94e1bfe7bf71d94d5bd52f2de2af658b'";
                Assert.AreEqual(hash, "94e1bfe7bf71d94d5bd52f2de2af658b", errMessage);
            }
        }


        /// <summary>
        /// Test case when decimal separator is dot. "."
        /// </summary>
        [Test]
        public void DecimalTestDot1()
        {
            TestTools.RemoveOutputFile(_outputFileName);
            RunDecimalTestNullable("en-US", _inputCsvFileNameDecDot);

            Assert.AreEqual(12345.6789m, ReturnFirstDecimal(_outputFileName, 1));
            Assert.AreEqual(12345.6789f, ReturnFirstDecimal(_outputFileName, 2));
            Assert.AreEqual(12345.6789d, ReturnFirstDecimal(_outputFileName, 3));
        }

        /// <summary>
        /// Now cultere is empty -> default should be CultureInfo.InvariantCulture.
        /// </summary>
        [Test]
        public void DecimalTestDefault()
        {
            TestTools.RemoveOutputFile(_outputFileName);
            RunDecimalTestNormal("", _inputCsvFileNameDecComma);

            Assert.AreEqual(12345.6789m, ReturnFirstDecimal(_outputFileName, 1));
            Assert.AreEqual(12345.6789f, ReturnFirstDecimal(_outputFileName, 2));
            Assert.AreEqual(12345.6789d, ReturnFirstDecimal(_outputFileName, 3));
        }

        /// <summary>
        /// Test case when decimal separator is comma. ","
        /// </summary>
        [Test]
        public void DecimalTestComma()
        {
            TestTools.RemoveOutputFile(_outputFileName);
            RunDecimalTestNullable("fi-FI", _inputCsvFileNameDecComma);

            Assert.AreEqual(12345.6789m, ReturnFirstDecimal(_outputFileName, 1));
            Assert.AreEqual(12345.6789f, ReturnFirstDecimal(_outputFileName, 2));
            Assert.AreEqual(12345.6789d, ReturnFirstDecimal(_outputFileName, 3));
        }

        /// <summary>
        /// Reads first decimal from first group and given column.
        /// </summary>
        /// <param name="parquetFilePath">Full filepath</param>
        /// <param name="columnIndex">Column index 0...n</param>
        /// <returns></returns>

        private object ReturnFirstDecimal(string parquetFilePath, int columnIndex)
        {
            var encoding = Definitions.GetEncoding(FileEncoding.UTF8, false, "");

            using var filereader = File.Open(parquetFilePath, FileMode.Open, FileAccess.Read);

            var options = new ParquetOptions { TreatByteArrayAsString = true };
            var parquetReader = new ParquetReader(filereader, options);

            var dataFields = parquetReader.Schema.GetDataFields();

            using var groupReader = parquetReader.OpenRowGroupReader(0);

            var columns = dataFields.Select(groupReader.ReadColumn).ToArray();
            var decimalColumn = columns[columnIndex];

            if (dataFields[columnIndex].HasNulls)
            {
                switch (dataFields[columnIndex].DataType)
                {
                    case DataType.Decimal:
                        var dec = (decimal?[])decimalColumn.Data;
                        return dec[0];
                    case DataType.Float:
                        var flo = (float?[])decimalColumn.Data;
                        return flo[0];
                    case DataType.Double:
                        var dou = (double?[])decimalColumn.Data;
                        return dou[0];
                    default:
                        throw new Exception("Unknown nullable datatype:" + dataFields[columnIndex].DataType);
                }
            }
            else
            {
                switch (dataFields[columnIndex].DataType)
                {
                    case DataType.Decimal:
                        var dec = (decimal[])decimalColumn.Data;
                        return dec[0];
                    case DataType.Float:
                        var flo = (float[])decimalColumn.Data;
                        return flo[0];
                    case DataType.Double:
                        var dou = (double[])decimalColumn.Data;
                        return dou[0];
                    default:
                        throw new Exception("Unknown datatype:" + dataFields[columnIndex].DataType);
                }
            }
        }

        /// <summary>
        /// Runs decimal tests using static schema and given input.
        /// </summary>
        /// <param name="decimalType"></param>
        /// <param name="inputFileName"></param>
        private void RunDecimalTestNullable(string cultureStr, string inputFileName)
        {
            var options = new WriteCSVOptions()
            {
                CsvDelimiter = ";",
                FileEncoding = FileEncoding.UTF8,
                EnableBom = false,
                EncodingInString = ""
            };

            var poptions = new WriteParquetOptions()
            {
                ParquetRowGroupSize = 5,
                ParquetCompressionMethod = CompressionType.Snappy
            };

            var input = new WriteInput()
            {
                CsvFileName = inputFileName,
                OuputFileName = _outputFileName,
                ThrowExceptionOnErrorResponse = true,
                Schema = @"[
    {""name"": ""Id"", ""type"": ""int?""},
    {""name"": ""Decimal"", ""type"": ""decimal?""" + (string.IsNullOrEmpty(cultureStr) ? "}" : @",""culture"": """ + cultureStr + @"""}") + @",
    {""name"": ""Float"", ""type"": ""float?""" + (string.IsNullOrEmpty(cultureStr) ? "}" : @",""culture"": """ + cultureStr + @"""}") + @",
    {""name"": ""Double"", ""type"": ""double?""" + (string.IsNullOrEmpty(cultureStr) ? "}" : @",""culture"": """ + cultureStr + @"""}") + @",
]"
            };

            ApacheTasks.ConvertCsvToParquet(input, options, poptions, new CancellationToken());
        }

        private void RunDecimalTestNormal(string cultureStr, string inputFileName)
        {
            var options = new WriteCSVOptions()
            {
                CsvDelimiter = ";",
                FileEncoding = FileEncoding.UTF8,
                EnableBom = false,
                EncodingInString = ""
            };

            var poptions = new WriteParquetOptions()
            {
                ParquetRowGroupSize = 5,
                ParquetCompressionMethod = CompressionType.Snappy
            };

            var input = new WriteInput()
            {
                CsvFileName = inputFileName,
                OuputFileName = _outputFileName,
                ThrowExceptionOnErrorResponse = true,
                Schema = @"[
    {""name"": ""Id"", ""type"": ""int""},
    {""name"": ""Decimal"", ""type"": ""decimal""" + (string.IsNullOrEmpty(cultureStr) ? "}" : @",""culture"": """ + cultureStr + @"""}") + @",
    {""name"": ""Float"", ""type"": ""float""" + (string.IsNullOrEmpty(cultureStr) ? "}" : @",""culture"": """ + cultureStr + @"""}") + @",
    {""name"": ""Double"", ""type"": ""double""" + (string.IsNullOrEmpty(cultureStr) ? "}" : @",""culture"": """ + cultureStr + @"""}") + @",
]"
            };

            ApacheTasks.ConvertCsvToParquet(input, options, poptions, new CancellationToken());
        }

        /// <summary>
        /// Test case for counting rows before processing.
        /// </summary>
        [Test]
        public void WriteParquetFileCountRows()
        {
            TestTools.RemoveOutputFile(_outputFileName);

            var options = new WriteCSVOptions()
            {
                CsvDelimiter = ";",
                FileEncoding = FileEncoding.UTF8,
                EnableBom = false,
                EncodingInString = ""
            };

            var poptions = new WriteParquetOptions()
            {
                ParquetRowGroupSize = 5000,
                ParquetCompressionMethod = CompressionType.Gzip,
                CountRowsBeforeProcessing = true
            };

            var input = new WriteInput()
            {
                CsvFileName = _inputCsvFileName,
                OuputFileName = _outputFileName,
                ThrowExceptionOnErrorResponse = true,
                Schema = _commonSchema
            };

            ApacheTasks.ConvertCsvToParquet(input, options, poptions, new CancellationToken());

            var hash = TestTools.MD5Hash(_outputFileName);
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
            {
                var errMessage = $"File checksum doesn't match. Generated checksum: '{hash}' differs the expected checksum: '045c6a617a1d37e0a1b464ccdeea2979'";
                Assert.AreEqual(hash, "045c6a617a1d37e0a1b464ccdeea2979", errMessage);
            }
            else
            {
                var errMessage = $"File checksum doesn't match. Generated checksum: '{hash}' differs the expected checksum: '3d6f72d1664b6a4040d2f12457264060'";
                Assert.AreEqual(hash, "3d6f72d1664b6a4040d2f12457264060", errMessage);
            }
        }

        /// <summary>
        /// Test case for selecting timezone for datetime.
        /// </summary>
        [Test]
        public void WriteParquetFileDatetime()
        {
            TestTools.RemoveOutputFile(_outputFileName);

            var options = new WriteCSVOptions()
            {
                CsvDelimiter = ";",
                FileEncoding = FileEncoding.UTF8,
                EnableBom = false,
                EncodingInString = ""
            };

            var poptions = new WriteParquetOptions()
            {
                ParquetRowGroupSize = 5000,
                ParquetCompressionMethod = CompressionType.Gzip,
                Timezone = Timezone.FLEStandardTime
            };

            var input = new WriteInput()
            {
                CsvFileName = _inputCsvFileName,
                OuputFileName = _outputFileName,
                ThrowExceptionOnErrorResponse = true,
                Schema = _commonSchema
            };

            ApacheTasks.ConvertCsvToParquet(input, options, poptions, new CancellationToken());

            var hash = TestTools.MD5Hash(_outputFileName);
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
            {
                var errMessage = $"File checksum doesn't match. Generated checksum: '{hash}' differs the expected checksum: '045c6a617a1d37e0a1b464ccdeea2979'";
                Assert.AreEqual(hash, "045c6a617a1d37e0a1b464ccdeea2979", errMessage);
            }
            else
            {
                var errMessage = $"File checksum doesn't match. Generated checksum: '{hash}' differs the expected checksum: '3d6f72d1664b6a4040d2f12457264060'";
                Assert.AreEqual(hash, "3d6f72d1664b6a4040d2f12457264060", errMessage);
            }
        }

        /// <summary>
        /// Simple csv -> parquet test case with large group size.
        /// </summary>
        [Test]
        public void WriteParquetFileMaxMemory()
        {
            TestTools.RemoveOutputFile(_outputFileName);

            var schema = CreateLargeCSVFile(_inputCsvFileNameLarge, 1000000);

            var options = new WriteCSVOptions()
            {
                CsvDelimiter = ";",
                FileEncoding = FileEncoding.UTF8,
                EnableBom = false,
                EncodingInString = ""
            };

            var poptions = new WriteParquetOptions()
            {
                ParquetRowGroupSize = 100000000,
                ParquetCompressionMethod = CompressionType.Gzip
            };

            var input = new WriteInput()
            {
                CsvFileName = _inputCsvFileNameLarge,
                OuputFileName = _outputFileName,
                ThrowExceptionOnErrorResponse = true,
                Schema = schema
            };

            ApacheTasks.ConvertCsvToParquet(input, options, poptions, new CancellationToken());

            var hash = TestTools.MD5Hash(_outputFileName);
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
            {
                var errMessage = $"File checksum didn't match. Generated checksum: '{hash}' differs the expected checksum: '936a383c5b5f5665114f48c804f52bd3'";
                Assert.AreEqual(hash, "936a383c5b5f5665114f48c804f52bd3", errMessage);
            }
            else
            {
                var errMessage = $"File checksum didn't match. Generated checksum: '{hash}' differs the expected checksum: 'd214884cf6a6596cbc69a174bdd60805'";
                Assert.AreEqual(hash, "d214884cf6a6596cbc69a174bdd60805", errMessage);
            }
        }


        /// <summary>
        /// Creates CSV file and returns JSON schema for Parquet.
        /// </summary>
        /// <param name="fileName">CSV filename, full path</param>
        /// <param name="rows">Number of rows</param>
        /// <returns>JSON schema of the csv file.</returns>
        private string CreateLargeCSVFile(string fileName, int rows)
        {
            // Has to be fixed date, otherwise the MD5 hash of the final file changes every day and the unit test starts to fail.
            var dateTimeToWrite = new DateTime(2000, 01, 01).ToString("dd.MM.yyyy", CultureInfo.InvariantCulture);

            using (var outputFile = new StreamWriter(fileName, false, Encoding.UTF8))
            {
                outputFile.WriteLine("Id;Date;Decimal;Text");
                for (var i = 0; i < rows; i++)
                {
                    var dec1 = (i + 1.0) + (i % 100) / 100.0;
                    outputFile.WriteLine((i + 1) + ";" + dateTimeToWrite + ";" +
                        dec1.ToString(CultureInfo.InvariantCulture) + ";" +
                        "Testirivi " + (i + 1) + " ja jotain teksti�.");
                }
            }

            return _commonSchema.Replace("int?","int");
        }
    }
}