using CsvHelper;
using CsvHelper.Configuration;
using Newtonsoft.Json.Linq;
using Parquet;
using Parquet.Data;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Globalization;
using System.IO;
using System.Threading;

#pragma warning disable 1591 // ReSharper disable InconsistentNaming
#pragma warning disable 1573 // disable warning for missing documentation for internal cancellation tokens

namespace Frends.Community.Apache
{
    public class ApacheTasks
    {
        /// <summary>
        /// Converts csv file to Parquet file.
        /// </summary>
        /// <param name="input">Input parameters</param>
        /// <param name="csvOptions">CSV file options</param>
        /// <param name="parquetOptions">Parquet file options</param>
        /// <returns>Object with following properties: bool Success, string StatusMsg, string ParquetFileName</returns>
        public static WriteResult ConvertCsvToParquet([PropertyTab] WriteInput input, [PropertyTab] WriteCSVOptions csvOptions, [PropertyTab] WriteParquetOptions parquetOptions, CancellationToken cancellationToken)
        {
            var csvRowCount = 0;

            try
            {
                var encoding = Definitions.GetEncoding(csvOptions.FileEncoding, csvOptions.EnableBom, csvOptions.EncodingInString);
                var jsonConf = JToken.Parse(input.Schema);

                // Check schema.
                try
                {
                    var arr = JArray.Parse(jsonConf.ToString());

                    // Check data columns types. Accepted data types: name, type, format and culture.
                    var lineCount = 1;
                    foreach (var content in arr.Children<JObject>())
                    {
                        lineCount++;
                        foreach (var prop in content.Properties())
                        {
                            cancellationToken.ThrowIfCancellationRequested();
                            if (!prop.Name.Equals("name") && !prop.Name.Equals("type") && !prop.Name.Equals("format") && !prop.Name.Equals("culture"))
                            {
                                throw new ArgumentException($"Data columns type was incorrect at line {lineCount}. Incorrect data type: {prop.Name}");
                            }
                        }
                    }
                }
                catch (Exception e)
                {
                    throw new Exception("Invalid schema", e);
                }
                
                // Create CSV configuration.
                var csvConfiguration = new CsvConfiguration(new CultureInfo(csvOptions.CultureInfo))
                {
                    Delimiter = csvOptions.CsvDelimiter,
                    HasHeaderRecord = csvOptions.ContainsHeaderRow,
                    Mode = csvOptions.IgnoreQuotes ? CsvMode.NoEscape : CsvMode.RFC4180,
                    TrimOptions = csvOptions.TrimOutput ? TrimOptions.Trim : TrimOptions.None
                };

                var config = new Config(jsonConf);

                // Create schema for Parquet.
                var dataFields = CreateDataFields(jsonConf);
                var schema = new Schema(dataFields);
                var colCount = schema.Fields.Count;

                // Decrease ParquetRowGroupSize if possible.
                var fileInfo = new FileInfo(input.CsvFileName);
                var csvFileSize = fileInfo.Length;

                var parquetRowGroupSize = parquetOptions.ParquetRowGroupSize;

                if (parquetRowGroupSize < 1)
                {
                    throw new ArgumentException("ParguetRowGroupSize must be greater than 0.");
                }

                // Maximum rows of csv file: size of the file.
                if (parquetRowGroupSize > csvFileSize)
                {
                    parquetRowGroupSize = csvFileSize;
                }

                // Count rows.
                if (parquetOptions.CountRowsBeforeProcessing)
                {
                    var rowCount = 0;
                    using (var reader = new StreamReader(input.CsvFileName, encoding))
                    using(var csv = new CsvReader(reader, csvConfiguration))
                    {
                        if (csvOptions.ContainsHeaderRow)
                        {
                            csv.Read();
                            csv.ReadHeader();
                        }
                        while (csv.Read())
                        {
                            cancellationToken.ThrowIfCancellationRequested();
                            rowCount++;
                        }
                    }
                    // Set the right count for memory optimization.
                    if (parquetRowGroupSize > rowCount) parquetRowGroupSize = rowCount;
                }


                // Open file and read csv.
                // CSV is row-oriented, Parquet is column-oriented.

                using (var reader = new StreamReader(input.CsvFileName, encoding))
                using (var csv = new CsvReader(reader, csvConfiguration))
                using (var fileStream = File.Open(input.OuputFileName, FileMode.CreateNew))
                using (var parquetWriter = new ParquetWriter(schema, fileStream))
                {
                    {
                        // Read header row.
                        if (csvOptions.ContainsHeaderRow)
                        {
                            csv.Read();
                            csv.ReadHeader();
                        }
                        parquetWriter.CompressionMethod = Definitions.GetCompressionMethod(parquetOptions.ParquetCompressionMethod);

                        var csvColumns = new List<object>();
                        for (var i = 0; i < colCount; i++) csvColumns.Add(DataTypes.GetCSVColumnStorage(dataFields[i], parquetRowGroupSize));

                        var dataIndex = 0;

                        try
                        {
                            // Read csv rows.
                            while (csv.Read())
                            {
                                // Insert data to structure.
                                for (var i = 0; i < colCount; i++)
                                {
                                    if (dataFields[i].HasNulls)
                                    {
                                        switch (dataFields[i].DataType)
                                        {
                                            case DataType.Boolean:
                                                ((bool?[])csvColumns[i])[dataIndex] = Writer.GetBooleanValueNullable(csv.GetField(i));
                                                break;
                                            case DataType.DateTimeOffset:
                                                ((DateTimeOffset?[])csvColumns[i])[dataIndex] = Writer.GetDateTimeOffsetValueNullable(csv.GetField(i), config.GetConfigValue(dataFields[i].Name));
                                                break;
                                            case DataType.Decimal:
                                                ((decimal?[])csvColumns[i])[dataIndex] = Writer.GetDecimalValueNullable(csv.GetField(i), config.GetConfigValue(dataFields[i].Name));
                                                break;
                                            case DataType.Double:
                                                ((double?[])csvColumns[i])[dataIndex] = Writer.GetDoubleValueNullable(csv.GetField(i), config.GetConfigValue(dataFields[i].Name));
                                                break;
                                            case DataType.Float:
                                                ((float?[])csvColumns[i])[dataIndex] = Writer.GetFloatValue(csv.GetField(i), config.GetConfigValue(dataFields[i].Name));
                                                break;
                                            case DataType.Int16:
                                                ((short?[])csvColumns[i])[dataIndex] = Writer.GetInt16ValueNullable(csv.GetField(i));
                                                break;
                                            case DataType.Int32:
                                                ((int?[])csvColumns[i])[dataIndex] = Writer.GetInt32ValueNullable(csv.GetField(i));
                                                break;
                                            case DataType.Int64:
                                                ((long?[])csvColumns[i])[dataIndex] = Writer.GetInt64ValueNullable(csv.GetField(i));
                                                break;
                                            case DataType.String:
                                                ((string[])csvColumns[i])[dataIndex] = csv.GetField(i);
                                                break;
                                            default:
                                                throw new ArgumentOutOfRangeException("CSV memory writer: Cannot identify datatype.");
                                        }
                                    }
                                    else
                                    {
                                        switch (dataFields[i].DataType)
                                        {
                                            case DataType.Boolean:
                                                ((bool[])csvColumns[i])[dataIndex] = Writer.GetBooleanValue(csv.GetField(i));
                                                break;
                                            case DataType.DateTimeOffset:
                                                ((DateTimeOffset[])csvColumns[i])[dataIndex] = Writer.GetDateTimeOffsetValue(csv.GetField(i), config.GetConfigValue(dataFields[i].Name));
                                                break;
                                            case DataType.Decimal:
                                                ((decimal[])csvColumns[i])[dataIndex] = decimal.Parse(csv.GetField(i), Writer.GetCultureInfo(config.GetConfigValue(dataFields[i].Name)));
                                                break;
                                            case DataType.Double:
                                                ((double[])csvColumns[i])[dataIndex] = double.Parse(csv.GetField(i), Writer.GetCultureInfo(config.GetConfigValue(dataFields[i].Name)));
                                                break;
                                            case DataType.Float:
                                                ((float[])csvColumns[i])[dataIndex] = float.Parse(csv.GetField(i), Writer.GetCultureInfo(config.GetConfigValue(dataFields[i].Name)));
                                                break;
                                            case DataType.Int16:
                                                ((short[])csvColumns[i])[dataIndex] = short.Parse(csv.GetField(i));
                                                break;
                                            case DataType.Int32:
                                                ((int[])csvColumns[i])[dataIndex] = int.Parse(csv.GetField(i));
                                                break;
                                            case DataType.Int64:
                                                ((long[])csvColumns[i])[dataIndex] = long.Parse(csv.GetField(i));
                                                break;
                                            case DataType.String:
                                                ((string[])csvColumns[i])[dataIndex] = csv.GetField(i);
                                                break;
                                            default:
                                                throw new ArgumentOutOfRangeException("CSV memory writer: Cannot identify datatype.");
                                        }
                                    }
                                }

                                dataIndex++;

                                // Write data if data structure is full.
                                if (dataIndex >= parquetRowGroupSize)
                                {
                                    Writer.WriteGroup(csvColumns, dataIndex, parquetWriter, dataFields, config);
                                    dataIndex = 0;
                                }

                                csvRowCount++;

                                // Check if process is terminated.
                                cancellationToken.ThrowIfCancellationRequested();
                            }
                        }
                        catch (Exception e)
                        {
                            throw new Exception($"CSV processing error in row {dataIndex + 1}", e);
                        }

                        // Write non-empty data structure.
                        if (dataIndex > 0)
                        {
                            if (dataIndex < parquetRowGroupSize)
                            {
                                for (var i = 0; i < colCount; i++)
                                {
                                    var elementType = csvColumns[i].GetType().GetElementType();
                                    var newArray = Array.CreateInstance(elementType, dataIndex);
                                    Array.Copy((Array)csvColumns[i], newArray, dataIndex);
                                    csvColumns[i] = newArray;
                                }
                            }
                            Writer.WriteGroup(csvColumns, dataIndex, parquetWriter, dataFields, config);
                        }
                    }
                }

                // Return success.
                return new WriteResult()
                {
                    Success = true,
                    ParquetFileName = input.OuputFileName,
                    StatusMessage = "ok",
                    Rows = csvRowCount
                };
            }
            catch (Exception e)
            {
                // Throw exception or return error.
                if (input.ThrowExceptionOnErrorResponse) throw;
                return new WriteResult()
                {
                    Success = false,
                    ParquetFileName = "",
                    StatusMessage = e.ToString(),
                    Rows = csvRowCount
                };
            }
        }

        /// <summary>
        /// Get Parquet Datafields from task's JSON configuration.
        /// </summary>
        /// <param name="json">JSON configuration</param>
        /// <returns>List of datafields</returns>
        private static List<DataField> CreateDataFields(JToken json)
        {
            var fields = new List<DataField>();

            foreach (var element in json)
            {
                var type = element.Value<string>("type");

                var nullable = false;
                if (type.EndsWith("?")) nullable = true;

                var field = new DataField(element.Value<string>("name"), DataTypes.GetDataType(type), nullable);
                fields.Add(field);
            }
            return fields;
        }
    }
}
