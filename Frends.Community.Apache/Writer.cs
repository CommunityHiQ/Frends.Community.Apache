using Parquet;
using Parquet.Data;
using System;
using System.Collections.Generic;
using System.Globalization;

#pragma warning disable 1591

namespace Frends.Community.Apache
{
    public static class Writer
    {
        /// <summary>
        /// Writes dataLen rows and typed columns to the file.
        /// </summary>
        /// <param name="csvColumns">Processed CSV data</param>
        /// <param name="dataLen">Row count</param>
        /// <param name="writer">ParquetWriter</param>
        /// <param name="fields">Field structure</param>
        /// <param name="config">Config structure</param>
        public static void WriteGroup(List<Object> csvColumns, long dataLen, ParquetWriter writer, List<DataField> fields, Config config)
        {
            using (ParquetRowGroupWriter rg = writer.CreateRowGroup())
            {
                for (int i = 0; i < fields.Count; i++)
                {
                    if (fields[i].HasNulls)
                    {
                        switch (fields[i].DataType)
                        {
                            case DataType.Boolean:
                                rg.WriteColumn(new DataColumn(fields[i], ((bool?[])csvColumns[i])));
                                break;
                            case DataType.DateTimeOffset:
                                rg.WriteColumn(new DataColumn(fields[i], ((DateTimeOffset?[])csvColumns[i])));
                                break;
                            case DataType.Decimal:
                                rg.WriteColumn(new DataColumn(fields[i], ((decimal?[])csvColumns[i])));
                                break;
                            case DataType.Double:
                                rg.WriteColumn(new DataColumn(fields[i], ((double?[])csvColumns[i])));
                                break;
                            case DataType.Float:
                                rg.WriteColumn(new DataColumn(fields[i], ((float?[])csvColumns[i])));
                                break;
                            case DataType.Int16:
                                rg.WriteColumn(new DataColumn(fields[i], ((Int16?[])csvColumns[i])));
                                break;
                            case DataType.Int32:
                                rg.WriteColumn(new DataColumn(fields[i], ((Int32?[])csvColumns[i])));
                                break;
                            case DataType.Int64:
                                rg.WriteColumn(new DataColumn(fields[i], ((Int64?[])csvColumns[i])));
                                break;
                            case DataType.String:
                                rg.WriteColumn(new DataColumn(fields[i], ((string[])csvColumns[i])));
                                break;
                            default:
                                throw new ArgumentOutOfRangeException(fields[i].DataType.ToString());
                        }
                    }
                    else
                    {
                        switch (fields[i].DataType)
                        {
                            case DataType.Boolean:
                                rg.WriteColumn(new DataColumn(fields[i], ((bool[])csvColumns[i])));
                                break;
                            case DataType.DateTimeOffset:
                                rg.WriteColumn(new DataColumn(fields[i], ((DateTimeOffset[])csvColumns[i])));
                                break;
                            case DataType.Decimal:
                                rg.WriteColumn(new DataColumn(fields[i], ((decimal[])csvColumns[i])));
                                break;
                            case DataType.Double:
                                rg.WriteColumn(new DataColumn(fields[i], ((double[])csvColumns[i])));
                                break;
                            case DataType.Float:
                                rg.WriteColumn(new DataColumn(fields[i], ((float[])csvColumns[i])));
                                break;
                            case DataType.Int16:
                                rg.WriteColumn(new DataColumn(fields[i], ((Int16[])csvColumns[i])));
                                break;
                            case DataType.Int32:
                                rg.WriteColumn(new DataColumn(fields[i], ((Int32[])csvColumns[i])));
                                break;
                            case DataType.Int64:
                                rg.WriteColumn(new DataColumn(fields[i], ((Int64[])csvColumns[i])));
                                break;
                            case DataType.String:
                                rg.WriteColumn(new DataColumn(fields[i], ((string[])csvColumns[i])));
                                break;
                            default:
                                throw new ArgumentOutOfRangeException(fields[i].DataType.ToString());
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Return CultureInfo for given cultero or fi-FI
        /// </summary>
        /// <param name="culture">Culture name</param>
        /// <returns>CultureInfo</returns>
        public static CultureInfo GetCultureInfo(string culture)
        {
            if (System.String.IsNullOrEmpty(culture))
            {
                //return CultureInfo.InvariantCulture;
                return new CultureInfo("fi-FI");
            }
            else
            {
                return new CultureInfo(culture);
            }
        }

        /// <summary>
        /// Parse Int16 or return null if value is empty
        /// </summary>
        /// <param name="value">Int16 as string</param>
        /// <returns>Int16</returns>
        public static Int16? GetInt16ValueNullable(string value)
        {
            if (!String.IsNullOrWhiteSpace(value))
            {
                return Int16.Parse(value);
            }
            return null;
        }

        /// <summary>
        /// Parse Int32 or return null if value is empty
        /// </summary>
        /// <param name="value">Int32 as string</param>
        /// <returns>Int32</returns>
        public static Int32? GetInt32ValueNullable(string value)
        {
            if (!String.IsNullOrWhiteSpace(value))
            {
                return Int32.Parse(value);
            }
            return null;
        }

        /// <summary>
        /// Parse Int64 or return null if value is empty
        /// </summary>
        /// <param name="value">Int64 as string</param>
        /// <returns>Int64</returns>
        public static Int64? GetInt64ValueNullable(string value)
        {
            if (!String.IsNullOrWhiteSpace(value))
            {
                return Int64.Parse(value);
            }
            return null;
        }

        /// <summary>
        /// Parse float or return null if value is empty
        /// </summary>
        /// <param name="value">float as string</param>
        /// <param name="culture">Optional CultureInfo object</param>
        /// <returns>float</returns>
        public static float? GetFloatValue(string value, string culture)
        {
            if (!String.IsNullOrWhiteSpace(value))
            {
                return float.Parse(value, Writer.GetCultureInfo(culture));
            }
            return null;
        }

        /// <summary>
        /// Parse decimal or return null if value is empty
        /// </summary>
        /// <param name="value">decimal as string</param>
        /// <param name="culture">Optional CultureInfo object</param>
        /// <returns>decimal</returns>
        public static decimal? GetDecimalValueNullable(string value, string culture)
        {
            if (!String.IsNullOrWhiteSpace(value))
            {
                return decimal.Parse(value, Writer.GetCultureInfo(culture));
            }
            return null;
        }

        /// <summary>
        /// Parse double or return null if value is empty
        /// </summary>
        /// <param name="value">double as string</param>
        /// <param name="culture">Optional CultureInfo object</param>
        /// <returns>double</returns>
        public static double? GetDoubleValueNullable(string value, string culture)
        {
            if (!String.IsNullOrWhiteSpace(value))
            {
                return double.Parse(value, Writer.GetCultureInfo(culture));
            }
            return null;
        }

        /// <summary>
        /// Parse boolean value. "true" or "1" is true, otherwise value is false. Empty values are null.
        /// </summary>
        /// <param name="value">boolean as string</param>
        /// <returns>boolean</returns>
        public static bool? GetBooleanValueNullable(string value)
        {
            if (!String.IsNullOrWhiteSpace(value))
            {
                if (value.ToLower() == "true" || value == "1")
                {
                    return true;
                }
                else
                {
                    return false;
                }
            }
            return null;
        }

        /// <summary>
        /// Returns true when string is true or 1.
        /// Otherwise returns false.
        /// </summary>
        /// <param name="value">boolean as string</param>
        /// <returns>bool</returns>
        public static bool GetBooleanValue(string value)
        {
            if (value.ToLower() == "true" || value == "1")
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        /// <summary>
        /// Parses datetime
        /// If format is given, tries parse that exact format.
        /// If date is empty, returns null.
        /// </summary>
        /// <param name="dtStr">Date as string</param>
        /// <param name="timeFormat">Optional exact format</param>
        /// <returns>datetimeoffset object</returns>
        public static DateTimeOffset? GetDateTimeOffsetValueNullable(string dtStr, string timeFormat)
        {
            if (!String.IsNullOrWhiteSpace(dtStr))
            {
                if (!String.IsNullOrWhiteSpace(timeFormat))
                {
                    if (DateTimeOffset.TryParseExact(dtStr, timeFormat, CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTimeOffset dt))
                    {
                        return dt;
                    }
                }
                else
                {
                    return DateTimeOffset.Parse(dtStr);
                }

            }
            return null;
        }

        /// <summary>
        /// Parses datetime
        /// If format is given, tries parse that exact format.
        /// If exact format parsing fails, throws exception.
        /// </summary>
        /// <param name="dtStr">Date as string</param>
        /// <param name="timeFormat">Optional exact format</param>
        /// <returns>datetimeoffset object</returns>
        public static DateTimeOffset GetDateTimeOffsetValue(string dtStr, string timeFormat)
        {
            if (!String.IsNullOrWhiteSpace(timeFormat))
            {
                if (DateTimeOffset.TryParseExact(dtStr, timeFormat, CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTimeOffset dt))
                {
                    return dt;
                }
                else
                {
                    throw new ArgumentException($"DateTimeOffset.TryParseExact failed. Value: {dtStr}, format: {timeFormat}");
                }
            }
            else
            {
                return DateTimeOffset.Parse(dtStr);
            }

        }
    }
}
