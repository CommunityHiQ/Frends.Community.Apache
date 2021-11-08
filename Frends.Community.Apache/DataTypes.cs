using Parquet.Data;
using System;

#pragma warning disable 1591

namespace Frends.Community.Apache
{
    public static class DataTypes
    {
        /// <summary>
        /// Return Parquet datatypes
        /// </summary>
        /// <param name="typeStr">Name of the data type</param>
        /// <returns>Parquet.Data.DataType</returns>
        public static DataType GetDataType(string typeStr)
        {
            string jsonType = typeStr.TrimEnd(new char[] { '?' });

            switch (jsonType.ToLower())
            {
                case "boolean":
                    return DataType.Boolean;
                case "datetime":
                case "datetimeoffset":
                    return DataType.DateTimeOffset;
                case "decimal":
                    return DataType.Decimal;
                case "double":
                    return DataType.Double;
                case "float":
                    return DataType.Float;
                case "int16":
                    return DataType.Int16;
                case "int":
                case "int32":
                    return DataType.Int32;
                case "int64":
                    return DataType.Int64;
                //case "int96":
                // this is for datetimes. Use datetimeoffset
                case "string":
                    return DataType.String;
                case "unspecified":
                    return DataType.Unspecified;
                default:
                    throw new ArgumentOutOfRangeException(jsonType);
            }
        }

        /// <summary>
        /// Returns array of specific type
        /// </summary>
        /// <param name="field">Datatype</param>
        /// <param name="groupSize">Array size</param>
        /// <returns>Array</returns>
        public static object GetCSVColumnStorage(DataField field, long groupSize)
        {
            DataType fieldType = field.DataType;

            if (field.HasNulls)
            {
                switch (fieldType)
                {
                    case DataType.Boolean:
                        return new bool?[groupSize];
                    case DataType.DateTimeOffset:
                        return new DateTimeOffset?[groupSize];
                    case DataType.Decimal:
                        return new decimal?[groupSize];
                    case DataType.Double:
                        return new double?[groupSize];
                    case DataType.Float:
                        return new float?[groupSize];
                    case DataType.Int16:
                        return new short?[groupSize];
                    case DataType.Int32:
                        return new int?[groupSize];
                    case DataType.Int64:
                        return new long?[groupSize];
                    case DataType.String:
                        return new string[groupSize];
                    default:
                        throw new ArgumentOutOfRangeException(field.DataType.ToString());
                }
            }
            else
            {
                switch (fieldType)
                {
                    case DataType.Boolean:
                        return new bool[groupSize];
                    case DataType.DateTimeOffset:
                        return new DateTimeOffset[groupSize];
                    case DataType.Decimal:
                        return new decimal[groupSize];
                    case DataType.Double:
                        return new double[groupSize];
                    case DataType.Float:
                        return new float[groupSize];
                    case DataType.Int16:
                        return new short[groupSize];
                    case DataType.Int32:
                        return new int[groupSize];
                    case DataType.Int64:
                        return new long[groupSize];
                    case DataType.String:
                        return new string[groupSize];
                    default:
                        throw new ArgumentOutOfRangeException(field.DataType.ToString());
                }
            }
        }
    }
}
