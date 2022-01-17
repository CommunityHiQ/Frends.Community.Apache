using Parquet.Data;
using System;

#pragma warning disable 1591

namespace Frends.Community.Apache
{
    public static class DataTypes
    {
        /// <summary>
        /// Return Parquet datatypes.
        /// </summary>
        /// <param name="typeStr">Name of the data type</param>
        /// <returns>Parquet.Data.DataType</returns>
        public static DataType GetDataType(string typeStr)
        {
            var jsonType = typeStr.TrimEnd(new char[] { '?' });

            return jsonType.ToLower() switch
            {
                "boolean" => DataType.Boolean,
                "datetime" or "datetimeoffset" => DataType.DateTimeOffset,
                "decimal" => DataType.Decimal,
                "double" => DataType.Double,
                "float" => DataType.Float,
                "int16" => DataType.Int16,
                "int" or "int32" => DataType.Int32,
                "int64" => DataType.Int64,
                // this is for datetimes.
                // Use datetimeoffset.
                "string" => DataType.String,
                "unspecified" => DataType.Unspecified,
                _ => throw new ArgumentOutOfRangeException(jsonType),
            };
        }

        /// <summary>
        /// Returns array of specific type.
        /// </summary>
        /// <param name="field">Datatype</param>
        /// <param name="groupSize">Array size</param>
        /// <returns>Array</returns>
        public static object GetCSVColumnStorage(DataField field, long groupSize)
        {
            var fieldType = field.DataType;

            if (field.HasNulls)
            {
                return fieldType switch
                {
                    DataType.Boolean => new bool?[groupSize],
                    DataType.DateTimeOffset => new DateTimeOffset?[groupSize],
                    DataType.Decimal => new decimal?[groupSize],
                    DataType.Double => new double?[groupSize],
                    DataType.Float => new float?[groupSize],
                    DataType.Int16 => new short?[groupSize],
                    DataType.Int32 => new int?[groupSize],
                    DataType.Int64 => new long?[groupSize],
                    DataType.String => new string[groupSize],
                    _ => throw new ArgumentOutOfRangeException(field.DataType.ToString()),
                };
            }
            else
            {
                return fieldType switch
                {
                    DataType.Boolean => new bool[groupSize],
                    DataType.DateTimeOffset => new DateTimeOffset[groupSize],
                    DataType.Decimal => new decimal[groupSize],
                    DataType.Double => new double[groupSize],
                    DataType.Float => new float[groupSize],
                    DataType.Int16 => new short[groupSize],
                    DataType.Int32 => new int[groupSize],
                    DataType.Int64 => new long[groupSize],
                    DataType.String => new string[groupSize],
                    _ => throw new ArgumentOutOfRangeException(field.DataType.ToString()),
                };
            }
        }
    }
}
