# Frends.Community.Apache

frends Community Task for Apache related operations

[![Actions Status](https://github.com/CommunityHiQ/Frends.Community.Apache/workflows/PackAndPushAfterMerge/badge.svg)](https://github.com/CommunityHiQ/Frends.Community.Apache/actions) ![MyGet](https://img.shields.io/myget/frends-community/v/Frends.Community.Apache) [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT) 

- [Installing](#installing)
- [Tasks](#tasks)
     - [ConvertCsvToParquet](#ConvertCsvToParquet)
- [Building](#building)
- [Contributing](#contributing)
- [Change Log](#change-log)

# Installing

You can install the Task via frends UI Task View or you can find the NuGet package from the following NuGet feed
https://www.myget.org/F/frends-community/api/v3/index.json and in Gallery view in MyGet https://www.myget.org/feed/frends-community/package/nuget/Frends.Community.Apache

# Tasks

## ConvertCsvToParquet

Converts CSV file to Parquet format using given metadata.
Uses parquet-dotnet library for writing Parquet files
https://github.com/elastacloud/parquet-dotnet

### Properties

#### Input
| Property | Type | Description | Example |
| -------- | -------- | -------- | -------- |
| Schema | json | Schema for CSV data. Types: boolean, datetime, datetimeoffset, decimal, double, float, int16, int, int32, int64, string and unspecified where ? means nullable column. Special characters are not allowed in column names. (Eg. characters '. ,;{}()=' have proven to be problematic.) | [  {"name": "Id", "type": "int"}, {"name": "Name", "type": "string"}, {"name": "Desc", "type": "string?"}, {"name": "number", "type": "decimal?", "culture": "en-US"} ] |
| CSV Filename | string |  Full path to the file to be read. | c:\temp\test.csv |
| Output Filename | string |Full path to the file to be write. | 'UseDevelopmentStorage=true' |
| Throw exception on error response | bool | Do not handle exceptions if set true. Otherwise catch exception and return error message. | true |

Note: Decimals, floats and doubles have "fi-FI" default culture. Decimal separator is ","

#### CSV Options
| Property | Type | Description | Example |
| -------- | -------- | -------- | -------- |
| CSV Delimeter | string | The separator used in the csv string. | ; |
| ContainsHeaderRow    | bool                 | This flag tells the reader if there is a header row in the CSV string. Default is true. |  true |
| TrimOutput           | bool                 | This flag tells the reader to trim whitespace from the beginning and ending of the field value when reading.              |  false |
| Ignore quotes | bool | This flag indicates if quotes should be ignored when parsing and be treated like any other character. | true |
| CultureInfo          | string               | The culture info to parse the file with, e.g. for decimal separators. InvariantCulture will be used by default. See list of cultures [here](https://msdn.microsoft.com/en-us/library/ee825488(v=cs.20).aspx); use the Language Culture Name. <br> NOTE: Due to an issue with the CsvHelpers library, all CSV tasks will use the culture info setting of the first CSV task in the process; you cannot use different cultures for reading and parsing CSV files in the same process.|   |
| FileEncoding                                | Enum           | Encoding for the read content. By selecting 'Other' you can use any encoding. | |
| EncodingInString                            | string         | The name of encoding to use. Required if the FileEncoding choice is 'Other'. A partial list of supported encoding names: https://msdn.microsoft.com/en-us/library/system.text.encoding.getencodings(v=vs.110).aspx | `iso-8859-1` |


#### Parquet Options
| Property | Type | Description | Example |
| -------- | -------- | -------- | -------- |
| Parquet row group size | number | Parquet files row group size. Batch size should be large enough because of perfomance later. | 5000 |
| Parquet compression method | Enum | Parquet's compression level. GZip (smallest filesize) / Snappy / None | Gzip |
| Count rows before processing | bool | Count CSV file rows before processing. If row count if smaller than Parquet row group size, decrease group size. Because this operation reads CSV file before processing, CSV file is processed two times. | false |

### Returns

| Property | Type | Description | Example |
| -------- | -------- | -------- | -------- |
| Success | bool | Tells if the operation was successful (true/false)  | true|
| StatusMessage | string | "ok" when Success==true. Otherwise returns error message |  "ok" |
| ParquetFileName | string | Full path to the written file. (Same as Output filename parameter) | c:\temp\test.parquet |
| Rows | number | Number of handled rows. | 124 |

# License

This project is licensed under the MIT License - see the LICENSE file for details

# Building

Clone a copy of the repository

`git clone https://github.com/CommunityHiQ/Frends.Community.Apache.git`

Rebuild the project

`dotnet build`

Run tests

`dotnet test`

Create a NuGet package

`dotnet pack --configuration Release`

# Contributing
When contributing to this repository, please first discuss the change you wish to make via issue, email, or any other method with the owners of this repository before making a change.

1. Fork the repository on GitHub
2. Clone the project to your own machine
3. Commit changes to your own branch
4. Push your work back up to your fork
5. Submit a Pull request so that we can review your changes

NOTE: Be sure to merge the latest from "upstream" before making a pull request!

# Change Log

| Version | Changes |
| ------- | ------- |
| 1.0.0  | First version. Includes task ConvertCsvToParquet  |
