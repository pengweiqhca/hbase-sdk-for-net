﻿// Copyright (c) Microsoft Corporation
// All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License.  You may obtain a copy
// of the License at http://www.apache.org/licenses/LICENSE-2.0
//
// THIS CODE IS PROVIDED *AS IS* BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, EITHER EXPRESS OR IMPLIED, INCLUDING WITHOUT LIMITATION ANY IMPLIED
// WARRANTIES OR CONDITIONS OF TITLE, FITNESS FOR A PARTICULAR PURPOSE,
// MERCHANTABLITY OR NON-INFRINGEMENT.
//
// See the Apache Version 2.0 License for specific language governing
// permissions and limitations under the License.

namespace Microsoft.HBase.Client
{
    using Google.Protobuf;
    using Microsoft.HBase.Client.Internal;
    using Microsoft.HBase.Client.Requester;
    using Org.Apache.Hadoop.Hbase.Rest.Protobuf.Generated;
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.IO;
    using System.Linq;
    using System.Net;
    using System.Net.Http;
    using System.Text;
    using System.Threading.Tasks;

    /// <summary>
    /// A C# connector to HBase.
    /// </summary>
    /// <remarks>
    /// It currently targets HBase 0.96.2 and HDInsight 3.0 on Microsoft Azure.
    /// The communication works through HBase REST (StarGate) which uses ProtoBuf as a serialization format.
    ///
    /// The usage is quite simple:
    ///
    /// <code>
    /// var credentials = ClusterCredentials.FromFile("credentials.txt");
    /// var client = new HBaseClient(credentials);
    /// var version = await client.GetVersionAsync();
    ///
    /// Console.WriteLine(version);
    /// </code>
    /// </remarks>
    public sealed class HBaseClient : IHBaseClient, IDisposable
    {
        private IWebRequester _requester;

        private const string CheckAndPutQuery = "check=put";
        private const string CheckAndDeleteQuery = "check=delete";
        private const string RowKeyColumnFamilyTimeStampFormat = "{0}/{1}/{2}";

        /// <summary>
        /// Used to detect redundant calls to <see cref="IDisposable.Dispose"/>.
        /// </summary>
        private bool _disposed;

        /// <summary>
        /// Initializes a new instance of the <see cref="HBaseClient"/> class.
        /// </summary>
        public HBaseClient() : this(RequestOptions.GetDefaultOptions()) { }

        /// <summary>
        /// Initializes a new instance of the <see cref="HBaseClient"/> class.
        /// </summary>
        /// <param name="globalRequestOptions">The global request options.</param>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope", Justification = "_requester disposed of in Dispose() if it is an IDisposable")]
        public HBaseClient(RequestOptions globalRequestOptions)
        {
            _requester = new VNetWebRequester(globalRequestOptions ?? RequestOptions.GetDefaultOptions());
        }

        /// <summary>
        /// Creates a scanner on the server side.
        /// The resulting ScannerInformation can be used to read query the CellSets returned by this scanner in the #ScannerGetNext/Async method.
        /// </summary>
        /// <param name="tableName">the table to scan</param>
        /// <param name="scannerSettings">the settings to e.g. set the batch size of this scan</param>
        /// <returns>A ScannerInformation which contains the continuation url/token and the table name</returns>
        public async Task<ScannerInformation> CreateScannerAsync(string tableName, Scanner scannerSettings)
        {
            tableName.ArgumentNotNullNorEmpty("tableName");
            scannerSettings.ArgumentNotNull("scannerSettings");

            using (var response = await PostRequestAsync(tableName + "/scanner", scannerSettings).ConfigureAwait(false))
            {
                if (response.WebResponse.StatusCode != HttpStatusCode.Created)
                {
                    using (var output = new StreamReader(await response.WebResponse.Content.ReadAsStreamAsync().ConfigureAwait(false)))
                    {
                        var message = output.ReadToEnd();
                        throw new WebException($"Couldn't create a scanner for table {tableName}! Response code was: {response.WebResponse.StatusCode}, expected 201! Response body was: {message}");
                    }
                }

                var location = response.WebResponse.Headers.Location;
                if (location == null)
                {
                    throw new ArgumentException("Couldn't find header 'Location' in the response!");
                }

                return new ScannerInformation(location, tableName, response.WebResponse.Headers);
            }
        }

        /// <summary>
        /// Deletes scanner.
        /// </summary>
        /// <param name="tableName">the table the scanner is associated with.</param>
        /// <param name="scannerInfo">the scanner information retrieved by #CreateScanner()</param>
        public async Task DeleteScannerAsync(string tableName, ScannerInformation scannerInfo)
        {
            tableName.ArgumentNotNullNorEmpty("tableName");
            scannerInfo.ArgumentNotNull("scannerInfo");

            using (var webResponse = await DeleteRequestAsync<Scanner>(tableName + "/scanner/" + scannerInfo.ScannerId, null).ConfigureAwait(false))
            {
                if (webResponse.WebResponse.StatusCode != HttpStatusCode.OK)
                {
                    using (var output = new StreamReader(await webResponse.WebResponse.Content.ReadAsStreamAsync().ConfigureAwait(false)))
                    {
                        var message = output.ReadToEnd();
                        throw new WebException($"Couldn't delete scanner {scannerInfo.ScannerId} associated with {tableName} table.! Response code was: {webResponse.WebResponse.StatusCode}, expected 200! Response body was: {message}");
                    }
                }
            }
        }

        public Task DeleteCellsAsync(string tableName, string rowKey)
        {
            tableName.ArgumentNotNullNorEmpty("tableName");
            rowKey.ArgumentNotNullNorEmpty("rowKey");

            return DeleteCellsAsyncInternal(tableName, rowKey);
        }

        public Task DeleteCellsAsync(string tableName, string rowKey, string columnFamily, long timestamp)
        {
            tableName.ArgumentNotNullNorEmpty("tableName");
            rowKey.ArgumentNotNullNorEmpty("rowKey");
            columnFamily.ArgumentNotNullNorEmpty("columnFamily");

            return DeleteCellsAsyncInternal(tableName, string.Format(CultureInfo.InvariantCulture, RowKeyColumnFamilyTimeStampFormat, rowKey, columnFamily, timestamp));
        }

        private async Task DeleteCellsAsyncInternal(string tableName, string path)
        {
            using (var webResponse = await DeleteRequestAsync<Scanner>(tableName + "/" + path, null).ConfigureAwait(false))
            {
                if (webResponse.WebResponse.StatusCode != HttpStatusCode.OK)
                {
                    using (var output = new StreamReader(await webResponse.WebResponse.Content.ReadAsStreamAsync().ConfigureAwait(false)))
                    {
                        var message = output.ReadToEnd();
                        throw new WebException($"Couldn't delete row {path} associated with {tableName} table.! Response code was: {webResponse.WebResponse.StatusCode}, expected 200! Response body was: {message}");
                    }
                }
            }
        }

        /// <summary>
        /// Creates a table and/or fully replaces its schema.
        /// </summary>
        /// <param name="schema">the schema</param>
        /// <returns>returns true if the table was created, false if the table already exists. In case of any other error it throws a WebException.</returns>
        public async Task<bool> CreateTableAsync(TableSchema schema)
        {
            schema.ArgumentNotNull("schema");

            if (string.IsNullOrEmpty(schema.Name))
            {
                throw new ArgumentException("schema.Name was either null or empty!", "schema");
            }

            using (var webResponse = await PutRequestAsync(schema.Name + "/schema", null, schema).ConfigureAwait(false))
            {
                if (webResponse.WebResponse.StatusCode == HttpStatusCode.Created)
                {
                    return true;
                }

                // table already exits
                if (webResponse.WebResponse.StatusCode == HttpStatusCode.OK)
                {
                    return false;
                }

                // throw the exception otherwise
                using (var output = new StreamReader(await webResponse.WebResponse.Content.ReadAsStreamAsync().ConfigureAwait(false)))
                {
                    var message = output.ReadToEnd();
                    throw new WebException($"Couldn't create table {schema.Name}! Response code was: {webResponse.WebResponse.StatusCode}, expected either 200 or 201! Response body was: {message}");
                }
            }
        }

        /// <summary>
        /// Deletes a table.
        /// If something went wrong, a WebException is thrown.
        /// </summary>
        /// <param name="table">the table name</param>
        public Task DeleteTableAsync(string table)
        {
            table.ArgumentNotNullNorEmpty("table");

            return DeleteTableAsyncInternal(table);
        }

        public async Task DeleteTableAsyncInternal(string table)
        {
            using (var webResponse = await DeleteRequestAsync<TableSchema>(table + "/schema", null).ConfigureAwait(false))
            {
                if (webResponse.WebResponse.StatusCode != HttpStatusCode.OK)
                {
                    using (var output = new StreamReader(await webResponse.WebResponse.Content.ReadAsStreamAsync().ConfigureAwait(false)))
                    {
                        var message = output.ReadToEnd();
                        throw new WebException($"Couldn't delete table {table}! Response code was: {webResponse.WebResponse.StatusCode}, expected 200! Response body was: {message}");
                    }
                }
            }
        }

        /// <summary>
        /// Gets the cells asynchronously.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="rowKey">The row key.</param>
        /// <param name="columnName"></param>
        /// <param name="numOfVersions"></param>
        /// <returns></returns>
        public Task<CellSet> GetCellsAsync(string tableName, string rowKey, string columnName = null, string numOfVersions = null)
        {
            tableName.ArgumentNotNullNorEmpty("tableName");
            rowKey.ArgumentNotNull("rowKey");

            var endpoint = tableName + "/" + rowKey;
            if (columnName != null)
            {
                endpoint += "/" + columnName;
            }
            string query = null;
            if (numOfVersions != null)
            {
                query = "v=" + numOfVersions;
            }

            return GetRequestAndDeserializeAsync<CellSet>(endpoint, query);
        }

        /// <summary>
        /// Gets the cells asynchronous.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="rowKeys">The row keys.</param>
        /// <returns>A cell set</returns>
        public Task<CellSet> GetCellsAsync(string tableName, string[] rowKeys)
        {
            tableName.ArgumentNotNullNorEmpty("tableName");
            rowKeys.ArgumentNotNull("rowKey");

            var endpoint = tableName + "/multiget";

            string query = null;
            for (var i = 0; i < rowKeys.Length; i++)
            {
                var prefix = "&";
                if (i == 0)
                {
                    prefix = "";
                }

                query += prefix + "row=" + rowKeys[i];
            }

            return GetRequestAndDeserializeAsync<CellSet>(endpoint, query);
        }


        /// <summary>
        /// Gets the storage cluster status asynchronous.
        /// </summary>
        /// <returns>
        /// </returns>
        public Task<StorageClusterStatus> GetStorageClusterStatusAsync() =>
           GetRequestAndDeserializeAsync<StorageClusterStatus>("/status/cluster", null);

        /// <summary>
        /// Gets the table information asynchronously.
        /// </summary>
        /// <param name="table">The table.</param>
        /// <returns></returns>
        public Task<TableInfo> GetTableInfoAsync(string table) =>
            GetRequestAndDeserializeAsync<TableInfo>(table + "/regions", null);

        /// <summary>
        /// Gets the table schema asynchronously.
        /// </summary>
        /// <param name="table">The table.</param>
        /// <returns>
        /// </returns>
        public Task<TableSchema> GetTableSchemaAsync(string table) =>
            GetRequestAndDeserializeAsync<TableSchema>(table + "/schema", null);

        /// <summary>
        /// Gets the version asynchronously.
        /// </summary>
        /// <returns>
        /// </returns>
        public Task<Org.Apache.Hadoop.Hbase.Rest.Protobuf.Generated.Version> GetVersionAsync() =>
            GetRequestAndDeserializeAsync<Org.Apache.Hadoop.Hbase.Rest.Protobuf.Generated.Version>("version", null);

        /// <summary>
        /// Lists the tables asynchronously.
        /// </summary>
        /// <returns>
        /// </returns>
        public Task<TableList> ListTablesAsync() => GetRequestAndDeserializeAsync<TableList>("", null);

        /// <summary>
        /// Modifies a table schema.
        /// If necessary it creates a new table with the given schema.
        /// If something went wrong, a WebException is thrown.
        /// </summary>
        /// <param name="table">the table name</param>
        /// <param name="schema">the schema</param>
        public async Task ModifyTableSchemaAsync(string table, TableSchema schema)
        {
            table.ArgumentNotNullNorEmpty("table");
            schema.ArgumentNotNull("schema");

            using (var webResponse = await PostRequestAsync(table + "/schema", schema).ConfigureAwait(false))
            {
                if (webResponse.WebResponse.StatusCode == HttpStatusCode.OK || webResponse.WebResponse.StatusCode == HttpStatusCode.Created) return;

                using (var output = new StreamReader(await webResponse.WebResponse.Content.ReadAsStreamAsync().ConfigureAwait(false)))
                {
                    var message = output.ReadToEnd();
                    throw new WebException($"Couldn't modify table schema {table}! Response code was: {webResponse.WebResponse.StatusCode}, expected either 200 or 201! Response body was: {message}");
                }
            }
        }

        /// <summary>
        /// Scans the next set of messages.
        /// </summary>
        /// <param name="scannerInfo">the scanner information retrieved by #CreateScanner()</param>
        /// <returns>a cellset, or null if the scanner is exhausted</returns>
        public async Task<CellSet> ScannerGetNextAsync(ScannerInformation scannerInfo)
        {
            scannerInfo.ArgumentNotNull("scannerInfo");

            using (var webResponse = await GetRequestAsync(scannerInfo.TableName + "/scanner/" + scannerInfo.ScannerId, null).ConfigureAwait(false))
            {
                return webResponse.WebResponse.StatusCode == HttpStatusCode.OK ? new MessageParser<CellSet>(() => new CellSet()).ParseFrom(await webResponse.WebResponse.Content.ReadAsStreamAsync().ConfigureAwait(false)) : null;
            }
        }

        public async Task<IEnumerable<CellSet>> StatelessScannerAsync(string tableName, string optionalRowPrefix = null, string scanParameters = null)
        {
            tableName.ArgumentNotNullNorEmpty("tableName");

            using (var webResponse = await GetRequestAsync(tableName + "/" + optionalRowPrefix + "*", scanParameters).ConfigureAwait(false))
            {
                return webResponse.WebResponse.StatusCode == HttpStatusCode.OK ? Deserialize<CellSets>(await webResponse.WebResponse.Content.ReadAsStreamAsync().ConfigureAwait(false)).Sets : null;
            }
        }

        private static T Deserialize<T>(Stream stream) where T : IMessage<T>, new() =>
            new MessageParser<T>(() => new T()).ParseFrom(stream);

        /// <summary>
        /// Atomically checks if a row/family/qualifier value matches the expected value and updates
        /// </summary>
        /// <param name="table">the table</param>
        /// <param name="row">row to update</param>
        /// <param name="cellToCheck">cell to check</param>
        /// <returns>true if the record was updated; false if condition failed at check</returns>
        public Task<bool> CheckAndPutAsync(string table, CellSet.Types.Row row, Cell cellToCheck)
        {
            table.ArgumentNotNullNorEmpty("table");
            row.ArgumentNotNull("row");
            row.Values.Add(cellToCheck);
            var cellSet = new CellSet();
            cellSet.Rows.Add(row);

            return StoreCellsAsyncInternal(table, cellSet, Encoding.UTF8.GetString(row.Key.ToByteArray()), CheckAndPutQuery);
        }

        /// <summary>
        /// Automically checks if a row/family/qualifier value matches the expected value and deletes
        /// </summary>
        /// <param name="table">the table</param>
        /// <param name="cellToCheck">cell to check for deleting the row</param>
        /// <param name="rowToDelete"></param>
        /// <returns>true if the record was deleted; false if condition failed at check</returns>
        public Task<bool> CheckAndDeleteAsync(string table, Cell cellToCheck, CellSet.Types.Row rowToDelete = null)
        {
            table.ArgumentNotNullNorEmpty("table");
            cellToCheck.ArgumentNotNull("cellToCheck");

            if (rowToDelete == null) return CheckAndDeleteAsyncInternal(cellToCheck, cellToCheck.Row);

            rowToDelete.Values.ArgumentNotNullNorEmpty("rowToDelete");

            if (rowToDelete.Values.Count == 1) return CheckAndDeleteAsyncInternal(rowToDelete.Values[0], rowToDelete.Key);

            return Task.WhenAll(rowToDelete.Values.Select(cell => CheckAndDeleteAsyncInternal(cell, rowToDelete.Key)))
                .ContinueWith(tasks => tasks.GetAwaiter().GetResult().All(_ => _));

            Task<bool> CheckAndDeleteAsyncInternal(Cell cell, ByteString key)
            {
                var row = new CellSet.Types.Row { Key = key };

                row.Values.Add(cell);
                var cellSet = new CellSet();
                cellSet.Rows.Add(row);

                return StoreCellsAsyncInternal(table, cellSet, Encoding.UTF8.GetString(row.Key.ToByteArray()), CheckAndDeleteQuery);
            }
        }

        /// <summary>
        /// Stores the given cells in the supplied table.
        /// </summary>
        /// <param name="table">the table</param>
        /// <param name="cells">the cells to insert</param>
        /// <returns>a task that is awaitable, signifying the end of this operation</returns>
        public Task StoreCellsAsync(string table, CellSet cells)
        {
            table.ArgumentNotNullNorEmpty("table");
            cells.ArgumentNotNull("cells");

            return StoreCellsAsyncInternal(table, cells);
        }

        private async Task<bool> StoreCellsAsyncInternal(string table, CellSet cells, string key = null, string query = null)
        {
            var path = key == null ? table + "/somefalsekey" : table + "/" + key;
            // note the fake row key to insert a set of cells
            using (var webResponse = await PutRequestAsync(path, query, cells).ConfigureAwait(false))
            {
                if (webResponse.WebResponse.StatusCode == HttpStatusCode.NotModified) return false;

                if (webResponse.WebResponse.StatusCode != HttpStatusCode.OK)
                {
                    using (var output = new StreamReader(await webResponse.WebResponse.Content.ReadAsStreamAsync().ConfigureAwait(false)))
                    {
                        var message = output.ReadToEnd();
                        throw new WebException($"Couldn't insert into table {table}! Response code was: {webResponse.WebResponse.StatusCode}, expected 200! Response body was: {message}");
                    }
                }
            }
            return true;
        }

        private Task<Response> DeleteRequestAsync<TReq>(string endpoint, TReq request)
           where TReq : IMessage<TReq> =>
            ExecuteMethodAsync(HttpMethod.Delete, null, endpoint, request);

        private Task<Response> ExecuteMethodAsync<TReq>(HttpMethod method, string query, string endpoint, TReq request) where TReq : IMessage<TReq>
        {
            endpoint.ArgumentNotNull("endpoint");

            return _requester.IssueWebRequestAsync(endpoint, query, method, request?.ToByteArray());
        }

        private async Task<T> GetRequestAndDeserializeAsync<T>(string endpoint, string query) where T : IMessage<T>, new()
        {
            endpoint.ArgumentNotNull("endpoint");

            using (var response = await _requester.IssueWebRequestAsync(endpoint, query, HttpMethod.Get, null).ConfigureAwait(false))
            {
                response.WebResponse.EnsureSuccessStatusCode();

                using (var responseStream = await response.WebResponse.Content.ReadAsStreamAsync().ConfigureAwait(false))
                {
                    var parser = new MessageParser<T>(() => new T());

                    return parser.ParseFrom(responseStream);
                }
            }
        }

        private Task<Response> GetRequestAsync(string endpoint, string query)
        {
            endpoint.ArgumentNotNull("endpoint");

            return _requester.IssueWebRequestAsync(endpoint, query, HttpMethod.Get, null);
        }

        private Task<Response> PostRequestAsync<TReq>(string endpoint, TReq request) where TReq : IMessage<TReq> =>
            ExecuteMethodAsync(HttpMethod.Post, null, endpoint, request);

        private Task<Response> PutRequestAsync<TReq>(string endpoint, string query, TReq request) where TReq : IMessage<TReq> =>
            ExecuteMethodAsync(HttpMethod.Put, query, endpoint, request);

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting
        /// unmanaged resources.
        /// </summary>
        /// <remarks>
        /// Since this class is <see langword="sealed"/>, the standard <see
        /// cref="IDisposable.Dispose"/> pattern is not required. Also, <see
        /// cref="GC.SuppressFinalize"/> is not needed.
        /// </remarks>
        public void Dispose()
        {
            if (!_disposed)
            {
                if (_requester != null)
                {
                    _requester.Dispose();
                    _requester = null;
                }
                _disposed = true;
            }
        }
    }
}