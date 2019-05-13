// Copyright (c) Microsoft Corporation
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

using System.Globalization;

namespace Microsoft.HBase.Client.Tests
{
    using Google.Protobuf;
    using Microsoft.HBase.Client.Tests.Utilities;
    using Org.Apache.Hadoop.Hbase.Rest.Protobuf.Generated;
    using System;
    using System.Linq;
    using System.Net.Http;
    using System.Threading.Tasks;
    using Xunit;


    public class CellOperationsTests : DisposableContextSpecification
    {
        private const string TableNamePrefix = "celltest";

        private const string ColumnFamilyName1 = "first";
        private const string ColumnFamilyName2 = "second";

        private static bool _arrangementCompleted;
        private static string _tableName;
        private static TableSchema _tableSchema;

        public CellOperationsTests()
        {
            if (!_arrangementCompleted)
            {
                var client = GetClient();
                // ensure tables from previous tests are cleaned up
                var tables = client.ListTablesAsync().Result;
                foreach (var name in tables.Name)
                {
                    var pinnedName = name;
                    if (name.StartsWith(TableNamePrefix, StringComparison.Ordinal))
                    {
                        client.DeleteTableAsync(pinnedName).Wait();
                    }
                }

                AddTable();
                _arrangementCompleted = true;
            }
        }

        private HBaseClient GetClient()
        {
            var options = RequestOptions.GetDefaultOptions();

            var client = new HBaseClient(options);
            #region VNet
            //options.TimeoutMillis = 30000;
            //options.KeepAlive = false;
            //options.Port = 8090;
            //options.AlternativeEndpoint = "/";
            //var client = new HBaseClient(null, options, new LoadBalancerRoundRobin(new List<string> { "ip address" }));
            #endregion

            return client;
        }

        [Fact]
        public void WhenIDeleteCellsWithTimeStampICanAddWithHigherTimestamp()
        {
            var client = GetClient();

            client.StoreCellsAsync(_tableName, CreateCellSet(GetCellSet("1", "c1", "1A", 10))).Wait();
            client.StoreCellsAsync(_tableName, CreateCellSet(GetCellSet("1", "c2", "1A", 10))).Wait();

            client.DeleteCellsAsync(_tableName, "1", ColumnFamilyName1, 10).Wait();

            try
            {
                client.GetCellsAsync(_tableName, "1").Wait();
                Assert.True(false, "Expected to throw an exception as the row is deleted");
            }
            catch (AggregateException ex) when (ex.InnerException is HttpRequestException exception)
            {
                exception.Message.ShouldContain("404");
            }

            client.StoreCellsAsync(_tableName, CreateCellSet(GetCellSet("1", "c1", "1A", 11))).Wait();

            var retrievedCells = client.GetCellsAsync(_tableName, "1").Result;

            retrievedCells.Rows.Count.ShouldEqual(1);
            retrievedCells.Rows[0].Values[0].Timestamp.ShouldEqual(11);
        }

        [Fact]

        public void WhenIDeleteCellsWithTimeStampICannotAddWithLowerTimestamp()
        {
            var client = GetClient();

            client.StoreCellsAsync(_tableName, CreateCellSet(GetCellSet("2", "c1", "1A", 10))).Wait();
            client.StoreCellsAsync(_tableName, CreateCellSet(GetCellSet("2", "c2", "1A", 10))).Wait();

            client.DeleteCellsAsync(_tableName, "2", ColumnFamilyName1, 10).Wait();

            try
            {
                client.GetCellsAsync(_tableName, "2").Wait();
                Assert.True(false, "Expected to throw an exception as the row is deleted");
            }
            catch (AggregateException ex) when (ex.InnerException is HttpRequestException exception)
            {
                exception.Message.ShouldContain("404");
            }

            client.StoreCellsAsync(_tableName, CreateCellSet(GetCellSet("2", "c1", "1A", 9))).Wait();

            try
            {
                client.GetCellsAsync(_tableName, "2").Wait();
                Assert.True(false, "Expected to throw an exception as the row cannot be added with lower timestamp");
            }
            catch (AggregateException ex) when (ex.InnerException is HttpRequestException exception)
            {
                exception.Message.ShouldContain("404");
            }
        }


        [Fact]

        public async Task WhenICheckAndDeleteCellsWithTimeStampICannotAddWithLowerTimestampThanHbaseserver()
        {
            var client = GetClient();

            client.StoreCellsAsync(_tableName, CreateCellSet(GetCellSet("3", "c1", "1A", 10))).Wait();
            client.StoreCellsAsync(_tableName, CreateCellSet(GetCellSet("3", "c2", "1A", 10))).Wait();

            var deleted = await client.CheckAndDeleteAsync(_tableName, GetCell("3", "c1", "1A", 10));

            deleted.ShouldEqual(true);

            var retrievedCells = client.GetCellsAsync(_tableName, "3").Result;
            // Deletes in the Cell c1 so c2 should be present.
            retrievedCells.Rows[0].Values.Count.ShouldEqual(1);

            deleted = await client.CheckAndDeleteAsync(_tableName, GetCell("3", "c2", "1A", 10));
            deleted.ShouldEqual(true);

            try
            {
                // All  cells are deleted so this should fail
                await client.GetCellsAsync(_tableName, "3");
                Assert.True(false, "expecting Get '3' to fail as all cells are removed");
            }
            catch (HttpRequestException ex)
            {
                ex.Message.ShouldContain("404");
            }

            client.StoreCellsAsync(_tableName, CreateCellSet(GetCellSet("3", "c1", "1B", 11))).Wait();

            try
            {
                await client.GetCellsAsync(_tableName, "3");
                Assert.True(false, "Expected to throw an exception as the row cannot be added with lower timestamp than servers timestamp");
            }
            catch (HttpRequestException ex)
            {
                ex.Message.ShouldContain("404");
            }
        }


        // These need fixes from https://issues.apache.org/jira/browse/HBASE-15323
        [Fact]
        public async Task WhenICheckAndDeleteCellsWithTimeStampAndCellsToDeleteICanAddWithHigherTimestamp()
        {
            var client = GetClient();

            var cellSets = CreateCellSet(GetCellSet("3", "c1", "1A", 10), GetCellSet("3", "c2", "1A", 20));

            await client.StoreCellsAsync(_tableName, cellSets);

            // Deletes all the ColumnFamily with timestamp less than 10
            var rowToDelete = new CellSet.Types.Row { Key = ByteString.CopyFromUtf8("3") };
            rowToDelete.Values.AddRange(cellSets.Rows.SelectMany(row => row.Values));
            var deleted = await client.CheckAndDeleteAsync(_tableName, cellSets.Rows[0].Values[0], rowToDelete);

            deleted.ShouldEqual(true);

            try
            {
                // All  cells are deleted so this should fail
                await client.GetCellsAsync(_tableName, "3");
                Assert.True(false, "expecting Get '3' to fail as all cells are removed");
            }
            catch (HttpRequestException ex)
            {
                ex.Message.ShouldContain("404");
            }

            client.StoreCellsAsync(_tableName, CreateCellSet(GetCellSet("3", "c1", "1B", 11))).Wait();

            try
            {
                var retrievedCells = client.GetCellsAsync(_tableName, "3").Result;
                retrievedCells.Rows[0].Values.Count.ShouldEqual(1);
                retrievedCells.Rows[0].Values[0].Column.ToStringUtf8().ShouldBeEqualOrdinalIgnoreCase("c1");
            }
            catch (AggregateException ex) when (ex.InnerException is HttpRequestException exception)
            {
                exception.Message.ShouldContain("404");
            }
        }

        // These need fixes from https://issues.apache.org/jira/browse/HBASE-15323
        [Fact]
        public async Task WhenICheckAndDeleteCellsWithColumnFamilyDeletesAllCells()
        {
            var client = GetClient();

            var cellSets = CreateCellSet(GetCellSet("3", "c1", "1A", 10), GetCellSet("3", "c2", "1A", 20));

            await client.StoreCellsAsync(_tableName, cellSets);

            // Deletes all the ColumnFamily with timestamp less than 10
            var rowToDelete = new CellSet.Types.Row { Key = ByteString.CopyFromUtf8("3") };
            rowToDelete.Values.Add(new Cell { Row = rowToDelete.Key, Column = ByteString.CopyFromUtf8(ColumnFamilyName1), Timestamp = 10 });
            var deleted = await client.CheckAndDeleteAsync(_tableName, cellSets.Rows[1].Values[0], rowToDelete);

            deleted.ShouldEqual(true);

            try
            {
                // All  cells are deleted so this should fail
                await client.GetCellsAsync(_tableName, "3");
                Assert.True(false, "expecting Get '3' to fail as all cells are removed");
            }
            catch (HttpRequestException ex)
            {
                ex.Message.ShouldContain("404");
            }

            client.StoreCellsAsync(_tableName, CreateCellSet(GetCellSet("3", "c1", "1B", 11))).Wait();

            try
            {
                var retrievedCells = client.GetCellsAsync(_tableName, "3").Result;
                retrievedCells.Rows[0].Values.Count.ShouldEqual(1);
            }
            catch (AggregateException ex) when (ex.InnerException is HttpRequestException exception)
            {
                exception.Message.ShouldContain("404");
            }
        }

        private CellSet CreateCellSet(params CellSet.Types.Row[] rows)
        {
            var cellSet = new CellSet();
            cellSet.Rows.AddRange(rows);
            return cellSet;
        }

        private Cell GetCell(string key, string columnName, string value = null, long timestamp = 0)
        {
            var cell = new Cell { Column = BuildCellColumn(ColumnFamilyName1, columnName), Row = ByteString.CopyFromUtf8(key) };
            if (value != null)
            {
                cell.Data = ByteString.CopyFromUtf8(value);
            }
            if (timestamp > 0)
            {
                cell.Timestamp = timestamp;
            }
            return cell;
        }
        private CellSet.Types.Row GetCellSet(string key, string columnName, string value, long timestamp)
        {
            var row = new CellSet.Types.Row { Key = ByteString.CopyFromUtf8(key) };
            var c1 = new Cell { Column = BuildCellColumn(ColumnFamilyName1, columnName), Row = row.Key };
            if (value != null)
            {
                c1.Data = ByteString.CopyFromUtf8(value);
            }

            if (timestamp > 0)
            {
                c1.Timestamp = timestamp;
            }
            row.Values.Add(c1);
            return row;
        }

        private ByteString BuildCellColumn(string columnFamilyName, string columnName)
        {
            return ByteString.CopyFromUtf8(string.Format(CultureInfo.InvariantCulture, "{0}:{1}", columnFamilyName, columnName));
        }

        private void AddTable()
        {
            // add a table specific to this test
            var client = GetClient();
            _tableName = TableNamePrefix + Guid.NewGuid().ToString("N");
            _tableSchema = new TableSchema { Name = _tableName };
            _tableSchema.Columns.Add(new ColumnSchema { Name = ColumnFamilyName1 });
            _tableSchema.Columns.Add(new ColumnSchema { Name = ColumnFamilyName2 });

            client.CreateTableAsync(_tableSchema).Wait();
        }
    }
}
