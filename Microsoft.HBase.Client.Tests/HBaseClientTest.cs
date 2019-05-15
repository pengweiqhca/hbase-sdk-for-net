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

namespace Microsoft.HBase.Client.Tests
{
    using Google.Protobuf;
    using Microsoft.HBase.Client.Tests.Utilities;
    using Org.Apache.Hadoop.Hbase.Rest.Protobuf.Generated;
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Net.Http;
    using System.Text;
    using System.Threading.Tasks;
    using Xunit;

    public class HBaseClientTest : DisposableContextSpecification
    {
        // TEST TODOS:
        // TODO: add test for ModifyTableSchema

        private const string TestTablePrefix = "marlintest";
        private readonly Random _random = new Random();

        public string TestTableName;
        private TableSchema _testTableSchema;

        public HBaseClientTest()
        {
            var client = CreateClient();

            // ensure tables from previous tests are cleaned up
            var tables = client.ListTablesAsync().Result;
            foreach (var name in tables.Name)
            {
                if (name.StartsWith(TestTablePrefix, StringComparison.Ordinal))
                {
                    client.DeleteTableAsync(name).Wait();
                }
            }

            // add a table specific to this test
            TestTableName = TestTablePrefix + _random.Next(10000);
            _testTableSchema = new TableSchema();
            _testTableSchema.Name = TestTableName;
            _testTableSchema.Columns.Add(new ColumnSchema { Name = "d", MaxVersions = 3 });

            client.CreateTableAsync(_testTableSchema).Wait();
        }

        public IHBaseClient CreateClient()
        {
            var options = RequestOptionsFactory.GetDefaultOptions();
            options.Timeout = TimeSpan.FromMilliseconds(30000);
            return new HBaseClient(options);
        }

        [Fact]
        public void TestFullScan()
        {
            var client = CreateClient();

            StoreTestData(client);



            // full range scan
            var scanSettings = new Scanner { Batch = 10 };
            ScannerInformation scannerInfo = null;
            try
            {
                scannerInfo = client.CreateScannerAsync(TestTableName, scanSettings).Result;

                CellSet next;
                var expectedSet = new HashSet<int>(Enumerable.Range(0, 100));
                while ((next = client.ScannerGetNextAsync(scannerInfo).Result) != null)
                {
                    Assert.Equal(10, next.Rows.Count);
                    foreach (var row in next.Rows)
                    {
                        var k = BitConverter.ToInt32(row.Key.ToByteArray(), 0);
                        expectedSet.Remove(k);
                    }
                }
                Assert.True(0 == expectedSet.Count, $"The expected set wasn't empty! Items left {string.Join(",", expectedSet)}!");
            }
            finally
            {
                if (scannerInfo != null)
                {
                    client.DeleteScannerAsync(TestTableName, scannerInfo).Wait();
                }
            }
        }

        [Fact]
        public async Task TestCellsDeletion()
        {
            const string testKey = "content";
            const string testValue = "the force is strong in this column";

            var client = CreateClient();
            var set = new CellSet();
            var row = new CellSet.Types.Row { Key = ByteString.CopyFromUtf8(testKey) };
            set.Rows.Add(row);

            var value = new Cell { Column = ByteString.CopyFromUtf8("d:starwars"), Data = ByteString.CopyFromUtf8(testValue) };
            row.Values.Add(value);

            client.StoreCellsAsync(TestTableName, set).Wait();
            var cell = client.GetCellsAsync(TestTableName, testKey).Result;
            // make sure the cell is in the table
            Assert.Equal(Encoding.UTF8.GetString(cell.Rows[0].Key.ToByteArray()), testKey);
            // delete cell
            client.DeleteCellsAsync(TestTableName, testKey).Wait();

            Assert.Null(await client.GetCellsAsync(TestTableName, testKey));
        }

        [Fact]

        public void TestGetStorageClusterStatus()
        {
            var client = CreateClient();
            var status = client.GetStorageClusterStatusAsync().Result;
            // TODO not really a good test
            Assert.True(status.Requests >= 0, "number of requests is negative");
            Assert.True(status.LiveNodes.Count >= 1, "number of live nodes is zero or negative");
            Assert.True(status.LiveNodes[0].Requests >= 0, "number of requests to the first node is negative");
        }

        [Fact]

        public void TestGetVersion()
        {
            var client = CreateClient();
            var version = client.GetVersionAsync().Result;

            Trace.WriteLine(version);

            version.RestVersion.ShouldNotBeNullOrEmpty();
            version.JvmVersion.ShouldNotBeNullOrEmpty();
            version.OsVersion.ShouldNotBeNullOrEmpty();
            version.ServerVersion.ShouldNotBeNullOrEmpty();
            version.JerseyVersion.ShouldNotBeNullOrEmpty();
        }

        [Fact]

        public void TestListTables()
        {
            var client = CreateClient();

            var tables = client.ListTablesAsync().Result;
            var testtables = tables.Name.Where(item => item.StartsWith("marlintest", StringComparison.Ordinal)).ToList();
            Assert.Single(testtables);
            Assert.Equal(TestTableName, testtables[0]);
        }

        [Fact]

        public void TestStoreSingleCell()
        {
            const string testKey = "content";
            const string testValue = "the force is strong in this column";
            var client = CreateClient();
            var set = new CellSet();
            var row = new CellSet.Types.Row { Key = ByteString.CopyFromUtf8(testKey) };
            set.Rows.Add(row);

            var value = new Cell { Column = ByteString.CopyFromUtf8("d:starwars"), Data = ByteString.CopyFromUtf8(testValue) };
            row.Values.Add(value);

            client.StoreCellsAsync(TestTableName, set).Wait();

            var cells = client.GetCellsAsync(TestTableName, testKey).Result;
            Assert.Single(cells.Rows);
            Assert.Single(cells.Rows[0].Values);
            Assert.Equal(testValue, cells.Rows[0].Values[0].Data.ToStringUtf8());
        }

        [Fact]

        public void TestGetCellsWithMultiGetRequest()
        {
            var testKey1 = Guid.NewGuid().ToString();
            var testKey2 = Guid.NewGuid().ToString();
            var testValue1 = "the force is strong in this Column " + testKey1;
            var testValue2 = "the force is strong in this Column " + testKey2;

            var client = CreateClient();
            var set = new CellSet();
            var row1 = new CellSet.Types.Row { Key = ByteString.CopyFromUtf8(testKey1) };
            var row2 = new CellSet.Types.Row { Key = ByteString.CopyFromUtf8(testKey2) };
            set.Rows.Add(row1);
            set.Rows.Add(row2);

            var value1 = new Cell { Column = ByteString.CopyFromUtf8("d:starwars"), Data = ByteString.CopyFromUtf8(testValue1) };
            var value2 = new Cell { Column = ByteString.CopyFromUtf8("d:starwars"), Data = ByteString.CopyFromUtf8(testValue2) };
            row1.Values.Add(value1);
            row2.Values.Add(value2);

            client.StoreCellsAsync(TestTableName, set).Wait();

            var cells = client.GetCellsAsync(TestTableName, new[] { testKey1, testKey2 }).Result;
            Assert.Equal(2, cells.Rows.Count);
            Assert.Single(cells.Rows[0].Values);
            Assert.Equal(testValue1, cells.Rows[0].Values[0].Data.ToStringUtf8());
            Assert.Single(cells.Rows[1].Values);
            Assert.Equal(testValue2, cells.Rows[1].Values[0].Data.ToStringUtf8());
        }

        [Fact]
        public void TestSubsetScan()
        {
            var client = CreateClient();
            const int startRow = 15;
            const int endRow = 15 + 13;
            StoreTestData(client);

            // subset range scan
            var scanSettings = new Scanner { Batch = 10, StartRow = ByteString.CopyFrom(BitConverter.GetBytes(startRow)), EndRow = ByteString.CopyFrom(BitConverter.GetBytes(endRow)) };

            ScannerInformation scannerInfo = null;
            try
            {
                scannerInfo = client.CreateScannerAsync(TestTableName, scanSettings).Result;

                CellSet next;
                var expectedSet = new HashSet<int>(Enumerable.Range(startRow, endRow - startRow));
                while ((next = client.ScannerGetNextAsync(scannerInfo).Result) != null)
                {
                    foreach (var row in next.Rows)
                    {
                        var k = BitConverter.ToInt32(row.Key.ToByteArray(), 0);
                        expectedSet.Remove(k);
                    }
                }
                Assert.True(0 == expectedSet.Count, $"The expected set wasn't empty! Items left {string.Join(",", expectedSet)}!");
            }
            finally
            {
                if (scannerInfo != null)
                {
                    client.DeleteScannerAsync(TestTableName, scannerInfo).Wait();
                }
            }
        }

        [Fact]

        public void TestTableSchema()
        {
            var client = CreateClient();
            var schema = client.GetTableSchemaAsync(TestTableName).Result;
            Assert.Equal(TestTableName, schema.Name);
            Assert.Equal(_testTableSchema.Columns.Count, schema.Columns.Count);
            Assert.Equal(_testTableSchema.Columns[0].Name, schema.Columns[0].Name);
        }

        private void StoreTestData(IHBaseClient hbaseClient)
        {
            // we are going to insert the keys 0 to 100 and then do some range queries on that
            const string testValue = "the force is strong in this column";
            var set = new CellSet();
            for (var i = 0; i < 100; i++)
            {
                var row = new CellSet.Types.Row { Key = ByteString.CopyFrom(BitConverter.GetBytes(i)) };
                var value = new Cell { Column = ByteString.CopyFromUtf8("d:starwars"), Data = ByteString.CopyFromUtf8(testValue) };
                row.Values.Add(value);
                set.Rows.Add(row);
            }

            hbaseClient.StoreCellsAsync(TestTableName, set).Wait();
        }

        [Fact]

        public void TestScannerCreation()
        {
            var client = CreateClient();
            var scanSettings = new Scanner { Batch = 2 };

            ScannerInformation scannerInfo = null;
            try
            {
                scannerInfo = client.CreateScannerAsync(TestTableName, scanSettings).Result;
                Assert.Equal(TestTableName, scannerInfo.TableName);
                Assert.NotNull(scannerInfo.ScannerId);
                Assert.False(scannerInfo.ScannerId.StartsWith("/"), "scanner id starts with a slash");
                Assert.NotNull(scannerInfo.ResponseHeaderCollection);
            }
            finally
            {
                if (scannerInfo != null)
                {
                    client.DeleteScannerAsync(TestTableName, scannerInfo).Wait();
                }
            }
        }

        [Fact]
        public void TestScannerDeletion()
        {
            var client = CreateClient();

            // full range scan
            var scanSettings = new Scanner { Batch = 10 };

            ScannerInformation scannerInfo = null;

            try
            {
                scannerInfo = client.CreateScannerAsync(TestTableName, scanSettings).Result;
                Assert.Equal(TestTableName, scannerInfo.TableName);
                Assert.NotNull(scannerInfo.ScannerId);
                Assert.False(scannerInfo.ScannerId.StartsWith("/"), "scanner id starts with a slash");
                Assert.NotNull(scannerInfo.ResponseHeaderCollection);
                // delete the scanner
                client.DeleteScannerAsync(TestTableName, scannerInfo).Wait();
                // try to fetch data use the deleted scanner

                client.ScannerGetNextAsync(scannerInfo).Wait();
            }
            catch (AggregateException ex) when (ex.InnerException is HttpRequestException hre && hre.Message.Contains("404")) { }
            finally
            {
                if (scannerInfo != null)
                {
                    client.DeleteScannerAsync(TestTableName, scannerInfo).Wait();
                }
            }
        }
    }
}
