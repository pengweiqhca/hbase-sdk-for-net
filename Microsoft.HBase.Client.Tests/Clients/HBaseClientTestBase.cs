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

namespace Microsoft.HBase.Client.Tests.Clients
{
    using Google.Protobuf;
    using Microsoft.HBase.Client.Tests.Utilities;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Org.Apache.Hadoop.Hbase.Rest.Protobuf.Generated;
    using System;
    using System.Diagnostics;
    using System.Linq;
    using System.Text;

    public abstract class HBaseClientTestBase : DisposableContextSpecification
    {
        // TEST TODOS:
        // TODO: add test for ModifyTableSchema

        private const string TestTablePrefix = "marlintest";
        private readonly Random _random = new Random();

        public string TestTableName;
        private TableSchema _testTableSchema;

        protected override void Context()
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

        public abstract IHBaseClient CreateClient();

        [TestMethod]
        [TestCategory(TestRunMode.CheckIn)]
        public abstract void TestFullScan();

        [TestMethod]
        [TestCategory(TestRunMode.CheckIn)]
        [ExpectedException(typeof(System.AggregateException), "The remote server returned an error: (404) Not Found.")]
        public void TestCellsDeletion()
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
            Assert.AreEqual(Encoding.UTF8.GetString(cell.Rows[0].Key.ToByteArray()), testKey);
            // delete cell
            client.DeleteCellsAsync(TestTableName, testKey).Wait();
            // get cell again, 404 exception expected
            client.GetCellsAsync(TestTableName, testKey).Wait();
        }

        [TestMethod]
        [TestCategory(TestRunMode.CheckIn)]
        public void TestGetStorageClusterStatus()
        {
            var client = CreateClient();
            var status = client.GetStorageClusterStatusAsync().Result;
            // TODO not really a good test
            Assert.IsTrue(status.Requests >= 0, "number of requests is negative");
            Assert.IsTrue(status.LiveNodes.Count >= 1, "number of live nodes is zero or negative");
            Assert.IsTrue(status.LiveNodes[0].Requests >= 0, "number of requests to the first node is negative");
        }

        [TestMethod]
        [TestCategory(TestRunMode.CheckIn)]
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

        [TestMethod]
        [TestCategory(TestRunMode.CheckIn)]
        public void TestListTables()
        {
            var client = CreateClient();

            var tables = client.ListTablesAsync().Result;
            var testtables = tables.Name.Where(item => item.StartsWith("marlintest", StringComparison.Ordinal)).ToList();
            Assert.AreEqual(1, testtables.Count);
            Assert.AreEqual(TestTableName, testtables[0]);
        }

        [TestMethod]
        [TestCategory(TestRunMode.CheckIn)]
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
            Assert.AreEqual(1, cells.Rows.Count);
            Assert.AreEqual(1, cells.Rows[0].Values.Count);
            Assert.AreEqual(testValue, cells.Rows[0].Values[0].Data.ToStringUtf8());
        }

        [TestMethod]
        [TestCategory(TestRunMode.CheckIn)]
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
            Assert.AreEqual(2, cells.Rows.Count);
            Assert.AreEqual(1, cells.Rows[0].Values.Count);
            Assert.AreEqual(testValue1, cells.Rows[0].Values[0].Data.ToStringUtf8());
            Assert.AreEqual(1, cells.Rows[1].Values.Count);
            Assert.AreEqual(testValue2, cells.Rows[1].Values[0].Data.ToStringUtf8());
        }

        [TestMethod]
        [TestCategory(TestRunMode.CheckIn)]
        public abstract void TestSubsetScan();

        [TestMethod]
        [TestCategory(TestRunMode.CheckIn)]
        public void TestTableSchema()
        {
            var client = CreateClient();
            var schema = client.GetTableSchemaAsync(TestTableName).Result;
            Assert.AreEqual(TestTableName, schema.Name);
            Assert.AreEqual(_testTableSchema.Columns.Count, schema.Columns.Count);
            Assert.AreEqual(_testTableSchema.Columns[0].Name, schema.Columns[0].Name);
        }

        public void StoreTestData(IHBaseClient hbaseClient)
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
    }
}
