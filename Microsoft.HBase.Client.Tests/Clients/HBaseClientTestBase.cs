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

namespace Microsoft.HBase.Client.Tests.Clients
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;
    using Microsoft.HBase.Client.Tests.Utilities;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using org.apache.hadoop.hbase.rest.protobuf.generated;

    public abstract class HBaseClientTestBase : DisposableContextSpecification
    {
        // TEST TODOS:
        // TODO: add test for ModifyTableSchema

        private const string TestTablePrefix = "marlintest";
        private readonly Random _random = new Random();

        public string testTableName;
        private TableSchema _testTableSchema;

        protected override void Context()
        {
            var client = CreateClient();

            // ensure tables from previous tests are cleaned up
            TableList tables = client.ListTablesAsync().Result;
            foreach (string name in tables.name)
            {
                if (name.StartsWith(TestTablePrefix, StringComparison.Ordinal))
                {
                    client.DeleteTableAsync(name).Wait();
                }
            }

            // add a table specific to this test
            testTableName = TestTablePrefix + _random.Next(10000);
            _testTableSchema = new TableSchema();
            _testTableSchema.name = testTableName;
            _testTableSchema.columns.Add(new ColumnSchema { name = "d", maxVersions = 3 });

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
            var row = new CellSet.Row { key = Encoding.UTF8.GetBytes(testKey) };
            set.rows.Add(row);

            var value = new Cell { column = Encoding.UTF8.GetBytes("d:starwars"), data = Encoding.UTF8.GetBytes(testValue) };
            row.values.Add(value);

            client.StoreCellsAsync(testTableName, set).Wait();
            CellSet cell = client.GetCellsAsync(testTableName, testKey).Result;
            // make sure the cell is in the table
            Assert.AreEqual(Encoding.UTF8.GetString(cell.rows[0].key), testKey);
            // delete cell
            client.DeleteCellsAsync(testTableName, testKey).Wait();
            // get cell again, 404 exception expected
            client.GetCellsAsync(testTableName, testKey).Wait();
        }

        [TestMethod]
        [TestCategory(TestRunMode.CheckIn)]
        public void TestGetStorageClusterStatus()
        {
            var client = CreateClient();
            StorageClusterStatus status = client.GetStorageClusterStatusAsync().Result;
            // TODO not really a good test
            Assert.IsTrue(status.requests >= 0, "number of requests is negative");
            Assert.IsTrue(status.liveNodes.Count >= 1, "number of live nodes is zero or negative");
            Assert.IsTrue(status.liveNodes[0].requests >= 0, "number of requests to the first node is negative");
        }

        [TestMethod]
        [TestCategory(TestRunMode.CheckIn)]
        public void TestGetVersion()
        {
            var client = CreateClient();
            org.apache.hadoop.hbase.rest.protobuf.generated.Version version = client.GetVersionAsync().Result;

            Trace.WriteLine(version);

            version.stargateVersion.ShouldNotBeNullOrEmpty();
            version.jvmVersion.ShouldNotBeNullOrEmpty();
            version.osVersion.ShouldNotBeNullOrEmpty();
            version.serverVersion.ShouldNotBeNullOrEmpty();
            version.jerseyVersion.ShouldNotBeNullOrEmpty();
        }

        [TestMethod]
        [TestCategory(TestRunMode.CheckIn)]
        public void TestListTables()
        {
            var client = CreateClient();

            TableList tables = client.ListTablesAsync().Result;
            List<string> testtables = tables.name.Where(item => item.StartsWith("marlintest", StringComparison.Ordinal)).ToList();
            Assert.AreEqual(1, testtables.Count);
            Assert.AreEqual(testTableName, testtables[0]);
        }

        [TestMethod]
        [TestCategory(TestRunMode.CheckIn)]
        public void TestStoreSingleCell()
        {
            const string testKey = "content";
            const string testValue = "the force is strong in this column";
            var client = CreateClient();
            var set = new CellSet();
            var row = new CellSet.Row { key = Encoding.UTF8.GetBytes(testKey) };
            set.rows.Add(row);

            var value = new Cell { column = Encoding.UTF8.GetBytes("d:starwars"), data = Encoding.UTF8.GetBytes(testValue) };
            row.values.Add(value);

            client.StoreCellsAsync(testTableName, set).Wait();

            CellSet cells = client.GetCellsAsync(testTableName, testKey).Result;
            Assert.AreEqual(1, cells.rows.Count);
            Assert.AreEqual(1, cells.rows[0].values.Count);
            Assert.AreEqual(testValue, Encoding.UTF8.GetString(cells.rows[0].values[0].data));
        }

        [TestMethod]
        [TestCategory(TestRunMode.CheckIn)]
        public void TestGetCellsWithMultiGetRequest()
        {
            string testKey1 = Guid.NewGuid().ToString();
            string testKey2 = Guid.NewGuid().ToString();
            string testValue1 = "the force is strong in this column " + testKey1;
            string testValue2 = "the force is strong in this column " + testKey2;

            var client = CreateClient();
            var set = new CellSet();
            var row1 = new CellSet.Row { key = Encoding.UTF8.GetBytes(testKey1) };
            var row2 = new CellSet.Row { key = Encoding.UTF8.GetBytes(testKey2) };
            set.rows.Add(row1);
            set.rows.Add(row2);

            var value1 = new Cell { column = Encoding.UTF8.GetBytes("d:starwars"), data = Encoding.UTF8.GetBytes(testValue1) };
            var value2 = new Cell { column = Encoding.UTF8.GetBytes("d:starwars"), data = Encoding.UTF8.GetBytes(testValue2) };
            row1.values.Add(value1);
            row2.values.Add(value2);

            client.StoreCellsAsync(testTableName, set).Wait();

            CellSet cells = client.GetCellsAsync(testTableName, new string[] { testKey1, testKey2 }).Result;
            Assert.AreEqual(2, cells.rows.Count);
            Assert.AreEqual(1, cells.rows[0].values.Count);
            Assert.AreEqual(testValue1, Encoding.UTF8.GetString(cells.rows[0].values[0].data));
            Assert.AreEqual(1, cells.rows[1].values.Count);
            Assert.AreEqual(testValue2, Encoding.UTF8.GetString(cells.rows[1].values[0].data));
        }

        [TestMethod]
        [TestCategory(TestRunMode.CheckIn)]
        public abstract void TestSubsetScan();

        [TestMethod]
        [TestCategory(TestRunMode.CheckIn)]
        public void TestTableSchema()
        {
            var client = CreateClient();
            TableSchema schema = client.GetTableSchemaAsync(testTableName).Result;
            Assert.AreEqual(testTableName, schema.name);
            Assert.AreEqual(_testTableSchema.columns.Count, schema.columns.Count);
            Assert.AreEqual(_testTableSchema.columns[0].name, schema.columns[0].name);
        }

        public void StoreTestData(IHBaseClient hbaseClient)
        {
            // we are going to insert the keys 0 to 100 and then do some range queries on that
            const string testValue = "the force is strong in this column";
            var set = new CellSet();
            for (int i = 0; i < 100; i++)
            {
                var row = new CellSet.Row { key = BitConverter.GetBytes(i) };
                var value = new Cell { column = Encoding.UTF8.GetBytes("d:starwars"), data = Encoding.UTF8.GetBytes(testValue) };
                row.values.Add(value);
                set.rows.Add(row);
            }

            hbaseClient.StoreCellsAsync(testTableName, set).Wait();
        }
    }
}
