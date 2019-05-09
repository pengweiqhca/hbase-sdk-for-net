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
    using Microsoft.HBase.Client.LoadBalancing;
    using Microsoft.HBase.Client.Tests.Utilities;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Org.Apache.Hadoop.Hbase.Rest.Protobuf.Generated;
    using System;
    using System.Collections.Generic;
    using System.Linq;

    [TestClass]
    public class VNetClientTest : HBaseClientTestBase
    {
        public override IHBaseClient CreateClient()
        {
            var regionServerIPs = new List<string>();
            // TODO automatically retrieve IPs from Ambari REST APIs
            regionServerIPs.Add("10.17.0.11");
            regionServerIPs.Add("10.17.0.13");

            var options = RequestOptions.GetDefaultOptions();
            options.Port = 8090;
            options.AlternativeEndpoint = "";

            return new HBaseClient(null, options, new LoadBalancerRoundRobin(regionServerIPs));
        }

        [TestMethod]
        [TestCategory(TestRunMode.CheckIn)]
        public override void TestFullScan()
        {
            var client = CreateClient();

            StoreTestData(client);
            var expectedSet = new HashSet<int>(Enumerable.Range(0, 100));
            var cells = client.StatelessScannerAsync(TestTableName).Result;
            foreach (var cell in cells)
            {
                foreach (var row in cell.Rows)
                {
                    var k = BitConverter.ToInt32(row.Key.ToByteArray(), 0);
                    expectedSet.Remove(k);
                }
            }
            Assert.AreEqual(0, expectedSet.Count, "The expected set wasn't empty! Items left {0}!", string.Join(",", expectedSet));
        }

        [TestMethod]
        [TestCategory(TestRunMode.CheckIn)]
        public override void TestSubsetScan()
        {
            var client = CreateClient();
            const int startRow = 10;
            const int endRow = 10 + 5;
            StoreTestData(client);
            var expectedSet = new HashSet<int>(Enumerable.Range(startRow, endRow - startRow));
            // TODO how to change rowkey to internal hbase binary string
            var cells = client.StatelessScannerAsync(TestTableName, "", "startrow=\x0A\x00\x00\x00&endrow=\x0F\x00\x00\x00").Result;

            foreach (var cell in cells)
            {
                foreach (var row in cell.Rows)
                {
                    var k = BitConverter.ToInt32(row.Key.ToByteArray(), 0);
                    expectedSet.Remove(k);
                }
            }
            Assert.AreEqual(0, expectedSet.Count, "The expected set wasn't empty! Items left {0}!", string.Join(",", expectedSet));
        }

        [TestMethod]
        [TestCategory(TestRunMode.CheckIn)]
        public void TestCellsMultiVersionGet()
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
            client.StoreCellsAsync(TestTableName, set).Wait();
            var cell = client.GetCellsAsync(TestTableName, testKey, "d:starwars", "3").Result;
            Assert.AreEqual(2, cell.Rows[0].Values.Count);
        }
    }
}
