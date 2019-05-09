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
    using Microsoft.HBase.Client.LoadBalancing;
    using Microsoft.HBase.Client.Tests.Utilities;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Org.Apache.Hadoop.Hbase.Rest.Protobuf.Generated;
    using Polly;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Google.Protobuf;

    [TestClass]
    public class GatewayClientTest : HBaseClientTestBase
    {
        public override IHBaseClient CreateClient()
        {
            var options = RequestOptions.GetDefaultOptions();
            options.RetryPolicy = Policy.NoOpAsync();
            options.Timeout = TimeSpan.FromMilliseconds(30000);
            options.KeepAlive = false;
            return new HBaseClient(ClusterCredentialsFactory.CreateFromFile(@".\credentials.txt"), options);
        }

        [TestMethod]
        [TestCategory(TestRunMode.CheckIn)]
        public override void TestFullScan()
        {
            var client = CreateClient();

            StoreTestData(client);

            var scanOptions = RequestOptions.GetDefaultOptions();
            scanOptions.AlternativeEndpoint = Constants.RestEndpointBaseZero;

            // full range scan
            var scanSettings = new Scanner { Batch = 10 };
            ScannerInformation scannerInfo = null;
            try
            {
                scannerInfo = client.CreateScannerAsync(TestTableName, scanSettings, scanOptions).Result;

                CellSet next;
                var expectedSet = new HashSet<int>(Enumerable.Range(0, 100));
                while ((next = client.ScannerGetNextAsync(scannerInfo, scanOptions).Result) != null)
                {
                    Assert.AreEqual(10, next.Rows.Count);
                    foreach (var row in next.Rows)
                    {
                        var k = BitConverter.ToInt32(row.Key.ToByteArray(), 0);
                        expectedSet.Remove(k);
                    }
                }
                Assert.AreEqual(0, expectedSet.Count, "The expected set wasn't empty! Items left {0}!", string.Join(",", expectedSet));
            }
            finally
            {
                if (scannerInfo != null)
                {
                    client.DeleteScannerAsync(TestTableName, scannerInfo, scanOptions).Wait();
                }
            }
        }

        [TestMethod]
        [TestCategory(TestRunMode.CheckIn)]
        public void TestScannerCreation()
        {
            var client = CreateClient();
            var scanSettings = new Scanner { Batch = 2 };

            var scanOptions = RequestOptions.GetDefaultOptions();
            scanOptions.AlternativeEndpoint = Constants.RestEndpointBaseZero;
            ScannerInformation scannerInfo = null;
            try
            {
                scannerInfo = client.CreateScannerAsync(TestTableName, scanSettings, scanOptions).Result;
                Assert.AreEqual(TestTableName, scannerInfo.TableName);
                Assert.IsNotNull(scannerInfo.ScannerId);
                Assert.IsFalse(scannerInfo.ScannerId.StartsWith("/"), "scanner id starts with a slash");
                Assert.IsNotNull(scannerInfo.ResponseHeaderCollection);
            }
            finally
            {
                if (scannerInfo != null)
                {
                    client.DeleteScannerAsync(TestTableName, scannerInfo, scanOptions).Wait();
                }
            }
        }

        [TestMethod]
        [TestCategory(TestRunMode.CheckIn)]
        [ExpectedException(typeof(AggregateException), "The remote server returned an error: (404) Not Found.")]
        public void TestScannerDeletion()
        {
            var client = CreateClient();

            // full range scan
            var scanSettings = new Scanner { Batch = 10 };
            var scanOptions = RequestOptions.GetDefaultOptions();
            scanOptions.AlternativeEndpoint = Constants.RestEndpointBaseZero;
            ScannerInformation scannerInfo = null;

            try
            {
                scannerInfo = client.CreateScannerAsync(TestTableName, scanSettings, scanOptions).Result;
                Assert.AreEqual(TestTableName, scannerInfo.TableName);
                Assert.IsNotNull(scannerInfo.ScannerId);
                Assert.IsFalse(scannerInfo.ScannerId.StartsWith("/"), "scanner id starts with a slash");
                Assert.IsNotNull(scannerInfo.ResponseHeaderCollection);
                // delete the scanner
                client.DeleteScannerAsync(TestTableName, scannerInfo, scanOptions).Wait();
                // try to fetch data use the deleted scanner
                scanOptions.RetryPolicy = Policy.NoOpAsync();
                client.ScannerGetNextAsync(scannerInfo, scanOptions).Wait();
            }
            finally
            {
                if (scannerInfo != null)
                {
                    client.DeleteScannerAsync(TestTableName, scannerInfo, scanOptions).Wait();
                }
            }
        }

        [TestMethod]
        [TestCategory(TestRunMode.CheckIn)]
        public override void TestSubsetScan()
        {
            var client = CreateClient();
            const int startRow = 15;
            const int endRow = 15 + 13;
            StoreTestData(client);

            // subset range scan
            var scanSettings = new Scanner { Batch = 10, StartRow = ByteString.CopyFrom(BitConverter.GetBytes(startRow)), EndRow =  ByteString.CopyFrom(BitConverter.GetBytes(endRow)) };
            var scanOptions = RequestOptions.GetDefaultOptions();
            scanOptions.AlternativeEndpoint = Constants.RestEndpointBaseZero;
            ScannerInformation scannerInfo = null;
            try
            {
                scannerInfo = client.CreateScannerAsync(TestTableName, scanSettings, scanOptions).Result;

                CellSet next;
                var expectedSet = new HashSet<int>(Enumerable.Range(startRow, endRow - startRow));
                while ((next = client.ScannerGetNextAsync(scannerInfo, scanOptions).Result) != null)
                {
                    foreach (var row in next.Rows)
                    {
                        var k = BitConverter.ToInt32(row.Key.ToByteArray(), 0);
                        expectedSet.Remove(k);
                    }
                }
                Assert.AreEqual(0, expectedSet.Count, "The expected set wasn't empty! Items left {0}!", string.Join(",", expectedSet));
            }
            finally
            {
                if (scannerInfo != null)
                {
                    client.DeleteScannerAsync(TestTableName, scannerInfo, scanOptions).Wait();
                }
            }
        }
    }
}
