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
    using Microsoft.HBase.Client.Filters;
    using Microsoft.HBase.Client.Tests.Utilities;
    using Org.Apache.Hadoop.Hbase.Rest.Protobuf.Generated;
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;
    using System.Text;
    using Xunit;

    // ReSharper disable InconsistentNaming


    public class FilterTests : DisposableContextSpecification
    {
        private const string TableNamePrefix = "filtertest";

        private const string ColumnFamilyName1 = "first";
        private const string ColumnFamilyName2 = "second";
        private const string LineNumberColumnName = "line";
        private const string ColumnNameA = "a";
        private const string ColumnNameB = "b";

        private static bool _arrangementCompleted;
        private static readonly List<FilterTestRecord> _allExpectedRecords = new List<FilterTestRecord>();
        private static string _tableName;
        private static TableSchema _tableSchema;

        public FilterTests()
        {
            if (!_arrangementCompleted)
            {
                // at present, no tables are modified so only arrange the tables once per test pass
                // and putting the arrangement into a static context.
                // (this knocked test runs down to ~30 seconds from ~5 minutes).


                var client = new HBaseClient(RequestOptionsFactory.GetDefaultOptions());

                // ensure tables from previous tests are cleaned up
                var tables = client.ListTablesAsync().Result;
                foreach (var Name in tables.Name)
                {
                    var pinnedName = Name;
                    if (Name.StartsWith(TableNamePrefix, StringComparison.Ordinal))
                    {
                        client.DeleteTableAsync(pinnedName).Wait();
                    }
                }

                AddTable();
                PopulateTable();

                _arrangementCompleted = true;
            }
        }

        [Fact]

        public void When_I_Scan_all_I_get_the_expected_results()
        {
            var client = new HBaseClient(RequestOptionsFactory.GetDefaultOptions());
            var scan = new Scanner();


            ScannerInformation scanInfo = null;
            try
            {
                scanInfo = client.CreateScannerAsync(_tableName, scan).Result;
                var actualRecords = RetrieveResults(scanInfo).ToList();
                actualRecords.ShouldContainOnly(_allExpectedRecords);
            }
            finally
            {
                if (scanInfo != null)
                {
                    client.DeleteScannerAsync(_tableName, scanInfo).Wait();
                }
            }

        }

        [Fact]

        public void When_I_Scan_with_a_ColumnCountGetFilter_I_get_the_expected_results()
        {
            // B Column should not be returned, so set the value to null.
            var expectedRecords = (from r in _allExpectedRecords select r.WithBValue(null)).ToList();

            var client = new HBaseClient(RequestOptionsFactory.GetDefaultOptions());
            var scanner = new Scanner();
            var filter = new ColumnCountGetFilter(2);
            scanner.Filter = filter.ToEncodedString();


            ScannerInformation scanInfo = null;
            try
            {
                scanInfo = client.CreateScannerAsync(_tableName, scanner).Result;
                var actualRecords = RetrieveResults(scanInfo).ToList();

                actualRecords.ShouldContainOnly(expectedRecords);
            }
            finally
            {
                if (scanInfo != null)
                {
                    client.DeleteScannerAsync(_tableName, scanInfo).Wait();
                }
            }
        }

        [Fact]

        public void When_I_Scan_with_a_ColumnPaginationFilter_I_get_the_expected_results()
        {
            // only grabbing the LineNumber Column with (1, 1)
            var expectedRecords = (from r in _allExpectedRecords select r.WithAValue(null).WithBValue(null)).ToList();

            var client = new HBaseClient(RequestOptionsFactory.GetDefaultOptions());
            var scanner = new Scanner();
            var filter = new ColumnPaginationFilter(1, 1);
            scanner.Filter = filter.ToEncodedString();


            ScannerInformation scanInfo = null;
            try
            {
                scanInfo = client.CreateScannerAsync(_tableName, scanner).Result;
                var actualRecords = RetrieveResults(scanInfo).ToList();

                actualRecords.ShouldContainOnly(expectedRecords);
            }
            finally
            {
                if (scanInfo != null)
                {
                    client.DeleteScannerAsync(_tableName, scanInfo).Wait();
                }
            }
        }

        [Fact]

        public void When_I_Scan_with_a_ColumnPrefixFilter_I_get_the_expected_results()
        {
            var expectedRecords = (from r in _allExpectedRecords select r.WithAValue(null).WithBValue(null)).ToList();

            var client = new HBaseClient(RequestOptionsFactory.GetDefaultOptions());
            var scanner = new Scanner();
            var filter = new ColumnPrefixFilter(Encoding.UTF8.GetBytes(LineNumberColumnName));
            scanner.Filter = filter.ToEncodedString();


            ScannerInformation scanInfo = null;
            try
            {
                scanInfo = client.CreateScannerAsync(_tableName, scanner).Result;
                var actualRecords = RetrieveResults(scanInfo).ToList();

                actualRecords.ShouldContainOnly(expectedRecords);
            }
            finally
            {
                if (scanInfo != null)
                {
                    client.DeleteScannerAsync(_tableName, scanInfo).Wait();
                }
            }
        }

        [Fact]

        public void When_I_Scan_with_a_ColumnRangeFilter_I_get_the_expected_results()
        {
            var expectedRecords = (from r in _allExpectedRecords select r.WithLineNumberValue(0).WithBValue(null)).ToList();

            var client = new HBaseClient(RequestOptionsFactory.GetDefaultOptions());
            var scanner = new Scanner();
            var filter = new ColumnRangeFilter(Encoding.UTF8.GetBytes(ColumnNameA), true, Encoding.UTF8.GetBytes(ColumnNameB), false);
            scanner.Filter = filter.ToEncodedString();


            ScannerInformation scanInfo = null;
            try
            {
                scanInfo = client.CreateScannerAsync(_tableName, scanner).Result;
                var actualRecords = RetrieveResults(scanInfo).ToList();

                actualRecords.ShouldContainOnly(expectedRecords);
            }
            finally
            {
                if (scanInfo != null)
                {
                    client.DeleteScannerAsync(_tableName, scanInfo).Wait();
                }
            }

        }

        [Fact]

        public void When_I_Scan_with_a_DependentColumnFilter_and_a_BinaryComparator_with_the_operator_equal_I_get_the_expected_results()
        {
            var expectedRecords = (from r in _allExpectedRecords where r.LineNumber == 1 select r).ToList();

            var client = new HBaseClient(RequestOptionsFactory.GetDefaultOptions());
            var scanner = new Scanner();
            var filter = new DependentColumnFilter(
                Encoding.UTF8.GetBytes(ColumnFamilyName1),
                Encoding.UTF8.GetBytes(LineNumberColumnName),
                false,
                CompareFilter.CompareOp.Equal,
                new BinaryComparator(BitConverter.GetBytes(1)));
            scanner.Filter = filter.ToEncodedString();


            ScannerInformation scanInfo = null;
            try
            {
                scanInfo = client.CreateScannerAsync(_tableName, scanner).Result;
                var actualRecords = RetrieveResults(scanInfo).ToList();

                actualRecords.ShouldContainOnly(expectedRecords);
            }
            finally
            {
                if (scanInfo != null)
                {
                    client.DeleteScannerAsync(_tableName, scanInfo).Wait();
                }
            }
        }

        [Fact]

        public void When_I_Scan_with_a_FamilyFilter_I_get_the_expected_results()
        {
            // B is in Column family 2
            var expectedRecords = (from r in _allExpectedRecords select r.WithBValue(null)).ToList();

            var client = new HBaseClient(RequestOptionsFactory.GetDefaultOptions());
            var scanner = new Scanner();
            var filter = new FamilyFilter(CompareFilter.CompareOp.Equal, new BinaryComparator(Encoding.UTF8.GetBytes(ColumnFamilyName1)));
            scanner.Filter = filter.ToEncodedString();


            ScannerInformation scanInfo = null;
            try
            {
                scanInfo = client.CreateScannerAsync(_tableName, scanner).Result;
                var actualRecords = RetrieveResults(scanInfo).ToList();

                actualRecords.ShouldContainOnly(expectedRecords);
            }
            finally
            {
                if (scanInfo != null)
                {
                    client.DeleteScannerAsync(_tableName, scanInfo).Wait();
                }
            }

        }

        [Fact]

        public void When_I_Scan_with_a_FilterList_with_AND_logic_I_get_the_expected_results()
        {
            var expectedRecords = (from r in _allExpectedRecords where r.LineNumber == 1 select r).ToList();

            var client = new HBaseClient(RequestOptionsFactory.GetDefaultOptions());
            var scanner = new Scanner();

            Filter f0 = new SingleColumnValueFilter(
                Encoding.UTF8.GetBytes(ColumnFamilyName1),
                Encoding.UTF8.GetBytes(LineNumberColumnName),
                CompareFilter.CompareOp.Equal,
                BitConverter.GetBytes(1));

            Filter f1 = new SingleColumnValueFilter(
                Encoding.UTF8.GetBytes(ColumnFamilyName1),
                Encoding.UTF8.GetBytes(LineNumberColumnName),
                CompareFilter.CompareOp.LessThanOrEqualTo,
                BitConverter.GetBytes(2));

            var filter = new FilterList(FilterList.Operator.MustPassAll, f0, f1);
            scanner.Filter = filter.ToEncodedString();


            ScannerInformation scanInfo = null;
            try
            {
                scanInfo = client.CreateScannerAsync(_tableName, scanner).Result;
                var actualRecords = RetrieveResults(scanInfo).ToList();

                actualRecords.ShouldContainOnly(expectedRecords);
            }
            finally
            {
                if (scanInfo != null)
                {
                    client.DeleteScannerAsync(_tableName, scanInfo).Wait();
                }
            }

        }

        [Fact]

        public void When_I_Scan_with_a_FilterList_with_OR_logic_I_get_the_expected_results()
        {
            var expectedRecords = (from r in _allExpectedRecords where r.LineNumber <= 2 select r).ToList();

            var client = new HBaseClient(RequestOptionsFactory.GetDefaultOptions());
            var scanner = new Scanner();

            Filter f0 = new SingleColumnValueFilter(
                Encoding.UTF8.GetBytes(ColumnFamilyName1),
                Encoding.UTF8.GetBytes(LineNumberColumnName),
                CompareFilter.CompareOp.Equal,
                BitConverter.GetBytes(1));

            Filter f1 = new SingleColumnValueFilter(
                Encoding.UTF8.GetBytes(ColumnFamilyName1),
                Encoding.UTF8.GetBytes(LineNumberColumnName),
                CompareFilter.CompareOp.LessThanOrEqualTo,
                BitConverter.GetBytes(2));

            var filter = new FilterList(FilterList.Operator.MustPassOne, f0, f1);
            scanner.Filter = filter.ToEncodedString();


            ScannerInformation scanInfo = null;
            try
            {
                scanInfo = client.CreateScannerAsync(_tableName, scanner).Result;
                var actualRecords = RetrieveResults(scanInfo).ToList();

                actualRecords.ShouldContainOnly(expectedRecords);
            }
            finally
            {
                if (scanInfo != null)
                {
                    client.DeleteScannerAsync(_tableName, scanInfo).Wait();
                }
            }

        }

        [Fact]

        public void When_I_Scan_with_a_FirstKeyOnlyFilter_I_get_the_expected_results()
        {
            // a first key only filter does not return Column values
            var expectedRecords =
                (from r in _allExpectedRecords select new FilterTestRecord(r.RowKey, 0, string.Empty, string.Empty)).ToList();

            var client = new HBaseClient(RequestOptionsFactory.GetDefaultOptions());
            var scanner = new Scanner();
            var filter = new KeyOnlyFilter();
            scanner.Filter = filter.ToEncodedString();


            ScannerInformation scanInfo = null;
            try
            {
                scanInfo = client.CreateScannerAsync(_tableName, scanner).Result;
                var actualRecords = RetrieveResults(scanInfo).ToList();

                actualRecords.ShouldContainOnly(expectedRecords);
            }
            finally
            {
                if (scanInfo != null)
                {
                    client.DeleteScannerAsync(_tableName, scanInfo).Wait();
                }
            }

        }


        [Fact]

        public void When_I_Scan_with_a_InclusiveStopFilter_I_get_the_expected_results()
        {
            var example = (from r in _allExpectedRecords where r.LineNumber == 2 select r).Single();
            var rawRowKey = Encoding.UTF8.GetBytes(example.RowKey);

            var expectedRecords = (from r in _allExpectedRecords where r.LineNumber <= 2 select r).ToList();

            var client = new HBaseClient(RequestOptionsFactory.GetDefaultOptions());
            var scanner = new Scanner();
            var filter = new InclusiveStopFilter(rawRowKey);
            scanner.Filter = filter.ToEncodedString();


            ScannerInformation scanInfo = null;
            try
            {
                scanInfo = client.CreateScannerAsync(_tableName, scanner).Result;
                var actualRecords = RetrieveResults(scanInfo).ToList();

                actualRecords.ShouldContainOnly(expectedRecords);
            }
            finally
            {
                if (scanInfo != null)
                {
                    client.DeleteScannerAsync(_tableName, scanInfo).Wait();
                }
            }

        }


        [Fact]

        public void When_I_Scan_with_a_KeyOnlyFilter_I_get_the_expected_results()
        {
            // a key only filter does not return Column values
            var expectedRecords =
                (from r in _allExpectedRecords select new FilterTestRecord(r.RowKey, 0, string.Empty, string.Empty)).ToList();

            var client = new HBaseClient(RequestOptionsFactory.GetDefaultOptions());
            var scanner = new Scanner();
            var filter = new KeyOnlyFilter();
            scanner.Filter = filter.ToEncodedString();


            ScannerInformation scanInfo = null;
            try
            {
                scanInfo = client.CreateScannerAsync(_tableName, scanner).Result;
                var actualRecords = RetrieveResults(scanInfo).ToList();

                actualRecords.ShouldContainOnly(expectedRecords);
            }
            finally
            {
                if (scanInfo != null)
                {
                    client.DeleteScannerAsync(_tableName, scanInfo).Wait();
                }
            }

        }

        [Fact]

        public void When_I_Scan_with_a_MultipleColumnPrefixFilter_I_get_the_expected_results()
        {
            var expectedRecords = (from r in _allExpectedRecords select r.WithLineNumberValue(0)).ToList();

            var client = new HBaseClient(RequestOptionsFactory.GetDefaultOptions());
            var scanner = new Scanner();

            // set this large enough so that we get all records back
            var prefixes = new List<byte[]> { Encoding.UTF8.GetBytes(ColumnNameA), Encoding.UTF8.GetBytes(ColumnNameB) };
            var filter = new MultipleColumnPrefixFilter(prefixes);
            scanner.Filter = filter.ToEncodedString();


            ScannerInformation scanInfo = null;
            try
            {
                scanInfo = client.CreateScannerAsync(_tableName, scanner).Result;
                var actualRecords = RetrieveResults(scanInfo).ToList();

                actualRecords.ShouldContainOnly(expectedRecords);
            }
            finally
            {
                if (scanInfo != null)
                {
                    client.DeleteScannerAsync(_tableName, scanInfo).Wait();
                }
            }
        }

        [Fact]

        public void When_I_Scan_with_a_PageFilter_I_get_the_expected_results()
        {
            var client = new HBaseClient(RequestOptionsFactory.GetDefaultOptions());
            var scanner = new Scanner();
            var filter = new PageFilter(2);
            scanner.Filter = filter.ToEncodedString();


            ScannerInformation scanInfo = null;
            try
            {
                scanInfo = client.CreateScannerAsync(_tableName, scanner).Result;
                var actualRecords = RetrieveResults(scanInfo).ToList();

                actualRecords.Count.ShouldBeGreaterThanOrEqualTo(2);
            }
            finally
            {
                if (scanInfo != null)
                {
                    client.DeleteScannerAsync(_tableName, scanInfo).Wait();
                }
            }

        }

        [Fact]

        public void When_I_Scan_with_a_PrefixFilter_I_get_the_expected_results()
        {
            var example = _allExpectedRecords.First();
            var rawRowKey = Encoding.UTF8.GetBytes(example.RowKey);

            const int prefixLength = 4;
            var prefix = new byte[prefixLength];
            Array.Copy(rawRowKey, prefix, prefixLength);

            var expectedRecords = (from r in _allExpectedRecords
                                   let rawKey = ByteString.CopyFromUtf8(r.RowKey)
                                   where rawKey[0] == prefix[0] && rawKey[1] == prefix[1] && rawKey[2] == prefix[2] && rawKey[3] == prefix[3]
                                   select r).ToList();

            var client = new HBaseClient(RequestOptionsFactory.GetDefaultOptions());
            var scanner = new Scanner();
            var filter = new PrefixFilter(prefix);
            scanner.Filter = filter.ToEncodedString();


            ScannerInformation scanInfo = null;
            try
            {
                scanInfo = client.CreateScannerAsync(_tableName, scanner).Result;
                var actualRecords = RetrieveResults(scanInfo).ToList();

                actualRecords.ShouldContainOnly(expectedRecords);
            }
            finally
            {
                if (scanInfo != null)
                {
                    client.DeleteScannerAsync(_tableName, scanInfo).Wait();
                }
            }

        }

        [Fact]

        public void When_I_Scan_with_a_QualifierFilter_I_get_the_expected_results()
        {
            var expectedRecords = (from r in _allExpectedRecords select r.WithAValue(null).WithBValue(null)).ToList();

            var client = new HBaseClient(RequestOptionsFactory.GetDefaultOptions());
            var scanner = new Scanner();
            var filter = new QualifierFilter(CompareFilter.CompareOp.Equal, new BinaryComparator(Encoding.UTF8.GetBytes(LineNumberColumnName)));
            scanner.Filter = filter.ToEncodedString();


            ScannerInformation scanInfo = null;
            try
            {
                scanInfo = client.CreateScannerAsync(_tableName, scanner).Result;
                var actualRecords = RetrieveResults(scanInfo).ToList();

                actualRecords.ShouldContainOnly(expectedRecords);
            }
            finally
            {
                if (scanInfo != null)
                {
                    client.DeleteScannerAsync(_tableName, scanInfo).Wait();
                }
            }

        }

        [Fact]

        public void When_I_Scan_with_a_RandomRowFilter_I_get_the_expected_results()
        {
            var client = new HBaseClient(RequestOptionsFactory.GetDefaultOptions());
            var scanner = new Scanner();

            // set this large enough so that we get all records back
            var filter = new RandomRowFilter(2000.0F);
            scanner.Filter = filter.ToEncodedString();


            ScannerInformation scanInfo = null;
            try
            {
                scanInfo = client.CreateScannerAsync(_tableName, scanner).Result;
                var actualRecords = RetrieveResults(scanInfo).ToList();

                actualRecords.ShouldContainOnly(_allExpectedRecords);
            }
            finally
            {
                if (scanInfo != null)
                {
                    client.DeleteScannerAsync(_tableName, scanInfo).Wait();
                }
            }

        }

        [Fact]

        public void When_I_Scan_with_a_RowFilter_I_get_the_expected_results()
        {
            var example = _allExpectedRecords.First();

            var expectedRecords = (from r in _allExpectedRecords where r.RowKey == example.RowKey select r).ToList();

            var client = new HBaseClient(RequestOptionsFactory.GetDefaultOptions());
            var scanner = new Scanner();
            var filter = new RowFilter(CompareFilter.CompareOp.Equal, new BinaryComparator(Encoding.UTF8.GetBytes(example.RowKey)));
            scanner.Filter = filter.ToEncodedString();


            ScannerInformation scanInfo = null;
            try
            {
                scanInfo = client.CreateScannerAsync(_tableName, scanner).Result;
                var actualRecords = RetrieveResults(scanInfo).ToList();

                actualRecords.ShouldContainOnly(expectedRecords);
            }
            finally
            {
                if (scanInfo != null)
                {
                    client.DeleteScannerAsync(_tableName, scanInfo).Wait();
                }
            }

        }

        [Fact]

        public void When_I_Scan_with_a_SingleColumnValueExcludeFilter_and_a_BinaryComparator_with_the_operator_equal_I_get_the_expected_results()
        {
            var bValue = (from r in _allExpectedRecords select r.B).First();

            // B Column should not be returned, so set the value to null.
            var expectedRecords = (from r in _allExpectedRecords where r.B == bValue select r.WithBValue(null)).ToList();

            var client = new HBaseClient(RequestOptionsFactory.GetDefaultOptions());
            var scanner = new Scanner();

            var filter = new SingleColumnValueExcludeFilter(
                Encoding.UTF8.GetBytes(ColumnFamilyName2),
                Encoding.UTF8.GetBytes(ColumnNameB),
                CompareFilter.CompareOp.Equal,
                Encoding.UTF8.GetBytes(bValue));
            scanner.Filter = filter.ToEncodedString();


            ScannerInformation scanInfo = null;
            try
            {
                scanInfo = client.CreateScannerAsync(_tableName, scanner).Result;
                var actualRecords = RetrieveResults(scanInfo).ToList();

                actualRecords.ShouldContainOnly(expectedRecords);
            }
            finally
            {
                if (scanInfo != null)
                {
                    client.DeleteScannerAsync(_tableName, scanInfo).Wait();
                }
            }

        }

        [Fact]

        public void When_I_Scan_with_a_SingleColumnValueFilter_and_a_BinaryComparator_with_the_operator_equal_I_get_the_expected_results()
        {
            var expectedRecords = (from r in _allExpectedRecords where r.LineNumber == 1 select r).ToList();

            var client = new HBaseClient(RequestOptionsFactory.GetDefaultOptions());
            var scanner = new Scanner();

            var filter = new SingleColumnValueFilter(
                Encoding.UTF8.GetBytes(ColumnFamilyName1),
                Encoding.UTF8.GetBytes(LineNumberColumnName),
                CompareFilter.CompareOp.Equal,
                BitConverter.GetBytes(1),
                filterIfMissing: true);

            scanner.Filter = filter.ToEncodedString();


            ScannerInformation scanInfo = null;
            try
            {
                scanInfo = client.CreateScannerAsync(_tableName, scanner).Result;
                var actualRecords = RetrieveResults(scanInfo).ToList();

                actualRecords.ShouldContainOnly(expectedRecords);
            }
            finally
            {
                if (scanInfo != null)
                {
                    client.DeleteScannerAsync(_tableName, scanInfo).Wait();
                }
            }
        }

        [Fact]

        public void When_I_Scan_with_a_SingleColumnValueFilter_and_a_BinaryComparator_with_the_operator_greater_than_I_get_the_expected_results()
        {
            var expectedRecords = (from r in _allExpectedRecords where r.LineNumber > 1 select r).ToList();

            var client = new HBaseClient(RequestOptionsFactory.GetDefaultOptions());
            var scanner = new Scanner();

            var filter = new SingleColumnValueFilter(
                Encoding.UTF8.GetBytes(ColumnFamilyName1),
                Encoding.UTF8.GetBytes(LineNumberColumnName),
                CompareFilter.CompareOp.GreaterThan,
                BitConverter.GetBytes(1));
            scanner.Filter = filter.ToEncodedString();


            ScannerInformation scanInfo = null;
            try
            {
                scanInfo = client.CreateScannerAsync(_tableName, scanner).Result;
                var actualRecords = RetrieveResults(scanInfo).ToList();

                actualRecords.ShouldContainOnly(expectedRecords);
            }
            finally
            {
                if (scanInfo != null)
                {
                    client.DeleteScannerAsync(_tableName, scanInfo).Wait();
                }
            }

        }

        [Fact]

        public void
            When_I_Scan_with_a_SingleColumnValueFilter_and_a_BinaryComparator_with_the_operator_greater_than_or_equal_I_get_the_expected_results()
        {
            var expectedRecords = (from r in _allExpectedRecords where r.LineNumber >= 1 select r).ToList();

            var client = new HBaseClient(RequestOptionsFactory.GetDefaultOptions());
            var scanner = new Scanner();

            var filter = new SingleColumnValueFilter(
                Encoding.UTF8.GetBytes(ColumnFamilyName1),
                Encoding.UTF8.GetBytes(LineNumberColumnName),
                CompareFilter.CompareOp.GreaterThanOrEqualTo,
                BitConverter.GetBytes(1));
            scanner.Filter = filter.ToEncodedString();


            ScannerInformation scanInfo = null;
            try
            {
                scanInfo = client.CreateScannerAsync(_tableName, scanner).Result;
                var actualRecords = RetrieveResults(scanInfo).ToList();

                actualRecords.ShouldContainOnly(expectedRecords);
            }
            finally
            {
                if (scanInfo != null)
                {
                    client.DeleteScannerAsync(_tableName, scanInfo).Wait();
                }
            }
        }

        [Fact]

        public void When_I_Scan_with_a_SingleColumnValueFilter_and_a_BinaryComparator_with_the_operator_less_than_I_get_the_expected_results()
        {
            var expectedRecords = (from r in _allExpectedRecords where r.LineNumber < 1 select r).ToList();

            var client = new HBaseClient(RequestOptionsFactory.GetDefaultOptions());
            var scanner = new Scanner();

            var filter = new SingleColumnValueFilter(
                Encoding.UTF8.GetBytes(ColumnFamilyName1),
                Encoding.UTF8.GetBytes(LineNumberColumnName),
                CompareFilter.CompareOp.LessThan,
                BitConverter.GetBytes(1));
            scanner.Filter = filter.ToEncodedString();


            ScannerInformation scanInfo = null;
            try
            {
                scanInfo = client.CreateScannerAsync(_tableName, scanner).Result;
                var actualRecords = RetrieveResults(scanInfo).ToList();

                actualRecords.ShouldContainOnly(expectedRecords);
            }
            finally
            {
                if (scanInfo != null)
                {
                    client.DeleteScannerAsync(_tableName, scanInfo).Wait();
                }
            }
        }

        [Fact]

        public void When_I_Scan_with_a_SingleColumnValueFilter_and_a_BinaryComparator_with_the_operator_less_than_or_equal_I_get_the_expected_results(
            )
        {
            var expectedRecords = (from r in _allExpectedRecords where r.LineNumber <= 1 select r).ToList();

            var client = new HBaseClient(RequestOptionsFactory.GetDefaultOptions());
            var scanner = new Scanner();

            var filter = new SingleColumnValueFilter(
                Encoding.UTF8.GetBytes(ColumnFamilyName1),
                Encoding.UTF8.GetBytes(LineNumberColumnName),
                CompareFilter.CompareOp.LessThanOrEqualTo,
                BitConverter.GetBytes(1));
            scanner.Filter = filter.ToEncodedString();


            ScannerInformation scanInfo = null;
            try
            {
                scanInfo = client.CreateScannerAsync(_tableName, scanner).Result;
                var actualRecords = RetrieveResults(scanInfo).ToList();

                actualRecords.ShouldContainOnly(expectedRecords);
            }
            finally
            {
                if (scanInfo != null)
                {
                    client.DeleteScannerAsync(_tableName, scanInfo).Wait();
                }
            }

        }

        [Fact]

        public void When_I_Scan_with_a_SingleColumnValueFilter_and_a_BinaryComparator_with_the_operator_no_op_I_get_the_expected_results()
        {
            var expectedRecords = new List<FilterTestRecord>();

            var client = new HBaseClient(RequestOptionsFactory.GetDefaultOptions());
            var scanner = new Scanner();

            var filter = new SingleColumnValueFilter(
                Encoding.UTF8.GetBytes(ColumnFamilyName1),
                Encoding.UTF8.GetBytes(LineNumberColumnName),
                CompareFilter.CompareOp.NoOperation,
                BitConverter.GetBytes(1));
            scanner.Filter = filter.ToEncodedString();


            ScannerInformation scanInfo = null;
            try
            {
                scanInfo = client.CreateScannerAsync(_tableName, scanner).Result;
                var actualRecords = RetrieveResults(scanInfo).ToList();

                actualRecords.ShouldContainOnly(expectedRecords);
            }
            finally
            {
                if (scanInfo != null)
                {
                    client.DeleteScannerAsync(_tableName, scanInfo).Wait();
                }
            }
        }


        [Fact]

        public void When_I_Scan_with_a_SingleColumnValueFilter_and_a_BinaryComparator_with_the_operator_not_equal_I_get_the_expected_results()
        {
            var expectedRecords = (from r in _allExpectedRecords where r.LineNumber != 1 select r).ToList();

            var client = new HBaseClient(RequestOptionsFactory.GetDefaultOptions());
            var scanner = new Scanner();
            var filter = new SingleColumnValueFilter(
                Encoding.UTF8.GetBytes(ColumnFamilyName1),
                Encoding.UTF8.GetBytes(LineNumberColumnName),
                CompareFilter.CompareOp.NotEqual,
                BitConverter.GetBytes(1));
            scanner.Filter = filter.ToEncodedString();


            ScannerInformation scanInfo = null;
            try
            {
                scanInfo = client.CreateScannerAsync(_tableName, scanner).Result;
                var actualRecords = RetrieveResults(scanInfo).ToList();

                actualRecords.ShouldContainOnly(expectedRecords);
            }
            finally
            {
                if (scanInfo != null)
                {
                    client.DeleteScannerAsync(_tableName, scanInfo).Wait();
                }
            }

        }

        [Fact]

        public void When_I_Scan_with_a_SingleColumnValueFilter_and_a_BinaryPrefixComparator_with_the_operator_equal_I_get_the_expected_results()
        {
            var expectedRecords = (from r in _allExpectedRecords where r.LineNumber == 3 select r).ToList();

            var client = new HBaseClient(RequestOptionsFactory.GetDefaultOptions());
            var scanner = new Scanner();

            var comparer = new BinaryPrefixComparator(BitConverter.GetBytes(3));

            var filter = new SingleColumnValueFilter(
                Encoding.UTF8.GetBytes(ColumnFamilyName1),
                Encoding.UTF8.GetBytes(LineNumberColumnName),
                CompareFilter.CompareOp.Equal,
                comparer,
                filterIfMissing: false);
            scanner.Filter = filter.ToEncodedString();


            ScannerInformation scanInfo = null;
            try
            {
                scanInfo = client.CreateScannerAsync(_tableName, scanner).Result;
                var actualRecords = RetrieveResults(scanInfo).ToList();

                actualRecords.ShouldContainOnly(expectedRecords);
            }
            finally
            {
                if (scanInfo != null)
                {
                    client.DeleteScannerAsync(_tableName, scanInfo).Wait();
                }
            }
        }

        [Fact]

        public void
            When_I_Scan_with_a_SingleColumnValueFilter_and_a_BitComparator_with_the_operator_equal_and_the_bitop_XOR_I_get_the_expected_results()
        {
            var expectedRecords = (from r in _allExpectedRecords where r.LineNumber != 3 select r).ToList();

            var client = new HBaseClient(RequestOptionsFactory.GetDefaultOptions());
            var scanner = new Scanner();

            var comparer = new BitComparator(BitConverter.GetBytes(3), BitComparator.BitwiseOp.Xor);

            var filter = new SingleColumnValueFilter(
                Encoding.UTF8.GetBytes(ColumnFamilyName1),
                Encoding.UTF8.GetBytes(LineNumberColumnName),
                CompareFilter.CompareOp.Equal,
                comparer);
            scanner.Filter = filter.ToEncodedString();


            ScannerInformation scanInfo = null;
            try
            {
                scanInfo = client.CreateScannerAsync(_tableName, scanner).Result;
                var actualRecords = RetrieveResults(scanInfo).ToList();

                actualRecords.ShouldContainOnly(expectedRecords);
            }
            finally
            {
                if (scanInfo != null)
                {
                    client.DeleteScannerAsync(_tableName, scanInfo).Wait();
                }
            }

        }

        [Fact]

        public void When_I_Scan_with_a_SingleColumnValueFilter_and_a_NullComparator_with_the_operator_not_equal_I_get_the_expected_results()
        {
            var expectedRecords = new List<FilterTestRecord>();

            var client = new HBaseClient(RequestOptionsFactory.GetDefaultOptions());
            var scanner = new Scanner();

            var comparer = new NullComparator();

            var filter = new SingleColumnValueFilter(
                Encoding.UTF8.GetBytes(ColumnFamilyName1),
                Encoding.UTF8.GetBytes(LineNumberColumnName),
                CompareFilter.CompareOp.Equal,
                comparer);
            scanner.Filter = filter.ToEncodedString();


            ScannerInformation scanInfo = null;
            try
            {
                scanInfo = client.CreateScannerAsync(_tableName, scanner).Result;
                var actualRecords = RetrieveResults(scanInfo).ToList();

                actualRecords.ShouldContainOnly(expectedRecords);
            }
            finally
            {
                if (scanInfo != null)
                {
                    client.DeleteScannerAsync(_tableName, scanInfo).Wait();
                }
            }
        }

        [Fact]

        public void When_I_Scan_with_a_SingleColumnValueFilter_and_a_SubstringComparator_with_the_operator_equal_I_get_the_expected_results()
        {
            // grab a substring that is guaranteed to match at least one record.
            var ss = _allExpectedRecords.First().A.Substring(1, 2);
            //Debug.WriteLine("The substring value is: " + ss);

            var expectedRecords = (from r in _allExpectedRecords where r.A.Contains(ss) select r).ToList();

            var client = new HBaseClient(RequestOptionsFactory.GetDefaultOptions());
            var scanner = new Scanner();

            var comparer = new SubstringComparator(ss);

            var filter = new SingleColumnValueFilter(
                Encoding.UTF8.GetBytes(ColumnFamilyName1),
                Encoding.UTF8.GetBytes(ColumnNameA),
                CompareFilter.CompareOp.Equal,
                comparer);
            scanner.Filter = filter.ToEncodedString();


            ScannerInformation scanInfo = null;
            try
            {
                scanInfo = client.CreateScannerAsync(_tableName, scanner).Result;
                var actualRecords = RetrieveResults(scanInfo).ToList();

                actualRecords.ShouldContainOnly(expectedRecords);
            }
            finally
            {
                if (scanInfo != null)
                {
                    client.DeleteScannerAsync(_tableName, scanInfo).Wait();
                }
            }
        }

        [Fact]

        public void When_I_Scan_with_a_SkipFilter_I_get_the_expected_results()
        {
            var expectedRecords = (from r in _allExpectedRecords where r.LineNumber != 0 select r).ToList();

            var client = new HBaseClient(RequestOptionsFactory.GetDefaultOptions());
            var scanner = new Scanner();
            var filter = new SkipFilter(new ValueFilter(CompareFilter.CompareOp.NotEqual, new BinaryComparator(BitConverter.GetBytes(0))));
            scanner.Filter = filter.ToEncodedString();


            ScannerInformation scanInfo = null;
            try
            {
                scanInfo = client.CreateScannerAsync(_tableName, scanner).Result;
                var actualRecords = RetrieveResults(scanInfo).ToList();

                actualRecords.ShouldContainOnly(expectedRecords);
            }
            finally
            {
                if (scanInfo != null)
                {
                    client.DeleteScannerAsync(_tableName, scanInfo).Wait();
                }
            }
        }

        [Fact]

        public void When_I_Scan_with_a_TimestampsFilter_I_get_the_expected_results()
        {
            var expectedRecords = _allExpectedRecords;

            // scan all and retrieve timestamps
            var client = new HBaseClient(RequestOptionsFactory.GetDefaultOptions());
            var scanner = new Scanner();


            ScannerInformation scanAll = null;
            List<long> timestamps = null;
            try
            {
                scanAll = client.CreateScannerAsync(_tableName, scanner).Result;
                timestamps = RetrieveTimestamps(scanAll).ToList();
            }
            finally
            {
                if (scanAll != null)
                {
                    client.DeleteScannerAsync(_tableName, scanAll).Wait();
                }
            }

            Assert.NotNull(timestamps);

            // timestamps scan
            scanner = new Scanner();
            var filter = new TimestampsFilter(timestamps);
            scanner.Filter = filter.ToEncodedString();
            ScannerInformation scanInfo = null;
            try
            {
                scanInfo = client.CreateScannerAsync(_tableName, scanner).Result;
                var actualRecords = RetrieveResults(scanInfo).ToList();

                actualRecords.ShouldContainOnly(expectedRecords);
            }
            finally
            {
                if (scanInfo != null)
                {
                    client.DeleteScannerAsync(_tableName, scanInfo).Wait();
                }
            }
        }

        [Fact]

        public void When_I_Scan_with_a_ValueFilter_I_get_the_expected_results()
        {
            var expectedRecords =
                (from r in _allExpectedRecords where r.LineNumber == 3 select r.WithAValue(null).WithBValue(null)).ToList();

            var client = new HBaseClient(RequestOptionsFactory.GetDefaultOptions());
            var scanner = new Scanner();
            var filter = new ValueFilter(CompareFilter.CompareOp.Equal, new BinaryComparator(BitConverter.GetBytes(3)));
            scanner.Filter = filter.ToEncodedString();


            ScannerInformation scanInfo = null;
            try
            {
                scanInfo = client.CreateScannerAsync(_tableName, scanner).Result;
                var actualRecords = RetrieveResults(scanInfo).ToList();

                actualRecords.ShouldContainOnly(expectedRecords);
            }
            finally
            {
                if (scanInfo != null)
                {
                    client.DeleteScannerAsync(_tableName, scanInfo).Wait();
                }
            }
        }

        [Fact]

        public void When_I_Scan_with_a_ValueFilter_and_a_RegexStringComparator_I_get_the_expected_results()
        {
            var expectedRecords = _allExpectedRecords;
            var client = new HBaseClient(RequestOptionsFactory.GetDefaultOptions());
            var scanner = new Scanner();
            var filter = new ValueFilter(CompareFilter.CompareOp.Equal, new RegexStringComparator(".*"));
            scanner.Filter = filter.ToEncodedString();


            ScannerInformation scanInfo = null;
            try
            {
                scanInfo = client.CreateScannerAsync(_tableName, scanner).Result;
                var actualRecords = RetrieveResults(scanInfo).ToList();

                actualRecords.ShouldContainOnly(expectedRecords);
            }
            finally
            {
                if (scanInfo != null)
                {
                    client.DeleteScannerAsync(_tableName, scanInfo).Wait();
                }
            }
        }

        [Fact]

        public void When_I_Scan_with_a_WhileMatchFilter_I_get_the_expected_results()
        {
            var expectedRecords = (from r in _allExpectedRecords where r.LineNumber == 0 select r.WithBValue(null)).ToList();

            var client = new HBaseClient(RequestOptionsFactory.GetDefaultOptions());
            var scanner = new Scanner();
            var filter = new WhileMatchFilter(new ValueFilter(CompareFilter.CompareOp.NotEqual, new BinaryComparator(BitConverter.GetBytes(0))));
            scanner.Filter = filter.ToEncodedString();


            ScannerInformation scanInfo = null;
            try
            {
                scanInfo = client.CreateScannerAsync(_tableName, scanner).Result;
                var actualRecords = RetrieveResults(scanInfo).ToList();

                actualRecords.ShouldContainOnly(expectedRecords);
            }
            finally
            {
                if (scanInfo != null)
                {
                    client.DeleteScannerAsync(_tableName, scanInfo).Wait();
                }
            }
        }

        private IEnumerable<long> RetrieveTimestamps(ScannerInformation scanInfo)
        {
            var rv = new HashSet<long>();

            var client = new HBaseClient(RequestOptionsFactory.GetDefaultOptions());
            CellSet next;

            while ((next = client.ScannerGetNextAsync(scanInfo).Result) != null)
            {
                foreach (var row in next.Rows)
                {
                    var cells = row.Values;
                    foreach (var c in cells)
                    {
                        rv.Add(c.Timestamp);
                    }
                }
            }

            return rv;
        }

        private IEnumerable<FilterTestRecord> RetrieveResults(ScannerInformation scanInfo)
        {
            var rv = new List<FilterTestRecord>();

            var client = new HBaseClient(RequestOptionsFactory.GetDefaultOptions());
            CellSet next;

            while ((next = client.ScannerGetNextAsync(scanInfo).Result) != null)
            {
                foreach (var row in next.Rows)
                {
                    var rowKey = row.Key.ToStringUtf8();

                   var cells = row.Values;

                    string a = null;
                    string b = null;
                    var lineNumber = 0;
                    foreach (var c in cells)
                    {
                        var columnName = ExtractColumnName(c.Column);
                        switch (columnName)
                        {
                            case LineNumberColumnName:
                                lineNumber = c.Data.Length > 0 ? BitConverter.ToInt32(c.Data.ToByteArray(), 0) : 0;
                                break;

                            case ColumnNameA:
                                a = c.Data.ToStringUtf8();
                                break;

                            case ColumnNameB:
                                b = c.Data.ToStringUtf8();
                                break;

                            default:
                                throw new InvalidOperationException("Don't know what to do with column: " + columnName);
                        }
                    }

                    var rec = new FilterTestRecord(rowKey, lineNumber, a, b);
                    rv.Add(rec);
                }
            }

            return rv;
        }

        private void PopulateTable()
        {
            var client = new HBaseClient(RequestOptionsFactory.GetDefaultOptions());
            var cellSet = new CellSet();

            var id = Guid.NewGuid().ToString("N");
            for (var lineNumber = 0; lineNumber < 10; ++lineNumber)
            {
                var rowKey = string.Format(CultureInfo.InvariantCulture, "{0}-{1}", id, lineNumber);

                // add to expected records
                var rec = new FilterTestRecord(rowKey, lineNumber, Guid.NewGuid().ToString("N"), Guid.NewGuid().ToString("D"));
                _allExpectedRecords.Add(rec);

                // add to row
                var row = new CellSet.Types.Row { Key = ByteString.CopyFromUtf8(rec.RowKey) };

                var lineColumnValue = new Cell
                {
                    Column = BuildCellColumn(ColumnFamilyName1, LineNumberColumnName),
                    Data =ByteString.CopyFrom(BitConverter.GetBytes(rec.LineNumber))
                };
                row.Values.Add(lineColumnValue);

                var paragraphColumnValue = new Cell { Column = BuildCellColumn(ColumnFamilyName1, ColumnNameA), Data =ByteString.CopyFromUtf8(rec.A) };
                row.Values.Add(paragraphColumnValue);

                var columnValueB = new Cell { Column = BuildCellColumn(ColumnFamilyName2, ColumnNameB), Data = ByteString.CopyFromUtf8(rec.B) };
                row.Values.Add(columnValueB);

                cellSet.Rows.Add(row);
            }

            client.StoreCellsAsync(_tableName, cellSet).Wait();
        }

        private ByteString BuildCellColumn(string columnFamilyName, string columnName)
        {
            return ByteString.CopyFromUtf8(string.Format(CultureInfo.InvariantCulture, "{0}:{1}", columnFamilyName, columnName));
        }

        private string ExtractColumnName(ByteString cellColumn)
        {
            var qualifiedColumnName = cellColumn.ToStringUtf8();
            var parts = qualifiedColumnName.Split(new[] { ':' }, 2);
            return parts[1];
        }

        private void AddTable()
        {
            // add a table specific to this test
            var client = new HBaseClient(RequestOptionsFactory.GetDefaultOptions());
            _tableName = TableNamePrefix + Guid.NewGuid().ToString("N");
            _tableSchema = new TableSchema { Name = _tableName };
            _tableSchema.Columns.Add(new ColumnSchema { Name = ColumnFamilyName1 });
            _tableSchema.Columns.Add(new ColumnSchema { Name = ColumnFamilyName2 });

            client.CreateTableAsync(_tableSchema).Wait();
        }
    }
}
