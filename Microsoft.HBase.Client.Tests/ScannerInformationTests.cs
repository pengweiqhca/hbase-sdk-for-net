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
    using Microsoft.HBase.Client.Tests.Utilities;
    using Xunit;
    using System;
    using System.Net.Http;

    // ReSharper disable InconsistentNaming


    public class When_I_create_ScannerInformation : TestBase
    {
        private const string expectedScannerId = "/140614753560332aa73e8";
        private const string expectedTableName = "mytable";
        private readonly Uri expectedLocation = new Uri("https://headnodehost:8090/" + expectedTableName + "/scanner" + expectedScannerId);
        private ScannerInformation target;

        public When_I_create_ScannerInformation()
        {
            target = new ScannerInformation(expectedLocation, expectedTableName, new HttpResponseMessage().Headers);
        }

        [Fact]

        public void It_should_have_the_expected_location()
        {
            target.Location.ShouldEqual(expectedLocation);
        }

        [Fact]

        public void It_should_have_the_expected_scanner_identifier()
        {
            target.ScannerId.ShouldEqual(expectedScannerId.Substring(1));
        }

        [Fact]

        public void It_should_have_the_expected_table_name()
        {
            target.TableName.ShouldEqual(expectedTableName);
        }
    }


    public class When_I_call_a_ScannerInformation_ctor : TestBase
    {
        private const string validTableName = "mytable";
        private readonly Uri validLocation = new Uri("https://headnodehost:8090/" + validTableName + "/scanner/140614753560332aa73e8");

        [Fact]

        public void It_should_reject_empty_table_names()
        {
            object instance = null;
            typeof(ArgumentEmptyException).ShouldBeThrownBy(() => instance = new ScannerInformation(validLocation, string.Empty, null));
            instance.ShouldBeNull();
        }

        [Fact]

        public void It_should_reject_null_locations()
        {
            object instance = null;
            typeof(ArgumentNullException).ShouldBeThrownBy(() => instance = new ScannerInformation(null, validTableName, null));
            instance.ShouldBeNull();
        }

        [Fact]

        public void It_should_reject_null_table_names()
        {
            object instance = null;
            typeof(ArgumentNullException).ShouldBeThrownBy(() => instance = new ScannerInformation(validLocation, null, null));
            instance.ShouldBeNull();
        }
    }
}
