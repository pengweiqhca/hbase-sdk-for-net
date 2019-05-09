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

namespace Microsoft.HBase.Client
{
    using System;
    using System.Collections.Generic;
    using Microsoft.HBase.Client.Internal;
    using Microsoft.HBase.Client.LoadBalancing;
    using Polly;
    using Polly.Retry;

    public class RequestOptions
    {
        public IAsyncPolicy RetryPolicy { get; set; }
        public string AlternativeEndpoint { get; set; }
        public bool KeepAlive { get; set; }
        public TimeSpan Timeout { get; set; }
        public int SerializationBufferSize { get; set; }
        public int ReceiveBufferSize { get; set; }
        public bool UseNagle { get; set; }
        public int Port { get; set; }
        public Dictionary<string, string> AdditionalHeaders { get; set; }
        public string AlternativeHost { get; set; }

        public void Validate()
        {
            RetryPolicy.ArgumentNotNull("RetryPolicy");
            ArgumentGuardExtensions.ArgumentNotNegative((int)Timeout.TotalMilliseconds, "TimeoutMillis");
            ArgumentGuardExtensions.ArgumentNotNegative(ReceiveBufferSize, "ReceiveBufferSize");
            ArgumentGuardExtensions.ArgumentNotNegative(SerializationBufferSize, "SerializationBufferSize");
            ArgumentGuardExtensions.ArgumentNotNegative(Port, "Port");
        }

        public static RequestOptions GetDefaultOptions()
        {
            return new RequestOptions()
            {
                RetryPolicy = Policy.NoOpAsync(),
                KeepAlive = true,
                Timeout = TimeSpan.FromMilliseconds(30000),
                ReceiveBufferSize = 1024 * 1024 * 1,
                SerializationBufferSize = 1024 * 1024 * 1,
                UseNagle = false,
                //AlternativeEndpoint = Constants.RestEndpointBase,
                Port = 443,
                AlternativeHost = null
            };
        }

    }
}
