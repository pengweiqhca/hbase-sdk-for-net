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
    using Microsoft.HBase.Client.Internal;
    using System;
    using System.Collections.Generic;

    public class RequestOptions
    {
        public TimeSpan Timeout { get; set; }
        public Dictionary<string, string> AdditionalHeaders { get; set; }
        public Uri BaseUri { get; set; }

        public void Validate()
        {
            BaseUri.ArgumentNotNull(nameof(BaseUri));
            ArgumentGuardExtensions.ArgumentNotNegative((int)Timeout.TotalMilliseconds, nameof(Timeout));
        }
    }
}
