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
    using System.Net;
    using System.Net.Http.Headers;
    using Microsoft.HBase.Client.Internal;

    /// <summary>
    ///
    /// </summary>
    [Serializable]
    public sealed class ScannerInformation
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ScannerInformation"/> class.
        /// </summary>
        /// <param name="location">The location.</param>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="responseHeaderCollection">additional header information from the response</param>
        public ScannerInformation(Uri location, string tableName, HttpResponseHeaders responseHeaderCollection)
        {
            location.ArgumentNotNull(nameof(location));
            tableName.ArgumentNotNullNorEmpty(nameof(tableName));
            responseHeaderCollection.ArgumentNotNull(nameof(responseHeaderCollection));

            Location = location;
            TableName = tableName;
            ResponseHeaderCollection = responseHeaderCollection;
        }

        /// <summary>
        /// Gets the location.
        /// </summary>
        /// <value>
        /// The location.
        /// </value>
        public Uri Location { get; }

        /// <summary>
        /// Gets the scanner identifier.
        /// </summary>
        /// <value>
        /// The scanner identifier.
        /// </value>
        public string ScannerId
        {
            get { return Location.PathAndQuery.Substring(Location.PathAndQuery.LastIndexOf('/') + 1); }
        }

        /// <summary>
        /// Additional headers from the CreateScanner response.
        /// This can be used to implement a sticky load balancing, e.g. by supplying a remote identifier.
        /// </summary>
        public HttpResponseHeaders ResponseHeaderCollection { get; }

        /// <summary>
        /// Gets the name of the table.
        /// </summary>
        /// <value>
        /// The name of the table.
        /// </value>
        public string TableName { get; }
    }
}
