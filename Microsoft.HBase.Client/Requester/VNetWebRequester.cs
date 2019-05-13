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

namespace Microsoft.HBase.Client.Requester
{
    using System;
    using System.Diagnostics;
    using System.Linq;
    using System.Net.Http;
    using System.Threading.Tasks;

    /// <summary>
    ///
    /// </summary>
    public sealed class VNetWebRequester : IWebRequester
    {
        private readonly HttpClient _httpClient;

        /// <summary>
        /// Initializes a new instance of the <see cref="VNetWebRequester"/> class.
        /// </summary>
        /// <param name="options"></param>
        /// <param name="contentType">Type of the content.</param>
        public VNetWebRequester(RequestOptions options, string contentType = "application/x-protobuf")
        {
            options.Validate();

            _httpClient = new HttpClient(new HttpClientHandler
            {
                AllowAutoRedirect = false,
                PreAuthenticate = true,
            })
            {
                BaseAddress = options.BaseUri,
                Timeout = options.Timeout,
            };

            _httpClient.DefaultRequestHeaders.Accept.TryParseAdd(contentType);

            if (options.AdditionalHeaders != null)
                foreach (var kv in options.AdditionalHeaders)
                {
                    _httpClient.DefaultRequestHeaders.TryAddWithoutValidation(kv.Key, kv.Value);
                }
        }

        /// <summary>
        /// Issues the web request asynchronous.
        /// </summary>
        /// <param name="endpoint">The endpoint.</param>
        /// <param name="query"></param>
        /// <param name="method">The method.</param>
        /// <param name="input">The input.</param>
        /// <returns></returns>
        public async Task<Response> IssueWebRequestAsync(string endpoint, string query, HttpMethod method, byte[] input)
        {
            var watch = Stopwatch.StartNew();

            using (var request = new HttpRequestMessage(method, string.IsNullOrEmpty(query) ? endpoint : $"{endpoint}?{query}"))
            {
                if (input != null)
                    request.Content = new ByteArrayContent(input) { Headers = { ContentType = _httpClient.DefaultRequestHeaders.Accept.First() } };

                var response = await _httpClient.SendAsync(request).ConfigureAwait(false);

                return new Response { WebResponse = response, RequestLatency = watch.Elapsed };
            }
        }

        public void Dispose()
        {
            _httpClient.Dispose();

            GC.SuppressFinalize(this);
        }
    }
}
