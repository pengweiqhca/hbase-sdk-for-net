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
    using Microsoft.HBase.Client.LoadBalancing;
    using System;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Net;
    using System.Net.Http;
    using System.Text;
    using System.Threading.Tasks;

    /// <summary>
    ///
    /// </summary>
    public sealed class VNetWebRequester : IWebRequester
    {
        private readonly ILoadBalancer _balancer;
        private readonly HttpClient _httpClient;

        /// <summary>
        /// Initializes a new instance of the <see cref="VNetWebRequester"/> class.
        /// </summary>
        /// <param name="balancer">the load balancer for the vnet nodes</param>
        /// <param name="contentType">Type of the content.</param>
        public VNetWebRequester(ILoadBalancer balancer, string contentType = "application/x-protobuf")
        {
            _balancer = balancer;
            _httpClient = new HttpClient(new HttpClientHandler
            {
                PreAuthenticate = true,
                AllowAutoRedirect = false
            });
            _httpClient.DefaultRequestHeaders.Accept.TryParseAdd(contentType);
        }

        /// <summary>
        /// Issues the web request asynchronous.
        /// </summary>
        /// <param name="endpoint">The endpoint.</param>
        /// <param name="query"></param>
        /// <param name="method">The method.</param>
        /// <param name="input">The input.</param>
        /// <param name="options">request options</param>
        /// <returns></returns>
        public async Task<Response> IssueWebRequestAsync(string endpoint, string query, HttpMethod method, byte[] input, RequestOptions options)
        {
            options.Validate();
            var watch = Stopwatch.StartNew();
            Trace.CorrelationManager.ActivityId = Guid.NewGuid();
            var balancedEndpoint = _balancer.GetEndpoint();

            // Grab the host. Use the alternative host if one is specified
            var host = options.AlternativeHost ?? balancedEndpoint.Host;

            var builder = new UriBuilder(
                balancedEndpoint.Scheme,
                host,
                balancedEndpoint.Port,
                options.AlternativeEndpoint + endpoint);

            if (query != null)
            {
                builder.Query = query;
            }

            var target = builder.Uri;

            try
            {
                Debug.WriteLine("Issuing request {0} to endpoint {1}", Trace.CorrelationManager.ActivityId, target);

                var httpWebRequest = new HttpRequestMessage(method, target);

                if (options.AdditionalHeaders != null)
                {
                    foreach (var kv in options.AdditionalHeaders)
                    {
                        httpWebRequest.Headers.Add(kv.Key, kv.Value);
                    }
                }


                if (input != null)
                {
                    httpWebRequest.Content = new ByteArrayContent(input) { Headers = { ContentType = _httpClient.DefaultRequestHeaders.Accept.First() } };
                }

                var response = await _httpClient.SendAsync(httpWebRequest);

                return new Response()
                {
                    WebResponse = response,
                    RequestLatency = watch.Elapsed,
                    PostRequestAction = (r) =>
                    {
                        if (r.WebResponse.StatusCode == HttpStatusCode.OK || r.WebResponse.StatusCode == HttpStatusCode.Created || r.WebResponse.StatusCode == HttpStatusCode.NotModified)
                        {
                            _balancer.RecordSuccess(balancedEndpoint);
                        }
                        else
                        {
                            _balancer.RecordFailure(balancedEndpoint);
                        }
                    }
                };
            }
            catch (WebException we)
            {
                // 404 is valid response
                var resp = we.Response as HttpWebResponse;
                if (resp.StatusCode == HttpStatusCode.NotFound)
                {
                    _balancer.RecordSuccess(balancedEndpoint);
                    Debug.WriteLine("Web request {0} to endpoint {1} successful!", Trace.CorrelationManager.ActivityId, target);
                }
                else
                {
                    _balancer.RecordFailure(balancedEndpoint);
                    Debug.WriteLine("Web request {0} to endpoint {1} failed!", Trace.CorrelationManager.ActivityId, target);
                }
                throw we;
            }
            catch (Exception e)
            {
                _balancer.RecordFailure(balancedEndpoint);
                Debug.WriteLine("Web request {0} to endpoint {1} failed!", Trace.CorrelationManager.ActivityId, target);
                throw e;
            }
        }
    }
}
