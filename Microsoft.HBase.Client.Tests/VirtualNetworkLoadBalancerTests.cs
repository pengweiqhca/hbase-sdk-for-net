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
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.HBase.Client.LoadBalancing;
    using Microsoft.HBase.Client.Tests.Utilities;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class VirtualNetworkLoadBalancerTests : DisposableContextSpecification
    {
        protected override void Context()
        {
        }

        [TestCleanup]
        public override void TestCleanup()
        {
        }

        [TestMethod]
        public void TestLoadBalancerEndpointsInitialization()
        {
            var numServers = 4;

            var balancer = new LoadBalancerRoundRobin(numRegionServers: numServers);
            Assert.AreEqual(balancer.GetNumAvailableEndpoints(), numServers);

            var expectedServersList = BuildServersList(numServers);

            var actualServersList = new List<string>();

            foreach (var endpoint in balancer._allEndpoints)
            {
                actualServersList.Add(endpoint.OriginalString);
            }

            Assert.IsTrue(CompareLists(actualServersList, expectedServersList));
        }

        [TestMethod]
        public void TestConfigInit()
        {
            var balancer = new LoadBalancerRoundRobin();

            Assert.AreEqual(LoadBalancerRoundRobin._workerHostNamePrefix, "workernode");
            Assert.AreEqual(LoadBalancerRoundRobin._workerRestEndpointPort, 8090);
            Assert.AreEqual(LoadBalancerRoundRobin._refreshInterval.TotalMilliseconds, 10.0);
        }

        protected class IgnoreBlackListedEndpointsPolicy : IEndpointIgnorePolicy
        {

            private readonly List<string> _blackListedEndpoints;

            public IgnoreBlackListedEndpointsPolicy(List<string> blacklistedEndpoints)
            {
                _blackListedEndpoints = blacklistedEndpoints;
            }

            public IEndpointIgnorePolicy InnerPolicy { get; private set; }
            public void OnEndpointAccessStart(Uri endpointUri)
            {
            }

            public void OnEndpointAccessCompletion(Uri endpointUri, EndpointAccessResult accessResult)
            {
            }

            public bool ShouldIgnoreEndpoint(Uri endpoint)
            {
                if (_blackListedEndpoints == null)
                {
                    return false;
                }

                var blackListedEndpointFound = _blackListedEndpoints.Find(x => x.Equals(endpoint.OriginalString));

                return (blackListedEndpointFound != null);
            }

            public void RefreshIgnoredList()
            {
            }
        }

        [TestMethod]
        public void TestLoadBalancerRoundRobin()
        {
            var numServers = 10;

            var balancer = new LoadBalancerRoundRobin(numServers);
            var initRrIdx = balancer._endpointIndex;

            for (var i = 0; i < 2 * numServers; i++)
            {
                Uri selectedEndpoint = null;

                selectedEndpoint = balancer.GetEndpoint();

                var expectedRrIdx = (initRrIdx + i) % numServers;
                var expectedEndpoint = balancer._allEndpoints[expectedRrIdx];
                Assert.IsTrue(selectedEndpoint.OriginalString.Equals(expectedEndpoint.OriginalString));

                balancer.RecordSuccess(selectedEndpoint);
            }
        }

        [TestMethod]
        public void TestLoadBalancerDomainInit()
        {
            var numServers = 10;
            var testDomain = "test.fakedomain.com";
            var balancer = new LoadBalancerRoundRobin(numServers, testDomain);

            var endpoints = balancer._allEndpoints.Select(u => u.ToString()).OrderBy(s => s).ToArray();

            for (var i = 0; i < endpoints.Length; i++)
            {
                var expected = $"http://{LoadBalancerRoundRobin._workerHostNamePrefix}{i}.{testDomain}:{LoadBalancerRoundRobin._workerRestEndpointPort}/";
                Assert.AreEqual(expected, endpoints[i]);
            }
        }

        [TestMethod]
        public void TestLoadBalancerConcurrency()
        {
            var numServers = 20;
            var balancer = new LoadBalancerRoundRobin(numServers);

            var uniqueEndpointsFetched = new ConcurrentDictionary<string, bool>();

            Parallel.For(0, numServers, (i) =>
            {
                var endpoint = balancer.GetEndpoint();
                Assert.IsNotNull(endpoint);

                balancer.RecordSuccess(endpoint);

                Assert.IsFalse(uniqueEndpointsFetched.ContainsKey(endpoint.OriginalString));
                Assert.IsTrue(uniqueEndpointsFetched.TryAdd(endpoint.OriginalString, true));
            });
        }

        [TestMethod]
        public void TestLoadBalancerConfigInitialization()
        {
            var stringConfigInitial = Guid.NewGuid().ToString();
            var stringConfigDefault = Guid.NewGuid().ToString();
            var stringConfigExpected = "LoadBalancerTestConfigValue";
            var stringConfigValidKey = "LoadBalancerTestConfigString";
            var stringConfigInvalidKey = "LoadBalancerTestConfigStringInvalid";

            var stringReadInvalid = stringConfigInitial;
            stringReadInvalid = LoadBalancerRoundRobin.ReadFromConfig(stringConfigInvalidKey, string.Copy, stringConfigDefault);
            Assert.AreEqual(stringReadInvalid, stringConfigDefault);

            var stringReadValid = stringConfigInitial;
            stringReadValid = LoadBalancerRoundRobin.ReadFromConfig(stringConfigValidKey, string.Copy, stringConfigDefault);
            Assert.AreEqual(stringReadValid, stringConfigExpected);

            var rnd = new Random();

            var intConfigInitial = rnd.Next();
            var intConfigDefault = rnd.Next();
            var intConfigExpected = 10;
            var intConfigValidKey = "LoadBalancerTestConfigInt";
            var intConfigInvalidKey = "LoadBalancerTestConfigIntInvalid";

            var intReadInvalid = intConfigInitial;
            intReadInvalid = LoadBalancerRoundRobin.ReadFromConfig(intConfigInvalidKey, Int32.Parse, intConfigDefault);
            Assert.AreEqual(intReadInvalid, intConfigDefault);

            var intReadValid = intConfigInitial;
            intReadValid = LoadBalancerRoundRobin.ReadFromConfig(intConfigValidKey, Int32.Parse, intConfigDefault);
            Assert.AreEqual(intReadValid, intConfigExpected);

            var doubleConfigInitial = rnd.NextDouble();
            var doubleConfigDefault = rnd.NextDouble();
            var doubleConfigExpected = 20.0;
            var doubleConfigValidKey = "LoadBalancerTestConfigDouble";
            var doubleConfigInvalidKey = "LoadBalancerTestConfigDoubleInvalid";

            var doubleReadInvalid = doubleConfigInitial;
            doubleReadInvalid = LoadBalancerRoundRobin.ReadFromConfig(doubleConfigInvalidKey, Double.Parse, doubleConfigDefault);
            Assert.AreEqual(doubleReadInvalid, doubleConfigDefault);

            var doubleReadValid = doubleConfigInitial;
            doubleReadValid = LoadBalancerRoundRobin.ReadFromConfig(doubleConfigValidKey, Double.Parse, doubleConfigDefault);
            Assert.AreEqual(doubleReadValid, doubleConfigExpected);

        }

        [TestMethod]
        public void TestLoadBalancerIgnorePolicy()
        {
            var numServers = 10;
            var numBlackListedServers = 8;
            var balancer = new LoadBalancerRoundRobin(numServers);

            var blackListedServersList = BuildServersList(numBlackListedServers);

            balancer._endpointIgnorePolicy = new IgnoreBlackListedEndpointsPolicy(blackListedServersList);

            for (var i = 0; i < 2 * numServers; i++)
            {
                Uri selectedEndpoint = null;

                selectedEndpoint = balancer.GetEndpoint();
                var selectedEndpointFoundInBlackList = blackListedServersList.Find(x => x.Equals(selectedEndpoint.OriginalString));
                Assert.IsNull(selectedEndpointFoundInBlackList);

                balancer.RecordSuccess(selectedEndpoint);
            }
        }

        [TestMethod]
        public void TestFailedEndpointsExpiry()
        {
            var numServers = 5;

            Uri activeEndpoint;
            var expectedNumFailedEndpoints = 0;
            var expectedNumAvailableEndpoints = numServers;

            var balancer = new LoadBalancerRoundRobin(numRegionServers: numServers);

            Assert.AreEqual(LoadBalancerRoundRobin._refreshInterval.TotalMilliseconds, 10.0);

            for (var i = 0; i < numServers; i++)
            {
                activeEndpoint = balancer.GetEndpoint();
                Assert.IsNotNull(activeEndpoint);
                balancer.RecordFailure(activeEndpoint);

                expectedNumFailedEndpoints++;
                expectedNumAvailableEndpoints--;
            }

            var endpointsInfoList = (balancer._endpointIgnorePolicy as IgnoreFailedEndpointsPolicy)._endpoints.Values.ToArray();

            var failedBefore = Array.FindAll(endpointsInfoList, x => x.State == IgnoreFailedEndpointsPolicy.EndpointState.Failed);
            var availableBefore = Array.FindAll(endpointsInfoList, x => x.State == IgnoreFailedEndpointsPolicy.EndpointState.Available);

            Assert.AreEqual(failedBefore.Length, numServers);
            Assert.AreEqual(availableBefore.Length, 0);

            Thread.Sleep(100);

            var endpoint = balancer.GetEndpoint();
            Assert.IsNotNull(endpoint);
            balancer.RecordSuccess(endpoint);

            endpointsInfoList = (balancer._endpointIgnorePolicy as IgnoreFailedEndpointsPolicy)._endpoints.Values.ToArray();

            var failedAfter = Array.FindAll(endpointsInfoList, x => x.State == IgnoreFailedEndpointsPolicy.EndpointState.Failed);
            var availableAfter = Array.FindAll(endpointsInfoList, x => x.State == IgnoreFailedEndpointsPolicy.EndpointState.Available);

            Assert.AreEqual(failedAfter.Length, numServers - 1);
            Assert.AreEqual(availableAfter.Length, 1);
        }

        private List<string> BuildServersList(int n)
        {
            var list = new List<string>();
            for (var i = 0; i < n; i++)
            {
                list.Add($"http://{"workernode"}{i}:{8090}");
            }
            return list;
        }

        private bool CompareLists(List<string> a, List<string> b)
        {
            if ((a == null) && (b != null))
            {
                return false;
            }

            if ((a != null) && (b == null))
            {
                return false;
            }

            if (a.Count != b.Count)
            {
                return false;
            }

            foreach (var aElem in a)
            {
                if (b.FirstOrDefault(bElem => bElem.Equals(aElem, StringComparison.OrdinalIgnoreCase)) == default(string))
                {
                    return false;
                }
            }

            return true;
        }

        private async Task<int> EmitIntAsync(int count)
        {
            return await Task.FromResult(count);
        }
        private async Task NoOpTask()
        {
            await Task.FromResult(0);
        }
    }
}
