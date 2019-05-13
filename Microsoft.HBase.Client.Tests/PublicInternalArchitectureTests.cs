namespace Microsoft.HBase.Client.Tests
{
    using Microsoft.HBase.Client.Tests.Utilities;
    using Xunit;
    using System;
    using System.Collections.Generic;
    using System.Linq;

    // ReSharper disable InconsistentNaming
    //

    public class PublicInternalArchitecturalTests : TestBase
    {
        [Fact]
        public void All_declarations_not_under_an_internal_or_resources_namespace_are_public_or_nested()
        {
            var assemblies = GetAssembliesUnderTest().ToList();
            assemblies.Count.ShouldBeGreaterThan(0);

            var violatingTypes = new HashSet<Type>();

            foreach (var asm in assemblies)
            {
                var allAssemblyTypes = asm.GetTypes().ToList();
                foreach (var type in allAssemblyTypes)
                {
                    var namespaceName = type.Namespace ?? string.Empty;
                    if (namespaceName.Length == 0)
                    {
                        // skip anonymous types.
                        continue;
                    }

                    if (namespaceName == "JetBrains.Profiler.Core.Instrumentation" && type.Name == "DataOnStack")
                    {
                        // appears when performing test coverage using dotCover.
                        continue;
                    }

                    if (!namespaceName.Contains(".Internal") && !namespaceName.Contains(".Resources") && !type.IsPublic && !type.IsNested)
                    {
                        violatingTypes.Add(type);
                    }
                }
            }

            violatingTypes.ShouldContainOnly(new Type[] { });
        }

        [Fact]

        public void All_declarations_under_an_internal_namespace_are_not_public()
        {
            var assemblies = GetAssembliesUnderTest().ToList();
            assemblies.Count.ShouldBeGreaterThan(0);

            var violatingTypes = new HashSet<Type>();

            foreach (var asm in assemblies)
            {
                var allAssemblyTypes = asm.GetTypes().ToList();
                foreach (var type in allAssemblyTypes)
                {
                    var namespaceName = type.Namespace ?? string.Empty;
                    if (namespaceName.Contains(".Internal") && type.IsPublic)
                    {
                        violatingTypes.Add(type);
                    }
                }
            }

            violatingTypes.ShouldContainOnly(new Type[] { });
        }
    }
}
