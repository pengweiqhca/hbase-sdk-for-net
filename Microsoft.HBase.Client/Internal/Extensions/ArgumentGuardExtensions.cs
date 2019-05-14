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

namespace Microsoft.HBase.Client.Internal
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Linq;
    using JetBrains.Annotations;

    internal static class ArgumentGuardExtensions
    {
        internal static void ArgumentNotNull([ValidatedNotNull] this object value, [InvokerParameterName]string argumentName)
        {
            if (ReferenceEquals(value, null))
            {
                throw new ArgumentNullException(argumentName ?? string.Empty);
            }
        }

        internal static void ArgumentNotNullNorContainsNull<T>([ValidatedNotNull] this IEnumerable<T> value, [InvokerParameterName]string paramName) where T : class
        {
            value.ArgumentNotNull(paramName);

            if ((from v in value where ReferenceEquals(v, null) select v).Any())
            {
                throw new ArgumentContainsNullException(paramName ?? string.Empty, null, null);
            }
        }

        internal static void ArgumentNotNullNorEmpty<T>([ValidatedNotNull] this IEnumerable<T> value, [InvokerParameterName]string paramName)
        {
            value.ArgumentNotNull(paramName);
            if (!value.Any())
            {
                throw new ArgumentEmptyException(paramName ?? string.Empty, null, null);
            }
        }

        internal static void ArgumentNotNegative(int value, [InvokerParameterName]string name)
        {
            if (value < 0)
            {
                throw new ArgumentException($"Argument {name} wasn't >= 0! Given: {value}");
            }
        }
    }
}
