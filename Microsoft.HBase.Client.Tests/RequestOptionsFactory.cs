using System;

namespace Microsoft.HBase.Client.Tests
{
#if NETFRAMEWORK
    using System.Configuration;
#else
    using Microsoft.Extensions.Configuration;
#endif
    public static class RequestOptionsFactory
    {
        public static RequestOptions GetDefaultOptions()
        {
#if NETFRAMEWORK
            return new RequestOptions
            {
                Timeout = TimeSpan.FromMilliseconds(int.TryParse(ConfigurationManager.AppSettings[Constants.HBaseTimeout], out var timeout) ? timeout : 30000),
                BaseUri = Uri.TryCreate(ConfigurationManager.AppSettings[Constants.HBaseBaseUri], UriKind.Absolute, out var uri) ? uri : null
            };
#else
            var configuration = new ConfigurationBuilder().AddJsonFile("appsettings.json").Build();

            return new RequestOptions
            {
                Timeout = TimeSpan.FromMilliseconds(int.TryParse(configuration[Constants.HBaseTimeout], out var timeout) ? timeout : 30000),
                BaseUri = Uri.TryCreate(configuration[Constants.HBaseBaseUri], UriKind.Absolute, out var uri) ? uri : null
            };
#endif
        }
    }
}
