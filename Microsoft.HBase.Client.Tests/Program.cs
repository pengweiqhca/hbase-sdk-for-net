namespace Microsoft.HBase.Client.Tests
{
    using System;

    class Program
    {
        static void Main()
        {
            var a = new FilterTests();

            try
            {
                a.TestInitialize();

                a.When_I_Scan_all_I_get_the_expected_results();
            }
            finally
            {
                a.TestCleanup();
            }
        }
    }
}
