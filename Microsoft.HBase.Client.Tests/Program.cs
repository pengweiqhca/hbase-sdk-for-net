namespace Microsoft.HBase.Client.Tests
{
    using System;
    using System.Linq;
    using Microsoft.HBase.Client.Tests.Utilities;
    using Xunit;

    class Program
    {
        static void Main()
        {
            foreach (var testClass in typeof(Program).Assembly.GetTypes())
                foreach (var testMethod in testClass.GetMethods().Where(m => m.GetCustomAttributesData()
                    .Any(attr => attr.AttributeType == typeof(FactAttribute))))
                {

                    var test = (TestBase)Activator.CreateInstance(testClass);
                    try
                    {
                        testMethod.Invoke(test, null);

                        Console.WriteLine($"{DateTime.Now:HH:mm:ss.fff} {testClass.Name}.{testMethod.Name}");
                    }
                    catch
                    {
                        Console.ForegroundColor = ConsoleColor.Red;

                        Console.WriteLine($"{DateTime.Now:HH:mm:ss.fff} {testClass.Name}.{testMethod.Name}");

                        Console.ResetColor();
                    }
                    finally
                    {
                        if (test is IDisposable disposable) disposable.Dispose();
                    }
                }
        }
    }
}
