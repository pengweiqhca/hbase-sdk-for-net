using Microsoft.HBase.Client.Tests.Utilities;
using System;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace Microsoft.HBase.Client.Tests
{
    class Program
    {
        static async Task Main()
        {
            foreach (var testClass in typeof(Program).Assembly.GetTypes())
                foreach (var testMethod in testClass.GetMethods().Where(m => m.GetCustomAttributesData()
                    .Any(attr => attr.AttributeType == typeof(FactAttribute))))
                {
                    var test = (TestBase)Activator.CreateInstance(testClass);
                    try
                    {
                        var result = testMethod.Invoke(test, null);

                        if (result is Task task) await task;

                        Console.WriteLine($"{DateTime.Now:HH:mm:ss.fff} {testClass.Name}.{testMethod.Name}");
                    }
                    catch (Exception ex)
                    {
                        Console.ForegroundColor = ConsoleColor.Red;

                        Console.WriteLine($"{DateTime.Now:HH:mm:ss.fff} {testClass.Name}.{testMethod.Name}{Environment.NewLine}{ex}");

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
