using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Running;
using System;

namespace DBTrie.Bench
{
    class Program
    {
        static void Main(string[] args)
        {
            //var conf = new BenchmarkDotNet.Configs.DebugInProcessConfig()
            BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
        }
    }
}
