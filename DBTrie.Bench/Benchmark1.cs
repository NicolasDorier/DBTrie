using BenchmarkDotNet.Attributes;
using DBTrie.Storage;
using DBTrie.Tests;
using DBTrie.TrieModel;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace DBTrie.Bench
{
	[MemoryDiagnoser]
	public class Benchmark1
	{
		ReadOnlyMemory<byte> key;
		IStorage fs;
		LTrie trie;
		Tester t;
		[GlobalSetup]
		public void Setup()
		{
			t = new Tester();
			t.ConsistencyCheck = false;
			key = Encoding.UTF8.GetBytes("cf6c9e8cb9fb312862edbd5c2b7d1615db4f65ab");
			fs = t.CreateFileStorage("10000007", true, "Benchmark");
			trie = t.CreateTrie(fs).GetAwaiter().GetResult();
			(trie.EnumerateStartsWith("").ToArrayAsync().GetAwaiter().GetResult()).DisposeAll();
		}
		[GlobalCleanup]
		public async Task Cleanup()
		{
			await fs.DisposeAsync();
		}

		[Benchmark]
		public async Task EnumerateValues()
		{
			await foreach (var row in trie.EnumerateStartsWith(""))
			{
				row.Dispose();
			}
		}

		[Benchmark]
		public async Task RandomSearch()
		{
			(await trie.GetValue(key))?.Dispose();
		}
	}
}
