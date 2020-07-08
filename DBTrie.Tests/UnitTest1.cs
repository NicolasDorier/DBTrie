using DBTrie.Storage;
using DBTrie.TrieModel;
using NuGet.Frameworks;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection.PortableExecutable;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace DBTrie.Tests
{
	public class UnitTest1
	{
		private readonly ITestOutputHelper logs;

		public UnitTest1(ITestOutputHelper logs)
		{
			this.logs = logs;
		}

		[Fact]
		public async Task CanDoBasicTrieOperations()
		{
			CreateEmptyFile("Empty2", 0);
			await using var fs = new FileStorage("Empty2");
			var cache = new CacheStorage(fs, false);
			var trie = await LTrie.InitTrie(cache);
			await cache.Flush();
			trie = await ReloadTrie(trie);
			Assert.Null(await trie.GetValueString(1 + "test" + 1));
			for (int i = 0; i < 5; i++)
			{
				await trie.SetKey(i + "test" + i, "lol" + i);
				Assert.Equal("lol" + i, await trie.GetValueString(i + "test" + i));
			}
			for (int i = 0; i < 5; i++)
			{
				Assert.Equal("lol" + i, await trie.GetValueString(i + "test" + i));
			}
			trie = await LTrie.OpenFromStorage(fs);
			for (int i = 0; i < 5; i++)
			{
				Assert.Null(await trie.GetValueString(i + "test" + i));
			}
			await cache.Flush();
			trie = await ReloadTrie(trie);
			for (int i = 0; i < 5; i++)
			{
				Assert.Equal("lol" + i, await trie.GetValueString(i + "test" + i));
			}
		}

		class TrieTester : IAsyncDisposable
		{
			public FileStorage Fs;
			public LTrie Trie;
			string file;
			public TrieTester(string file, LTrie trie, FileStorage fs)
			{
				this.Trie = trie;
				this.Fs = fs;
				this.file = file;
			}

			public static async Task<TrieTester> Create([CallerMemberName] string? caller = null)
			{
				caller ??= "unnamed";
				var fs = new FileStorage(caller);
				var trie = await LTrie.InitTrie(fs);
				trie.ConsistencyCheck = true;
				return new TrieTester(caller, trie, fs);
			}
			public async ValueTask DisposeAsync()
			{
				await Fs.DisposeAsync();
				File.Delete(file);
			}
		}

		[Fact]
		public async Task CanDelete()
		{
			await using (var t = await TrieTester.Create())
			{
				await t.Trie.SetKey("test", "lol");
				Assert.Equal("lol", await t.Trie.GetValueString("test"));
				Assert.True(await t.Trie.DeleteRow("test"));
				Assert.False(await t.Trie.DeleteRow("test"));
				Assert.Null(await t.Trie.GetValueString("test"));
			}
			await using (var t = await TrieTester.Create())
			{
				await t.Trie.SetKey("test", "lol1");
				await t.Trie.SetKey("test2", "lol2");
				Assert.Equal(2, t.Trie.RecordCount);
				Assert.True(await t.Trie.DeleteRow("test"));
				Assert.False(await t.Trie.DeleteRow("test"));
				Assert.Null(await t.Trie.GetValueString("test"));
				Assert.Equal("lol2", await t.Trie.GetValueString("test2"));
				Assert.Equal(1, t.Trie.RecordCount);
			}
			await using (var t = await TrieTester.Create())
			{
				await t.Trie.SetKey("test", "lol1");
				await t.Trie.SetKey("test2", "lol2");
				Assert.True(await t.Trie.DeleteRow("test2"));
				Assert.False(await t.Trie.DeleteRow("test2"));
				Assert.Null(await t.Trie.GetValueString("test2"));
				Assert.Equal("lol1", await t.Trie.GetValueString("test"));
			}
			await using (var t = await TrieTester.Create())
			{
				Assert.Equal(0, t.Trie.RecordCount);
				await t.Trie.SetKey("test1", "lol1");
				await t.Trie.SetKey("test2", "lol2");
				Assert.False(await t.Trie.DeleteRow("test"));
				Assert.Equal(2, t.Trie.RecordCount);
				Assert.True(await t.Trie.DeleteRow("test1"));
				Assert.Null(await t.Trie.GetValueString("test1"));
				Assert.Equal("lol2", await t.Trie.GetValueString("test2"));
				Assert.False(await t.Trie.DeleteRow("test1"));
				Assert.True(await t.Trie.DeleteRow("test2"));
				Assert.Null(await t.Trie.GetValueString("test2"));
				Assert.Equal(0, t.Trie.RecordCount);
			}
			await using (var t = await TrieTester.Create())
			{
				Assert.Equal(0, t.Trie.RecordCount);
				await t.Trie.SetKey("test1", "lol1");
				await t.Trie.SetKey("test2", "lol2");
				Assert.True(await t.Trie.SetKey("test", "lol"));
				Assert.Equal(3, t.Trie.RecordCount);
				Assert.False(await t.Trie.SetKey("test", "newlol"));
				Assert.Equal(3, t.Trie.RecordCount);
				Assert.True(await t.Trie.DeleteRow("test"));
				Assert.Equal("lol2", await t.Trie.GetValueString("test2"));
				Assert.Equal("lol1", await t.Trie.GetValueString("test1"));
				Assert.Equal(2, t.Trie.RecordCount);
				Assert.True(await t.Trie.SetKey("test", "newlol"));
				Assert.Equal(3, t.Trie.RecordCount);
			}
		}

		[Fact]
		public async Task CacheTests()
		{
			CreateEmptyFile("Empty", 1030);
			await using var fs = new FileStorage("Empty");
			var cache = new CacheStorage(fs, pageSize: 128);
			await fs.Write(125, "abcdefgh");
			Assert.Equal("abcdefgh", await cache.Read(125, "abcdefgh".Length));
			Assert.Equal("abcdefgh", await fs.Read(125, "abcdefgh".Length));
			await cache.Write(127, "CDEF");
			Assert.Equal("abCDEFgh", await cache.Read(125, "abCDEFgh".Length));
			Assert.Equal("abcdefgh", await fs.Read(125, "abcdefgh".Length));

			Assert.Equal(2, cache.pages.Count);
			Assert.Equal(1030, fs.Length);
			Assert.Equal(1030, cache.Length);
			await cache.WriteToEnd("helloworld");
			Assert.Equal(1030 + "helloworld".Length, cache.Length);
			Assert.Equal("helloworld", await cache.Read(1030, "helloworld".Length));
			await cache.WriteToEnd("abdwuqiwiw");

			Assert.NotEqual("helloworld", await fs.Read(1030, "helloworld".Length));

			Assert.Equal("abcdefgh", await fs.Read(125, "abcdefgh".Length));
			await cache.Flush();
			Assert.Equal("helloworld", await fs.Read(1030, "helloworld".Length));
			Assert.Equal("abCDEFgh", await fs.Read(125, "abCDEFgh".Length));

			Assert.Equal(cache.Length, fs.Length);
			Assert.Equal(1030 + "helloworldabdwuqiwiw".Length, fs.Length);
			await fs.Reserve(10);
			Assert.Equal(1030 + "helloworldabdwuqiwiw".Length + 10, fs.Length);
			await fs.Reserve(1);
			Assert.Equal(1030 + "helloworldabdwuqiwiw".Length + 11, fs.Length);
			await fs.Reserve(0);
			Assert.Equal(1030 + "helloworldabdwuqiwiw".Length + 11, fs.Length);
		}

		private static void CreateEmptyFile(string name, int size)
		{
			if (File.Exists(name))
				File.Create(name).Close();
			var file = File.Create(name);
			if (size != 0)
			{
				file.Seek(size - 1, SeekOrigin.Begin);
				file.WriteByte(0);
			}
			file.Dispose();
		}

		[Fact]
		public async Task GeneralTests()
		{
			foreach (bool allowTrieCache in new[] { false, true })
				foreach (bool cacheStorageLayer in new[] { true, false })
				{
					await using var fs = CreateFileStorage("_DBreezeSchema", cacheStorageLayer);
					var trie = await CreateTrie(fs, allowTrieCache);
					var generationNode = await trie.ReadNode();
					var result = await trie.GetValue("@@@@LastFileNumber");
					Assert.NotNull(result);
					Assert.Equal(64, result!.Pointer);
					Assert.Equal(89, result.ValuePointer);
					Assert.Equal(8, result.ValueLength);
					Assert.Null(await trie.GetValue("notexists"));
					Assert.Null(await trie.GetValue("@u"));
					Assert.Equal(4282, trie.RecordCount);

					var schema = await Schema.OpenOrInitFromTrie(trie);
					Assert.True(await schema.TableExists("IndexProgress"));
					Assert.False(await schema.TableExists("In"));
					Assert.False(await schema.TableExists("IndexProgresss"));
					Assert.False(await schema.TableExists("IndexProgres"));

					var filename = await schema.GetFileNameOrCreate("IndexProgress");
					Assert.Equal(10000006UL, filename);
					Assert.Equal(10004281UL, schema.LastFileNumber);

					// This should create a new record
					filename = await schema.GetFileNameOrCreate("NotExists");
					Assert.Equal(10004282UL, filename);
					Assert.Equal(10004282UL, schema.LastFileNumber);
					Assert.Equal(4283, trie.RecordCount);

					// This should NOT create a new record
					filename = await schema.GetFileNameOrCreate("NotExists");
					Assert.Equal(10004282UL, filename);
					Assert.Equal(10004282UL, schema.LastFileNumber);
					Assert.Equal(4283, trie.RecordCount);

					// Reloading the tree
					trie = await ReloadTrie(trie);

					// We should get back our created table
					filename = await schema.GetFileNameOrCreate("NotExists");
					Assert.Equal(10004282UL, filename);
					Assert.Equal(10004282UL, schema.LastFileNumber);
					Assert.Equal(4283, trie.RecordCount);

					// Let's make sure this has been persisted as well
					schema = await Schema.OpenOrInitFromTrie(trie);
					Assert.Equal(10004282UL, schema.LastFileNumber);

					// Can list tables by name?
					schema = await Schema.OpenOrInitFromTrie(trie);
					var tables = await schema.GetTables("TestTa").ToArrayAsync();
					var ordered = tables.OrderBy(o => o).ToArray();
					Assert.True(tables.SequenceEqual(ordered));
					Assert.Equal(4, tables.Length);
					tables = await schema.GetTables("TestT").ToArrayAsync();
					ordered = tables.OrderBy(o => o).ToArray();
					Assert.True(tables.SequenceEqual(ordered));
					Assert.Equal(4, tables.Length);
					tables = await schema.GetTables("TestTab").ToArrayAsync();
					ordered = tables.OrderBy(o => o).ToArray();
					Assert.True(tables.SequenceEqual(ordered));
					Assert.Equal(3, tables.Length);
					Assert.NotNull(await trie.GetValue("@utTestTa"));

					await AssertMatch(trie, false, "POFwoinfOWu");
					await AssertMatch(trie, false, "@utTestT");
					await AssertMatch(trie, true, "@utTestTa");
					await AssertMatch(trie, true, "@utIndexProg");
					await AssertMatch(trie, true, "@utIndexProgT");
					await AssertMatch(trie, true, "@utIndexProgressss");
					await AssertMatch(trie, true, "@utIndexProgresa");

					tables = await schema.GetTables().ToArrayAsync();
					Assert.Equal(4282, tables.Length);
					ordered = tables.OrderBy(o => o).ToArray();
					Assert.True(tables.SequenceEqual(ordered));
					var r = new Random(0);
					for (int i = 0; i < 10; i++)
					{
						var keys = new string[3];
						keys[0] = RandomWord(5, r);
						keys[1] = keys[0] + RandomWord(1, r);
						keys[2] = keys[1] + RandomWord(1, r);
						var fromShortest = keys.ToArray();
						r.Shuffle(keys);

						// Try adding tables with intermediates
						var recordCountBefore = trie.RecordCount;
						foreach (var k in keys)
							await schema.GetFileNameOrCreate(k);
						tables = await schema.GetTables(fromShortest[0]).ToArrayAsync();
						ordered = tables.OrderBy(o => o).ToArray();
						Assert.True(tables.SequenceEqual(ordered));
						Assert.Equal(keys.Length, tables.Length);
						Assert.Equal(recordCountBefore + keys.Length, trie.RecordCount);
						tables = await schema.GetTables(fromShortest[1]).ToArrayAsync();
						Assert.Equal(keys.Length - 1, tables.Length);

						// Reloading
						trie = await ReloadTrie(trie);
						schema = await Schema.OpenOrInitFromTrie(trie);
						// Make sure our tables are still here
						foreach (var k in keys)
							Assert.True(await schema.TableExists(k));
						tables = await schema.GetTables(fromShortest[0]).ToArrayAsync();
						Assert.Equal(keys.Length, tables.Length);
						Assert.Equal(recordCountBefore + keys.Length, trie.RecordCount);
					}
				}
		}

		[Fact]
		public async Task CanListTransactions()
		{
			foreach (bool allowTrieCache in new[] { false })
				foreach (bool cacheStorageLayer in new[] { true })
				{
					logs.WriteLine($"allowTrieCache: {allowTrieCache}");
					logs.WriteLine($"cacheStorageLayer: {cacheStorageLayer}");
					await using var fs = CreateFileStorage("10000007", cacheStorageLayer);
					LTrie trie = await CreateTrie(fs, allowTrieCache);
					trie.ConsistencyCheck = false;
					DateTimeOffset now = DateTimeOffset.UtcNow;
					int records = 0;
					await foreach (var row in trie.EnumerateStartsWith(""))
					{
						records++;
					}
					logs.WriteLine($"Record count : {records}");
					logs.WriteLine($"Enumerate 1 time : {(int)(DateTimeOffset.UtcNow - now).TotalMilliseconds} ms");
					now = DateTimeOffset.UtcNow;
					await foreach (var row in trie.EnumerateStartsWith(""))
					{

					}
					logs.WriteLine($"Enumerate 2 time : {(int)(DateTimeOffset.UtcNow - now).TotalMilliseconds} ms");
					now = DateTimeOffset.UtcNow;
					await foreach (var row in trie.EnumerateStartsWith(""))
					{
						using var owner = trie.MemoryPool.Rent(row.ValueLength);
						await trie.Storage.Read(row.ValuePointer, owner.Memory.Slice(row.ValueLength));
					}
					logs.WriteLine($"Enumerate values : {(int)(DateTimeOffset.UtcNow - now).TotalMilliseconds} ms");
				}
		}

		[Fact]
		public async Task CanSetKeyValue()
		{
			foreach (bool allowTrieCache in new[] { false, true })
				foreach (bool cacheStorageLayer in new[] { true, false })
				{
					await using var fs = CreateFileStorage("_DBreezeSchema", cacheStorageLayer);
					LTrie trie = await CreateTrie(fs, allowTrieCache);
					var countBefore = trie.RecordCount;
					Assert.Null(await trie.GetValue("CanSetKeyValue"));
					await trie.SetKey("CanSetKeyValue", "CanSetKeyValue-r1");
					Assert.Equal("CanSetKeyValue-r1", await trie.GetValueString("CanSetKeyValue"));
					Assert.Equal(countBefore + 1, trie.RecordCount);
					await trie.SetKey("CanSetKeyValue", "CanSetKeyValue-r2");
					Assert.Equal("CanSetKeyValue-r2", await trie.GetValueString("CanSetKeyValue"));
					Assert.Equal(countBefore + 1, trie.RecordCount);
					trie = await ReloadTrie(trie);
					Assert.Equal(countBefore + 1, trie.RecordCount);
					Assert.Equal("CanSetKeyValue-r2", await trie.GetValueString("CanSetKeyValue"));

					Assert.Null(await trie.GetValue("Relocation"));
					await trie.SetKey("Relocation", "a");
					Assert.Equal("a", await trie.GetValueString("Relocation"));
					Assert.Equal(countBefore + 2, trie.RecordCount);

					Assert.Null(await trie.GetValue("NoRelocation"));
					await trie.SetKey("NoRelocation", "b");
					Assert.Equal("b", await trie.GetValueString("NoRelocation"));
					Assert.Equal(countBefore + 3, trie.RecordCount);

					trie = await ReloadTrie(trie);
					Assert.Equal("a", await trie.GetValueString("Relocation"));
					Assert.Equal("b", await trie.GetValueString("NoRelocation"));
					Assert.Equal("CanSetKeyValue-r2", await trie.GetValueString("CanSetKeyValue"));
					Assert.Equal(countBefore + 3, trie.RecordCount);

					Assert.Null(await trie.GetValue("k"));
					await trie.SetKey("k", "k-r1");
					Assert.Equal("k-r1", await trie.GetValueString("k"));
					await trie.SetKey("k", "k-r2");
					Assert.Equal("k-r2", await trie.GetValueString("k"));
					Assert.Equal(countBefore + 4, trie.RecordCount);

					Assert.Null(await trie.GetValue("CanSetKeyValue-Extended"));
					await trie.SetKey("CanSetKeyValue-Extended", "CanSetKeyValue-Extended-r1");
					Assert.Equal("CanSetKeyValue-Extended-r1", await trie.GetValueString("CanSetKeyValue-Extended"));
					await trie.SetKey("CanSetKeyValue-Extended", "CanSetKeyValue-Extended-r2");
					Assert.Equal(countBefore + 5, trie.RecordCount);

					Assert.Equal("CanSetKeyValue-Extended-r2", await trie.GetValueString("CanSetKeyValue-Extended"));
					Assert.Equal("CanSetKeyValue-r2", await trie.GetValueString("CanSetKeyValue"));
					Assert.Equal("k-r2", await trie.GetValueString("k"));
					Assert.Equal("a", await trie.GetValueString("Relocation"));
					Assert.Equal("b", await trie.GetValueString("NoRelocation"));

					trie = await ReloadTrie(trie);

					Assert.Equal("CanSetKeyValue-Extended-r2", await trie.GetValueString("CanSetKeyValue-Extended"));
					Assert.Equal("CanSetKeyValue-r2", await trie.GetValueString("CanSetKeyValue"));
					Assert.Equal("k-r2", await trie.GetValueString("k"));
					Assert.Equal("a", await trie.GetValueString("Relocation"));
					Assert.Equal("b", await trie.GetValueString("NoRelocation"));
					Assert.Equal(countBefore + 5, trie.RecordCount);

					List<string> insertedKeys = new List<string>();
					Random r = new Random(0);
					for (int i = 0; i < 100; i++)
					{
						countBefore = trie.RecordCount;
						var keys = new string[5];
						int o = 0;
						var startWith = r.PickRandom(new[] {
						"@ut",
						"@",
						"k",
						"CanSetKeyValue",
						"CanSetKeyValueee",
						"CanSetKeyValue-Extended",
						"Relo",
						"Relocationn",
						"R",
						"NoRelocation",
						"" });
						keys[o++] = startWith + RandomWord(5, r);
						keys[o++] = keys[o - 2] + RandomWord(1, r);
						keys[o++] = keys[o - 2] + RandomWord(1, r);
						keys[o++] = keys[o - 2] + RandomWord(1, r);
						keys[o++] = keys[o - 2] + RandomWord(1, r);
						var fromShortest = keys.ToArray();
						r.Shuffle(keys);

						foreach (var k in keys)
						{
							Assert.Equal("CanSetKeyValue-Extended-r2", await trie.GetValueString("CanSetKeyValue-Extended"));
							Assert.True(await trie.SetKey(k, k));
							Assert.Equal("CanSetKeyValue-Extended-r2", await trie.GetValueString("CanSetKeyValue-Extended"));
							Assert.Equal(k, await trie.GetValueString(k));
							insertedKeys.Add(k);
						}
						foreach (var k in keys)
						{
							Assert.Equal(k, await trie.GetValueString(k));
						}
						Assert.Equal(countBefore + keys.Length, trie.RecordCount);
						for (int f = 0; f < fromShortest.Length; f++)
						{
							var all = await trie.EnumerateStartsWith(fromShortest[f]).ToArrayAsync();
							Assert.Equal(keys.Length - f, all.Length);
							Assert.Equal(all.Length, all.Distinct().Count());
						}
					}
					countBefore = trie.RecordCount;
					// Everything kept value
					foreach (var k in insertedKeys)
					{
						Assert.Equal(k, await trie.GetValueString(k));
					}
					// Randomly edit stuff
					HashSet<string> edited = new HashSet<string>();
					foreach (var k in insertedKeys)
					{
						if (r.Next() % 2 == 0)
						{
							Assert.False(await trie.SetKey(k, k + "-r2"));
							edited.Add(k);
						}
					}
					// Everything kept value
					foreach (var k in insertedKeys)
					{
						var expected = edited.Contains(k) ? k + "-r2" : k;
						Assert.Equal(expected, await trie.GetValueString(k));
					}

					// Randomly trucate
					HashSet<string> truncated = new HashSet<string>();
					foreach (var k in insertedKeys)
					{
						if (r.Next() % 2 == 0)
						{
							Assert.False(await trie.SetKey(k, k.GetHashCode().ToString()));
							truncated.Add(k);
						}
					}

					// Randomly delete
					HashSet<string> deleted = new HashSet<string>();
					foreach (var k in insertedKeys)
					{
						if (r.Next() % 2 == 0)
						{
							Assert.True(await trie.DeleteRow(k));
							//Assert.True(await trie.DeleteRow(k), $"Failed to delete {k}");
							deleted.Add(k);
						}
					}

					// Everything kept value
					foreach (var k in insertedKeys)
					{
						if (deleted.Contains(k))
						{
							Assert.Null(await trie.GetValue(k));
						}
						else
						{
							var expected =
							truncated.Contains(k) ? k.GetHashCode().ToString() :
							edited.Contains(k) ? k + "-r2" : k;
							Assert.Equal(expected, await trie.GetValueString(k));
						}
					}

					countBefore -= deleted.Count;
					Assert.Equal(countBefore, trie.RecordCount);
					// Nothing else got edited
					Assert.Equal("CanSetKeyValue-Extended-r2", await trie.GetValueString("CanSetKeyValue-Extended"));
					Assert.Equal("CanSetKeyValue-r2", await trie.GetValueString("CanSetKeyValue"));
					Assert.Equal("k-r2", await trie.GetValueString("k"));
					Assert.Equal("a", await trie.GetValueString("Relocation"));
					Assert.Equal("b", await trie.GetValueString("NoRelocation"));

					// Reload the trie
					trie = await ReloadTrie(trie);
					// Everything kept value
					foreach (var k in insertedKeys)
					{
						if (deleted.Contains(k))
						{
							Assert.Null(await trie.GetValue(k));
						}
						else
						{
							var expected =
							truncated.Contains(k) ? k.GetHashCode().ToString() :
							edited.Contains(k) ? k + "-r2" : k;
							Assert.Equal(expected, await trie.GetValueString(k));
						}
					}
					Assert.Equal(countBefore, trie.RecordCount);
					// Nothing else got edited
					Assert.Equal("CanSetKeyValue-Extended-r2", await trie.GetValueString("CanSetKeyValue-Extended"));
					Assert.Equal("CanSetKeyValue-r2", await trie.GetValueString("CanSetKeyValue"));
					Assert.Equal("k-r2", await trie.GetValueString("k"));
					Assert.Equal("a", await trie.GetValueString("Relocation"));
					Assert.Equal("b", await trie.GetValueString("NoRelocation"));

					var longKey = new string(Enumerable.Range(0, 256).Select(o => 'a').ToArray());
					var longValue = new string(Enumerable.Range(0, 256).Select(o => 'b').ToArray());
					await trie.SetKey(longKey, longValue);
					Assert.Equal(longValue, await trie.GetValueString(longKey));
				}
		}

		[Fact]
		public async Task CanUseDBEngine()
		{
			await using (var engine = await CreateEngine())
			{
				using var tx = await engine.OpenTransaction();
				// Open existing table
				var table = tx.GetOrCreateTable("Transactions");
				Assert.Equal(7817, (await table.GetTrie()).RecordCount);
				await table.Insert("test", "value");
			}
			await using (var engine = await CreateEngine(false))
			{
				using var tx = await engine.OpenTransaction();
				// Open existing table
				var table = tx.GetOrCreateTable("Transactions");
				Assert.Equal(7817, (await table.GetTrie()).RecordCount);
				Assert.Null(await table.Get("test"));
			}
			await using (var engine = await CreateEngine())
			{
				using var tx = await engine.OpenTransaction();
				// Open existing table
				var table = tx.GetOrCreateTable("Transactions");
				engine.ConsistencyCheck = true;
				Assert.Equal(7817, (await table.GetTrie()).RecordCount);
				await table.Insert("test", "value");
				var row = await table.Get("test");
				Assert.NotNull(row);
				Assert.Equal("value", await row!.ReadValueString());
				await tx.Commit();
				row = await table.Get("test");
				Assert.NotNull(row);
				Assert.Equal("value", await row!.ReadValueString());
				Assert.Equal(7818, (await table.GetTrie()).RecordCount);
			}
			await using (var engine = await CreateEngine(false))
			{
				using var tx = await engine.OpenTransaction();
				// Open existing table
				var table = tx.GetOrCreateTable("Transactions");
				Assert.Equal(7818, (await table.GetTrie()).RecordCount);
				var row = await table.Get("test");
				Assert.NotNull(row);
				Assert.Equal("value", await row!.ReadValueString());
			}

			await using (var engine = await CreateEngine())
			{
				using var tx = await engine.OpenTransaction();
				var table = tx.GetOrCreateTable("Transactions");
				Assert.Equal(7817, (await table.GetTrie()).RecordCount);
				await table.Insert("test", "value");
				var row = await table.Get("test");
				Assert.NotNull(row);
				Assert.Equal("value", await row!.ReadValueString());
				Assert.Equal(7818, (await table.GetTrie()).RecordCount);
				tx.Rollback();
				row = await table.Get("test");
				Assert.Null(row);
				Assert.Equal(7817, (await table.GetTrie()).RecordCount);

				var allRows = await table.Enumerate().ToArrayAsync();
				Assert.Equal(7817, allRows.Length);
			}
			await using (var engine = await CreateEmptyEngine())
			{
				using var tx = await engine.OpenTransaction();
				var table = tx.GetOrCreateTable("MyTable");
				await table.Insert("qweq", "eqr");
				await table.Commit();
				Assert.Equal("eqr", await (await table.Get("qweq"))!.ReadValueString());
			}
			await using (var engine = await CreateEmptyEngine(false))
			{
				using var tx = await engine.OpenTransaction();
				var table = tx.GetOrCreateTable("MyTable");
				Assert.Equal("eqr", await (await table.Get("qweq"))!.ReadValueString());

				var elements = await table.Enumerate("qweq").ToArrayAsync();
				Assert.Single(elements);
			}

			{
				var engine = await CreateEmptyEngine(false);
				var tx = await engine.OpenTransaction();
				// stopping the engine should wait for the last transaction to be finished
				var disposing = engine.DisposeAsync();
				await Task.Delay(100);
				Assert.False(disposing.IsCompleted);
				tx.Dispose();
				await disposing;
			}
		}

		private async ValueTask<DBTrieEngine> CreateEmptyEngine(bool clean = true, [CallerMemberName] string? caller = null)
		{
			caller ??= "";
			CleanIfNecessary(clean, caller);
			var engine = await DBTrieEngine.OpenFromFolder(caller);
			engine.ConsistencyCheck = true;
			return engine;
		}

		private async ValueTask<DBTrieEngine> CreateEngine(bool clean = true, [CallerMemberName] string? caller = null)
		{
			caller ??= "";
			CleanIfNecessary(clean, caller);
			if (clean)
			{
				foreach (var file in Directory.GetFiles("Data"))
				{
					File.Copy(file, Path.Combine(caller, Path.GetFileName(file)));
				}
			}
			var engine = await DBTrieEngine.OpenFromFolder(caller);
			engine.ConsistencyCheck = true;
			return engine;
		}

		private static void CleanIfNecessary(bool clean, string? caller)
		{
			Directory.CreateDirectory(caller);
			if (clean)
			{
				foreach (var file in Directory.GetFiles(caller))
					File.Delete(file);
			}
		}

		private static async ValueTask<LTrie> ReloadTrie(LTrie trie)
		{
			var cache = trie.Storage as CacheStorage;
			var trie2 = await CreateTrie(trie.Storage, trie.NodeCache is { });
			trie2.ConsistencyCheck = trie.ConsistencyCheck;
			return trie2;
		}

		private static async ValueTask<LTrie> CreateTrie(IStorage fs, bool allowGenerationNodeCache)
		{
			var trie = await LTrie.OpenFromStorage(fs);
			trie.ConsistencyCheck = true;
			if (allowGenerationNodeCache)
				trie.ActivateCache();
			return trie;
		}

		private async Task AssertMatch(LTrie trie, bool linkToValue, string search)
		{
			var result = await trie.FindBestMatch(Encoding.UTF8.GetBytes(search));
			Assert.Equal(linkToValue, result.ValueLink is Link);
		}

		private string RandomWord(int minSize, Random r)
		{
			var alphabet = new[] { 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j' };
			var count = r.Next(minSize, 10);
			return new string(Enumerable.Range(0, count)
				.Select(_ => r.PickRandom(alphabet))
				.ToArray());
		}

		private IStorage CreateFileStorage(string file, bool cacheStorageLayer, [CallerMemberName] string? caller = null)
		{
			if (caller is null)
				throw new ArgumentNullException(nameof(caller));
			Directory.CreateDirectory(caller);
			File.Copy($"Data/{file}", $"{caller}/{file}", true);
			var fs = new FileStorage($"{caller}/{file}");
			if (!cacheStorageLayer)
				return fs;
			return new CacheStorage(fs);
		}
	}
}
