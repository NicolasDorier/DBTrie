using DBTrie.Storage;
using DBTrie.Storage.Cache;
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
using System.Text.Unicode;
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
			var r = new Random(0);
			foreach (var orderInserts in new[] { true, false })
			{
				foreach (var useCache in new[] { true, false })
				{
					using var t = CreateTester();
					t.CreateEmptyFile("Empty2", 0);
					await using var fs = new FileStorage("Empty2");
					var cache = new CacheStorage(fs, false);
					var trie = await LTrie.InitTrie(useCache ? (IStorage)cache : fs);
					await cache.Flush();
					trie = await t.ReloadTrie(trie);
					Assert.Null(await trie.GetValueString(1 + "test" + 1));

					int[] inserts = new[] { 0, 1, 2, 3, 4 };
					if (!orderInserts)
						r.Shuffle(inserts);
					foreach (var i in inserts)
					{
						await trie.SetValue(i + "test" + i, "lol" + i);
						Assert.Equal("lol" + i, await trie.GetValueString(i + "test" + i));
					}
					await trie.SetValue("1tes", "lol1tes");
					for (int i = 0; i < 5; i++)
					{
						Assert.Equal("lol" + i, await trie.GetValueString(i + "test" + i));
					}
					trie = await LTrie.OpenFromStorage(fs);
					for (int i = 0; i < 5; i++)
					{
						if (useCache)
							Assert.Null(await trie.GetValueString(i + "test" + i));
						else
							Assert.NotNull(await trie.GetValueString(i + "test" + i));
					}
					await cache.Flush();
					trie = await t.ReloadTrie(trie);
					for (int i = 0; i < 5; i++)
					{
						Assert.Equal("lol" + i, await trie.GetValueString(i + "test" + i));
					}

					var rows = await trie.EnumerateStartsWith("").ToArrayAsync();
					Assert.Equal(5 + 1, rows.Length);
					var actualOrder = rows.Select(r => UTF8Encoding.UTF8.GetString(r.Key.Span)).ToList();
					var ordered = actualOrder.OrderBy(o => o).ToArray();
					Assert.True(actualOrder.SequenceEqual(ordered));
					rows.DisposeAll();
				}
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
				await t.Trie.SetValue("test", "lol");
				Assert.Equal("lol", await t.Trie.GetValueString("test"));
				Assert.True(await t.Trie.DeleteRow("test"));
				Assert.False(await t.Trie.DeleteRow("test"));
				Assert.Null(await t.Trie.GetValueString("test"));
			}
			await using (var t = await TrieTester.Create())
			{
				await t.Trie.SetValue("test", "lol1");
				await t.Trie.SetValue("test2", "lol2");
				Assert.Equal(2, t.Trie.RecordCount);
				Assert.True(await t.Trie.DeleteRow("test"));
				Assert.False(await t.Trie.DeleteRow("test"));
				Assert.Null(await t.Trie.GetValueString("test"));
				Assert.Equal("lol2", await t.Trie.GetValueString("test2"));
				Assert.Equal(1, t.Trie.RecordCount);
			}
			await using (var t = await TrieTester.Create())
			{
				await t.Trie.SetValue("test", "lol1");
				await t.Trie.SetValue("test2", "lol2");
				Assert.True(await t.Trie.DeleteRow("test2"));
				Assert.False(await t.Trie.DeleteRow("test2"));
				Assert.Null(await t.Trie.GetValueString("test2"));
				Assert.Equal("lol1", await t.Trie.GetValueString("test"));
			}
			await using (var t = await TrieTester.Create())
			{
				Assert.Equal(0, t.Trie.RecordCount);
				await t.Trie.SetValue("test1", "lol1");
				await t.Trie.SetValue("test2", "lol2");
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
				await t.Trie.SetValue("test1", "lol1");
				await t.Trie.SetValue("test2", "lol2");
				Assert.True(await t.Trie.SetValue("test", "lol"));
				Assert.Equal(3, t.Trie.RecordCount);
				Assert.False(await t.Trie.SetValue("test", "newlol"));
				Assert.Equal(3, t.Trie.RecordCount);
				Assert.True(await t.Trie.DeleteRow("test"));
				Assert.Equal("lol2", await t.Trie.GetValueString("test2"));
				Assert.Equal("lol1", await t.Trie.GetValueString("test1"));
				Assert.Equal(2, t.Trie.RecordCount);
				Assert.True(await t.Trie.SetValue("test", "newlol"));
				Assert.Equal(3, t.Trie.RecordCount);
			}
		}

		[Fact]
		public async Task CanUseCacheFastGet()
		{
			using var t = CreateTester();
			t.CreateEmptyFile("CanUseCacheFastGet", 105);
			await using var fs = new FileStorage("CanUseCacheFastGet");
			var cache = new CacheStorage(fs, true, new CacheSettings() { PageSize = 10 });
			Assert.False(cache.TryDirectRead(0, 1, out var mem));
			await cache.Read(0, 1);
			Assert.True(cache.TryDirectRead(0, 1, out mem));
			Assert.True(cache.TryDirectRead(9, 1, out mem));
			Assert.False(cache.TryDirectRead(10, 1, out mem));
			Assert.False(cache.TryDirectRead(9, 2, out mem));
			Assert.False(cache.TryDirectRead(0, 11, out mem));
			await cache.Read(100, 1);
			Assert.True(cache.TryDirectRead(104, 1, out mem));
			Assert.False(cache.TryDirectRead(105, 1, out mem));
			Assert.False(cache.TryDirectRead(106, 1, out mem));
			Assert.False(cache.TryDirectRead(104, 2, out mem));
			Assert.False(cache.TryDirectRead(110, 1, out mem));

			Assert.True(cache.TryDirectRead(100, 5, out mem));
			Assert.Equal(5, mem.Length);
			await cache.Write(101, "helo");
			Assert.True(cache.TryDirectRead(101, 4, out mem));
			Assert.Equal("helo", Encoding.UTF8.GetString(mem.Span));
		}

		[Fact]
		public async Task CacheTests()
		{
			using var t = CreateTester();
			t.CreateEmptyFile("Empty", 1030);
			await using var fs = new FileStorage("Empty");
			var cache = new CacheStorage(fs, true, new CacheSettings() { PageSize = 128 });
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
			var l = (1030 + "helloworldabdwuqiwiw".Length);
			Assert.Equal(l, fs.Length);
			await fs.Resize(l + 10);
			Assert.Equal(l + 10, fs.Length);
			await fs.Resize(l + 10 + 1);
			Assert.Equal(l + 10 + 1, fs.Length);
			await fs.Resize(l + 10 + 1 + 0);
			Assert.Equal(l + 10 + 1 + 0, fs.Length);
		}
		[Fact]
		public async Task CanCacheWithLRU()
		{
			using var t = CreateTester();
			t.CreateEmptyFile("CanCacheWithLRU", 100);
			await using var fs = new FileStorage("CanCacheWithLRU");
			var cache = new CacheStorage(fs, false, new CacheSettings()
			{
				PageSize = 10,
				MaxPageCount = 2,
				AutoCommitEvictedPages = false
			});
			// If we write on page 0.
			await cache.Write(0, CreateString(5));
			Assert.Contains(0, cache.pages.Keys);
			Assert.Single(cache.pages);
			Assert.Equal(1, cache.PagePool.PageCount);
			Assert.Equal(1, cache.PagePool.FreePageCount);
			Assert.Equal(0, cache.PagePool.EvictablePageCount);
			// We read on page 1
			await cache.Read(10, 1);
			Assert.Contains(1, cache.pages.Keys);
			Assert.Equal(2, cache.pages.Count);
			Assert.Equal(0, cache.PagePool.FreePageCount);
			Assert.Equal(1, cache.PagePool.EvictablePageCount);
			// we now write on page 2, page 1 is only read so should be evicted, even though it is not the most recently used
			await cache.Write(20, "a");
			Assert.DoesNotContain(1, cache.pages.Keys);
			Assert.Contains(0, cache.pages.Keys);
			Assert.Contains(2, cache.pages.Keys);
			Assert.Equal(2, cache.PagePool.PageCount);
			Assert.Equal(0, cache.PagePool.FreePageCount);
			Assert.Equal(0, cache.PagePool.EvictablePageCount);
			// Make sure that writing again or reading, does not put this page back into evictable
			await cache.Write(20, "aa");
			Assert.Equal(0, cache.PagePool.EvictablePageCount);
			await cache.Read(20, 2);
			Assert.Equal(0, cache.PagePool.EvictablePageCount);

			// We write on the 4rd, but because both the 3rd and the 1st page are written, it should throw
			await Assert.ThrowsAsync<NoMorePageAvailableException>(async () => await cache.Write(30, "a"));
			await cache.Flush();
			Assert.Equal(2, cache.PagePool.PageCount);
			Assert.Equal(0, cache.PagePool.FreePageCount);
			Assert.Equal(2, cache.PagePool.EvictablePageCount);
			await cache.Write(30, "a");
			Assert.Equal(2, cache.PagePool.PageCount);
			Assert.Equal(0, cache.PagePool.FreePageCount);
			Assert.Equal(1, cache.PagePool.EvictablePageCount);
			cache.Clear(false);
			Assert.Equal(0, cache.PagePool.PageCount);
			Assert.Equal(2, cache.PagePool.FreePageCount);
			Assert.Equal(0, cache.PagePool.EvictablePageCount);
		}
		[Fact]
		public async Task CanCacheWithLRUWithAutoCommit()
		{
			using var t = CreateTester();
			t.CreateEmptyFile("CanCacheWithLRUWithAutoCommit", 100);
			await using var fs = new FileStorage("CanCacheWithLRUWithAutoCommit");
			var cache = new CacheStorage(fs, false, new CacheSettings()
			{
				PageSize = 10,
				MaxPageCount = 2,
				AutoCommitEvictedPages = true
			});
			// If we write on page 0.
			await cache.Write(0, CreateString(5));
			Assert.Contains(0, cache.pages.Keys);
			Assert.Single(cache.pages);
			Assert.Equal(1, cache.PagePool.PageCount);
			Assert.Equal(1, cache.PagePool.FreePageCount);
			Assert.Equal(1, cache.PagePool.EvictablePageCount);
			// We read on page 1
			await cache.Read(10, 1);
			Assert.Contains(1, cache.pages.Keys);
			Assert.Equal(2, cache.pages.Count);
			Assert.Equal(0, cache.PagePool.FreePageCount);
			Assert.Equal(2, cache.PagePool.EvictablePageCount);
			// we now write on page 2, page 0 should be evicted
			await cache.Write(20, "ab");
			Assert.DoesNotContain(0, cache.pages.Keys);
			Assert.Contains(1, cache.pages.Keys);
			Assert.Contains(2, cache.pages.Keys);
			Assert.Equal(2, cache.PagePool.PageCount);
			Assert.Equal(0, cache.PagePool.FreePageCount);
			Assert.Equal(2, cache.PagePool.EvictablePageCount);
			// Make sure that writing again or reading, does not remove this page from eviction
			await cache.Write(20, "ab");
			Assert.Equal(2, cache.PagePool.EvictablePageCount);
			await cache.Read(20, 2);
			Assert.Equal(2, cache.PagePool.EvictablePageCount);
			Assert.DoesNotContain(0, cache.pages.Keys);
			Assert.Contains(1, cache.pages.Keys);
			Assert.Contains(2, cache.pages.Keys);

			// We write page 3 so 1 should be evicted
			await cache.Write(30, "a");
			Assert.Equal(2, cache.PagePool.PageCount);
			Assert.Equal(0, cache.PagePool.FreePageCount);
			Assert.Equal(2, cache.PagePool.EvictablePageCount);
			Assert.DoesNotContain(0, cache.pages.Keys);
			Assert.DoesNotContain(1, cache.pages.Keys);
			Assert.Contains(2, cache.pages.Keys);
			Assert.Contains(3, cache.pages.Keys);

			// We write page 4 so 2 should be evicted
			await cache.Write(40, "a");
			Assert.DoesNotContain(0, cache.pages.Keys);
			Assert.DoesNotContain(1, cache.pages.Keys);
			Assert.DoesNotContain(2, cache.pages.Keys);
			Assert.Contains(3, cache.pages.Keys);
			Assert.Contains(4, cache.pages.Keys);

			// But should have been committed, should evict 3
			Assert.Equal("ab", await cache.Read(20, 2));
			Assert.DoesNotContain(0, cache.pages.Keys);
			Assert.DoesNotContain(1, cache.pages.Keys);
			Assert.DoesNotContain(3, cache.pages.Keys);
			Assert.Contains(2, cache.pages.Keys);
			Assert.Contains(4, cache.pages.Keys);
		}

		private static string CreateString(int len)
		{
			return new String(Enumerable.Range(0, len).Select(_ => 'a').ToArray());
		}

		[Fact]
		public void LRUTest()
		{
			var lru = new LRU<long>();
			lru.Accessed(1);
			lru.Accessed(2);
			lru.Accessed(3);
			Assert.True(lru.TryPop(out var p) && p == 1);
			Assert.True(lru.TryPop(out p) && p == 2);
			Assert.True(lru.TryPop(out p) && p == 3);
			Assert.False(lru.TryPop(out p));
			lru.Accessed(1);
			lru.Accessed(2);
			lru.Accessed(3);
			Assert.True(lru.TryPop(out p) && p == 1);
			lru.Accessed(2);
			Assert.True(lru.TryPop(out p) && p == 3);
			Assert.True(lru.TryPop(out p) && p == 2);
			Assert.False(lru.TryPop(out p));
			lru.Accessed(1);
			lru.Accessed(2);
			lru.Accessed(3);
			lru.Accessed(1);
			Assert.True(lru.TryPop(out p) && p == 2);
			Assert.True(lru.TryPop(out p) && p == 3);
			Assert.True(lru.TryPop(out p) && p == 1);
			Assert.False(lru.TryPop(out p));

			lru.Push(1);
			lru.Push(2);
			lru.Push(3);
			Assert.True(lru.TryPop(out p) && p == 3);
		}

		[Fact]
		public async Task TestResize()
		{
			using var t = CreateTester();
			t.CreateEmptyFile("EmptyResizable", 0);
			await using var fs = new FileStorage("EmptyResizable");
			await TestResizeCore(0, fs);

			t.CreateEmptyFile("EmptyResizable2", 0);
			await using var fs2 = new FileStorage("EmptyResizable2");
			await using var cache = new CacheStorage(fs2, false);
			await TestResizeCore(0, cache);
			await TestResizeCore(cache.PageSize, cache);
			await TestResizeCore(cache.PageSize - 1, cache);
			await TestResizeCore(cache.PageSize - 3, cache);
			await TestResizeCore(cache.PageSize + 4, cache);
			Assert.Equal(2, cache.MappedPageCount);
			await cache.Resize(cache.PageSize);
			Assert.Equal(1, cache.MappedPageCount);
			Assert.Equal(cache.PageSize, cache.Length);
			var cacheLengthBefore = cache.Length;
			var fsLengthBefore = fs2.Length;
			await cache.WriteToEnd("ab");
			Assert.Equal(2, cache.MappedPageCount);
			Assert.Equal(fsLengthBefore, fs2.Length);
			Assert.Equal(cacheLengthBefore + 2, cache.Length);
			await cache.Flush();
			Assert.Equal("ab", await fs2.Read(cacheLengthBefore, 2));
			Assert.Equal(fs2.Length, cache.Length);
			Assert.Equal(cacheLengthBefore + 2, cache.Length);
			await cache.Resize(0);
			Assert.Equal(0, cache.Length);
			Assert.Equal(cacheLengthBefore + 2, fs2.Length);
			await cache.Flush();
			Assert.Equal(0, fs2.Length);

			await cache.Resize(cache.PageSize - 1);
			await cache.WriteToEnd("ABCDE");
			await cache.Flush();
			Assert.Equal("ABCDE", await fs2.Read(cache.PageSize - 1, 5));
		}

		private async Task TestResizeCore(int offset, IStorage store)
		{
			await store.Write(offset, "hello");
			await store.Resize(offset + "hello".Length - 1);
			Assert.Equal("hell\0", await store.Read(offset, "hello".Length));
			Assert.Equal(offset + "hell".Length, store.Length);
			await store.Resize(offset + 13);
			Assert.Equal(offset + 13, store.Length);
			Assert.Equal("hell\0", await store.Read(offset, "hello".Length));
		}

		[Fact]
		public async Task GeneralTests()
		{
			foreach (bool allowTrieCache in new[] { false, true })
				foreach (bool cacheStorageLayer in new[] { true, false })
				{
					using var t = CreateTester();
					await using var fs = t.CreateFileStorage("_DBreezeSchema", cacheStorageLayer);
					var trie = await t.CreateTrie(fs, allowTrieCache);
					var generationNode = await trie.ReadNode();
					var result = await trie.GetValue("@@@@LastFileNumber");
					Assert.NotNull(result);
					Assert.Equal(64, result!.Pointer);
					Assert.Equal(89, result.ValuePointer);
					Assert.Equal(8, result.ValueLength);
					result.Dispose();
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
					trie = await t.ReloadTrie(trie);

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

					await trie.AssertExists("@utTestTa");

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
						trie = await t.ReloadTrie(trie);
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
		public async Task OrderTests()
		{
			await using (var engine = await CreateEmptyEngine())
			{
				using var tx = await engine.OpenTransaction();
				var table = tx.GetTable("Transactions");
				await table.Insert("1", "1");
				await table.Insert("2", "2");
				await table.Insert("3", "3");
				await table.Delete("1");
				await table.Insert("4", "4");
				var values = await EnumerateKeys(table, EnumerationOrder.Ordered);
				Assert.True(new string[] { "2", "3", "4" }.SequenceEqual(values));
				values = await EnumerateKeys(table, EnumerationOrder.Unordered);
				Assert.True(new string[] { "4", "2", "3" }.SequenceEqual(values));
				await table.Insert("5", "5");
				values = await EnumerateKeys(table, EnumerationOrder.Unordered);
				Assert.True(new string[] { "4", "2", "3", "5" }.SequenceEqual(values));
				await table.Delete("4");
				await table.Delete("2");
				await table.Delete("5");
				await table.Insert("6", "6");
				values = await EnumerateKeys(table, EnumerationOrder.Unordered);
				Assert.True(new string[] { "6", "3" }.SequenceEqual(values));
				await table.Insert("7", "7");
				await table.Insert("8", "8");
				values = await EnumerateKeys(table, EnumerationOrder.Unordered);
				Assert.True(new string[] { "6", "7", "3", "8" }.SequenceEqual(values));
				values = await EnumerateKeys(table, EnumerationOrder.Ordered);
				Assert.True(new string[] { "3", "6", "7", "8" }.SequenceEqual(values));
			}
		}

		private static async Task<string[]> EnumerateKeys(Table table, EnumerationOrder order)
		{
			var records = await table.Enumerate("", order).ToArrayAsync();
			var values = records.Select(r => UTF8Encoding.UTF8.GetString(r.Key.Span)).ToArray();
			records.DisposeAll();
			return values;
		}

		[Fact]
		public async Task CanDefragment()
		{
			int countBefore = 0;
			await using (var engine = await CreateEngine())
			{
				using var tx = await engine.OpenTransaction();
				var table = tx.GetTable("Transactions");
				countBefore = (await table.Enumerate().ToArrayAsync()).Length;
				var trie = await table.GetTrie();
				var lengthBefore = ((CacheStorage)trie.Storage).Length;
				Assert.Equal(lengthBefore, ((CacheStorage)trie.Storage).InnerStorage.Length);
				var allRows = await table.Enumerate().ToArrayAsync();
				var lastRowBefore = (LTrieValue)allRows.OrderByDescending(a => ((LTrieValue)a).Pointer).First();
				allRows.DisposeAll();
				var lastValueBefore = new byte[lastRowBefore.ValueLength];
				(await lastRowBefore.ReadValue()).CopyTo(lastValueBefore);
				var saving = await table.Defragment();
				Assert.NotEqual(0, saving);
				Assert.Equal(0, await table.Defragment());
				trie = await table.GetTrie();
				Assert.Equal(lengthBefore - saving, ((CacheStorage)trie.Storage).InnerStorage.Length);
				Assert.Equal(lengthBefore - saving, ((CacheStorage)trie.Storage).Length);
				allRows = await table.Enumerate().ToArrayAsync();
				var lastRow = (LTrieValue)allRows.OrderByDescending(a => ((LTrieValue)a).Pointer).First();
				var lastValue = new byte[lastRow.ValueLength];
				(await lastRow.ReadValue()).CopyTo(lastValue);
				Assert.Equal(countBefore, allRows.Length);
				Assert.True(lastRow.ValuePointer + lastRow.ValueLength <= ((CacheStorage)trie.Storage).Length);
				Assert.True(lastValue.SequenceEqual(lastValueBefore));
				allRows.DisposeAll();
			}
			await using (var engine = await CreateEngine(false))
			{
				using var tx = await engine.OpenTransaction();
				var table = tx.GetTable("Transactions");
				Assert.Equal(0, await table.Defragment());
				Assert.Equal(countBefore, (await table.Enumerate().ToArrayAsync()).Length);
			}

			await using (var engine = await CreateEngine(false))
			{
				using var tx = await engine.OpenTransaction();
				var table = tx.GetTable("Transactions");
				// Make sure table is open
				await table.Get("test");
				var tableFile = Path.Combine(nameof(CanDefragment), "10000007");
				Assert.True(File.Exists(tableFile));
				await table.Delete();
				Assert.False(File.Exists(tableFile));
				// No burn if double delete
				await table.Delete();
			}

			// Can defragment with tight memory requirements (same test as the first one, but with limits)
			await using (var engine = await CreateEngine())
			{
				engine.ConfigurePagePool(new PagePool(1024 * 4, 10));
				using var tx = await engine.OpenTransaction();
				var table = tx.GetTable("Transactions");
				countBefore = (await table.Enumerate().ToArrayAsync()).Length;
				var trie = await table.GetTrie();
				var lengthBefore = ((CacheStorage)trie.Storage).Length;
				Assert.Equal(lengthBefore, ((CacheStorage)trie.Storage).InnerStorage.Length);
				var saving = await table.Defragment();
				Assert.NotEqual(0, saving);
				Assert.Equal(0, await table.Defragment());
				trie = await table.GetTrie();
				Assert.Equal(lengthBefore - saving, ((CacheStorage)trie.Storage).InnerStorage.Length);
				Assert.Equal(lengthBefore - saving, ((CacheStorage)trie.Storage).Length);
				Assert.Equal(countBefore, (await table.Enumerate().ToArrayAsync()).Length);
			}

			// Can unsafedefragment
			await using (var engine = await CreateEngine())
			{
				engine.ConfigurePagePool(new PagePool(1024 * 4, 10));
				using var tx = await engine.OpenTransaction();
				var table = tx.GetTable("Transactions");
				countBefore = (await table.Enumerate().ToArrayAsync()).Length;
				var trie = await table.GetTrie();
				var lengthBefore = ((CacheStorage)trie.Storage).Length;
				Assert.Equal(lengthBefore, ((CacheStorage)trie.Storage).InnerStorage.Length);
				var saving = await table.UnsafeDefragment();
				Assert.NotEqual(0, saving);
				Assert.Equal(0, await table.UnsafeDefragment());
				trie = await table.GetTrie();
				Assert.Equal(lengthBefore - saving, ((CacheStorage)trie.Storage).InnerStorage.Length);
				Assert.Equal(lengthBefore - saving, ((CacheStorage)trie.Storage).Length);
				Assert.Equal(countBefore, (await table.Enumerate().ToArrayAsync()).Length);
			}
		}

		//[Fact]
		//public void CanListTransactionsDBriize()
		//{
		//	var eng = new DBriize.DBriizeEngine("Data");
		//	var tx = eng.GetTransaction();
		//	tx.ValuesLazyLoadingIsOn = false;
		//	DateTimeOffset now = DateTimeOffset.UtcNow;
		//	var a = tx.SelectForwardStartsWith<string,byte[]>("Transactions", "c").ToList();
		//	logs.WriteLine($"Enumerate 1 time : {(int)(DateTimeOffset.UtcNow - now).TotalMilliseconds} ms");
		//	now = DateTimeOffset.UtcNow;
		//	tx.SelectForwardStartsWith<string, byte[]>("Transactions", "c").ToList();
		//	logs.WriteLine($"Enumerate 2 time : {(int)(DateTimeOffset.UtcNow - now).TotalMilliseconds} ms");
		//}

		[Fact]
		public async Task EnumerateTestVector()
		{
			using var t = CreateTester();
			await using var fs = t.CreateFileStorage("_DBreezeSchema2", true);
			var trie = await DBTrie.TrieModel.LTrie.OpenFromStorage(fs);
			var records = await trie.EnumerateStartsWith("@uttx-").ToArrayAsync();
			records.DisposeAll();
			Assert.Equal(6, records.Length);
			records = await trie.EnumerateStartsWith("@uttx-", EnumerationOrder.Unordered).ToArrayAsync();
			records.DisposeAll();
			Assert.Equal(6, records.Length);
			var schema = await Schema.OpenOrInitFromTrie(trie);
			var tables = await schema.GetTables("tx-").ToArrayAsync();
			Assert.Equal(6, tables.Length);
			tables = tables.Distinct().ToArray();
			Assert.Equal(6, tables.Length);

			await foreach (var tableName in schema.GetTables("tx-"))
			{
				// Can't modify table while iterating on it
				await Assert.ThrowsAsync<InvalidOperationException>(async () => await trie.SetValue("lol", "lol"));
				await Assert.ThrowsAsync<InvalidOperationException>(async () => await trie.DeleteRow("lol"));
				break;
			}
			await trie.SetValue("lol", "lol");
			var enumerable = trie.EnumerateStartsWith("@uttx-", EnumerationOrder.Unordered);
			await trie.SetValue("lol", "lol");
			var enumerator = enumerable.GetAsyncEnumerator();
			await enumerator.MoveNextAsync();
			await Assert.ThrowsAsync<InvalidOperationException>(async () => await trie.DeleteRow("lol"));
			await enumerator.DisposeAsync();
			await trie.DeleteRow("lol");
		}

		[Fact]
		public async Task CanListTransactions()
		{
			for (int i = 0; i < 1; i++)
			{
				foreach (bool allowTrieCache in new[] { false })
					foreach (bool cacheStorageLayer in new[] { true })
					{
						using var t = CreateTester();
						logs.WriteLine($"allowTrieCache: {allowTrieCache}");
						logs.WriteLine($"cacheStorageLayer: {cacheStorageLayer}");
						await using var fs = t.CreateFileStorage("10000007", cacheStorageLayer);
						LTrie trie = await t.CreateTrie(fs, allowTrieCache);
						trie.ConsistencyCheck = false;
						DateTimeOffset now = DateTimeOffset.UtcNow;
						int records = 0;
						await foreach (var row in trie.EnumerateStartsWith(""))
						{
							records++;
							row.Dispose();
						}
						logs.WriteLine($"Record count : {records}");
						logs.WriteLine($"Enumerate 1 time : {(int)(DateTimeOffset.UtcNow - now).TotalMilliseconds} ms");
						now = DateTimeOffset.UtcNow;
						await foreach (var row in trie.EnumerateStartsWith(""))
						{
							row.Dispose();
						}
						logs.WriteLine($"Enumerate 2 time : {(int)(DateTimeOffset.UtcNow - now).TotalMilliseconds} ms");
						now = DateTimeOffset.UtcNow;
						await foreach (var row in trie.EnumerateStartsWith(""))
						{
							using var owner = trie.MemoryPool.Rent(row.ValueLength);
							await trie.Storage.Read(row.ValuePointer, owner.Memory.Slice(row.ValueLength));
							row.Dispose();
						}
						logs.WriteLine($"Enumerate values : {(int)(DateTimeOffset.UtcNow - now).TotalMilliseconds} ms");

						//logs.WriteLine($"Defrag saved {await trie.Defragment()} bytes");
						//await trie.Storage.Flush();
						//trie = await ReloadTrie(trie);
						//if (trie.Storage is CacheStorage c)
						//	c.Clear(false);
						//now = DateTimeOffset.UtcNow;
						//await foreach (var row in trie.EnumerateStartsWith(""))
						//{
						//	row.Dispose();
						//}
						//logs.WriteLine($"Enumerate values after defrag: {(int)(DateTimeOffset.UtcNow - now).TotalMilliseconds} ms");

						//now = DateTimeOffset.UtcNow;
						//await foreach (var row in trie.EnumerateStartsWith(""))
						//{
						//	row.Dispose();
						//}
						//logs.WriteLine($"Enumerate values after defrag 2 times: {(int)(DateTimeOffset.UtcNow - now).TotalMilliseconds} ms");
					}
			}
		}

		[Fact]
		public async Task CanSetKeyValue()
		{
			foreach (bool allowTrieCache in new[] { false, true })
				foreach (bool cacheStorageLayer in new[] { true, false })
				{
					using var t = CreateTester();
					await using var fs = t.CreateFileStorage("_DBreezeSchema", cacheStorageLayer);
					LTrie trie = await t.CreateTrie(fs, allowTrieCache);
					var countBefore = trie.RecordCount;
					Assert.Null(await trie.GetValue("CanSetKeyValue"));
					await trie.SetValue("CanSetKeyValue", "CanSetKeyValue-r1");
					Assert.Equal("CanSetKeyValue-r1", await trie.GetValueString("CanSetKeyValue"));
					Assert.Equal(countBefore + 1, trie.RecordCount);
					await trie.SetValue("CanSetKeyValue", "CanSetKeyValue-r2");
					Assert.Equal("CanSetKeyValue-r2", await trie.GetValueString("CanSetKeyValue"));
					Assert.Equal(countBefore + 1, trie.RecordCount);
					trie = await t.ReloadTrie(trie);
					Assert.Equal(countBefore + 1, trie.RecordCount);
					Assert.Equal("CanSetKeyValue-r2", await trie.GetValueString("CanSetKeyValue"));

					Assert.Null(await trie.GetValue("Relocation"));
					await trie.SetValue("Relocation", "a");
					Assert.Equal("a", await trie.GetValueString("Relocation"));
					Assert.Equal(countBefore + 2, trie.RecordCount);

					Assert.Null(await trie.GetValue("NoRelocation"));
					await trie.SetValue("NoRelocation", "b");
					Assert.Equal("b", await trie.GetValueString("NoRelocation"));
					Assert.Equal(countBefore + 3, trie.RecordCount);

					trie = await t.ReloadTrie(trie);
					Assert.Equal("a", await trie.GetValueString("Relocation"));
					Assert.Equal("b", await trie.GetValueString("NoRelocation"));
					Assert.Equal("CanSetKeyValue-r2", await trie.GetValueString("CanSetKeyValue"));
					Assert.Equal(countBefore + 3, trie.RecordCount);

					Assert.Null(await trie.GetValue("k"));
					await trie.SetValue("k", "k-r1");
					Assert.Equal("k-r1", await trie.GetValueString("k"));
					await trie.SetValue("k", "k-r2");
					Assert.Equal("k-r2", await trie.GetValueString("k"));
					Assert.Equal(countBefore + 4, trie.RecordCount);

					Assert.Null(await trie.GetValue("CanSetKeyValue-Extended"));
					await trie.SetValue("CanSetKeyValue-Extended", "CanSetKeyValue-Extended-r1");
					Assert.Equal("CanSetKeyValue-Extended-r1", await trie.GetValueString("CanSetKeyValue-Extended"));
					await trie.SetValue("CanSetKeyValue-Extended", "CanSetKeyValue-Extended-r2");
					Assert.Equal(countBefore + 5, trie.RecordCount);

					Assert.Equal("CanSetKeyValue-Extended-r2", await trie.GetValueString("CanSetKeyValue-Extended"));
					Assert.Equal("CanSetKeyValue-r2", await trie.GetValueString("CanSetKeyValue"));
					Assert.Equal("k-r2", await trie.GetValueString("k"));
					Assert.Equal("a", await trie.GetValueString("Relocation"));
					Assert.Equal("b", await trie.GetValueString("NoRelocation"));

					trie = await t.ReloadTrie(trie);

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
							Assert.True(await trie.SetValue(k, k));
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
							all.DisposeAll();
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
							Assert.False(await trie.SetValue(k, k + "-r2"));
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
							Assert.False(await trie.SetValue(k, k.GetHashCode().ToString()));
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
					trie = await t.ReloadTrie(trie);

					// Try to defrag, for sports
					var saved = await trie.Defragment();
					Assert.NotEqual(0, saved);
					Assert.True(30_000 < saved);

					saved = await trie.Defragment();
					Assert.Equal(0, saved);

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
					await trie.SetValue(longKey, longValue);
					Assert.Equal(longValue, await trie.GetValueString(longKey));
				}
		}

		private Tester CreateTester()
		{
			return new Tester();
		}

		[Fact]
		public async Task CanSetLotsOfKeysSaturatingNodes()
		{
			await using (var engine = await CreateEmptyEngine())
			{
				using var tx = await engine.OpenTransaction();
				var test = tx.GetTable("Test");
				var allBytes = Enumerable.Range(0, 256).Select(o => (byte)o).ToArray();
				for (int i = 0; i < allBytes.Length; i++)
				{
					await test.Insert(new byte[] { (byte)i }, new byte[] { (byte)i });
				}
				for (int i = 0; i < allBytes.Length; i++)
				{
					await test.Insert(new byte[] { (byte)i, 0 }, new byte[] { (byte)i, 0 });
				}
				for (int i = 0; i < allBytes.Length; i++)
				{
					var row = await test.Get(new byte[] { (byte)i });
					using (row)
					{
						Assert.NotNull(row);
						Assert.Equal(1, row!.ValueLength);
						var v = await row.ReadValue();
						Assert.Equal((byte)i, v.Span[0]);
					}
					row.Dispose();
					row = await test.Get(new byte[] { (byte)i, 0 });
					using (row)
					{
						Assert.NotNull(row);
						Assert.Equal(2, row!.ValueLength);
						var v = await row.ReadValue();
						Assert.Equal((byte)i, v.Span[0]);
						Assert.Equal((byte)0, v.Span[1]);
					}
				}
			}
		}

		[Fact]
		public async Task CanUseDBEngine()
		{
			await using (var engine = await CreateEngine())
			{
				using var tx = await engine.OpenTransaction();
				// Open existing table
				var table = tx.GetTable("Transactions");
				Assert.Equal(7817, (await table.GetTrie()).RecordCount);
				await table.Insert("test", "value");
			}
			await using (var engine = await CreateEngine(false))
			{
				using var tx = await engine.OpenTransaction();
				// Open existing table
				var table = tx.GetTable("Transactions");
				Assert.Equal(7817, (await table.GetTrie()).RecordCount);
				Assert.Null(await table.Get("test"));
			}
			await using (var engine = await CreateEngine())
			{
				using var tx = await engine.OpenTransaction();
				// Open existing table
				var table = tx.GetTable("Transactions");
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
				var table = tx.GetTable("Transactions");
				Assert.Equal(7818, (await table.GetTrie()).RecordCount);
				var row = await table.Get("test");
				Assert.NotNull(row);
				Assert.Equal("value", await row!.ReadValueString());
			}

			await using (var engine = await CreateEngine())
			{
				using var tx = await engine.OpenTransaction();
				var table = tx.GetTable("Transactions");
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
				var table = tx.GetTable("MyTable");
				await table.Insert("qweq", "eqr");
				await table.Commit();
				Assert.Equal("eqr", await (await table.Get("qweq"))!.ReadValueString());
			}
			await using (var engine = await CreateEmptyEngine(false))
			{
				using var tx = await engine.OpenTransaction();
				var table = tx.GetTable("MyTable");
				Assert.Equal("eqr", await (await table.Get("qweq"))!.ReadValueString());

				var elements = await table.Enumerate("qweq").ToArrayAsync();
				Assert.Single(elements);
			}

			// Can change the page size of internal cache storage
			await using (var engine = await CreateEmptyEngine(false))
			{
				using var tx = await engine.OpenTransaction();
				var table = tx.GetTable("MyTable");
				Assert.Equal("eqr", await (await table.Get("qweq"))!.ReadValueString());
				Assert.Equal(Sizes.DefaultPageSize, table.PagePool.PageSize);
				engine.ConfigurePagePool(table.Name, new PagePool());
				Assert.Equal("eqr", await (await table.Get("qweq"))!.ReadValueString());
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

			await using (var engine = await CreateEmptyEngine(false))
			{
				using var tx = await engine.OpenTransaction();
				var table = tx.GetTable("MyTable");
				Assert.Equal("eqr", await (await table.Get("qweq"))!.ReadValueString());
				Assert.True(await table.Exists());
				await table.Delete();
				Assert.False(await table.Exists());
				Assert.Null(await engine.Schema.GetFileName("MyTable"));
				await table.Close();
				await Assert.ThrowsAsync<InvalidOperationException>(async () => await table.Insert("", ""));
				table = tx.GetTable(table.Name);
				await table.Insert("", "");
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


	}
}
