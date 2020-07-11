using DBTrie.Storage;
using DBTrie.TrieModel;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace DBTrie.Tests
{
	internal class Tester : IDisposable
	{
		class SpyMemoryPool : MemoryPool<byte>
		{
			class SpyMemoryOwner : IMemoryOwner<byte>
			{
				private readonly SpyMemoryPool parent;
				private readonly IMemoryOwner<byte> inner;

				public SpyMemoryOwner(SpyMemoryPool parent, IMemoryOwner<byte> inner)
				{
					this.parent = parent;
					this.inner = inner;
				}
				public Memory<byte> Memory => inner.Memory;

				public void Dispose()
				{
					parent.Rented--;
					inner.Dispose();
				}
			}
			MemoryPool<byte> inner = MemoryPool<byte>.Shared;
			public int Rented = 0;
			public SpyMemoryPool()
			{

			}
			public override int MaxBufferSize => inner.MaxBufferSize;

			public override IMemoryOwner<byte> Rent(int minBufferSize = -1)
			{
				Rented++;
				var arr = inner.Rent(minBufferSize);
				return new SpyMemoryOwner(this, arr);
			}

			protected override void Dispose(bool disposing)
			{
				
			}
		}
		SpyMemoryPool pool = new SpyMemoryPool();
		public Tester(string testDataDirectory = "Data")
		{
			TestDataDirectory = testDataDirectory;
		}

		public int MaxPageCount { get; set; } = Sizes.DefaultPageSize;
		public bool ConsistencyCheck { get; set; } = true;

		public string TestDataDirectory { get; set; }
		public IStorage CreateFileStorage(string file, bool cacheStorageLayer, [CallerMemberName] string? caller = null)
		{
			if (caller is null)
				throw new ArgumentNullException(nameof(caller));
			Directory.CreateDirectory(caller);
			File.Copy($"{TestDataDirectory}/{file}", $"{caller}/{file}", true);
			var fs = new FileStorage($"{caller}/{file}");
			if (!cacheStorageLayer)
				return fs;
			return new CacheStorage(fs, settings: new CacheSettings()
			{
				MaxPageCount = MaxPageCount
			});
		}

		public async ValueTask<LTrie> ReloadTrie(LTrie trie)
		{
			var trie2 = await CreateTrie(trie.Storage, trie.NodeCache is { });
			trie2.ConsistencyCheck = trie.ConsistencyCheck;
			return trie2;
		}

		public async ValueTask<LTrie> CreateTrie(IStorage fs, bool allowGenerationNodeCache)
		{
			var trie = await LTrie.OpenFromStorage(fs, pool);
			trie.ConsistencyCheck = ConsistencyCheck;
			if (allowGenerationNodeCache)
				trie.ActivateCache();
			return trie;
		}

		public void CreateEmptyFile(string name, int size)
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

		public void Dispose()
		{
			Assert.Equal(0, pool.Rented);
		}
	}
}
