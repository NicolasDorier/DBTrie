using DBTrie.Storage;
using DBTrie.TrieModel;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Net.Http.Headers;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DBTrie
{
	public class Table
	{
		private CacheStorage? cache;
		private readonly Transaction tx;
		private readonly string tableName;
		bool checkConsistency;
		private readonly int pageSize;

		internal Table(Transaction tx, string tableName, bool checkConsistency, int pageSize = Sizes.DefaultPageSize)
		{
			this.tx = tx;
			this.tableName = tableName;
			this.checkConsistency = checkConsistency;
			this.pageSize = pageSize;
		}

		async ValueTask<CacheStorage> GetCacheStorage()
		{
			if (cache is CacheStorage)
				return cache;
			var fileName = await tx.Schema.GetFileNameOrCreate(tableName);
			var tableFs = await tx._Engine.Storages.OpenStorage(fileName.ToString());
			cache = new CacheStorage(tableFs, true, pageSize);
			return cache;
		}

		internal LTrie? trie;

		internal async ValueTask<LTrie> GetTrie()
		{
			if (trie is LTrie)
				return trie;
			trie = await LTrie.OpenOrInitFromStorage(await GetCacheStorage());
			trie.ConsistencyCheck = checkConsistency;
			return trie;
		}

		internal async ValueTask Commit()
		{
			if (this.cache is null)
				return;
			await this.cache.Flush();
		}
		internal void Rollback()
		{
			if (this.cache is CacheStorage c && c.Clear())
				trie = null;
		}

		public async ValueTask<bool> Delete(ReadOnlyMemory<byte> key)
		{
			return await (await this.GetTrie()).DeleteRow(key);
		}
		public async ValueTask<bool> Delete(string key)
		{
			if (key == null)
				throw new ArgumentNullException(nameof(key));
			return await (await this.GetTrie()).DeleteRow(key);
		}

		internal async ValueTask DisposeAsync()
		{
			if (cache is CacheStorage)
			{
				this.cache.Clear();
				await cache.DisposeAsync();
			}
			trie = null;
		}

		internal async ValueTask Reserve()
		{
			 await (await this.GetCacheStorage()).Reserve();
		}
		public async ValueTask<bool> Insert(string key, string value)
		{
			if (key == null)
				throw new ArgumentNullException(nameof(key));
			if (value == null)
				throw new ArgumentNullException(nameof(value));
			return await (await GetTrie()).SetKey(key, value);
		}
		public async ValueTask<bool> Insert(string key, ReadOnlyMemory<byte> value)
		{
			if (key == null)
				throw new ArgumentNullException(nameof(key));
			return await (await GetTrie()).SetValue(key, value);
		}
		public async ValueTask<bool> Insert(ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value)
		{
			return await (await GetTrie()).SetValue(key, value);
		}

		public async ValueTask<IRow?> Get(string key)
		{
			if (key == null)
				throw new ArgumentNullException(nameof(key));
			return await (await GetTrie()).GetValue(key);
		}
		public async ValueTask<IRow?> Get(ReadOnlyMemory<byte> key)
		{
			return await (await GetTrie()).GetValue(key);
		}

		public IAsyncEnumerable<IRow> Enumerate(string? startsWith = null)
		{
			return new DeferredAsyncEnumerable(GetTrie(), t => t.EnumerateStartsWith(startsWith ?? string.Empty));
		}
		public IAsyncEnumerable<IRow> Enumerate(ReadOnlyMemory<byte> startsWith)
		{
			return new DeferredAsyncEnumerable(GetTrie(), t => t.EnumerateStartsWith(startsWith));
		}

		class DeferredAsyncEnumerable : IAsyncEnumerable<IRow>, IAsyncEnumerator<IRow>
		{
			ValueTask<LTrie> trieTask;
			private readonly Func<LTrie, IAsyncEnumerable<IRow>> enumerate;
			IAsyncEnumerator<IRow>? internalEnumerator;
			IAsyncEnumerator<IRow> InternalEnumerator
			{
				get
				{
					if (internalEnumerator is null)
						throw new InvalidOperationException("MoveNext is not called");
					return internalEnumerator;
				}
			}
			public DeferredAsyncEnumerable(ValueTask<LTrie> trie, Func<LTrie, IAsyncEnumerable<IRow>> enumerate)
			{
				this.trieTask = trie;
				this.enumerate = enumerate;
			}

			public IRow Current => InternalEnumerator.Current;

			public ValueTask DisposeAsync()
			{
				return default;
			}

			public IAsyncEnumerator<IRow> GetAsyncEnumerator(CancellationToken cancellationToken = default)
			{
				if (!(internalEnumerator is null))
					throw new InvalidOperationException("Impossible to enumerate this enumerable twice");
				return this;
			}

			public async ValueTask<bool> MoveNextAsync()
			{
				if (internalEnumerator is null)
				{
					var trie = await trieTask;
					internalEnumerator = enumerate(trie).GetAsyncEnumerator();
				}
				return await internalEnumerator.MoveNextAsync();
			}
		}
	}
}
