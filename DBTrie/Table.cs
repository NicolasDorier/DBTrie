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
		private IStorage? tableFs;
		private readonly Transaction tx;
		private readonly string tableName;
		bool checkConsistency;

		public string Name => tableName;

		internal Table(Transaction tx, string tableName, bool checkConsistency)
		{
			this.tx = tx;
			this.tableName = tableName;
			this.checkConsistency = checkConsistency;
		}

		async ValueTask<CacheStorage> GetCacheStorage()
		{
			if (cache is CacheStorage)
				return cache;
			if (tableFs is null)
			{
				var fileName = await tx.Schema.GetFileNameOrCreate(tableName);
				tableFs = await tx._Engine.Storages.OpenStorage(fileName.ToString());
			}
			cache = new CacheStorage(tableFs, false, CacheSettings);
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
			if (this.cache is CacheStorage c && c.Clear(true))
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
			ClearTrie();
			if (this.tableFs is FileStorage f)
			{
				await f.DisposeAsync();
				tableFs = null;
			}
		}

		internal async ValueTask Reserve()
		{
			await (await this.GetCacheStorage()).ResizeInner();
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

		public async ValueTask Delete()
		{
			if (tx._Tables.Remove(tableName))
			{
				ClearTrie();
				if (this.tableFs is FileStorage f)
				{
					await f.DisposeAsync();
					tableFs = null;
				}
			}
			if (await tx.Schema.GetFileName(tableName) is ulong fileName)
			{
				await tx.Schema.RemoveFileName(tableName);
				await tx.Storages.Delete(fileName.ToString());
			}
			await tx.Storages.Delete(tableName);
		}

		private void ClearTrie()
		{
			if (this.cache is CacheStorage c)
			{
				c.Clear(false);
				cache = null;
			}
			trie = null;
		}

		public void ClearCache()
		{
			cache?.Clear(false);
		}
		public async ValueTask<bool> Exists()
		{
			if (await tx.Schema.GetFileName(tableName) is ulong fileName)
				return await tx.Storages.Exists(fileName.ToString());
			return false;
		}

		/// <summary>
		/// Reclaim unused space
		/// </summary>
		/// <returns>The number of bytes saved</returns>
		public async ValueTask<int> Defragment(int maxPageCountInMemory = 256)
		{
			ClearTrie();
			var oldCacheSettings = _CacheSettings;
			try
			{
				_CacheSettings = new CacheSettings()
				{
					PageSize = CacheSettings.PageSize,
					MaxPageCount = maxPageCountInMemory,
					AutoCommitEvictedPages = true
				};
				var trie = await GetTrie();
				var saved = await trie.Defragment();
				await Commit();
				return saved;
			}
			finally
			{
				_CacheSettings = oldCacheSettings;
				ClearTrie();
			}
		}

		public async ValueTask<long> GetSize()
		{
			return (await GetTrie()).Storage.Length;
		}
		public async ValueTask<long> GetRecordCount()
		{
			return (await GetTrie()).RecordCount;
		}

		internal CacheSettings? _CacheSettings;
		internal CacheSettings? LocalCacheSettings
		{
			get
			{
				return _CacheSettings;
			}
			set
			{
				ClearTrie();
				_CacheSettings = value;
			}
		}

		/// <summary>
		/// The cache settings of this table, can get configured via the <see cref="DBTrieEngine"/>
		/// </summary>
		public CacheSettings CacheSettings => _CacheSettings ?? GlobalCacheSettings;

		internal CacheSettings GlobalCacheSettings
		{
			get 
			{
				return tx._Engine._GlobalCacheSettings;
			}
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
