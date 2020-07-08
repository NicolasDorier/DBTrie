using DBTrie.Storage;
using DBTrie.TrieModel;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace DBTrie
{
	public class Table
	{
		private readonly CacheStorage cache;
		bool checkConsistency;
		internal Table(IStorage storage, bool checkConsistency, int pageSize = Sizes.DefaultPageSize)
		{
			this.checkConsistency = checkConsistency;
			this.cache = new CacheStorage(storage, true, pageSize);
		}

		LTrie? trie;

		internal async ValueTask<LTrie> GetTrie()
		{
			if (trie is LTrie)
				return trie;
			trie = await LTrie.OpenOrInitFromStorage(this.cache);
			trie.ConsistencyCheck = checkConsistency;
			return trie;
		}

		internal async ValueTask Commit()
		{
			await this.cache.Flush();
		}
		internal void Rollback()
		{
			if (this.cache.Clear())
				trie = null;
		}
		internal async ValueTask DisposeAsync()
		{
			this.cache.Clear();
			await cache.DisposeAsync();
			trie = null;
		}

		internal ValueTask Reserve()
		{
			return this.cache.Reserve();
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

		public async ValueTask<IRow?> GetRow(string key)
		{
			if (key == null)
				throw new ArgumentNullException(nameof(key));
			return await (await GetTrie()).GetValue(key);
		}
	}
}
