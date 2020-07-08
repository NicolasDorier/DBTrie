using DBTrie.Storage;
using DBTrie.TrieModel;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace DBTrie
{
	public class DBTrieEngine : IAsyncDisposable
	{
		List<CacheStorage> caches = new List<CacheStorage>();
		Channel<Transaction> _TransactionWaiter = Channel.CreateUnbounded<Transaction>();
		Transaction _Transaction;
		public static ValueTask<DBTrieEngine> OpenFromFolder(string folderName)
		{
			var storages = new FileStorages(folderName);
			return OpenFromStorages(storages);
		}

		private static async ValueTask<DBTrieEngine> OpenFromStorages(IStorages storages)
		{
			if (storages == null)
				throw new ArgumentNullException(nameof(storages));
			var schemaFile = await storages.OpenStorage(Schema.StorageName);
			var trie = await LTrie.OpenOrInitFromStorage(schemaFile);
			var s = await Schema.OpenOrInitFromTrie(trie);
			return new DBTrieEngine(storages, s);
		}

		public IStorages Storages { get; }
		public Schema Schema { get; }

		/// <summary>
		/// For debug, check consistency of created trie
		/// </summary>
		public bool ConsistencyCheck
		{
			get
			{
				return _ConsistencyCheck;
			}
			set
			{
				Schema.Trie.ConsistencyCheck = true;
				foreach (var table in _Transaction._Tables.Values)
					if (table.trie is LTrie t)
						t.ConsistencyCheck = value;
				_ConsistencyCheck = value;
			}
		}
		bool _ConsistencyCheck = false;

		internal DBTrieEngine(IStorages storages, Schema schema)
		{
			Storages = storages;
			Schema = schema;
			_Transaction = new Transaction(this);
			_TransactionWaiter.Writer.TryWrite(_Transaction);
		}

		public async Task<Transaction> OpenTransaction(CancellationToken cancellationToken = default)
		{
			while (await _TransactionWaiter.Reader.WaitToReadAsync(cancellationToken))
			{
				if (_TransactionWaiter.Reader.TryRead(out var tx) && tx is Transaction)
				{
					tx.Init();
					return tx;
				}
			}
			throw new ObjectDisposedException(nameof(DBTrieEngine));
		}

		internal bool Return(Transaction transaction)
		{
			return _TransactionWaiter.Writer.TryWrite(transaction);
		}

		public async ValueTask DisposeAsync()
		{
			_TransactionWaiter.Writer.TryComplete();
			await _Transaction.Completion;
			await _Transaction.RealDispose();
		}
	}
}
