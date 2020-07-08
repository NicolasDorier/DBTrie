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

		private static ValueTask<DBTrieEngine> OpenFromStorages(IStorages storages)
		{
			if (storages == null)
				throw new ArgumentNullException(nameof(storages));
			return new ValueTask<DBTrieEngine>(new DBTrieEngine(storages));
		}

		public IStorages Storages { get; }
		/// <summary>
		/// For debug, check consistency of created trie
		/// </summary>
		public bool ConsistencyCheck { get; set; }

		internal DBTrieEngine(IStorages storages)
		{
			Storages = storages;
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
