﻿using DBTrie.Storage;
using DBTrie.TrieModel;
using System;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace DBTrie
{
	public class Transaction : IDisposable
	{
		internal Dictionary<string, Table> _Tables = new Dictionary<string, Table>();
		TaskCompletionSource<bool> _Completion;
		internal DBTrieEngine _Engine;
		public Schema Schema => _Engine.Schema;
		public IStorages Storages => _Engine.Storages;
		public Transaction(DBTrieEngine engine)
		{
			_Completion = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
			_Completion.SetResult(true);
			_Engine = engine;
		}

		public Table GetTable(string tableName)
		{
			if (TryGetTable(tableName, out var table) && table is Table)
				return table;
			table = new Table(this, tableName, _Engine.ConsistencyCheck);
			_Tables.Add(tableName, table);
			return table;
		}
		bool TryGetTable(string tableName, out Table? table)
		{
			if (tableName == null)
				throw new ArgumentNullException(nameof(tableName));
			return _Tables.TryGetValue(tableName, out table);
		}

		internal void Init()
		{
			_Completion = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
		}

		internal Task Completion => _Completion.Task;

		public async ValueTask Commit()
		{
			foreach (var table in _Tables.Values)
			{
				await table.Reserve();
			}
			foreach (var table in _Tables.Values)
			{
				await table.Commit();
			}
		}

		public void Rollback()
		{
			foreach (var table in _Tables.Values)
			{
				table.Rollback();
			}
		}

		public void Dispose()
		{
			// If already committed, this is no-op
			Rollback();
			_Completion.SetResult(true);
			_Engine.Return(this);
		}
		internal async ValueTask RealDispose()
		{
			await Schema.Trie.Storage.DisposeAsync();
			foreach (var table in _Tables.Values)
				await table.DisposeAsync();
		}
	}
}
