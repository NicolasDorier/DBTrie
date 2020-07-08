using DBTrie.Storage;
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
		Dictionary<string, Table> _Tables = new Dictionary<string, Table>();
		TaskCompletionSource<bool> _Completion;
		DBTrieEngine _Engine;
		public Transaction(DBTrieEngine engine)
		{
			_Completion = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
			_Engine = engine;
		}

		Schema? _Schema;
		public async ValueTask<Schema> GetSchema()
		{
			if (_Schema is Schema s)
				return s;
			var schemaFile = await _Engine.Storages.OpenStorage(Schema.StorageName);
			var trie = await LTrie.OpenOrInitFromStorage(schemaFile);
			trie.ConsistencyCheck = _Engine.ConsistencyCheck;
			s = await Schema.OpenFromTrie(trie);
			_Schema = s;
			return s;
		}

		public async ValueTask<Table> GetOrCreateTable(string tableName)
		{
			if (TryGetTable(tableName, out var table) && table is Table)
				return table;
			var schema = await GetSchema();
			var fileName = await schema.GetFileNameOrCreate(tableName);
			var tableFs = await _Engine.Storages.OpenStorage(fileName.ToString());
			table = new Table(tableFs, _Engine.ConsistencyCheck);
			_Tables.Add(tableName, table);
			return table;
		}
		public bool TryGetTable(string tableName, out Table? table)
		{
			if (tableName == null)
				throw new ArgumentNullException(nameof(tableName));
			return _Tables.TryGetValue(tableName, out table);
		}

		public void Init()
		{
			_Completion = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
		}

		public Task Completion => _Completion.Task;

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
			_Engine.Return(this);
			_Completion.SetResult(true);
		}
		internal async ValueTask RealDispose()
		{
			if (_Schema is Schema s)
				await s.Trie.Storage.DisposeAsync();
			foreach (var table in _Tables.Values)
				await table.DisposeAsync();
		}
	}
}
