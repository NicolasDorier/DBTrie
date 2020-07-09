using DBTrie.TrieModel;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace DBTrie
{
	public class Schema
	{
		public const string StorageName = "_DBreezeSchema";
		static Schema()
		{
			LastFileNumberKey = Encoding.UTF8.GetBytes("@@@@LastFileNumber").AsMemory().ToReadOnly();
		}
		static ReadOnlyMemory<byte> LastFileNumberKey { get; }
		internal LTrie Trie { get; }

		internal Schema(LTrie trie, ulong lastFileNumber)
		{
			this.Trie = trie;
			this.LastFileNumber = lastFileNumber;
		}

		public ulong LastFileNumber { get; internal set; }

		internal static async ValueTask<Schema> OpenOrInitFromTrie(LTrie trie)
		{
			using var key = await trie.GetValue(LastFileNumberKey);
			ulong number = 10000000UL;
			if (key is LTrieValue)
			{
				number = await key.ReadValueULong();
			}
			else
			{
				await trie.SetValue(LastFileNumberKey, number);
				await trie.Storage.Flush();
			}
			return new Schema(trie, number);
		}

		public async ValueTask<bool> TableExists(string tableName)
		{
			using var owner = GetTableNameBytes(tableName);
			var key = await Trie.GetValue(owner.Memory);
			return key is LTrieValue;
		}

		public async ValueTask<ulong?> GetFileName(string tableName)
		{
			using var owner = GetTableNameBytes(tableName);
			var key = await Trie.GetValue(owner.Memory);
			if (key is null)
				return null;
			return await Trie.StorageHelper.ReadULong(key.ValuePointer + 2);
		}
		public async ValueTask<bool> RemoveFileName(string tableName)
		{
			if (tableName == null)
				throw new ArgumentNullException(nameof(tableName));
			using var owner = GetTableNameBytes(tableName);
			return await Trie.DeleteRow(owner.Memory);
		}

		public async ValueTask<ulong> GetFileNameOrCreate(string tableName)
		{
			using var owner = GetTableNameBytes(tableName);
			var key = await Trie.GetValue(owner.Memory);
			if (key is null)
			{
				var fileNumber = LastFileNumber + 1;
				var bytes = new byte[10];
				bytes[1] = 1;
				bytes.AsSpan().Slice(2).ToBigEndian(fileNumber);
				await Trie.SetValue(owner.Memory, bytes);
				var row = await Trie.GetValue(LastFileNumberKey);
				if (row is null)
					throw new InvalidOperationException("LastFileNumberKey is not found");
				await Trie.SetValue(LastFileNumberKey, fileNumber);
				await Trie.Storage.Flush();
				LastFileNumber = fileNumber;
				return fileNumber;
			}
			return await Trie.StorageHelper.ReadULong(key.ValuePointer + 2);
		}
		private IMemoryOwner<byte> GetTableNameBytes(string? tableName)
		{
			tableName ??= string.Empty;
			var bytes = Encoding.UTF8.GetByteCount(tableName, 0, tableName.Length);
			var owner = Trie.MemoryPool.Rent(3 + bytes);
			owner = owner.Slice(0, 3 + bytes);
			Encoding.UTF8.GetBytes(tableName.AsSpan(), owner.Memory.Span.Slice(3));
			owner.Memory.Span[0] = (byte)'@';
			owner.Memory.Span[1] = (byte)'u';
			owner.Memory.Span[2] = (byte)'t';
			return owner;
		}

		public async IAsyncEnumerable<string> GetTables(string? startWith = null)
		{
			using var key = this.GetTableNameBytes(startWith);
			await foreach (var value in Trie.EnumerateStartsWith(key.Memory))
			{
				yield return Encoding.UTF8.GetString(value.Key.Span.Slice(3));
			}
		}
	}
}
