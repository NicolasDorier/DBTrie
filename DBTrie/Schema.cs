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
		static Schema()
		{
			LastFileNumberKey = Encoding.UTF8.GetBytes("@@@@LastFileNumber").AsMemory().ToReadOnly();
			UserTableKey = Encoding.UTF8.GetBytes("@ut").AsMemory().ToReadOnly();
		}
		static ReadOnlyMemory<byte> LastFileNumberKey { get; }
		static ReadOnlyMemory<byte> UserTableKey { get; }
		public LTrie Trie { get; }

		public Schema(LTrie trie)
		{
			if (trie == null)
				throw new ArgumentNullException(nameof(trie));
			this.Trie = trie;
		}

		public async ValueTask<ulong> GetLastFileNumber()
		{
			var key = await Trie.RootNode.GetRow(LastFileNumberKey);
			if (key is null)
				throw new KeyNotFoundException("@@@@LastFileNumber table does not exists");
			return await key.ReadAsULong();
		}

		public async ValueTask<bool> TableExists(string tableName)
		{
			using var owner = GetTableNameBytes(tableName);
			var key = await Trie.RootNode.GetRow(owner.Memory);
			return key is LTrieRow;
		}
		public async ValueTask<ulong> GetFileNameOrCreate(string tableName)
		{
			using var owner = GetTableNameBytes(tableName);
			var key = await Trie.RootNode.GetRow(owner.Memory);
			if (key is null)
			{
				var lastNumberKey = await Trie.RootNode.GetRow(LastFileNumberKey);
				if (lastNumberKey is null)
					throw new KeyNotFoundException("@@@@LastFileNumber table does not exists");
				var lastFileNumber = await lastNumberKey.ReadAsULong();
				lastFileNumber++;
				var bytes = new byte[10];
				bytes[1] = 1;
				bytes.AsSpan().Slice(2).ToBigEndian(lastFileNumber);
				await Trie.RootNode.SetKey(owner.Memory, bytes);
				await lastNumberKey.Write(lastFileNumber);
				return lastFileNumber;
			}
			using var owner2 = Trie.MemoryPool.Rent(key.ValueLength);
			await key.ReadValue(owner2.Memory);
			return owner2.Memory.Slice(2, 8).ToReadOnly().Span.BigEndianToULong();
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
			await foreach (var value in Trie.RootNode.EnumerateStartWith(key.Memory))
			{
				yield return Encoding.UTF8.GetString(value.Key.Span.Slice(3));
			}
		}
	}
}
