using DBTrie.Storage;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace DBTrie
{
	public class LTrieRow
	{
		public LTrieRow(IStorage storage, ReadOnlyMemory<byte> key)
		{
			Key = key;
			Storage = storage;
		}
		internal IStorage Storage { get; }
		public ReadOnlyMemory<byte> Key { get; internal set; }
		public long Pointer { get; set; }
		public long ValuePointer { get; internal set; }
		public int ValueLength { get; internal set; }

		public async ValueTask<int> ReadValue(Memory<byte> memory)
		{
			await Storage.Read(ValuePointer, memory.Slice(0, ValueLength));
			return ValueLength;
		}
		public async ValueTask<ulong> ReadAsULong()
		{
			if (ValueLength != 8)
				throw new InvalidOperationException("The value is not a ulong");
			var v = new byte[8];
			await ReadValue(v.AsMemory());
			return ((ReadOnlySpan<byte>)v.AsSpan()).BigEndianToULong();
		}

		public async ValueTask Write(ulong value)
		{
			if (ValueLength != 8)
				throw new InvalidOperationException("The value is not a ulong");
			var v = new byte[8];
			v.AsSpan().ToBigEndian(value);
			await Storage.Write(ValuePointer, v);
		}
	}
}
